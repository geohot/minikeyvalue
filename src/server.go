package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb/util"
)

// *** Master Server ***

type ListResponse struct {
	Next string   `json:"next"`
	Keys []string `json:"keys"`
}

func (a *App) QueryHandler(key []byte, w http.ResponseWriter, r *http.Request) {
	// operation is first query parameter (e.g. ?list&limit=10)
	operation := strings.Split(r.URL.RawQuery, "&")[0]
	switch operation {
	case "list", "unlinked":
		start := r.URL.Query().Get("start")
		limit := 0
		qlimit := r.URL.Query().Get("limit")
		if qlimit != "" {
			nlimit, err := strconv.Atoi(qlimit)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			limit = nlimit
		}

		slice := util.BytesPrefix(key)
		if start != "" {
			slice.Start = []byte(start)
		}
		iter := a.db.NewIterator(slice, nil)
		defer iter.Release()
		keys := make([]string, 0)
		next := ""
		for iter.Next() {
			rec := toRecord(iter.Value())
			if (rec.deleted != NO && operation == "list") ||
				(rec.deleted != SOFT && operation == "unlinked") {
				continue
			}
			if len(keys) > 1000000 { // too large (need to specify limit)
				w.WriteHeader(413)
				return
			}
			if limit > 0 && len(keys) == limit { // limit results returned
				next = string(iter.Key())
				break
			}
			keys = append(keys, string(iter.Key()))
		}
		str, err := json.Marshal(ListResponse{Next: next, Keys: keys})
		if err != nil {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write(str)
		return
	default:
		w.WriteHeader(403)
		return
	}
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.URL.Path)

	// this is a list query
	if len(r.URL.RawQuery) > 0 {
		if r.Method != "GET" {
			w.WriteHeader(403)
			return
		}
		a.QueryHandler(key, w, r)
		return
	}

	// lock the key while a PUT or DELETE is in progress
	const unlink = "UNLINK"
	if r.Method == "PUT" || r.Method == "DELETE" || r.Method == unlink {
		if !a.LockKey(key) {
			// Conflict, retry later
			w.WriteHeader(409)
			return
		}
		defer a.UnlockKey(key)
	}

	switch r.Method {
	case "GET", "HEAD":
		rec := a.GetRecord(key)
		var volume string
		if rec.deleted == SOFT || rec.deleted == HARD {
			if a.fallback == "" {
				w.Header().Set("Content-Length", "0")
				w.WriteHeader(404)
				return
			}
			// fall through to fallback
			volume = a.fallback
		} else {
			kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)
			if needsRebalance(rec.rvolumes, kvolumes) {
				fmt.Println("on wrong volumes, needs rebalance")
			}
			// fetch from a random valid volume
			volume = rec.rvolumes[rand.Intn(len(rec.rvolumes))]
		}
		remote := fmt.Sprintf("http://%s%s", volume, key2path(key))
		w.Header().Set("Location", remote)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(302)
	case "PUT":
		// no empty values
		if r.ContentLength == 0 {
			w.WriteHeader(411)
			return
		}

		// check if we already have the key, and it's not deleted
		rec := a.GetRecord(key)
		if rec.deleted == NO {
			// Forbidden to overwrite with PUT
			w.WriteHeader(403)
			return
		}

		// we don't have the key, compute the remote URL
		kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)

		// push to leveldb initially as deleted
		if !a.PutRecord(key, Record{kvolumes, SOFT}) {
			w.WriteHeader(500)
			return
		}

		// write to each replica
		var buf bytes.Buffer
		body := io.TeeReader(r.Body, &buf)
		bodylen := r.ContentLength
		for i := 0; i < len(kvolumes); i++ {
			if i != 0 {
				// if we have already read the contents into the TeeReader
				body = bytes.NewReader(buf.Bytes())
			}
			remote := fmt.Sprintf("http://%s%s", kvolumes[i], key2path(key))
			if remotePut(remote, bodylen, body) != nil {
				// we assume the remote wrote nothing if it failed
				fmt.Printf("replica %d write failed: %s\n", i, remote)
				w.WriteHeader(500)
				return
			}
		}

		// push to leveldb as existing
		// note that the key is locked, so nobody wrote to the leveldb
		if !a.PutRecord(key, Record{kvolumes, NO}) {
			w.WriteHeader(500)
			return
		}

		// 201, all good
		w.WriteHeader(201)
	case "DELETE", unlink:
		unlink := r.Method == unlink

		// delete the key, first locally
		rec := a.GetRecord(key)
		if rec.deleted == HARD || (unlink && rec.deleted == SOFT) {
			w.WriteHeader(404)
			return
		}

		if a.protect && rec.deleted == NO {
			w.WriteHeader(403)
			return
		}

		// mark as deleted
		if !a.PutRecord(key, Record{rec.rvolumes, SOFT}) {
			w.WriteHeader(500)
			return
		}

		if !unlink {
			// then remotely, if this is not an unlink
			deleteError := false
			for _, volume := range rec.rvolumes {
				remote := fmt.Sprintf("http://%s%s", volume, key2path(key))
				if remoteDelete(remote) != nil {
					// if this fails, it's possible to get an orphan file
					// but i'm not really sure what else to do?
					deleteError = true
				}
			}

			if deleteError {
				w.WriteHeader(500)
				return
			}

			// this is a hard delete in the database, aka nothing
			a.db.Delete(key, nil)
		}

		// 204, all good
		w.WriteHeader(204)
	}
}
