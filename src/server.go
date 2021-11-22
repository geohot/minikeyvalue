package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// *** Master Server ***

type ListResponse struct {
	Next string   `json:"next"`
	Keys []string `json:"keys"`
}

func (a *App) QueryHandler(key []byte, w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("list-type") == "2" {
		// this is an S3 style query
		// TODO: this is very incomplete
		key = []byte(string(key) + "/" + r.URL.Query().Get("prefix"))
		iter := a.db.NewIterator(util.BytesPrefix(key), nil)
		defer iter.Release()

		ret := "<ListBucketResult>"
		for iter.Next() {
			rec := toRecord(iter.Value())
			if rec.deleted != NO {
				continue
			}
			ret += "<Contents><Key>" + string(iter.Key()[len(key):]) + "</Key></Contents>"
		}
		ret += "</ListBucketResult>"
		w.WriteHeader(200)
		w.Write([]byte(ret))
		return
	}

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

func (a *App) Delete(key []byte, unlink bool) int {
	// delete the key, first locally
	rec := a.GetRecord(key)
	if rec.deleted == HARD || (unlink && rec.deleted == SOFT) {
		return 404
	}

	if !unlink && a.protect && rec.deleted == NO {
		return 403
	}

	// mark as deleted
	if !a.PutRecord(key, Record{rec.rvolumes, SOFT, rec.hash}) {
		return 500
	}

	if !unlink {
		// then remotely, if this is not an unlink
		delete_error := false
		for _, volume := range rec.rvolumes {
			remote := fmt.Sprintf("http://%s%s", volume, key2path(key))
			if remote_delete(remote) != nil {
				// if this fails, it's possible to get an orphan file
				// but i'm not really sure what else to do?
				delete_error = true
			}
		}

		if delete_error {
			return 500
		}

		// this is a hard delete in the database, aka nothing
		a.db.Delete(key, nil)
	}

	// 204, all good
	return 204
}

func (a *App) WriteToReplicas(key []byte, value io.Reader, valuelen int64) int {
	// we don't have the key, compute the remote URL
	kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)

	// push to leveldb initially as deleted, and without a hash since we don't have it yet
	if !a.PutRecord(key, Record{kvolumes, SOFT, ""}) {
		return 500
	}

	// write to each replica
	var buf bytes.Buffer
	body := io.TeeReader(value, &buf)
	for i := 0; i < len(kvolumes); i++ {
		if i != 0 {
			// if we have already read the contents into the TeeReader
			body = bytes.NewReader(buf.Bytes())
		}
		remote := fmt.Sprintf("http://%s%s", kvolumes[i], key2path(key))
		if remote_put(remote, valuelen, body) != nil {
			// we assume the remote wrote nothing if it failed
			fmt.Printf("replica %d write failed: %s\n", i, remote)
			return 500
		}
	}

	var hash = ""
	if a.md5sum {
		// compute the hash of the value
		hash = fmt.Sprintf("%x", md5.Sum(buf.Bytes()))
	}

	// push to leveldb as existing
	// note that the key is locked, so nobody wrote to the leveldb
	if !a.PutRecord(key, Record{kvolumes, NO, hash}) {
		return 500
	}

	// 201, all good
	return 201
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.URL.Path)
	lkey := []byte(r.URL.Path + r.URL.Query().Get("partNumber"))

	log.Println(r.Method, r.URL, r.ContentLength, r.Header["Range"])

	// this is a list query
	if len(r.URL.RawQuery) > 0 && r.Method == "GET" {
		a.QueryHandler(key, w, r)
		return
	}

	// lock the key while a PUT or DELETE is in progress
	if r.Method == "POST" || r.Method == "PUT" || r.Method == "DELETE" || r.Method == "UNLINK" || r.Method == "REBALANCE" {
		if !a.LockKey(lkey) {
			// Conflict, retry later
			w.WriteHeader(409)
			return
		}
		defer a.UnlockKey(lkey)
	}

	switch r.Method {
	case "GET", "HEAD":
		rec := a.GetRecord(key)
		var remote string
		if len(rec.hash) != 0 {
			// note that the hash is always of the whole file, not the content requested
			w.Header().Set("Content-Md5", rec.hash)
		}
		if rec.deleted == SOFT || rec.deleted == HARD {
			if a.fallback == "" {
				w.Header().Set("Content-Length", "0")
				w.WriteHeader(404)
				return
			}
			// fall through to fallback
			remote = fmt.Sprintf("http://%s%s", a.fallback, key)
		} else {
			kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)
			if needs_rebalance(rec.rvolumes, kvolumes) {
				w.Header().Set("Key-Balance", "unbalanced")
				fmt.Println("on wrong volumes, needs rebalance")
			} else {
				w.Header().Set("Key-Balance", "balanced")
			}
			w.Header().Set("Key-Volumes", strings.Join(rec.rvolumes, ","))

			// check the volume servers in a random order
			good := false
			for _, vn := range rand.Perm(len(rec.rvolumes)) {
				remote = fmt.Sprintf("http://%s%s", rec.rvolumes[vn], key2path(key))
				found, _ := remote_head(remote, a.voltimeout)
				if found {
					good = true
					break
				}
			}
			// if not found on any volume servers, fail before the redirect
			if !good {
				w.Header().Set("Content-Length", "0")
				w.WriteHeader(404)
				return
			}
			// note: this can race and fail, but in that case the client will handle the retry
		}
		w.Header().Set("Location", remote)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(302)
	case "POST":
		// check if we already have the key, and it's not deleted
		rec := a.GetRecord(key)
		if rec.deleted == NO {
			// Forbidden to overwrite with POST
			w.WriteHeader(403)
			return
		}

		// this will handle multipart uploads in "S3"
		if r.URL.RawQuery == "uploads" {
			uploadid := uuid.New().String()
			a.uploadids[uploadid] = true

			// init multipart upload
			w.WriteHeader(200)
			w.Write([]byte(`<InitiateMultipartUploadResult>
        <UploadId>` + uploadid + `</UploadId>
      </InitiateMultipartUploadResult>`))
		} else if r.URL.RawQuery == "delete" {
			del, err := parseDelete(r.Body)
			if err != nil {
				log.Println(err)
				w.WriteHeader(500)
				return
			}

			for _, subkey := range del.Keys {
				fullkey := fmt.Sprintf("%s/%s", key, subkey)
				status := a.Delete([]byte(fullkey), false)
				if status != 204 {
					w.WriteHeader(status)
					return
				}
			}
			w.WriteHeader(204)
		} else if uploadid := r.URL.Query().Get("uploadId"); uploadid != "" {
			if a.uploadids[uploadid] != true {
				w.WriteHeader(403)
				return
			}
			delete(a.uploadids, uploadid)

			// finish multipart upload
			cmu, err := parseCompleteMultipartUpload(r.Body)
			if err != nil {
				log.Println(err)
				w.WriteHeader(500)
				return
			}

			// open all the part files
			var fs []io.Reader
			sz := int64(0)
			for _, part := range cmu.PartNumbers {
				fn := fmt.Sprintf("/tmp/%s-%d", uploadid, part)
				f, err := os.Open(fn)
				os.Remove(fn)
				if err != nil {
					w.WriteHeader(403)
					return
				}
				defer f.Close()
				fi, _ := f.Stat()
				sz += fi.Size()
				fs = append(fs, f)
			}

			status := a.WriteToReplicas(key, io.MultiReader(fs...), sz)
			w.WriteHeader(status)
			w.Write([]byte("<CompleteMultipartUploadResult></CompleteMultipartUploadResult>"))
			return
		}
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

		if pn := r.URL.Query().Get("partNumber"); pn != "" {
			uploadid := r.URL.Query().Get("uploadId")
			if a.uploadids[uploadid] != true {
				w.WriteHeader(403)
				return
			}

			pnnum, _ := strconv.Atoi(pn)
			f, err := os.OpenFile(fmt.Sprintf("/tmp/%s-%d", uploadid, pnnum), os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				w.WriteHeader(403)
				return
			}
			defer f.Close()
			io.Copy(f, r.Body)
			w.WriteHeader(200)
		} else {
			status := a.WriteToReplicas(key, r.Body, r.ContentLength)
			w.WriteHeader(status)
		}
	case "DELETE", "UNLINK":
		status := a.Delete(key, r.Method == "UNLINK")
		w.WriteHeader(status)
	case "REBALANCE":
		rec := a.GetRecord(key)
		if rec.deleted != NO {
			w.WriteHeader(404)
			return
		}

		kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)
		rbreq := RebalanceRequest{key: key, volumes: rec.rvolumes, kvolumes: kvolumes}
		if !rebalance(a, rbreq) {
			w.WriteHeader(400)
			return
		}

		// 204, all good
		w.WriteHeader(204)
	}
}
