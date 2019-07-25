package main

import (
  "io"
  "sync"
  "bytes"
  "strings"
  "strconv"
  "fmt"
  "math/rand"
  "net/http"
  "encoding/json"
  "github.com/syndtr/goleveldb/leveldb"
  "github.com/syndtr/goleveldb/leveldb/util"
)

// *** Shared App struct and methods ***

type App struct {
  db *leveldb.DB
  mlock sync.Mutex
  lock map[string]struct{}

  // params
  volumes []string
  fallback string
  replicas int
  subvolumes int
  softdelete bool
}

func (a *App) UnlockKey(key []byte) {
  a.mlock.Lock()
  delete(a.lock, string(key))
  a.mlock.Unlock()
}

func (a *App) LockKey(key []byte) bool {
  a.mlock.Lock()
  defer a.mlock.Unlock()
  if _, prs := a.lock[string(key)]; prs {
    return false
  }
  a.lock[string(key)] = struct{}{}
  return true
}

func (a *App) GetRecord(key []byte) Record {
  data, err := a.db.Get(key, nil)
  rec := Record{[]string{}, true}
  if err != leveldb.ErrNotFound { rec = toRecord(data) }
  return rec
}

func (a *App) PutRecord(key []byte, rec Record) bool {
  return a.db.Put(key, fromRecord(rec), nil) == nil
}

// *** Master Server ***

type ListResponse struct {
  Next string `json:"next"`
  Keys []string `json:"keys"`
}

func (a *App) QueryHandler(key []byte, w http.ResponseWriter, r *http.Request) {
  // operation is first query parameter (e.g. ?list&limit=10)
  switch strings.Split(r.URL.RawQuery, "&")[0] {
  case "list":
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
  if r.Method == "PUT" || r.Method == "DELETE" {
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
    if rec.deleted {
      if a.fallback == "" {
        w.Header().Set("Content-Length", "0")
        w.WriteHeader(404)
        return
      } else {
        // fall through to fallback
        volume = a.fallback
      }
    } else {
      kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)
      if needs_rebalance(rec.rvolumes, kvolumes) {
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
    if !rec.deleted {
      // Forbidden to overwrite with PUT
      w.WriteHeader(403)
      return
    }

    // we don't have the key, compute the remote URL
    kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)

    // push to leveldb initially as deleted
    if !a.PutRecord(key, Record{kvolumes, true}) {
      w.WriteHeader(500)
      return
    }

    // write to each replica
    var buf bytes.Buffer
    body := io.TeeReader(r.Body, &buf)
    bodylen := r.ContentLength
    for i := 0; i < len(kvolumes); i++ {
      if (i != 0) {
        // if we have already read the contents into the TeeReader
        body = bytes.NewReader(buf.Bytes())
      }
      remote := fmt.Sprintf("http://%s%s", kvolumes[i], key2path(key))
      if remote_put(remote, bodylen, body) != nil {
        // we assume the remote wrote nothing if it failed
        fmt.Printf("replica %d write failed: %s\n", i, remote)
        w.WriteHeader(500)
        return
      }
    }

    // push to leveldb as existing
    // note that the key is locked, so nobody wrote to the leveldb
    if !a.PutRecord(key, Record{kvolumes, false}) {
      w.WriteHeader(500)
      return
    }

    // 201, all good
    w.WriteHeader(201)
  case "DELETE":
    // delete the key, first locally
    rec := a.GetRecord(key)
    if rec.deleted {
      w.WriteHeader(404)
      return
    }

    // mark as deleted
    if !a.PutRecord(key, Record{rec.rvolumes, true}) {
      w.WriteHeader(500)
      return
    }

    if !a.softdelete {
      // then remotely, if softdelete is disabled
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
        w.WriteHeader(500)
        return
      }

      a.db.Delete(key, nil)
    }

    // 204, all good
    w.WriteHeader(204)
  }
}

