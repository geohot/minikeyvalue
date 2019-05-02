package main

import (
  "os"
  "sync"
  "strings"
  "fmt"
  "net/http"
  "encoding/json"
  "github.com/syndtr/goleveldb/leveldb"
  "github.com/syndtr/goleveldb/leveldb/util"
)

// *** Master Server ***

type App struct {
  db *leveldb.DB
  mlock sync.Mutex
  lock map[string]struct{}
  volumes []string
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

func (a *App) QueryHandler(key []byte, w http.ResponseWriter, r *http.Request) {
  switch r.URL.RawQuery {
  case "list":
    iter := a.db.NewIterator(util.BytesPrefix(key), nil)
    defer iter.Release()
    keys := make([]string, 0)
    for iter.Next() {
      keys = append(keys, string(iter.Key()))
      if len(keys) > 1000000 {   // too large
        w.WriteHeader(http.StatusRequestEntityTooLarge)
        return
      }
    }
    str, err := json.Marshal(keys)
    if err != nil {
      w.WriteHeader(http.StatusInternalServerError)
      return
    }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    w.Write(str)
    return
  default:
    w.WriteHeader(http.StatusForbidden)
    return
  }
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  key := []byte(r.URL.Path)

  // this is a list query
  if len(r.URL.RawQuery) > 0 {
    if r.Method != http.MethodGet {
      w.WriteHeader(http.StatusForbidden)
      return
    }
    a.QueryHandler(key, w, r)
    return
  }

  // lock the key while a PUT or DELETE is in progress
  if r.Method == http.MethodPut || r.Method == http.MethodDelete {
    if !a.LockKey(key) {
      // Conflict, retry later
      w.WriteHeader(http.StatusConflict)
      return
    }
    defer a.UnlockKey(key)
  }

  switch r.Method {
  case http.MethodGet, http.MethodHead:
    data, err := a.db.Get(key, nil)
    if err == leveldb.ErrNotFound {
      w.WriteHeader(http.StatusNotFound)
      return
    }
    volume := string(data)
    kvolume := key2volume(key, a.volumes)
    if volume != kvolume {
      fmt.Println("on wrong volume, needs rebalance")
    }
    remote := fmt.Sprintf("http://%s%s", volume, key2path(key))
    w.Header().Set("Location", remote)
    w.WriteHeader(http.StatusFound)
  case http.MethodPut:
    // no empty values
    if r.ContentLength == 0 {
      w.WriteHeader(http.StatusLengthRequired)
      return
    }

    _, err := a.db.Get(key, nil)

    // check if we already have the key
    if err != leveldb.ErrNotFound {
      // Forbidden to overwrite with PUT
      w.WriteHeader(http.StatusForbidden)
      return
    }

    // we don't, compute the remote URL
    kvolume := key2volume(key, a.volumes)
    remote := fmt.Sprintf("http://%s%s", kvolume, key2path(key))

    if remote_put(remote, r.ContentLength, r.Body) != nil {
      // we assume the remote wrote nothing if it failed
      w.WriteHeader(http.StatusInternalServerError)
      return
    }

    // push to leveldb
    // note that the key is locked, so nobody wrote to the leveldb
    err = a.db.Put(key, []byte(kvolume), nil)
    if err != nil {
      // should we delete?
      w.WriteHeader(http.StatusInternalServerError)
      return
    }

    // 201, all good
    w.WriteHeader(http.StatusCreated)
  case http.MethodDelete:
    // delete the key, first locally
    data, err := a.db.Get(key, nil)
    if err == leveldb.ErrNotFound {
      w.WriteHeader(http.StatusNotFound)
      return
    }

    a.db.Delete(key, nil)

    // then remotely
    remote := fmt.Sprintf("http://%s%s", string(data), key2path(key))
    if remote_delete(remote) != nil {
      // if this fails, it's possible to get an orphan file
      // but i'm not really sure what else to do?
      w.WriteHeader(http.StatusInternalServerError)
      return
    }

    // 204, all good
    w.WriteHeader(http.StatusNoContent)
  }
}

func main() {
  fmt.Printf("hello from go %s\n", os.Args[3])

  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

  db, err := leveldb.OpenFile(os.Args[1], nil)
  if err != nil {
    fmt.Errorf("LevelDB open failed %s", err)
    return
  }
  defer db.Close()

  http.ListenAndServe("127.0.0.1:"+os.Args[2], &App{db: db,
    lock: make(map[string]struct{}),
    volumes: strings.Split(os.Args[3], ",")})
}

