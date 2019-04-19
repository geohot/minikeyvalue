package main

import (
  "encoding/base64"
  "crypto/md5"
  "os"
  "bytes"
  "sync"
  "strings"
  "fmt"
  "net/http"
  "encoding/json"
  "github.com/syndtr/goleveldb/leveldb"
  "github.com/syndtr/goleveldb/leveldb/util"
)

// *** Hash Functions ***

func key2path(key []byte) string {
  mkey := md5.Sum(key)
  b64key := base64.StdEncoding.EncodeToString(key)

  return fmt.Sprintf("/%02x/%02x/%s", mkey[0], mkey[1], b64key)
}

func key2volume(key []byte, volumes []string) string {
  var best_score []byte = nil
  var ret string = ""
  for _, v := range volumes {
    hash := md5.New()
    hash.Write(key)
    hash.Write([]byte(v))
    score := hash.Sum(nil)
    if best_score == nil || bytes.Compare(best_score, score) == -1 {
      best_score = score
      ret = v
    }
  }
  //fmt.Println(string(key), ret, best_score)
  return ret
}

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

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  key := []byte(r.URL.Path)

  // empty url = no go
  if len(key) <= 1 {
    // could put a status page here
    w.WriteHeader(403)
    return
  }

  // this is a list query
  if key[len(key)-1] == '/' {
    if r.Method != "GET" {
      w.WriteHeader(403)
      return
    }
    iter := a.db.NewIterator(util.BytesPrefix(key[:len(key)-1]), nil)
    defer iter.Release()
    keys := make([]string, 0)
    for iter.Next() {
      keys = append(keys, string(iter.Key()))
      if len(keys) > 1000000 {   // too large
        w.WriteHeader(413)
        return
      }
    }
    str, err := json.Marshal(keys)
    if err != nil {
      w.WriteHeader(500)
      return
    }
    w.WriteHeader(200)
    w.Write(str)
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
    data, err := a.db.Get(key, nil)
    if err == leveldb.ErrNotFound {
      w.WriteHeader(404)
      return
    }
    volume := string(data)
    // TODO: Check volume is in a.volumes
    kvolume := key2volume(key, a.volumes)
    if volume != kvolume {
      fmt.Println("on wrong volume, needs rebalance")
    }
    remote := fmt.Sprintf("http://%s%s", volume, key2path(key))
    w.Header().Set("Location", remote)
    w.WriteHeader(302)
  case "PUT":
    // no empty values
    if r.ContentLength == 0 {
      w.WriteHeader(411)
      return
    }

    _, err := a.db.Get(key, nil)

    // check if we already have the key
    if err != leveldb.ErrNotFound {
      // Forbidden to overwrite with PUT
      w.WriteHeader(403)
      return
    }

    // we don't, compute the remote URL
    kvolume := key2volume(key, a.volumes)
    remote := fmt.Sprintf("http://%s%s", kvolume, key2path(key))

    if remote_put(remote, r.ContentLength, r.Body) != nil {
      // we assume the remote wrote nothing if it failed
      w.WriteHeader(500)
      return
    }

    // push to leveldb
    // note that the key is locked, so nobody wrote to the leveldb
    a.db.Put(key, []byte(kvolume), nil)

    // 201, all good
    w.WriteHeader(201)
  case "DELETE":
    // delete the key, first locally
    data, err := a.db.Get(key, nil)
    if err == leveldb.ErrNotFound {
      w.WriteHeader(404)
      return
    }

    a.db.Delete(key, nil)

    // then remotely
    remote := fmt.Sprintf("http://%s%s", string(data), key2path(key))
    if remote_delete(remote) != nil {
      // if this fails, it's possible to get an orphan file
      // but i'm not really sure what else to do?
      w.WriteHeader(500)
      return
    }

    // 204, all good
    w.WriteHeader(204)
  }
}

func main() {
  fmt.Printf("hello from go %s\n", os.Args[3])

  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 256

  db, err := leveldb.OpenFile(os.Args[1], nil)
  defer db.Close()

  if err != nil {
    fmt.Errorf("LevelDB open failed %s", err)
    return
  }

  http.ListenAndServe("127.0.0.1:"+os.Args[2], &App{db: db,
    lock: make(map[string]struct{}),
    volumes: strings.Split(os.Args[3], ",")})
}

