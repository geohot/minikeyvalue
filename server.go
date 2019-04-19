package main

import (
  "encoding/base64"
  "crypto/md5"
  "io"
  "os"
  "bytes"
  "strings"
  "fmt"
  "net/http"
  "github.com/syndtr/goleveldb/leveldb"
)

// *** Global ***

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
    hash.Write([]byte(v))
    hash.Write(key)
    score := hash.Sum(nil)
    if best_score == nil || bytes.Compare(best_score, score) == -1 {
      best_score = score
      ret = v
    }
  }
  return ret
}

func remote_put(remote string, length int64, body io.Reader) bool {
  client := &http.Client{}
  req, err := http.NewRequest("PUT", remote, body)
  req.ContentLength = length
  resp, err := client.Do(req)
  if err != nil {
    fmt.Println("remote put failed", err)
    return false
  }
  return resp.StatusCode == 201 || resp.StatusCode == 204
}

func remote_delete(remote string) bool {
  client := &http.Client{}
  req, err := http.NewRequest("DELETE", remote, nil)
  resp, err := client.Do(req)
  if err != nil {
    fmt.Println("remote delete failed", err)
    return false
  }
  return resp.StatusCode == 204
}

// *** Master Server ***

type App struct {
  db *leveldb.DB
  volumes []string
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  key := []byte(r.URL.Path)

  switch r.Method {
  case "GET", "HEAD":
    data, err := a.db.Get(key, nil)
    if err == leveldb.ErrNotFound {
      w.WriteHeader(404)
      return
    }
    remote := fmt.Sprintf("http://%s%s", string(data), key2path(key))
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
      w.WriteHeader(409)
      return
    }

    // we don't, compute the remote URL
    volume := key2volume(key, a.volumes)
    remote := fmt.Sprintf("http://%s%s", volume, key2path(key))

    if remote_put(remote, r.ContentLength, r.Body) == false {
      w.WriteHeader(500)
      return
    }

    // note, this currently is a race
    // see note below
    _, err = a.db.Get(key, nil)
    if err != leveldb.ErrNotFound {
      // not safe to delete here
      w.WriteHeader(409)
      return
    }

    // push to leveldb
    // note that this may possibly overwrite if there's a race
    // this is fine, but can possibly create an orphan file
    a.db.Put(key, []byte(volume), nil)

    // 201, all good
    w.WriteHeader(201)
  case "DELETE":
    // delete the key, first locally
    data, err := a.db.Get(key, nil)
    if err == leveldb.ErrNotFound {
      w.WriteHeader(404)
      return
    }

    // note that this may not actually delete the key if there's a race
    // this is fine, extra remote delete won't hurt
    a.db.Delete(key, nil)

    // then remotely
    remote := fmt.Sprintf("http://%s%s", string(data), key2path(key))
    if remote_delete(remote) == false {
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
    volumes: strings.Split(os.Args[3], ",")})
}

