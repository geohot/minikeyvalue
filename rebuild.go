package main

import (
  "os"
  "fmt"
  "sync"
  "encoding/json"
  "net/http"
  "github.com/syndtr/goleveldb/leveldb"
  "strings"
  "encoding/base64"
)

type File struct {
  Name string
  Type string
  Mtime string
}

type RebuildRequest struct {
  vol string
  url string
}

func rebuild(db *leveldb.DB, req RebuildRequest) bool {
  dat, err := remote_get(req.url)
  if err != nil {
    fmt.Println("ugh", err)
    return false
  }
  var files []File
  json.Unmarshal([]byte(dat), &files)
  for _, f := range files {
    key, err := base64.StdEncoding.DecodeString(f.Name)
    if err != nil {
      fmt.Println("ugh", err)
      return false
    }
    if err := db.Put(key, []byte(req.vol), nil); err != nil {
      fmt.Println("ugh", err)
      return false
    }
    fmt.Println(string(key), req.vol)
  }
  return true
}

func main() {
  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

  volumes := strings.Split(os.Args[1], ",")
  fmt.Println("rebuilding on", volumes)

  db, err := leveldb.OpenFile(os.Args[2], nil)
  if err != nil {
    fmt.Println(fmt.Errorf("LevelDB open failed %s", err))
    return
  }
  defer db.Close()

  // TODO: empty leveldb

  var wg sync.WaitGroup
  reqs := make(chan RebuildRequest, 20000)

  for i := 0; i < 4; i++ {
    go func() {
      for req := range reqs {
        rebuild(db, req)
        wg.Done()
      }
    }()
  }

  for i := 0; i < 256; i++ {
    for j := 0; j < 256; j++ {
      for sv := 0; sv < int(subvolumes); sv++ {
        for _, vol := range volumes {
          wg.Add(1)
          tvol := vol
          if subvolumes != 1 {
            tvol = fmt.Sprintf("%s/sv%02x", vol, sv)
          }
          url := fmt.Sprintf("http://%s/%02x/%02x/", tvol, i, j)
          reqs <- RebuildRequest{tvol, url}
        }
      }
    }
  }
  close(reqs)

  wg.Wait()
}

