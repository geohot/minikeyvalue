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

var dblock sync.Mutex

func rebuild(db *leveldb.DB, volumes []string, req RebuildRequest) bool {
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
    pvolumes := key2volume(key, volumes, replicas)

    dblock.Lock()
    data, err := db.Get(key, nil)
    values := make([]string, 0)
    if err != leveldb.ErrNotFound {
      values = strings.Split(string(data), ",")
    }
    values = append(values, req.vol)

    // sort by order in pvolumes (sorry it's n^2 but n is small)
    pvalues := make([]string, 0)
    for _, v := range pvolumes {
      for _, v2 := range values {
        if v == v2 {
          pvalues = append(pvalues, v)
        }
      }
    }
    // insert the ones that aren't there at the end
    for _, v2 := range values {
      insert := true
      for _, v := range pvolumes {
        if v == v2 {
          insert = false
          break
        }
      }
      if insert {
        pvalues = append(pvalues, v2)
      }
    }

    if err := db.Put(key, []byte(strings.Join(pvalues, ",")), nil); err != nil {
      fmt.Println("ugh", err)
      return false
    }
    dblock.Unlock()
    fmt.Println(string(key), pvalues)
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

  iter := db.NewIterator(nil, nil)
  for iter.Next() {
    db.Delete(iter.Key(), nil)
  }

  var wg sync.WaitGroup
  reqs := make(chan RebuildRequest, 20000)

  for i := 0; i < 4; i++ {
    go func() {
      for req := range reqs {
        rebuild(db, volumes, req)
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

