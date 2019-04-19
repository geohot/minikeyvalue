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

type Job struct {
  vol string
  req string
}

func main() {
  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

  volumes := strings.Split(os.Args[1], ",")
  fmt.Println("rebuilding on", volumes)

  db, err := leveldb.OpenFile(os.Args[2], nil)
  if err != nil {
    fmt.Errorf("LevelDB open failed %s", err)
    return
  }
  defer db.Close()

  var wg sync.WaitGroup
  reqs := make(chan Job, 20000)

  for i := 0; i < 128; i++ {
    go func() {
      for job := range reqs {
        dat, err := remote_get(job.req)
        if err != nil {
          fmt.Println("ugh", err)
        }
        var files []File
        json.Unmarshal([]byte(dat), &files)
        for _, f := range files {
          key, err := base64.StdEncoding.DecodeString(f.Name)
          if err != nil {
            fmt.Println("ugh", err)
          }
          err = db.Put(key, []byte(job.vol), nil)
          if err != nil {
            fmt.Println("ugh", err)
          }
          fmt.Println(string(key), job.vol)
        }
        wg.Done()
      }
    }()
  }

  for i := 0; i < 256; i++ {
    for j := 0; j < 256; j++ {
      for _, vol := range volumes {
        wg.Add(1)
        req := fmt.Sprintf("http://%s/%02x/%02x/", vol, i, j)
        reqs <- Job{vol, req}
      }
    }
  }
  close(reqs)

  wg.Wait()

}

