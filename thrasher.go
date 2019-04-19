package main

import (
  "os"
  "fmt"
  "net/http"
  "time"
  "strings"
  "math/rand"
)

func main() {
  rand.Seed(time.Now().UTC().UnixNano())

  reqs := make(chan string, 20000)
  resp := make(chan bool, 20000)
  fmt.Println("starting thrasher")

  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

  // 16 concurrent processes
  for i := 0; i < 16; i++ {
    go func() {
      for {
        key := <-reqs
        value := fmt.Sprintf("value-%d", rand.Int())
        err := remote_put("http://localhost:3000/"+key, int64(len(value)), strings.NewReader(value))
        if err != nil {
          fmt.Println("PUT FAILED", err)
          resp <- false
          continue
        }

        ss, err := remote_get("http://localhost:3000/"+key)
        if err != nil || ss != value {
          fmt.Println("GET FAILED", err, ss, value)
          resp <- false
          continue
        }

        err = remote_delete("http://localhost:3000/"+key)
        if err != nil {
          fmt.Println("DELETE FAILED", err)
          resp <- false
          continue
        }
        resp <- true
      }
    }()
  }

  count := 10000

  start := time.Now()
  for i := 0; i < count; i++ {
    key := fmt.Sprintf("benchmark-%d", rand.Int())
    reqs <- key
  }

  for i := 0; i < count; i++ {
    if <-resp == false {
      fmt.Println("ERROR on", i)
      os.Exit(-1)
    }
  }

  fmt.Println(count, "write/read/delete in", time.Since(start))
  fmt.Printf("thats %.2f/sec\n", float32(count)/(float32(time.Since(start))/1e9))
}

