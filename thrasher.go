package main

import (
  "fmt"
  "time"
  "strings"
  "math/rand"
)

func main() {
  rand.Seed(time.Now().UTC().UnixNano())

  reqs := make(chan string, 1000)
  messages := make(chan string)
  fmt.Println("starting thrasher")

  // 16 concurrent processes
  for i := 0; i < 16; i++ {
    go func() {
      for {
        key := <-reqs
        value := fmt.Sprintf("value-%d", rand.Int())
        err := remote_put("http://localhost:3000/"+key, int64(len(value)), strings.NewReader(value))
        if err != nil {
          fmt.Println("PUT FAILED", err)
        }

        ss, err := remote_get("http://localhost:3000/"+key)
        if err != nil || ss != value {
          fmt.Println("GET FAILED", err, ss, value)
        }

        err = remote_delete("http://localhost:3000/"+key)
        if err != nil {
          fmt.Println("DELETE FAILED", err)
        }
        messages <- ss
      }
    }()
  }

  start := time.Now()
  for i := 0; i < 1000; i++ {
    key := fmt.Sprintf("benchmark-%d", rand.Int())
    reqs <- key
  }

  for i := 0; i < 1000; i++ {
    <-messages
  }

  fmt.Println("1000 write/read/delete in", time.Since(start))
}

