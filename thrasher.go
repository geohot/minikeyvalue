package main

import (
  "io"
  "io/ioutil"
  "fmt"
  "time"
  "strings"
  "net/http"
  "math/rand"
)

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

func remote_get(remote string) string {
  resp, err := http.Get(remote)
  if err != nil {
    return ""
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  return string(body)
}

func main() {
  rand.Seed(time.Now().UTC().UnixNano())

  reqs := make(chan string, 1000)
  messages := make(chan string)
  fmt.Println("start")

  // 16 concurrent processes
  for i := 0; i < 16; i++ {
    go func() {
      for {
        key := <-reqs
        value := fmt.Sprintf("value-%d", rand.Int())
        ret := remote_put("http://localhost:3000/"+key, int64(len(value)), strings.NewReader(value))
        if ret == false {
          fmt.Println("PUT FAILED")
        }

        ss := remote_get("http://localhost:3000/"+key)
        if ss != value {
          fmt.Println("GET FAILED", value)
        }

        remote_delete("http://localhost:3000/"+key)
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

  fmt.Println("1000 writes", time.Since(start))
}

