package main

import (
  "os"
  "math/rand"
  "time"
  "net/http"
  "fmt"
  "strings"
  "github.com/syndtr/goleveldb/leveldb"
)

func main() {
  // setup
  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
  rand.Seed(time.Now().Unix())

  fmt.Printf("database: %s\n", os.Args[1])
  fmt.Printf("server port: %s\n", os.Args[2])
  fmt.Printf("volume servers: %s\n", os.Args[3])
  var volumes = strings.Split(os.Args[3], ",")

  replicas := 3

  if len(volumes) < replicas {
    panic("Need at least as many volumes as replicas")
  }

  fallback := ""
  if len(os.Args) > 4 {
    fallback = os.Args[4]
  }

  db, err := leveldb.OpenFile(os.Args[1], nil)
  if err != nil {
    panic(fmt.Sprintf("LevelDB open failed: %s", err))
  }
  defer db.Close()

  a := App{db: db,
    lock: make(map[string]struct{}),
    volumes: volumes,
    fallback: fallback,
    // TODO: make these command line arguments
    replicas: replicas,
    subvolumes: 10,
    softdelete: false,
  }

  http.ListenAndServe(":"+os.Args[2], &a)
}

