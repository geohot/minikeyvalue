package main

import (
  "flag"
  "sync"
  "math/rand"
  "time"
  "net/http"
  "fmt"
  "strings"
  "github.com/syndtr/goleveldb/leveldb"
)

// *** App struct and methods ***

type App struct {
  db *leveldb.DB
  mlock sync.Mutex
  lock map[string]struct{}

  // params
  volumes []string
  fallback string
  replicas int
  subvolumes int
  softdelete bool
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

func (a *App) GetRecord(key []byte) Record {
  data, err := a.db.Get(key, nil)
  rec := Record{[]string{}, true}
  if err != leveldb.ErrNotFound { rec = toRecord(data) }
  return rec
}

func (a *App) PutRecord(key []byte, rec Record) bool {
  return a.db.Put(key, fromRecord(rec), nil) == nil
}

// *** Entry Point ***

func main() {
  // setup
  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
  rand.Seed(time.Now().Unix())

  port := flag.Int("port", 3000, "Port for the server to listen on")
  pdb := flag.String("database", "", "Path to leveldb")
  fallback := flag.String("fallback", "", "Fallback server for 404")
  replicas := flag.Int("replicas", 3, "Amount of replicas to make of the data")
  subvolumes := flag.Int("subvolumes", 10, "Amount of subvolumes for sharding")
  softdelete := flag.Bool("softdelete", false, "Make deletes only virtual")
  pvolumes := flag.String("volumes", "", "Volumes to use for storage")
  fmt.Println(*pvolumes)
  volumes := strings.Split(*pvolumes, ",")
  flag.Parse()

  command := flag.Arg(0)

  if command != "server" && command != "rebuild" && command != "rebalance" {
    fmt.Println("minikeyvalue needs a command, either server, rebuild, or rebalance\n")
    flag.PrintDefaults()
    return
  }

  if *pdb == "" {
    panic("Need a path to the database")
  }

  //fmt.Printf("database: %s\n", os.Args[1])
  fmt.Printf("server port: %d\n", *port)
  fmt.Printf("volume servers: %s\n", volumes)

  if len(volumes) < *replicas {
    panic("Need at least as many volumes as replicas")
  }

  db, err := leveldb.OpenFile(*pdb, nil)
  if err != nil {
    panic(fmt.Sprintf("LevelDB open failed: %s", err))
  }
  defer db.Close()

  a := App{db: db,
    lock: make(map[string]struct{}),
    volumes: volumes,
    fallback: *fallback,
    replicas: *replicas,
    subvolumes: *subvolumes,
    softdelete: *softdelete,
  }

  if command == "server" {
    http.ListenAndServe(fmt.Sprintf(":%d", *port), &a)
  }

}

