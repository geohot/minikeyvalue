package main

import (
  "os"
  "fmt"
  "sync"
  "net/http"
  "strings"
  "github.com/syndtr/goleveldb/leveldb"
)

type RebalanceRequest struct {
  key []byte
  volume string
  kvolume string
}

func rebalance(db *leveldb.DB, req RebalanceRequest) bool {
  remote_from := fmt.Sprintf("http://%s%s", req.volume, key2path(req.key))
  remote_to := fmt.Sprintf("http://%s%s", req.kvolume, key2path(req.key))

  // debug
  fmt.Println("rebalancing", string(req.key), "from", req.volume, "to", req.kvolume)

  // read
  ss, err := remote_get(remote_from)
  if err != nil {
    fmt.Println("get error", err, remote_from)
    return false
  }

  // write
  if err := remote_put(remote_to, int64(len(ss)), strings.NewReader(ss)); err != nil {
    fmt.Println("put error", err, remote_to)
    return false
  }

  // update db
  if err := db.Put(req.key, []byte(req.kvolume), nil); err != nil {
    fmt.Println("put db error", err)
    return false
  }

  // delete
  if err := remote_delete(remote_from); err != nil {
    fmt.Println("delete error", err, remote_from)
    return false
  }
  return true
}

func main() {
  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

  volumes := strings.Split(os.Args[1], ",")
  fmt.Println("rebalancing to", volumes)

  db, err := leveldb.OpenFile(os.Args[2], nil)
  if err != nil {
    fmt.Println(fmt.Errorf("LevelDB open failed %s", err))
    return
  }
  defer db.Close()

  var wg sync.WaitGroup
  reqs := make(chan RebalanceRequest, 20000)

  for i := 0; i < 16; i++ {
    go func() {
      for req := range reqs {
        rebalance(db, req)
        wg.Done()
      }
    }()
  }

  iter := db.NewIterator(nil, nil)
  defer iter.Release()
  for iter.Next() {
    key := make([]byte, len(iter.Key()))
    copy(key, iter.Key())
    volume := string(iter.Value())
    kvolume := key2volume(key, volumes)
    if volume != kvolume {
      wg.Add(1)
      reqs <- RebalanceRequest{
        key: key,
        volume: volume,
        kvolume: kvolume}
    }
  }
  close(reqs)

  wg.Wait()
}

