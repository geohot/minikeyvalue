package main

import (
  "os"
  "fmt"
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
  fmt.Println("rebalancing", string(req.key), "from", req.volume, "to", req.kvolume)
  remote_from := fmt.Sprintf("http://%s%s", req.volume, key2path(req.key))
  remote_to := fmt.Sprintf("http://%s%s", req.kvolume, key2path(req.key))

  // read
  ss, err := remote_get(remote_from)
  if err != nil {
    fmt.Println("get error", err, remote_from)
    return false
  }

  // write
  err = remote_put(remote_to, int64(len(ss)), strings.NewReader(ss))
  if err != nil {
    fmt.Println("put error", err, remote_to)
    return false
  }

  // update db
  err = db.Put(req.key, []byte(req.kvolume), nil)
  if err != nil {
    fmt.Println("put db error", err)
    return false
  }

  // delete
  err = remote_delete(remote_from)
  if err != nil {
    fmt.Println("delete error", err, remote_from)
    return false
  }
  return true
}

func main() {
  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 256

  volumes := strings.Split(os.Args[1], ",")
  fmt.Println("rebalancing to", volumes)

  db, err := leveldb.OpenFile(os.Args[2], nil)
  defer db.Close()

  if err != nil {
    fmt.Errorf("LevelDB open failed %s", err)
    return
  }

  iter := db.NewIterator(nil, nil)
  defer iter.Release()
  for iter.Next() {
    key := iter.Key()
    volume := string(iter.Value())
    kvolume := key2volume(key, volumes)
    if volume != kvolume {
      rebalance(db, RebalanceRequest{key: key,
        volume: volume,
        kvolume: kvolume})
    }
  }
}

