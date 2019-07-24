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
  volumes []string
  kvolumes []string
}

func rebalance(db *leveldb.DB, req RebalanceRequest) bool {
  // debug
  fmt.Println("rebalancing", string(req.key), "from", req.volumes, "to", req.kvolumes)

  // always read from the first one
  remote_from := fmt.Sprintf("http://%s%s", req.volumes[0], key2path(req.key))

  // read
  ss, err := remote_get(remote_from)
  if err != nil {
    fmt.Println("get error", err, remote_from)
    return false
  }

  // write to the kvolumes
  for _, v := range req.kvolumes {
    needs_write := true
    // see if it's already there
    for _, v2 := range req.volumes {
      if v == v2 {
        needs_write = false
        break
      }
    }
    if needs_write {
      remote_to := fmt.Sprintf("http://%s%s", v, key2path(req.key))
      // write
      if err := remote_put(remote_to, int64(len(ss)), strings.NewReader(ss)); err != nil {
        fmt.Println("put error", err, remote_to)
        return false
      }
    }
  }

  // update db
  if err := db.Put(req.key, []byte(strings.Join(req.kvolumes, ",")), nil); err != nil {
    fmt.Println("put db error", err)
    return false
  }

  // delete from the volumes that now aren't kvolumes
  for _, v2 := range req.volumes {
    needs_delete := true
    for _, v := range req.kvolumes {
      if v == v2 {
        needs_delete = false
        break
      }
    }
    if needs_delete {
      remote_del := fmt.Sprintf("http://%s%s", v2, key2path(req.key))
      if err := remote_delete(remote_del); err != nil {
        fmt.Println("delete error", err, remote_del)
        return false
      }
    }
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
    rvolumes := strings.Split(string(iter.Value()), ",")
    kvolumes := key2volume(key, volumes, replicas)
    if needs_rebalance(rvolumes, kvolumes) {
      wg.Add(1)
      reqs <- RebalanceRequest{
        key: key,
        volumes: rvolumes,
        kvolumes: kvolumes}
    }
  }
  close(reqs)

  wg.Wait()
}

