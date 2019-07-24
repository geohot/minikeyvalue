package main

import (
  "os"
  "fmt"
  "github.com/syndtr/goleveldb/leveldb"
  "github.com/syndtr/goleveldb/leveldb/opt"
  //"github.com/syndtr/goleveldb/leveldb/util"
)

func main() {
  opts := &opt.Options{ErrorIfMissing: true, ReadOnly: true}
  db1, err1 := leveldb.OpenFile(os.Args[1], opts)
  if err1 != nil { panic(fmt.Sprintf("db1 open failed: %s", err1)) }
  db2, err2 := leveldb.OpenFile(os.Args[2], opts)
  if err2 != nil { panic(fmt.Sprintf("db2 open failed: %s", err2)) }

  iter1 := db1.NewIterator(nil, nil)
  iter2 := db2.NewIterator(nil, nil)
  bad := false
  for iter1.Next() {
    iter2.Next()
    k1 := string(iter1.Key())
    v1 := string(iter1.Value())
    k2 := string(iter2.Key())
    v2 := string(iter2.Value())
    if k1 != k2 {
      panic(fmt.Sprintf("key mismatch %s != %s", k1, k2))
    }
    if v1 != v2 {
      // we can continue with a value mismatch
      fmt.Printf("%s: %s != %s\n", k1, v1, v2)
      bad = true
    }
  }
  if bad { panic("not all values matched") }
}

