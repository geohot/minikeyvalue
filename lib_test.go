package main

import (
  "testing"
  "strings"
  "reflect"
  //"fmt"
)

// ensure the path hashing function doesn't change
func Test_key2path(t *testing.T) {
  tests := map[string]string{
    "hello": "/5d/41/aGVsbG8=",
    "helloworld": "/fc/5e/aGVsbG93b3JsZA==",
  }
  for k,v := range tests {
    ret := key2path([]byte(k))
    if ret != v {
      t.Fatal("key2path function broke", k, ret, v)
    }
  }
}

// ensure the volume hashing function doesn't change
func Test_key2volume(t *testing.T) {
  volumes := []string{"larry", "moe", "curly"}
  tests := map[string]string{
    "hello": "larry",
    "helloworld": "curly",
    "world": "moe",
    "blah": "curly",
  }
  for k,v := range tests {
    ret := key2volume([]byte(k), volumes, 1)
    if strings.Split(ret[0], "/")[0] != v {
      t.Fatal("key2volume function broke", k, ret, v)
    }
  }
}

func fromToRecordExample(t *testing.T, rec Record, val string) {
  recs := fromRecord(rec)
  if val != string(recs) {
    t.Fatal("record string didn't match")
  }
  reca := toRecord(recs)
  if !reflect.DeepEqual(rec, reca) {
    t.Fatal("toRecord(fromRecord(rec)) failed")
  }
}

func Test_fromToRecord(t *testing.T) {
  fromToRecordExample(t, Record{[]string{"hello", "world"}, true}, "DELETEDhello,world")
  fromToRecordExample(t, Record{[]string{"hello", "world"}, false}, "hello,world")
  fromToRecordExample(t, Record{[]string{"hello"}, false}, "hello")
  fromToRecordExample(t, Record{[]string{"hello"}, true}, "DELETEDhello")
}

