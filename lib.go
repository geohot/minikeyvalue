package main

import (
  "bytes"
  "errors"
  "encoding/base64"
  "crypto/md5"
  "fmt"
  "io"
  "io/ioutil"
  "net/http"
  "sort"
)

// *** Hash Functions ***

func key2path(key []byte) string {
  mkey := md5.Sum(key)
  b64key := base64.StdEncoding.EncodeToString(key)

  // 2 byte layers deep, meaning a fanout of 256
  // optimized for 2^24 = 16M files per volume server
  return fmt.Sprintf("/%02x/%02x/%s", mkey[0], mkey[1], b64key)
}

func key2volume(key []byte, volumes []string, count int) []string {
  // this is an intelligent way to pick the volume server for a file
  // stable in the volume server name (not position!)
  // and if more are added the correct portion will move (yay md5!)
  type svolume struct {
    score []byte
    volume string
  }
  var svolumes []svolume
  for _, v := range volumes {
    hash := md5.New()
    hash.Write(key)
    hash.Write([]byte(v))
    score := hash.Sum(nil)
    svolumes = append(svolumes, svolume{score, v})
  }
  sort.SliceStable(svolumes, func(i int, j int) bool { return bytes.Compare(svolumes[i].score, svolumes[j].score) == -1 })
  // go should have a map function
  var ret []string
  for i := 0; i < count; i++ {
    ret = append(ret, svolumes[i].volume)
  }
  //fmt.Println(string(key), ret[0])
  return ret
}


// *** Remote Access Functions ***

func remote_delete(remote string) error {
  req, err := http.NewRequest("DELETE", remote, nil)
  if err != nil {
    return err
  }
  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    return err
  }
  defer resp.Body.Close()
  if resp.StatusCode != 204 {
    return fmt.Errorf("remote_delete: wrong status code %d", resp.StatusCode)
  }
  return nil
}

func remote_put(remote string, length int64, body io.Reader) error {
  req, err := http.NewRequest("PUT", remote, body)
  if err != nil {
    return err
  }
  req.ContentLength = length
  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    return err
  }
  defer resp.Body.Close()
  if resp.StatusCode != 201 && resp.StatusCode != 204 {
    return fmt.Errorf("remote_put: wrong status code %d", resp.StatusCode)
  }
  return nil
}

func remote_get(remote string) (string, error) {
  resp, err := http.Get(remote)
  if err != nil {
    return "", err
  }
  if resp.StatusCode != 200 {
    return "", errors.New(fmt.Sprintf("remote_get: wrong status code %d", resp.StatusCode))
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return "", err
  }
  return string(body), nil
}

