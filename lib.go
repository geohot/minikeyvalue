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

// *** Params ***

var fallback string = ""
var replicas int = 3
var subvolumes uint = 2

// *** Hash Functions ***

func key2path(key []byte) string {
  mkey := md5.Sum(key)
  b64key := base64.StdEncoding.EncodeToString(key)

  // 2 byte layers deep, meaning a fanout of 256
  // optimized for 2^24 = 16M files per volume server
  return fmt.Sprintf("/%02x/%02x/%s", mkey[0], mkey[1], b64key)
}

type sortvol struct {
  score []byte
  volume string
}
type byScore []sortvol
func (s byScore) Len() int { return len(s) }
func (s byScore) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byScore) Less(i, j int) bool {
  return bytes.Compare(s[i].score, s[j].score) == 1
}

func key2volume(key []byte, volumes []string, count int) []string {
  // this is an intelligent way to pick the volume server for a file
  // stable in the volume server name (not position!)
  // and if more are added the correct portion will move (yay md5!)
  var sortvols []sortvol
  for _, v := range volumes {
    hash := md5.New()
    hash.Write(key)
    hash.Write([]byte(v))
    score := hash.Sum(nil)
    sortvols = append(sortvols, sortvol{score, v})
  }
  sort.Stable(byScore(sortvols))
  // go should have a map function
  // this adds the subvolumes
  var ret []string
  for i := 0; i < count; i++ {
    sv := sortvols[i]
    var volume string
    if subvolumes == 1 {
      // if it's one, don't use the path structure for it
      volume = sv.volume
    } else {
      // use the least significant compare byte for the subvolume
      volume = fmt.Sprintf("%s/sv%02X", sv.volume, uint(sv.score[15]) % subvolumes)
    }
    ret = append(ret, volume)
  }
  //fmt.Println(string(key), ret[0])
  return ret
}

func needs_rebalance(volumes []string, kvolumes []string) bool {
  if len(volumes) != len(kvolumes) {
    return true
  }
  for i := 0; i < len(volumes); i++ {
    if volumes[i] != kvolumes[i] {
      return true
    }
  }
  return false
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

