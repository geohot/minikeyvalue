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
)

// *** Hash Functions ***

func key2path(key []byte) string {
  mkey := md5.Sum(key)
  b64key := base64.StdEncoding.EncodeToString(key)

  return fmt.Sprintf("/%02x/%02x/%s", mkey[0], mkey[1], b64key)
}

func key2volume(key []byte, volumes []string) string {
  var best_score []byte = nil
  var ret string = ""
  for _, v := range volumes {
    hash := md5.New()
    hash.Write(key)
    hash.Write([]byte(v))
    score := hash.Sum(nil)
    if best_score == nil || bytes.Compare(best_score, score) == -1 {
      best_score = score
      ret = v
    }
  }
  //fmt.Println(string(key), ret, best_score)
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
    return errors.New(fmt.Sprintf("remote_delete: wrong status code %d", resp.StatusCode))
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
    return errors.New(fmt.Sprintf("remote_put: wrong status code %d", resp.StatusCode))
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
