package main

import (
  "errors"
  "fmt"
  "io"
  "io/ioutil"
  "net/http"
)

func remote_delete(remote string) error {
  client := &http.Client{}
  req, err := http.NewRequest("DELETE", remote, nil)
  resp, err := client.Do(req)
  if err != nil {
    return err
  }
  if resp.StatusCode != 204 {
    return errors.New(fmt.Sprintf("remote_delete: wrong status code %d", resp.StatusCode))
  }
  return nil
}

func remote_put(remote string, length int64, body io.Reader) error {
  client := &http.Client{}
  req, err := http.NewRequest("PUT", remote, body)
  req.ContentLength = length
  resp, err := client.Do(req)
  if err != nil {
    return err
  }
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
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return "", err
  }
  return string(body), nil
}
