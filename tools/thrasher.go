package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

// copied from lib.go
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
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", errors.New(fmt.Sprintf("remote_get: wrong status code %d", resp.StatusCode))
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	reqs := make(chan string, 20000)
	resp := make(chan bool, 20000)
	fmt.Println("starting thrasher")

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	// 16 concurrent processes
	for i := 0; i < 16; i++ {
		go func() {
			for {
				key := <-reqs
				value := fmt.Sprintf("value-%d", rand.Int())
				if err := remote_put("http://localhost:3000/"+key, int64(len(value)), strings.NewReader(value)); err != nil {
					fmt.Println("PUT FAILED", err)
					resp <- false
					continue
				}

				ss, err := remote_get("http://localhost:3000/" + key)
				if err != nil || ss != value {
					fmt.Println("GET FAILED", err, ss, value)
					resp <- false
					continue
				}

				if err := remote_delete("http://localhost:3000/" + key); err != nil {
					fmt.Println("DELETE FAILED", err)
					resp <- false
					continue
				}
				resp <- true
			}
		}()
	}

	count := 10000

	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("benchmark-%d", rand.Int())
		reqs <- key
	}

	for i := 0; i < count; i++ {
		if <-resp == false {
			fmt.Println("ERROR on", i)
			os.Exit(-1)
		}
	}

	fmt.Println(count, "write/read/delete in", time.Since(start))
	fmt.Printf("thats %.2f/sec\n", float32(count)/(float32(time.Since(start))/1e9))
}
