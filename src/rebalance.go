package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type RebalanceRequest struct {
	key      []byte
	volumes  []string
	kvolumes []string
}

func rebalance(a *App, req RebalanceRequest) bool {
	kp := key2path(req.key)

	// find the volumes that are real
	rvolumes := make([]string, 0)
	for _, rv := range req.volumes {
		remote_test := fmt.Sprintf("http://%s%s", rv, kp)
		found, err := remote_head(remote_test, 1*time.Minute)
		if err != nil {
			fmt.Println("rebalance head error", err, remote_test)
			return false
		}
		if found {
			rvolumes = append(rvolumes, rv)
		}
	}

	if len(rvolumes) == 0 {
		fmt.Printf("rebalance impossible, %s is missing!\n", string(req.key))
		return false
	}

	if !needs_rebalance(rvolumes, req.kvolumes) {
		return true
	}

	// debug
	fmt.Println("rebalancing", string(req.key), "from", rvolumes, "to", req.kvolumes)

	// find a good rvolume
	var err error = nil
	var ss string
	for _, v := range rvolumes {
		remote_from := fmt.Sprintf("http://%s%s", v, kp)

		// read
		ss, err = remote_get(remote_from)
		if err != nil {
			fmt.Println("rebalance get error", err, remote_from)
		} else {
			break
		}
	}
	if err != nil {
		return false
	}

	// write to the kvolumes
	rebalance_error := false
	for _, v := range req.kvolumes {
		needs_write := true
		// see if it's already there
		for _, v2 := range rvolumes {
			if v == v2 {
				needs_write = false
				break
			}
		}
		if needs_write {
			remote_to := fmt.Sprintf("http://%s%s", v, kp)
			// write
			if err := remote_put(remote_to, int64(len(ss)), strings.NewReader(ss)); err != nil {
				fmt.Println("rebalance put error", err, remote_to)
				rebalance_error = true
			}
		}
	}
	if rebalance_error {
		return false
	}

	// update db
	if !a.PutRecord(req.key, Record{req.kvolumes, NO, ""}) {
		fmt.Println("rebalance put db error", err)
		return false
	}

	// delete from the volumes that now aren't kvolumes
	delete_error := false
	for _, v2 := range rvolumes {
		needs_delete := true
		for _, v := range req.kvolumes {
			if v == v2 {
				needs_delete = false
				break
			}
		}
		if needs_delete {
			remote_del := fmt.Sprintf("http://%s%s", v2, kp)
			if err := remote_delete(remote_del); err != nil {
				fmt.Println("rebalance delete error", err, remote_del)
				delete_error = true
			}
		}
	}
	if delete_error {
		return false
	}
	return true
}

func (a *App) Rebalance() {
	fmt.Println("rebalancing to", a.volumes)

	var wg sync.WaitGroup
	reqs := make(chan RebalanceRequest, 20000)

	for i := 0; i < 16; i++ {
		go func() {
			for req := range reqs {
				rebalance(a, req)
				wg.Done()
			}
		}()
	}

	iter := a.db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		rec := toRecord(iter.Value())
		kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)
		wg.Add(1)
		reqs <- RebalanceRequest{
			key:      key,
			volumes:  rec.rvolumes,
			kvolumes: kvolumes}
	}
	close(reqs)

	wg.Wait()
}
