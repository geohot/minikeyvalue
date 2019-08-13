package main

import (
	"fmt"
	"strings"
	"sync"
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
		if remoteHead(fmt.Sprintf("http://%s%s", rv, kp)) {
			rvolumes = append(rvolumes, rv)
		}
	}

	if len(rvolumes) == 0 {
		fmt.Printf("can't rebalance, %s is missing!\n", string(req.key))
		return false
	}

	if !needsRebalance(rvolumes, req.kvolumes) {
		return true
	}

	// debug
	fmt.Println("rebalancing", string(req.key), "from", rvolumes, "to", req.kvolumes)

	// always read from the first one
	remoteFrom := fmt.Sprintf("http://%s%s", rvolumes[0], kp)

	// read
	ss, err := remoteGet(remoteFrom)
	if err != nil {
		fmt.Println("get error", err, remoteFrom)
		return false
	}

	// write to the kvolumes
	for _, v := range req.kvolumes {
		needsWrite := true
		// see if it's already there
		for _, v2 := range rvolumes {
			if v == v2 {
				needsWrite = false
				break
			}
		}
		if needsWrite {
			remoteTo := fmt.Sprintf("http://%s%s", v, kp)
			// write
			if err := remotePut(remoteTo, int64(len(ss)), strings.NewReader(ss)); err != nil {
				fmt.Println("put error", err, remoteTo)
				return false
			}
		}
	}

	// update db
	if !a.PutRecord(req.key, Record{req.kvolumes, NO}) {
		fmt.Println("put db error", err)
		return false
	}

	// delete from the volumes that now aren't kvolumes
	deleteError := false
	for _, v2 := range rvolumes {
		needsDelete := true
		for _, v := range req.kvolumes {
			if v == v2 {
				needsDelete = false
				break
			}
		}
		if needsDelete {
			remoteDel := fmt.Sprintf("http://%s%s", v2, kp)
			if err := remoteDelete(remoteDel); err != nil {
				fmt.Println("delete error", err, remoteDel)
				deleteError = true
			}
		}
	}
	return deleteError
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
