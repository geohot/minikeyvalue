package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

// *** App struct and methods ***

type App struct {
	db    *leveldb.DB
	mlock sync.Mutex
	lock  map[string]struct{}

	// params
	uploadids  map[string]bool
	volumes    []string
	fallback   string
	replicas   int
	subvolumes int
	protect    bool
	md5sum     bool
	voltimeout time.Duration
}

func (a *App) UnlockKey(key []byte) {
	a.mlock.Lock()
	delete(a.lock, string(key))
	a.mlock.Unlock()
}

func (a *App) LockKey(key []byte) bool {
	a.mlock.Lock()
	defer a.mlock.Unlock()
	if _, prs := a.lock[string(key)]; prs {
		return false
	}
	a.lock[string(key)] = struct{}{}
	return true
}

func (a *App) GetRecord(key []byte) Record {
	data, err := a.db.Get(key, nil)
	rec := Record{[]string{}, HARD, ""}
	if err != leveldb.ErrNotFound {
		rec = toRecord(data)
	}
	return rec
}

func (a *App) PutRecord(key []byte, rec Record) bool {
	return a.db.Put(key, fromRecord(rec), nil) == nil
}

// *** Entry Point ***

func main() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
	rand.Seed(time.Now().Unix())

	port := flag.Int("port", 3000, "Port for the server to listen on")
	pdb := flag.String("db", "", "Path to leveldb")
	fallback := flag.String("fallback", "", "Fallback server for missing keys")
	replicas := flag.Int("replicas", 3, "Amount of replicas to make of the data")
	subvolumes := flag.Int("subvolumes", 10, "Amount of subvolumes, disks per machine")
	pvolumes := flag.String("volumes", "", "Volumes to use for storage, comma separated")
	protect := flag.Bool("protect", false, "Force UNLINK before DELETE")
	verbose := flag.Bool("v", false, "Verbose output")
	md5sum := flag.Bool("md5sum", true, "Calculate and store MD5 checksum of values")
	voltimeout := flag.Duration("voltimeout", 1*time.Second, "Volume servers must respond to GET/HEAD requests in this amount of time or they are considered down, as duration")
	flag.Parse()

	volumes := strings.Split(*pvolumes, ",")
	command := flag.Arg(0)

	if command != "server" && command != "rebuild" && command != "rebalance" {
		fmt.Println("Usage: ./mkv <server, rebuild, rebalance>")
		flag.PrintDefaults()
		return
	}

	if !*verbose {
		log.SetOutput(ioutil.Discard)
	} else {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}

	if *pdb == "" {
		panic("Need a path to the database")
	}

	if len(volumes) < *replicas {
		panic("Need at least as many volumes as replicas")
	}

	db, err := leveldb.OpenFile(*pdb, nil)
	if err != nil {
		panic(fmt.Sprintf("LevelDB open failed: %s", err))
	}
	defer db.Close()

	fmt.Printf("volume servers: %s\n", volumes)
	a := App{db: db,
		lock:       make(map[string]struct{}),
		uploadids:  make(map[string]bool),
		volumes:    volumes,
		fallback:   *fallback,
		replicas:   *replicas,
		subvolumes: *subvolumes,
		protect:    *protect,
		md5sum:     *md5sum,
		voltimeout: *voltimeout,
	}

	if command == "server" {
		http.ListenAndServe(fmt.Sprintf(":%d", *port), &a)
	} else if command == "rebuild" {
		a.Rebuild()
	} else if command == "rebalance" {
		a.Rebalance()
	}
}
