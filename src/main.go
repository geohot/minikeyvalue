package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
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
	uploadids       map[string]bool
	volumes         []string
	fallback        string
	replicas        int
	subvolumes      int
	protect         bool
	md5sum          bool
	voltimeout      time.Duration
	vdir_colocation bool
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
	rec := Record{[]string{}, HARD, "", false}
	if err != leveldb.ErrNotFound {
		rec = toRecord(data)
	}
	return rec
}

func (a *App) PutRecord(key []byte, rec Record) bool {
	return a.db.Put(key, fromRecord(rec), nil) == nil
}

func promptBasicAuth() string {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Volume server username: ")
	username, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Username prompt failed: %s\n", err)
		return ""
	}

	fmt.Print("Volume server password: ")
	password, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Password prompt failed: %s\n", err)
		return ""
	}

	username = strings.TrimSpace(username)
	password = strings.TrimSpace(password)
	if username == "" && password == "" {
		return ""
	}
	auth := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}

// *** Entry Point ***

func main() {
	http.DefaultTransport.(*http.Transport).MaxIdleConns = 2500
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
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
	vdir_colocation := flag.Bool("vdircolocation", false, "Enable virtual directory colocation, which places items with the same prefix before the last slash on the same volume server(s)")
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
		lock:            make(map[string]struct{}),
		uploadids:       make(map[string]bool),
		volumes:         volumes,
		fallback:        *fallback,
		replicas:        *replicas,
		subvolumes:      *subvolumes,
		protect:         *protect,
		md5sum:          *md5sum,
		voltimeout:      *voltimeout,
		vdir_colocation: *vdir_colocation,
	}

	if command == "server" {
		http.ListenAndServe(fmt.Sprintf(":%d", *port), &a)
	} else if command == "rebuild" {
		auth := promptBasicAuth()
		a.Rebuild(auth)
	} else if command == "rebalance" {
		auth := promptBasicAuth()
		a.Rebalance(auth)
	}
}
