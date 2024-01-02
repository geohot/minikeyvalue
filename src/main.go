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
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Record struct {
	Data []byte
	Type string 
}

type App struct {
	db         *leveldb.DB
	mu         sync.Mutex
	lock       map[string]struct{}
	uploadIDs  map[string]bool
	volumes    []string
	fallback   string
	replicas   int
	subVolumes int
	protect    bool
	md5sum     bool
	volTimeout time.Duration
}

func NewApp(db *leveldb.DB, volumes []string, fallback string, replicas, subVolumes int, protect, md5sum bool, volTimeout time.Duration) *App {
	return &App{
		db:         db,
		lock:       make(map[string]struct{}),
		uploadIDs:  make(map[string]bool),
		volumes:    volumes,
		fallback:   fallback,
		replicas:   replicas,
		subVolumes: subVolumes,
		protect:    protect,
		md5sum:     md5sum,
		volTimeout: volTimeout,
	}
}

func (a *App) UnlockKey(key []byte) {
	a.mu.Lock()
	delete(a.lock, string(key))
	a.mu.Unlock()
}

func (a *App) LockKey(key []byte) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.lock[string(key)]; exists {
		return false
	}
	a.lock[string(key)] = struct{}{}
	return true
}

func (a *App) GetRecord(key []byte) (Record, error) {
	data, err := a.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return Record{}, nil
		}
		return Record{}, err
	}
	return Record{Data: data}, nil
}

func (a *App) PutRecord(key []byte, rec Record) error {
	return a.db.Put(key, rec.Data, nil)
}

func main() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
	rand.Seed(time.Now().Unix())

	port := flag.Int("port", 3000, "Port for the server to listen on")
	dbPath := flag.String("db", "", "Path to leveldb")
	fallback := flag.String("fallback", "", "Fallback server for missing keys")
	replicas := flag.Int("replicas", 3, "Amount of replicas to make of the data")
	subVolumes := flag.Int("subvolumes", 10, "Amount of subvolumes, disks per machine")
	volumesStr := flag.String("volumes", "", "Volumes to use for storage, comma separated")
	protect := flag.Bool("protect", false, "Force UNLINK before DELETE")
	verbose := flag.Bool("v", false, "Verbose output")
	md5sum := flag.Bool("md5sum", true, "Calculate and store MD5 checksum of values")
	volTimeout := flag.Duration("voltimeout", 1*time.Second, "Volume servers must respond to GET/HEAD requests in this amount of time or they are considered down, as duration")
	flag.Parse()

	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	if *dbPath == "" {
		log.Fatal("Database path is required")
	}

	volumes := strings.Split(*volumesStr, ",")
	if len(volumes) < *replicas {
		log.Fatal("The number of volumes must be at least equal to the number of replicas")
	}

	db, err := leveldb.OpenFile(*dbPath, nil)
	if err != nil {
		log.Fatalf("Failed to open LevelDB: %s", err)
	}
	defer db.Close()

	app := NewApp(db, volumes, *fallback, *replicas, *subVolumes, *protect, *md5sum, *volTimeout)

	command := flag.Arg(0)
	switch command {
	case "server":
		log.Printf("Starting server on port %d\n", *port)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), app))
	case "rebuild":
	case "rebalance":
	default:
		fmt.Println("Usage: ./app [server|rebuild|rebalance]")
		flag.PrintDefaults()
	}
}

