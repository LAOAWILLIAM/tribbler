// DO NOT MODIFY!

package main

import (
	crand "crypto/rand"
	"flag"
	"log"
	"math"
	"math/big"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/cmu440/tribbler/storageserver"
)

const defaultMasterPort = 9009

var (
	port           = flag.Int("port", defaultMasterPort, "port number to listen on")
	masterHostPort = flag.String("master", "", "master storage server host port (if non-empty then this storage server is a slave)")
	numNodes       = flag.Int("N", 1, "the number of nodes in the ring (including the master)")
	virtualIDs     = flag.String("vids", "", "a list of 32-bit unsigned virtual IDs to use for consistent hashing")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	flag.Parse()
	if *masterHostPort == "" && *port == 0 {
		// If masterHostPort string is empty, then this storage server is the master.
		*port = defaultMasterPort
	}

	// If virtualIDs is empty, then assign a random 32-bit integer instead.
	parsedIDs := []uint32{}
	if len(*virtualIDs) == 0 {
		randInt, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
		rand.Seed(randInt.Int64())
		randId := rand.Uint32()

		parsedIDs = append(parsedIDs, randId)
	} else {
		stringIDs := strings.Split(*virtualIDs, ",")
		for _, stringID := range stringIDs {
			parsedID, err := strconv.ParseUint(stringID, 10, 64)
			if err != nil {
				log.Fatalln("Failed to parse virtual IDs:", err)
			} else {
				parsedIDs = append(parsedIDs, uint32(parsedID))
			}
		}
	}

	// Create and start the StorageServer.
	ss, err := storageserver.NewStorageServer(*masterHostPort, *numNodes, *port, parsedIDs)
	if err != nil {
		log.Fatalln("Failed to create storage server:", err)
	}

	shutdownChan := make(chan os.Signal)
	signal.Notify(shutdownChan, syscall.SIGUSR1)
	go func() {
		for {
			<-shutdownChan
			ss.SetAlive(false)
		}
	}()

	restoreChan := make(chan os.Signal)
	signal.Notify(restoreChan, syscall.SIGUSR2)
	go func() {
		for {
			<-restoreChan
			ss.SetAlive(true)
		}
	}()

	// Run the storage server forever.
	select {}
}
