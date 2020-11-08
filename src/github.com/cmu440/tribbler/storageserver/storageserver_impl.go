package storageserver

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	isAlive 	bool       // DO NOT MODIFY
	mux     	sync.Mutex // DO NOT MODIFY

	// TODO: implement this!
	numNodes	int
	node    	storagerpc.Node
	servers 	Servers
	db			map[string]string
	dbMux		sync.Mutex
	holders		map[string][]LeaseItem
	holdersMux	sync.Mutex
	revoking	map[string]bool
	revokingMux	sync.Mutex
}

type Servers []storagerpc.Node

type LeaseItem struct {
	hostPort	string
	leaseEnd	time.Time
}

// USED FOR TESTS, DO NOT MODIFY
func (ss *storageServer) SetAlive(alive bool) {
	ss.mux.Lock()
	ss.isAlive = alive
	ss.mux.Unlock()
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// virtualIDs is a list of random, unsigned 32-bits IDs identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, virtualIDs []uint32) (StorageServer, error) {
	/****************************** DO NOT MODIFY! ******************************/
	ss := new(storageServer)
	ss.isAlive = true
	/****************************************************************************/

	// TODO: implement this!
	host := "localhost"
	ss.node = storagerpc.Node{HostPort: fmt.Sprintf("%s:%d", host, port), VirtualIDs: virtualIDs}
	ss.numNodes = numNodes
	ss.db = make(map[string]string)
	ss.holders = make(map[string][]LeaseItem)
	ss.revoking	= make(map[string]bool)

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	// Wrap the StorageServer before registering it for RPC.
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	if masterServerHostPort == "" {
		ss.servers = append(ss.servers, ss.node)
		if numNodes > 1 {
			for {
				ss.mux.Lock()
				if len(ss.servers) == ss.numNodes {
					ss.mux.Unlock()
					break
				}
				ss.mux.Unlock()
				time.Sleep(1 * time.Second)
			}
		}
	} else {
		slaveClient, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}

		args := &storagerpc.RegisterArgs{ServerInfo: ss.node}
		var reply storagerpc.RegisterReply

		for {
			err = slaveClient.Call("StorageServer.RegisterServer", args, &reply)
			if err != nil {
				return nil, err
			}

			if reply.Status == storagerpc.OK {
				ss.servers = reply.Servers
				break
			}

			// sleep for one second before sending another RegisterServer request
			time.Sleep(1 * time.Second)
		}
	}

	return ss, nil
}

func (ss *storageServer) registerServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.mux.Lock()
	defer ss.mux.Unlock()

	if len(ss.servers) == ss.numNodes {
		reply.Servers = ss.servers
		reply.Status = storagerpc.OK
		return nil
	}

	for _, server := range ss.servers {
		if server.HostPort == args.ServerInfo.HostPort {
			return nil
		}
	}

	ss.servers = append(ss.servers, args.ServerInfo)

	if len(ss.servers) == ss.numNodes {
		reply.Servers = ss.servers
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) getServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.mux.Lock()
	defer ss.mux.Unlock()

	if len(ss.servers) == ss.numNodes {
		reply.Servers = ss.servers
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	ss.mux.Lock()
	defer ss.mux.Unlock()

	ss.dbMux.Lock()
	value, prs := ss.db[args.Key]
	ss.dbMux.Unlock()

	if prs {
		reply.Status = storagerpc.OK
		reply.Value = value
		if args.WantLease {
			ss.revokingMux.Lock()
			v, prs := ss.revoking[args.Key]
			ss.revokingMux.Unlock()
			if prs && v {
				reply.Lease = storagerpc.Lease{Granted: false, ValidSeconds: 0}
			} else {
				reply.Lease = storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
				ss.holdersMux.Lock()
				ss.holders[args.Key] = append(ss.holders[args.Key],
					LeaseItem{
					args.HostPort,
					time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds + storagerpc.LeaseGuardSeconds)),
					},
				)
				ss.holdersMux.Unlock()
			}
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	ss.dbMux.Lock()
	_, prs := ss.db[args.Key]

	if prs {
		delete(ss.db, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ss.dbMux.Unlock()

	if reply.Status == storagerpc.OK {
		ss.revokeLease(args.Key)
	}

	return nil
}

func (ss *storageServer) getList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	ss.mux.Lock()
	defer ss.mux.Unlock()

	ss.dbMux.Lock()
	value, prs := ss.db[args.Key]
	ss.dbMux.Unlock()

	if prs {
		var list []string
		err := json.Unmarshal([]byte(value), &list)
		if err != nil {
			return err
		}
		reply.Value = list
		reply.Status = storagerpc.OK
		if args.WantLease {
			ss.revokingMux.Lock()
			v, prs := ss.revoking[args.Key]
			ss.revokingMux.Unlock()
			if prs && v {
				reply.Lease = storagerpc.Lease{Granted: false, ValidSeconds: 0}
			} else {
				reply.Lease = storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
				ss.holdersMux.Lock()
				ss.holders[args.Key] = append(ss.holders[args.Key],
					LeaseItem{
						args.HostPort,
						time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds + storagerpc.LeaseGuardSeconds)),
					},
				)
				ss.holdersMux.Unlock()
			}
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.revokeLease(args.Key)

	ss.dbMux.Lock()
	ss.db[args.Key] = args.Value
	ss.dbMux.Unlock()
	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) appendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.revokeLease(args.Key)

	ss.dbMux.Lock()
	defer ss.dbMux.Unlock()

	value, prs := ss.db[args.Key]
	//ss.dbMux.Unlock()
	var list []string

	if prs {
		err := json.Unmarshal([]byte(value), &list)
		if err != nil {
			return err
		}

		for _, item := range list {
			if item == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
	}

	list = append(list, args.Value)

	listStr, err := json.Marshal(list)
	if err != nil {
		return err
	}

	//ss.dbMux.Lock()
	ss.db[args.Key] = string(listStr)
	//ss.dbMux.Unlock()

	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) removeFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.revokeLease(args.Key)

	ss.dbMux.Lock()
	defer ss.dbMux.Unlock()

	value, prs := ss.db[args.Key]
	var list []string

	if prs {
		err := json.Unmarshal([]byte(value), &list)
		if err != nil {
			return err
		}

		for index, item := range list {
			if item == args.Value {
				list = append(list[:index], list[index+1:]...)

				listStr, err := json.Marshal(list)
				if err != nil {
					return err
				}

				ss.db[args.Key] = string(listStr)
				//ss.dbMux.Unlock()

				reply.Status = storagerpc.OK
				return nil
			}
		}
	}
	//ss.dbMux.Unlock()

	reply.Status = storagerpc.ItemNotFound

	return nil
}

func (ss *storageServer) revokeLease(key string) {
	ss.holdersMux.Lock()
	holders := ss.holders[key]
	ss.holdersMux.Unlock()

	count := len(holders)
	if count > 0 {
		ss.revokingMux.Lock()
		ss.revoking[key] = true
		ss.revokingMux.Unlock()
		countChan := make(chan bool, count)
		for _, holder := range holders {
			holder := holder
			go func() {
				duration := holder.leaseEnd.Sub(time.Now())
				if duration > 0 {
					t := time.NewTimer(duration)
					revokeChan := make(chan bool)
					go func() {
						holderClient, err := rpc.DialHTTP("tcp", holder.hostPort)
						if err != nil {
							return
						}
						args := &storagerpc.RevokeLeaseArgs{Key: key}
						var reply storagerpc.RevokeLeaseReply

						err = holderClient.Call("LeaseCallbacks.RevokeLease", args, &reply)
						if err != nil {
							return
						}

						if reply.Status == storagerpc.OK {
							revokeChan <- true
						}
					}()

					select {
					case <- revokeChan:
						countChan <- true
						break
					case <- t.C:
						countChan <- true
						break
					}
				} else {
					countChan <- true
				}
			}()
		}

		for i := 0; i < count; i++ {
			<- countChan
		}

		ss.holdersMux.Lock()
		delete(ss.holders, key)
		ss.holdersMux.Unlock()

		ss.revokingMux.Lock()
		ss.revoking[key] = false
		ss.revokingMux.Unlock()
	}
}
