package storageserver

import (
	"errors"
	"sync"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	isAlive bool       // DO NOT MODIFY
	mux     sync.Mutex // DO NOT MODIFY

	// TODO: implement this!
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

	return nil, errors.New("not implemented")
}

func (ss *storageServer) registerServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) getServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) getList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) appendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) removeFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}
