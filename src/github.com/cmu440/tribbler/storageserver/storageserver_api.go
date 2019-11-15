// DO NOT MODIFY!

package storageserver

import (
	"errors"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

// StorageServer defines the set of methods that can be invoked remotely via RPCs.
type StorageServer interface {
	// USED FOR TESTS, DO NOT MODIFY
	SetAlive(alive bool)

	// RegisterServer adds a storage server to the ring. It replies with
	// status NotReady if not all nodes in the ring have joined. Once
	// all nodes have joined, it should reply with status OK and a list
	// of all connected nodes in the ring.
	RegisterServer(*storagerpc.RegisterArgs, *storagerpc.RegisterReply) error

	// GetServers retrieves a list of all connected nodes in the ring. It
	// replies with status NotReady if not all nodes in the ring have joined.
	GetServers(*storagerpc.GetServersArgs, *storagerpc.GetServersReply) error

	// Get retrieves the specified key from the data store and replies with
	// the key's value and a lease if one was requested. If the key is not
	// found, it should reply with status KeyNotFound.
	Get(*storagerpc.GetArgs, *storagerpc.GetReply) error

	// Delete remove the specified key from the data store.
	// If the key is not found, it should reply with status KeyNotFound.
	Delete(*storagerpc.DeleteArgs, *storagerpc.DeleteReply) error

	// GetList retrieves the specified key from the data store and replies with
	// the key's list value and a lease if one was requested. If the key is not
	// found, it should reply with status KeyNotFound.
	GetList(*storagerpc.GetArgs, *storagerpc.GetListReply) error

	// Put inserts the specified key/value pair into the data store.
	Put(*storagerpc.PutArgs, *storagerpc.PutReply) error

	// AppendToList retrieves the specified key from the data store and appends
	// the specified value to its list. If the specified value is already
	// contained in the list, it should reply with status ItemExists.
	AppendToList(*storagerpc.PutArgs, *storagerpc.PutReply) error

	// RemoveFromList retrieves the specified key from the data store and removes
	// the specified value from its list. If the specified value is not already
	// contained in the list, it should reply with status ItemNotFound.
	RemoveFromList(*storagerpc.PutArgs, *storagerpc.PutReply) error
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if !ss.checkAlive() {
		return errors.New("Server Crashed")
	}
	return ss.registerServer(args, reply)
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if !ss.checkAlive() {
		return errors.New("Server Crashed")
	}
	return ss.getServers(args, reply)
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.checkAlive() {
		return errors.New("Server Crashed")
	}
	return ss.get(args, reply)
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.checkAlive() {
		return errors.New("Server Crashed")
	}
	return ss.delete(args, reply)
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.checkAlive() {
		return errors.New("Server Crashed")
	}
	return ss.getList(args, reply)
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkAlive() {
		return errors.New("Server Crashed")
	}
	return ss.put(args, reply)
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkAlive() {
		return errors.New("Server Crashed")
	}
	return ss.appendToList(args, reply)
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkAlive() {
		return errors.New("Server Crashed")
	}
	return ss.removeFromList(args, reply)
}

func (ss *storageServer) checkAlive() bool {
	ss.mux.Lock()
	defer ss.mux.Unlock()
	return ss.isAlive
}
