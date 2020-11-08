package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	// TODO: implement this!
	mux            		sync.Mutex
	myHostPort     		string
	leaseMode      		LeaseMode
	storageServers 		[]storagerpc.Node
	clients        		map[string]*rpc.Client
	vidList        		[]NodeOneVid
	localGetCache  		map[string]GetCacheItem
	localGetCacheMux	sync.Mutex
	localListCache map[string]ListCacheItem
	localListCacheMux	sync.Mutex
	queryRequests  map[string]int
}

type NodeOneVid struct {
	HostPort	string
	VirtualID	uint32
}

//type GetRequest struct {
//	requestTime	time.Time
//	key			string
//}

type GetCacheItem struct {
	key			string
	value		string
	leaseEnd	time.Time
}

type ListCacheItem struct {
	key			string
	value		[]string
	leaseEnd	time.Time
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	ls := new(libstore)
	err := rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
	if err != nil {
		return nil, err
	}
	ls.myHostPort = myHostPort
	ls.leaseMode = mode
	ls.localGetCache = make(map[string]GetCacheItem)
	ls.localListCache = make(map[string]ListCacheItem)
	if mode == Normal {
		ls.queryRequests = make(map[string]int)
	}

	lsClient, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	// retry up to 5 times
	for i := 0; i < 5; i++ {
		args := &storagerpc.GetServersArgs{}
		var reply storagerpc.GetServersReply

		err = lsClient.Call("StorageServer.GetServers", args, &reply)
		if err != nil {
			return nil, err
		}

		if reply.Status == storagerpc.OK {
			ls.clients = make(map[string]*rpc.Client)
			var vidList []NodeOneVid
			ls.storageServers = reply.Servers
			for _, server := range reply.Servers {
				var c *rpc.Client
				c, err = rpc.DialHTTP("tcp", server.HostPort)
				if err != nil {
					return nil, err
				}
				ls.clients[server.HostPort] = c
				for _, vid := range server.VirtualIDs {
					vidList = append(vidList, NodeOneVid{server.HostPort, vid})
				}
			}

			sort.Slice(vidList, func(i, j int) bool {
				return vidList[i].VirtualID < vidList[j].VirtualID
			})

			ls.vidList = vidList

			break
		}

		// sleep for one second before sending another GetServers request
		time.Sleep(1 * time.Second)
	}

	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	if ls.leaseMode != Never {
		t := time.Now()
		ls.localGetCacheMux.Lock()
		value, prs := ls.localGetCache[key]

		if prs {
			if value.leaseEnd.Sub(t) >= 0 {
				ls.localGetCacheMux.Unlock()
				return value.value, nil
			} else {
				delete(ls.localGetCache, key)
			}
		}
		ls.localGetCacheMux.Unlock()
	}

	hash := StoreHash(key)

	client, err := ls.getHandleStorageServer(hash)
	if err != nil {
		return "", err
	}

	cache := false
	var args *storagerpc.GetArgs
	var reply storagerpc.GetReply

	if ls.leaseMode == Always {
		args = &storagerpc.GetArgs{Key: key, WantLease: true, HostPort: ls.myHostPort}
		cache = true
	} else if ls.leaseMode == Never {
		args = &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	} else if ls.leaseMode == Normal {
		ls.queryRequests[key]++
		queryCacheThresh := ls.queryRequests[key]
		if queryCacheThresh >= storagerpc.QueryCacheThresh {
			args = &storagerpc.GetArgs{Key: key, WantLease: true, HostPort: ls.myHostPort}
			cache = true
		} else {
			args = &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
		}
	}

	err = client.Call("StorageServer.Get", args, &reply)
	if err != nil {
		return "", err
	}

	if reply.Status != storagerpc.OK {
		return "", errors.New(fmt.Sprintf("%v", reply.Status))
	}

	if cache {
		ls.localGetCacheMux.Lock()
		ls.localGetCache[key] = GetCacheItem{
			key: key,
			value: reply.Value,
			leaseEnd: time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds)),
		}
		ls.localGetCacheMux.Unlock()

		go func() {
			time.Sleep(time.Second * time.Duration(reply.Lease.ValidSeconds))

			t := time.Now()
			ls.localGetCacheMux.Lock()
			value, prs := ls.localGetCache[key]

			if prs && value.leaseEnd.Sub(t) < 0 {
				delete(ls.localGetCache, key)
			}
			ls.localGetCacheMux.Unlock()
		}()
	}

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	hash := StoreHash(key)

	client, err := ls.getHandleStorageServer(hash)
	if err != nil {
		return err
	}

	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply

	err = client.Call("StorageServer.Put", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("%v", reply.Status))
	}

	return nil
}

func (ls *libstore) Delete(key string) error {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	hash := StoreHash(key)

	client, err := ls.getHandleStorageServer(hash)
	if err != nil {
		return err
	}

	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply

	err = client.Call("StorageServer.Delete", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("%v", reply.Status))
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	if ls.leaseMode != Never {
		t := time.Now()
		ls.localListCacheMux.Lock()
		value, prs := ls.localListCache[key]

		if prs {
			if value.leaseEnd.Sub(t) >= 0 {
				ls.localListCacheMux.Unlock()
				return value.value, nil
			} else {
				delete(ls.localListCache, key)
			}
		}
		ls.localListCacheMux.Unlock()
	}

	hash := StoreHash(key)

	client, err := ls.getHandleStorageServer(hash)
	if err != nil {
		return nil, err
	}

	cache := false
	var args *storagerpc.GetArgs
	var reply storagerpc.GetListReply

	if ls.leaseMode == Always {
		args = &storagerpc.GetArgs{Key: key, WantLease: true, HostPort: ls.myHostPort}
		cache = true
	} else if ls.leaseMode == Never {
		args = &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	} else if ls.leaseMode == Normal {
		ls.queryRequests[key]++
		queryCacheThresh := ls.queryRequests[key]
		if queryCacheThresh >= storagerpc.QueryCacheThresh {
			args = &storagerpc.GetArgs{Key: key, WantLease: true, HostPort: ls.myHostPort}
			cache = true
		} else {
			args = &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
		}
	}

	err = client.Call("StorageServer.GetList", args, &reply)
	if err != nil {
		return nil, err
	}

	if reply.Status != storagerpc.OK {
		return nil, errors.New(fmt.Sprintf("%v", reply.Status))
	}

	if cache {
		ls.localListCacheMux.Lock()
		ls.localListCache[key] = ListCacheItem{
			key: key,
			value: reply.Value,
			leaseEnd: time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds)),
		}
		ls.localListCacheMux.Unlock()

		go func() {
			time.Sleep(time.Second * time.Duration(reply.Lease.ValidSeconds))

			t := time.Now()
			ls.localListCacheMux.Lock()
			value, prs := ls.localListCache[key]

			if prs && value.leaseEnd.Sub(t) < 0 {
				delete(ls.localListCache, key)
			}
			ls.localListCacheMux.Unlock()
		}()
	}

	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	hash := StoreHash(key)

	client, err := ls.getHandleStorageServer(hash)
	if err != nil {
		return err
	}

	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply

	err = client.Call("StorageServer.RemoveFromList", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("%v", reply.Status))
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	hash := StoreHash(key)

	client, err := ls.getHandleStorageServer(hash)
	if err != nil {
		return err
	}

	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply

	err = client.Call("StorageServer.AppendToList", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("%v", reply.Status))
	}

	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	ls.localGetCacheMux.Lock()
	_, prs1 := ls.localGetCache[args.Key]

	if prs1 {
		delete(ls.localGetCache, args.Key)
		reply.Status = storagerpc.OK
		ls.localGetCacheMux.Unlock()
		return nil
	}
	ls.localGetCacheMux.Unlock()

	ls.localListCacheMux.Lock()
	_, prs2 := ls.localListCache[args.Key]

	if prs2 {
		delete(ls.localListCache, args.Key)
		reply.Status = storagerpc.OK
		ls.localListCacheMux.Unlock()
		return nil
	}
	ls.localListCacheMux.Unlock()

	reply.Status = storagerpc.KeyNotFound

	return nil
}

// helper functions

func (ls *libstore) getHandleStorageServer(hash uint32) (*rpc.Client, error) {
	if hash > ls.vidList[len(ls.vidList)-1].VirtualID {
		for i, nodeOneVid := range ls.vidList {
			err := ls.aliveTest(i)
			if err == nil {
				return ls.clients[nodeOneVid.HostPort], nil
			}
		}
	}

	vidListLen := len(ls.vidList)
	loop := false
	for i := 0; i < vidListLen; i++ {
		if !loop && hash <= ls.vidList[i].VirtualID {
			err := ls.aliveTest(i)
			if err == nil {
				return ls.clients[ls.vidList[i].HostPort], nil
			} else {
				if i == vidListLen - 1 {
					i = 0
					loop = true
				}
			}
		}
		if loop {
			err := ls.aliveTest(i)
			if err == nil {
				return ls.clients[ls.vidList[i].HostPort], nil
			}
		}
	}

	return nil, errors.New("no client available")
}

func (ls *libstore) aliveTest(i int) error {
	args := &storagerpc.GetArgs{Key: ""}
	var reply storagerpc.GetReply

	return ls.clients[ls.vidList[i].HostPort].Call("StorageServer.Get", args, &reply)
}
