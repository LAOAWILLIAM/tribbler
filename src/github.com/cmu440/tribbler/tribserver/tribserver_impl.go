package tribserver

import (
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"

	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	// TODO: implement this!
	libstore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	ts := new(tribServer)
	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, err
	}
	ts.libstore = ls

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	if ts.ExistUser(args.UserID) {
		reply.Status = tribrpc.Exists
		return nil
	}

	err := ts.libstore.Put(util.FormatUserKey(args.UserID), "")
	if err != nil {
		reply.Status = tribrpc.Exists
	} else {
		reply.Status = tribrpc.OK
	}

	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	if !ts.ExistUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	if !ts.ExistUser(args.TargetUserID) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	err := ts.libstore.AppendToList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.Exists
	} else {
		reply.Status = tribrpc.OK
	}

	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	if !ts.ExistUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	if !ts.ExistUser(args.TargetUserID) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	err := ts.libstore.RemoveFromList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		reply.Status = tribrpc.OK
	}

	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	if !ts.ExistUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	subs, err := ts.libstore.GetList(util.FormatSubListKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.OK
		return nil
	}

	var _subs []string
	var friends []string
	for _, sub := range subs {
		_subs, err = ts.libstore.GetList(util.FormatSubListKey(sub))
		if err != nil {
			reply.Status = tribrpc.OK
			return nil
		}

		for _, _sub := range _subs {
			if _sub == args.UserID {
				friends = append(friends, sub)
			}
		}
	}

	reply.Status = tribrpc.OK
	reply.UserIDs = friends

	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	if !ts.ExistUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	postKey := util.FormatPostKey(args.UserID, time.Now().UnixNano())
	err := ts.libstore.Put(postKey, args.Contents)
	if err != nil {
		return err
	}

	err = ts.libstore.AppendToList(util.FormatTribListKey(args.UserID), postKey)
	if err != nil {
		reply.Status = tribrpc.Exists
	} else {
		reply.Status = tribrpc.OK
		reply.PostKey = postKey
	}

	return err
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	if !ts.ExistUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	err := ts.libstore.RemoveFromList(util.FormatTribListKey(args.UserID), args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	err = ts.libstore.Delete(args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return err
	} else {
		reply.Status = tribrpc.OK
	}

	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if !ts.ExistUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	postKeys, err := ts.libstore.GetList(util.FormatTribListKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.OK
		return nil
	}

	var tribbles []tribrpc.Tribble
	var postedTime int64
	var contents string
	var srvId string
	count := 0
	for i := len(postKeys)-1; i >= 0; i-- {
		_, err = fmt.Sscanf(postKeys[i], args.UserID+":post_%x_%s", &postedTime, &srvId)
		if err != nil {
			return err
		}
		contents, err = ts.libstore.Get(postKeys[i])
		tribbles = append(tribbles, tribrpc.Tribble{UserID: args.UserID, Posted: time.Unix(0, postedTime), Contents: contents})
		count++
		if count == 100 {
			break
		}
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles

	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if !ts.ExistUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	subs, err := ts.libstore.GetList(util.FormatSubListKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.OK
		return nil
	}

	var tribbles []tribrpc.Tribble
	for _, sub := range subs {
		postKeys, err := ts.libstore.GetList(util.FormatTribListKey(sub))
		if err != nil {
			continue
		}

		var postedTime int64
		var contents string
		var srvId string
		for i := len(postKeys)-1; i >= 0; i-- {
			_, err = fmt.Sscanf(postKeys[i], sub+":post_%x_%s", &postedTime, &srvId)
			if err != nil {
				return err
			}
			contents, err = ts.libstore.Get(postKeys[i])
			tribbles = append(tribbles, tribrpc.Tribble{UserID: sub, Posted: time.Unix(0, postedTime), Contents: contents})
		}
	}

	sort.Slice(tribbles, func(i, j int) bool {
		return tribbles[i].Posted.UnixNano() > tribbles[j].Posted.UnixNano()
	})

	if len(tribbles) > 100 {
		tribbles = tribbles[:100]
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles

	return nil
}

// helper functions

func (ts *tribServer) ExistUser(UserID string) bool {
	_, err := ts.libstore.Get(util.FormatUserKey(UserID))
	if err != nil {
		return false
	} else {
		return true
	}
}
