package kvslib

import (
	"errors"
	"log"
	"math/rand"
	"net/rpc"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
)

// Actions to be recorded by kvslib (as part of ktrace, put trace, get trace):

type KvslibStart struct {
	ClientId string
}

type KvslibStop struct {
	ClientId string
}

type Put struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutResultRecvd struct {
	OpId uint32
	GId  uint64
	Key  string
}

type Get struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetResultRecvd struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
}

type HeadReq struct {
	ClientId string
}

type HeadResRecvd struct {
	ClientId string
	ServerId uint8
}

type TailReq struct {
	ClientId string
}

type TailResRecvd struct {
	ClientId string
	ServerId uint8
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint32
	GId    uint64
	Result string
}

type KVS struct {
	notifyCh    NotifyChannel
	coordClient *rpc.Client
	headClient  *rpc.Client
	tailClient  *rpc.Client
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh:    nil,
		coordClient: nil,
		headClient:  nil,
		tailClient:  nil,
	}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string, localCoordIPPort string, localHeadServerIPPort string, localTailServerIPPort string, chCapacity int) (NotifyChannel, error) {
	coordClient, err := util.GetRPCClient(localCoordIPPort, coordIPPort)
	if err != nil {
		log.Println("Error in connecting to coord:", err)
		return nil, err
	}
	var headServerIPPort string
	err = coordClient.Call("Coord.GetHead", chainedkv.NodeRequest{ClientId: clientId, Token: nil}, &headServerIPPort)
	if err != nil {
		log.Println("Error in getting head server:", err)
		return nil, err
	}
	headClient, err := util.GetRPCClient(localHeadServerIPPort, headServerIPPort)
	if err != nil {
		log.Println("Error in connecting to head server:", err)
		return nil, err
	}
	var tailServerIPPort string
	err = coordClient.Call("Coord.GetTail", chainedkv.NodeRequest{ClientId: clientId, Token: nil}, &tailServerIPPort)
	if err != nil {
		log.Println("Error in getting tail server:", err)
		return nil, err
	}
	tailClient, err := util.GetRPCClient(localTailServerIPPort, tailServerIPPort)
	if err != nil {
		log.Println("Error in connecting to tail server:", err)
		return nil, err
	}
	d.notifyCh = make(NotifyChannel, chCapacity)
	d.coordClient = coordClient
	d.headClient = headClient
	d.tailClient = tailClient
	return d.notifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	// Should return opId or error
	return 0, errors.New("not implemented")
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value opId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	log.Printf("Client %s calling put on head server", clientId)
	// Ignore the output
	err := d.headClient.Call(
		"Server.Put",
		// TODO determine client addr and reuse in receiveputresult
		chainedkv.PutArgs{Key: key, Value: value, ClientId: clientId, ClientAddr: "", GId: rand.Uint64(), Token: nil},
		&chainedkv.PutReply{},
	) // TODO client.Go()
	return 0, err
}

type PutResultArgs struct {
	OpId   uint32
	GId    uint64
	Result string
}

func (d *KVS) ReceivePutResult(args PutResultArgs, reply *bool) error {
	return errors.New("not implemented")
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	d.headClient.Close()
	d.tailClient.Close()
	d.coordClient.Close()
	newKVS := NewKVS()
	*d = *newKVS
}
