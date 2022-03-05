package kvslib

import (
	"errors"
	"net"
	"net/rpc"
	"strconv"
	"sync"

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

// Local

type KVS struct {
	Cond     *sync.Cond
	NotifyCh NotifyChannel
	Clients  RPCClients
	Data     LocalData
}

type RPCClients struct {
	CoordClient *rpc.Client
	HeadClient  *rpc.Client
	TailClient  *rpc.Client
}

type ServerInfo struct {
	ServerId     uint8
	LocalPortIp  string
	RemotePortIp string
}

type Request struct {
	Key    string
	Value  *string
	Tracer *tracing.Tracer
	Trace  *tracing.Trace
}

type LocalData struct {
	Tracer          *tracing.Tracer
	Trace           *tracing.Trace
	PendingRequests map[uint32]Request
	ClientId        string
	ClientIPPort    string
	ChCapacity      int
	HeadServerInfo  ServerInfo
	TailServerInfo  ServerInfo
	CurrOpId        uint32
	ChCount         int
	Done            chan bool
	Listener        net.Listener
}

func NewKVS() *KVS {
	return &KVS{}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of
// operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an
// appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string,
	localCoordIPPort string, localHeadServerIPPort string,
	localTailServerIPPort string, chCapacity int) (NotifyChannel, error) {

	// Populate KVS
	d.Cond = sync.NewCond(&sync.Mutex{})
	d.NotifyCh = make(NotifyChannel, chCapacity)
	d.Data.Tracer = localTracer
	d.Data.Trace = localTracer.CreateTrace()
	d.Data.ClientId = clientId
	d.Data.ChCapacity = chCapacity
	d.Data.CurrOpId = 0
	d.Data.ChCount = 0
	d.Data.Done = make(chan bool, 1)
	d.Data.PendingRequests = make(map[uint32]Request)
	d.Data.HeadServerInfo.LocalPortIp = localHeadServerIPPort
	d.Data.TailServerInfo.LocalPortIp = localTailServerIPPort

	d.Data.Trace.RecordAction(KvslibStart{ClientId: clientId})

	// Setup local rpc
	err := rpc.Register(d)

	if err != nil {
		return nil, err
	}

	localTailServerIP, _, err := net.SplitHostPort(localTailServerIPPort)

	if err != nil {
		return nil, err
	}

	port, err := util.GetFreeTCPPort(localTailServerIP)

	if err != nil {
		return nil, err
	}

	d.Data.ClientIPPort = net.JoinHostPort(localTailServerIP, strconv.Itoa(port))
	tcpAddrClient, err := net.ResolveTCPAddr("tcp", d.Data.ClientIPPort)

	if err != nil {
		return nil, err
	}

	d.Data.Listener, err = net.ListenTCP("tcp", tcpAddrClient)

	if err != nil {
		return nil, err
	}

	go rpc.Accept(d.Data.Listener)

	// Connect to RPC clients
	d.Clients.CoordClient, err = util.GetRPCClient(localCoordIPPort, coordIPPort)

	if err != nil {
		return nil, err
	}

	opReady := make(chan bool)
	go d.handleOps(opReady)
	<-opReady

	return d.NotifyCh, nil
}

func (d *KVS) handleGet(opId uint32, request Request) {
	var err error
	var getReply interface{}

	trace := request.Tracer.CreateTrace()
	trace.RecordAction(Get{
		ClientId: d.Data.ClientId,
		OpId:     opId,
		Key:      request.Key,
	})

	getArgs := chainedkv.GetArgs{
		ClientId:   d.Data.ClientId,
		OpId:       opId,
		Key:        request.Key,
		ClientAddr: d.Data.ClientIPPort,
		Token:      trace.GenerateToken(),
	}

	for {
		d.Cond.L.Unlock()

		if d.Clients.TailClient != nil {
			// Invoke Get
			err = d.Clients.TailClient.Call(
				"Server.Get",
				getArgs,
				&getReply,
			)
		}

		d.Cond.L.Lock()

		if err != nil {
			d.getTail()

			// Check if the request successfully completed even
			// though the tail server failed to reply. If so, we
			// simply return since the command has completed
			if _, ok := d.Data.PendingRequests[opId]; !ok {
				return
			}
		} else {
			break
		}
	}
}

func (d *KVS) handlePut(opId uint32, request Request) {
	var err error
	var putReply interface{}

	trace := request.Tracer.CreateTrace()
	trace.RecordAction(Put{
		ClientId: d.Data.ClientId,
		OpId:     opId,
		Key:      request.Key,
		Value:    *request.Value,
	})

	putArgs := chainedkv.PutArgs{
		ClientId:   d.Data.ClientId,
		OpId:       opId,
		Key:        request.Key,
		Value:      *request.Value,
		ClientAddr: d.Data.ClientIPPort,
		Token:      trace.GenerateToken(),
	}

	for {
		d.Cond.L.Unlock()

		if d.Clients.HeadClient != nil {
			// Invoke Put
			err = d.Clients.HeadClient.Call(
				"Server.Put",
				putArgs,
				&putReply,
			)
		}

		d.Cond.L.Lock()

		if err != nil {
			d.getHead()

			// Check if the request successfully completed even
			// though the head server failed to reply. If so, we
			// simply return since the command has completed
			if _, ok := d.Data.PendingRequests[opId]; !ok {
				return
			}
		} else {
			break
		}
	}
}

func (d *KVS) handleOps(opReady chan<- bool) {
	nextOpId := uint32(0)

	d.getHead()
	d.getTail()

	d.Cond.L.Lock()
	defer d.Cond.L.Unlock()

	opReady <- true

	for {
		// Wait until expected next opId arrives
	InnerLoop:
		for {
			select {
			case <-d.Data.Done:
				return
			default:
				if _, ok := d.Data.PendingRequests[nextOpId]; !ok {
					d.Cond.Wait()
				} else {
					break InnerLoop
				}
			}
		}

		request := d.Data.PendingRequests[nextOpId]

		if request.Value == nil {
			d.handleGet(nextOpId, request)
		} else {
			d.handlePut(nextOpId, request)
		}

		// Wait until next opId finishes
		for {
			if _, ok := d.Data.PendingRequests[nextOpId]; ok {
				d.Cond.Wait()
			} else {
				break
			}
		}

		nextOpId++
	}
}

func (d *KVS) startOp(tracer *tracing.Tracer, clientId string, key string, value *string) (uint32, error) {
	d.Cond.L.Lock()
	defer d.Cond.L.Unlock()

	// Verify capacity hasn't been exceeded
	if d.Data.ChCount >= d.Data.ChCapacity {
		return 0, errors.New("concurrency capacity has been exceeded")
	}

	opId := d.Data.CurrOpId

	// Update state
	d.Data.CurrOpId++
	d.Data.ChCount++

	d.Data.PendingRequests[opId] = Request{
		Key:    key,
		Value:  value,
		Tracer: tracer,
	}

	d.Cond.Broadcast()

	return opId, nil
}

// Get non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil.
// Note that this call is non-blocking. The returned value must be delivered asynchronously
// to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	return d.startOp(tracer, clientId, key, nil)
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil.
// Note that this call is non-blocking. The value opId is used to identify this request
// and associate the returned value with this request. The returned value must be
// delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	return d.startOp(tracer, clientId, key, &value)
}

func (d *KVS) ReceiveGetResult(args chainedkv.ReplyArgs, reply *interface{}) error {
	d.Cond.L.Lock()
	defer d.Cond.L.Unlock()

	if _, ok := d.Data.PendingRequests[args.OpId]; !ok {
		return nil
	}

	trace := d.Data.PendingRequests[args.OpId].Tracer.ReceiveToken(args.Token)
	trace.RecordAction(GetResultRecvd{OpId: args.OpId, GId: args.GId, Key: args.Key, Value: args.Value})

	// Notify client
	d.NotifyCh <- ResultStruct{OpId: args.OpId, GId: args.GId, Result: args.Value}

	// Update state
	d.Data.ChCount--
	delete(d.Data.PendingRequests, args.OpId)

	d.Cond.Broadcast()

	return nil
}

func (d *KVS) ReceivePutResult(args chainedkv.ReplyArgs, reply *interface{}) error {
	d.Cond.L.Lock()
	defer d.Cond.L.Unlock()

	if _, ok := d.Data.PendingRequests[args.OpId]; !ok {
		return nil
	}

	trace := d.Data.PendingRequests[args.OpId].Tracer.ReceiveToken(args.Token)
	trace.RecordAction(PutResultRecvd{OpId: args.OpId, GId: args.GId, Key: args.Key})

	// Notify client
	d.NotifyCh <- ResultStruct{OpId: args.OpId, GId: args.GId, Result: args.Value}

	// Update state
	d.Data.ChCount--
	delete(d.Data.PendingRequests, args.OpId)

	d.Cond.Broadcast()

	return nil
}

func (d *KVS) getHead() {
	var servResp chainedkv.NodeResponse

	d.Data.Trace.RecordAction(HeadReq{ClientId: d.Data.ClientId})

	d.Clients.CoordClient.Call(
		"Coord.GetHead",
		chainedkv.NodeRequest{
			ClientId: d.Data.ClientId,
			Token:    d.Data.Trace.GenerateToken(),
		},
		&servResp,
	)

	d.Data.Trace = d.Data.Tracer.ReceiveToken(servResp.Token)
	d.Data.Trace.RecordAction(
		HeadResRecvd{
			ClientId: d.Data.ClientId,
			ServerId: servResp.ServerId,
		},
	)

	d.Data.HeadServerInfo.ServerId = servResp.ServerId
	d.Data.HeadServerInfo.RemotePortIp = servResp.ServerIpPort

	if d.Clients.HeadClient != nil {
		d.Clients.HeadClient.Close()
	}

	d.Clients.HeadClient, _ = util.GetRPCClient(
		d.Data.HeadServerInfo.LocalPortIp,
		servResp.ServerIpPort,
	)
}

func (d *KVS) getTail() {
	var servResp chainedkv.NodeResponse

	d.Data.Trace.RecordAction(TailReq{ClientId: d.Data.ClientId})

	d.Clients.CoordClient.Call(
		"Coord.GetTail",
		chainedkv.NodeRequest{
			ClientId: d.Data.ClientId,
			Token:    d.Data.Trace.GenerateToken(),
		},
		&servResp,
	)

	d.Data.Trace = d.Data.Tracer.ReceiveToken(servResp.Token)
	d.Data.Trace.RecordAction(
		TailResRecvd{
			ClientId: d.Data.ClientId,
			ServerId: servResp.ServerId,
		},
	)

	d.Data.TailServerInfo.ServerId = servResp.ServerId
	d.Data.TailServerInfo.RemotePortIp = servResp.ServerIpPort

	if d.Clients.TailClient != nil {
		d.Clients.TailClient.Close()
	}

	d.Clients.TailClient, _ = util.GetRPCClient(
		d.Data.TailServerInfo.LocalPortIp,
		servResp.ServerIpPort,
	)
}

// Stop Stops the KVS instance from communicating with the KVS and
// from delivering any results via the notify-channel. This call always succeeds.
func (d *KVS) Stop() {
	d.Cond.L.Lock()
	defer d.Cond.L.Unlock()

	d.Data.Trace.RecordAction(KvslibStop{ClientId: d.Data.ClientId})

	if d.Clients.HeadClient != nil {
		d.Clients.HeadClient.Close()
	}

	if d.Clients.TailClient != nil {
		d.Clients.TailClient.Close()
	}

	if d.Clients.CoordClient != nil {
		d.Clients.CoordClient.Close()
	}

	if d.Data.Listener != nil {
		d.Data.Listener.Close()
	}

	d.Data.Done <- true
	d.Cond.Broadcast()

	*d = *NewKVS()
}
