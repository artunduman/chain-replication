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
	Mutex      *sync.Mutex
	NotifyCh   NotifyChannel
	RpcClients RPCClients
	Data       LocalData
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
	Value  string
	Tracer *tracing.Tracer
	Trace  *tracing.Trace
}

type LocalData struct {
	Tracer             *tracing.Tracer
	Trace              *tracing.Trace
	PendingGetRequests map[uint32]Request
	PendingPutRequests map[uint32]Request
	ClientId           string
	ClientIPPort       string
	LocalCoordIPPort   string
	ChCapacity         int
	HeadServerInfo     ServerInfo
	TailServerInfo     ServerInfo
	CurrOpId           uint32
	ChCount            int
	DoneCh             chan *rpc.Call
	TerminateDoneCh    chan bool
	Listener           net.Listener
}

func NewKVS() *KVS {
	return &KVS{
		Mutex:      &sync.Mutex{},
		NotifyCh:   nil,
		RpcClients: RPCClients{},
		Data:       LocalData{},
	}
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

	var servResp chainedkv.NodeResponse

	// Reserve critical section
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	// Populate KVS
	d.NotifyCh = make(NotifyChannel, chCapacity)
	d.Data.Tracer = localTracer
	d.Data.Trace = localTracer.CreateTrace()
	d.Data.ClientId = clientId
	d.Data.LocalCoordIPPort = localCoordIPPort
	d.Data.ChCapacity = chCapacity
	d.Data.CurrOpId = 0
	d.Data.ChCount = 0
	d.Data.DoneCh = make(chan *rpc.Call, chCapacity)
	d.Data.TerminateDoneCh = make(chan bool)
	d.Data.PendingGetRequests = make(map[uint32]Request)
	d.Data.PendingPutRequests = make(map[uint32]Request)

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
	d.RpcClients.CoordClient, err = util.GetRPCClient(localCoordIPPort, coordIPPort)

	if err != nil {
		return nil, err
	}

	var test interface{}
	err = d.RpcClients.CoordClient.Call(
		"Coord.ClientJoin",
		chainedkv.ClientRequest{ClientId: clientId, ClientIpPort: d.Data.ClientIPPort},
		&test,
	)

	if err != nil {
		return nil, err
	}

	err = d.getHead(&servResp)

	if err != nil {
		return nil, err
	}

	d.Data.HeadServerInfo.ServerId = servResp.ServerId
	d.Data.HeadServerInfo.LocalPortIp = localHeadServerIPPort
	d.Data.HeadServerInfo.RemotePortIp = servResp.ServerIpPort

	d.RpcClients.HeadClient, err = util.GetRPCClient(localHeadServerIPPort, servResp.ServerIpPort)

	if err != nil {
		return nil, err
	}

	err = d.getTail(&servResp)

	if err != nil {
		return nil, err
	}

	d.Data.TailServerInfo.ServerId = servResp.ServerId
	d.Data.TailServerInfo.LocalPortIp = localTailServerIPPort
	d.Data.TailServerInfo.RemotePortIp = servResp.ServerIpPort

	d.RpcClients.TailClient, err = util.GetRPCClient(localTailServerIPPort, servResp.ServerIpPort)

	if err != nil {
		return nil, err
	}

	go d.launchDoneChannel()
	return d.NotifyCh, nil
}

// Get non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil.
// Note that this call is non-blocking. The returned value must be delivered asynchronously
// to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	var getReply interface{}

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	// Verify capacity hasn't been exceeded
	if d.Data.ChCount >= d.Data.ChCapacity {
		return 0, errors.New("concurrency capacity has been exceeded")
	}

	trace := tracer.CreateTrace()
	opId := d.Data.CurrOpId

	trace.RecordAction(Get{ClientId: clientId, OpId: opId, Key: key})

	getArgs := chainedkv.GetArgs{
		ClientId:   clientId,
		OpId:       opId,
		Key:        key,
		ClientAddr: d.Data.ClientIPPort,
		Token:      trace.GenerateToken(),
	}

	// Invoke Get
	d.RpcClients.TailClient.Go(
		"Server.Get",
		getArgs,
		&getReply,
		d.Data.DoneCh,
	)

	// Update state
	d.Data.CurrOpId++
	d.Data.ChCount++

	d.Data.PendingGetRequests[opId] = Request{
		Key:    key,
		Tracer: tracer,
		Trace:  trace,
	}

	return opId, nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil.
// Note that this call is non-blocking. The value opId is used to identify this request
// and associate the returned value with this request. The returned value must be
// delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	var putReply interface{}

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	// Verify capacity hasn't been exceeded
	if d.Data.ChCount >= d.Data.ChCapacity {
		return 0, errors.New("concurrency capacity has been exceeded")
	}

	trace := tracer.CreateTrace()
	opId := d.Data.CurrOpId

	trace.RecordAction(Put{ClientId: clientId, OpId: opId, Key: key, Value: value})

	putArgs := chainedkv.PutArgs{
		ClientId:   clientId,
		OpId:       opId,
		Key:        key,
		Value:      value,
		ClientAddr: d.Data.ClientIPPort,
		Token:      trace.GenerateToken(),
	}

	// Invoke Put
	d.RpcClients.HeadClient.Go(
		"Server.Put",
		putArgs,
		&putReply,
		d.Data.DoneCh,
	)

	// Update state
	d.Data.CurrOpId++
	d.Data.ChCount++

	d.Data.PendingPutRequests[opId] = Request{
		Key:    key,
		Value:  value,
		Tracer: tracer,
		Trace:  trace,
	}

	return opId, nil
}

func (d *KVS) ReceiveGetResult(args chainedkv.ReplyArgs, reply *interface{}) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	if _, ok := d.Data.PendingGetRequests[args.OpId]; !ok {
		return nil
	}

	trace := d.Data.PendingGetRequests[args.OpId].Tracer.ReceiveToken(args.Token)
	trace.RecordAction(GetResultRecvd{OpId: args.OpId, GId: args.GId, Key: args.Key, Value: args.Value})

	// Notify client
	d.NotifyCh <- ResultStruct{OpId: args.OpId, GId: args.GId, Result: args.Value}

	// Update state
	d.Data.ChCount--
	delete(d.Data.PendingGetRequests, args.OpId)

	return nil
}

func (d *KVS) ReceivePutResult(args chainedkv.ReplyArgs, reply *interface{}) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	if _, ok := d.Data.PendingPutRequests[args.OpId]; !ok {
		return nil
	}

	trace := d.Data.PendingPutRequests[args.OpId].Tracer.ReceiveToken(args.Token)
	trace.RecordAction(PutResultRecvd{OpId: args.OpId, GId: args.GId, Key: args.Key})

	// Notify client
	d.NotifyCh <- ResultStruct{OpId: args.OpId, GId: args.GId, Result: args.Value}

	// Update state
	d.Data.ChCount--
	delete(d.Data.PendingPutRequests, args.OpId)

	return nil
}

func (d *KVS) launchDoneChannel() {
	var call *rpc.Call

Loop:
	for {
		select {
		case <-d.Data.TerminateDoneCh:
			break Loop
		case call = <-d.Data.DoneCh:
			// Ignore successful requests
			if call.Error == nil {
				continue Loop
			}

			if call.ServiceMethod == "Server.Get" {
				d.resendGetRequest(call)
			} else {
				d.resendPutRequest(call)
			}
		}
	}
}

func (d *KVS) resendGetRequest(call *rpc.Call) {
	var tailServResp chainedkv.NodeResponse
	var getReply interface{}

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	d.getTail(&tailServResp)

	if tailServResp.ServerId == d.Data.TailServerInfo.ServerId {
		// Tail hasn't changed, resend data through existing RPC client
		d.RpcClients.TailClient.Go(
			"Server.Get",
			call.Args,
			&getReply,
			d.Data.DoneCh,
		)
	} else {
		// Update tail server info and reinitialize client
		d.Data.TailServerInfo.ServerId = tailServResp.ServerId
		d.Data.TailServerInfo.RemotePortIp = tailServResp.ServerIpPort

		d.RpcClients.TailClient.Close()

		d.RpcClients.TailClient, _ = util.GetRPCClient(
			d.Data.TailServerInfo.LocalPortIp,
			tailServResp.ServerIpPort,
		)

		for opId, getArgs := range d.Data.PendingGetRequests {
			d.RpcClients.TailClient.Go(
				"Server.Get",
				chainedkv.GetArgs{
					ClientId:   d.Data.ClientId,
					OpId:       opId,
					Key:        getArgs.Key,
					ClientAddr: d.Data.ClientIPPort,
					Token:      getArgs.Trace.GenerateToken(),
				},
				&getReply,
				d.Data.DoneCh,
			)
		}
	}
}

func (d *KVS) resendPutRequest(call *rpc.Call) {
	var headServResp chainedkv.NodeResponse
	var putReply interface{}

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	d.getHead(&headServResp)

	if headServResp.ServerId == d.Data.HeadServerInfo.ServerId {
		// Head hasn't changed, resend data through existing RPC client
		d.RpcClients.HeadClient.Go(
			"Server.Put",
			call.Args,
			&putReply,
			d.Data.DoneCh,
		)
	} else {
		// Update head server info and reinitialize client
		d.Data.HeadServerInfo.ServerId = headServResp.ServerId
		d.Data.HeadServerInfo.RemotePortIp = headServResp.ServerIpPort

		d.RpcClients.HeadClient.Close()

		d.RpcClients.HeadClient, _ = util.GetRPCClient(
			d.Data.HeadServerInfo.LocalPortIp,
			headServResp.ServerIpPort,
		)

		for opId, putArgs := range d.Data.PendingPutRequests {
			d.RpcClients.HeadClient.Go(
				"Server.Put",
				chainedkv.PutArgs{
					ClientId:   d.Data.ClientId,
					OpId:       opId,
					Key:        putArgs.Key,
					Value:      putArgs.Value,
					ClientAddr: d.Data.ClientIPPort,
					Token:      putArgs.Trace.GenerateToken(),
				},
				&putReply,
				d.Data.DoneCh,
			)
		}
	}
}

func (d *KVS) getHead(servResp *chainedkv.NodeResponse) error {
	d.Data.Trace.RecordAction(HeadReq{ClientId: d.Data.ClientId})

	err := d.RpcClients.CoordClient.Call(
		"Coord.GetHead",
		chainedkv.NodeRequest{
			ClientId: d.Data.ClientId,
			Token:    nil,
		},
		servResp,
	)

	if err != nil {
		return err
	}

	d.Data.Trace.RecordAction(
		HeadResRecvd{
			ClientId: d.Data.ClientId,
			ServerId: servResp.ServerId,
		},
	)

	return nil
}

func (d *KVS) getTail(servResp *chainedkv.NodeResponse) error {
	d.Data.Trace.RecordAction(TailReq{ClientId: d.Data.ClientId})

	err := d.RpcClients.CoordClient.Call(
		"Coord.GetTail",
		chainedkv.NodeRequest{
			ClientId: d.Data.ClientId,
			Token:    nil,
		},
		servResp,
	)

	if err != nil {
		return err
	}

	d.Data.Trace.RecordAction(
		TailResRecvd{
			ClientId: d.Data.ClientId,
			ServerId: servResp.ServerId,
		},
	)

	return nil
}

// Stop Stops the KVS instance from communicating with the KVS and
// from delivering any results via the notify-channel. This call always succeeds.
func (d *KVS) Stop() {
	var clientLeaveReply interface{}

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	d.Data.Trace.RecordAction(KvslibStop{ClientId: d.Data.ClientId})

	d.RpcClients.CoordClient.Call(
		"Coord.ClientLeave",
		chainedkv.ClientRequest{
			ClientId:     d.Data.ClientId,
			ClientIpPort: d.Data.ClientIPPort,
		},
		&clientLeaveReply,
	)

	d.RpcClients.HeadClient.Close()
	d.RpcClients.TailClient.Close()
	d.RpcClients.CoordClient.Close()
	d.Data.Listener.Close()
	d.Data.TerminateDoneCh <- true

	*d = *NewKVS()
}
