package kvslib

import (
	"errors"
	"log"
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

// RPC

type ServerArgs struct {
	ServerId     uint8
	ServerIpPort string
}

// Local

type KVS struct {
	Mutex      *sync.Mutex
	NotifyCh   NotifyChannel
	RpcClients RPCClients
	Data       LocalData
	State      LocalState
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

type LocalData struct {
	Tracer           *tracing.Tracer
	ClientId         string
	ClientIpPort     string
	LocalCoordIPPort string
	ChCapacity       int
	HeadServerInfo   ServerInfo
	TailServerInfo   ServerInfo
}

type LocalState struct {
	CurrOpId    uint32
	ChCount     int
	GetOpId     []uint32
	PutOpId     []uint32
	OpIdToCalls map[uint32](*rpc.Call)
}

func NewKVS() *KVS {
	return &KVS{
		Mutex:      &sync.Mutex{},
		NotifyCh:   nil,
		RpcClients: RPCClients{},
		Data:       LocalData{},
		State:      LocalState{},
	}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string, localCoordIPPort string, localHeadServerIPPort string, localTailServerIPPort string, chCapacity int) (NotifyChannel, error) {
	var trace *tracing.Trace
	var headServResp chainedkv.NodeResponse
	var tailServResp chainedkv.NodeResponse
	var clientIp string

	trace = localTracer.CreateTrace()

	// Reserve critical section
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	// Populate KVS
	d.NotifyCh = make(NotifyChannel)
	d.Data.Tracer = localTracer
	d.Data.ClientId = clientId
	d.Data.LocalCoordIPPort = localCoordIPPort
	d.Data.ChCapacity = chCapacity
	d.State.CurrOpId = 0
	d.State.ChCount = 0
	d.State.GetOpId = make([]uint32, chCapacity)
	d.State.PutOpId = make([]uint32, chCapacity)
	d.State.OpIdToCalls = make(map[uint32]*rpc.Call)

	trace.RecordAction(KvslibStart{ClientId: clientId})

	// Connect to rpc clients
	coordClient, err := util.GetRPCClient(localCoordIPPort, coordIPPort)
	if err != nil {
		log.Println("kvslib.Start() - Error in connecting to coord:", err)
		return nil, err
	}

	// Obtain head server info
	trace.RecordAction(HeadReq{ClientId: clientId})

	err = coordClient.Call("Coord.GetHead", chainedkv.NodeRequest{ClientId: clientId, Token: nil}, &headServResp)
	if err != nil {
		log.Println("kvslib.Start() - Error in getting head server:", err)
		return nil, err
	}
	d.Data.HeadServerInfo.ServerId = headServResp.ServerId
	d.Data.HeadServerInfo.LocalPortIp = localHeadServerIPPort
	d.Data.HeadServerInfo.RemotePortIp = headServResp.ServerIpPort

	trace.RecordAction(HeadResRecvd{ClientId: clientId, ServerId: headServResp.ServerId})

	headClient, err := util.GetRPCClient(localHeadServerIPPort, headServResp.ServerIpPort)
	if err != nil {
		log.Println("kvslib.Start() - Error in connecting to head server:", err)
		return nil, err
	}

	// Obtain tail server info
	trace.RecordAction(TailReq{ClientId: clientId})

	err = coordClient.Call("Coord.GetTail", chainedkv.NodeRequest{ClientId: clientId, Token: nil}, &tailServResp)
	if err != nil {
		log.Println("kvslib.Start() - Error in getting tail server:", err)
		return nil, err
	}
	d.Data.TailServerInfo.ServerId = tailServResp.ServerId
	d.Data.TailServerInfo.LocalPortIp = localTailServerIPPort
	d.Data.TailServerInfo.RemotePortIp = tailServResp.ServerIpPort

	trace.RecordAction(TailResRecvd{ClientId: clientId, ServerId: tailServResp.ServerId})

	tailClient, err := util.GetRPCClient(localTailServerIPPort, tailServResp.ServerIpPort)
	if err != nil {
		log.Println("kvslib.Start() - Error in connecting to tail server:", err)
		return nil, err
	}

	d.RpcClients.CoordClient = coordClient
	d.RpcClients.HeadClient = headClient
	d.RpcClients.TailClient = tailClient

	// Setup local rpc
	err = rpc.Register(d)
	if err != nil {
		log.Println("kvslib.Start() - Error in initiating rpc: ", err)
		return nil, err
	}

	clientIp = util.GetPreferredOutboundIp().String()
	port, err := util.GetFreeTCPPort(clientIp)
	if err != nil {
		log.Println("kvslib.Start() - Error in obtaining local client ip: ", err)
		return nil, err
	}

	d.Data.ClientIpPort = net.JoinHostPort(clientIp, strconv.Itoa(port))
	tcpAddrClient, err := net.ResolveTCPAddr("tcp", d.Data.ClientIpPort)
	if err != nil {
		log.Println("kvslib.Start() - Error in resolving local client ip: ", err)
		return nil, err
	}

	clientListener, err := net.ListenTCP("tcp", tcpAddrClient)
	if err != nil {
		log.Println("kvslib.Start() - Error in listening to tcp on local client ip: ", err)
		return nil, err
	}

	go rpc.Accept(clientListener)

	return d.NotifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	var trace *tracing.Trace
	var getArgs chainedkv.GetArgs
	var opId uint32

	// Reserve critical section
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	d.Data.Tracer = tracer
	trace = tracer.CreateTrace()
	opId = d.State.CurrOpId

	// Verify capacity hasn't been exceeded
	if d.State.ChCount > d.Data.ChCapacity {
		err := errors.New("concurrency capacity has been exceeded")
		log.Println("kvslib.Get() - Error: ", err)
		return 0, err
	}

	getArgs = chainedkv.GetArgs{ClientId: clientId, OpId: opId, Key: key, ClientAddr: d.Data.ClientIpPort, Token: trace.GenerateToken()}

	trace.RecordAction(Get{ClientId: clientId, OpId: opId, Key: key})

	// Invoke Get
	cbCall := d.RpcClients.TailClient.Go(
		"Server.Get",
		getArgs,
		nil,
		nil,
	)
	if cbCall.Error != nil {
		log.Println("kvslib.Get() - Error: ", cbCall.Error)
		return 0, cbCall.Error
	}

	// Update state
	d.State.CurrOpId += 1
	d.State.ChCount += 1
	d.State.GetOpId = append(d.State.GetOpId, opId)
	d.State.OpIdToCalls[opId] = cbCall // TODO gotta clean this up

	return opId, nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value opId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	var trace *tracing.Trace
	var putArgs chainedkv.PutArgs
	var opId uint32

	// Reserve critical section
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	d.Data.Tracer = tracer
	trace = tracer.CreateTrace()
	opId = d.State.CurrOpId

	// Verify capacity hasn't been exceeded
	if d.State.ChCount > d.Data.ChCapacity {
		err := errors.New("concurrency capacity has been exceeded")
		log.Println("kvslib.Put() - Error: ", err)
		return 0, err
	}

	putArgs = chainedkv.PutArgs{ClientId: clientId, OpId: opId, Key: key, Value: value, ClientAddr: d.Data.ClientIpPort, Token: trace.GenerateToken()}

	trace.RecordAction(Put{ClientId: clientId, OpId: opId, Key: key, Value: value})

	// Invoke Put
	cbCall := d.RpcClients.HeadClient.Go(
		"Server.Put",
		putArgs,
		nil,
		nil,
	)
	if cbCall.Error != nil {
		log.Println("kvslib.Put() - Error: ", cbCall.Error)
		return 0, cbCall.Error
	}

	// Update state
	d.State.CurrOpId += 1
	d.State.ChCount += 1
	d.State.PutOpId = append(d.State.PutOpId, opId)
	d.State.OpIdToCalls[opId] = cbCall // TODO gotta clean this up

	return opId, nil
}

func (d *KVS) ReceiveGetResult(args chainedkv.GetReply, reply *interface{}) error {
	// Reserve critical section
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	// GetResultRecvd
	d.Data.Tracer.ReceiveToken(args.Token)
	d.Data.Tracer.CreateTrace().RecordAction(GetResultRecvd{OpId: args.OpId, GId: args.GId, Key: args.Key, Value: args.Value})

	// Notify client
	d.NotifyCh <- ResultStruct{OpId: args.OpId, GId: args.GId, Result: args.Value}

	// Update state
	d.State.ChCount -= 1
	d.State.GetOpId = util.RemoveUInt32(d.State.GetOpId, args.OpId)

	return nil
}

func (d *KVS) ReceivePutResult(args chainedkv.PutResultArgs, reply *interface{}) error {
	// Reserve critical section
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	// PutResultRecvd
	d.Data.Tracer.ReceiveToken(args.Token)
	d.Data.Tracer.CreateTrace().RecordAction(PutResultRecvd{OpId: args.OpId, GId: args.GId, Key: args.Key})

	// Notify client
	d.NotifyCh <- ResultStruct{OpId: args.OpId, GId: args.GId, Result: args.Value}

	// Update state
	d.State.ChCount -= 1
	d.State.PutOpId = util.RemoveUInt32(d.State.GetOpId, args.OpId)

	return nil
}

func (d *KVS) NewTailServer(serverArgs ServerArgs, reply *interface{}) error {
	// Reserve critical section
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	// Obtain tail server info
	d.Data.TailServerInfo.ServerId = serverArgs.ServerId
	d.Data.TailServerInfo.RemotePortIp = serverArgs.ServerIpPort

	tailClient, err := util.GetRPCClient(d.Data.TailServerInfo.LocalPortIp, serverArgs.ServerIpPort)
	if err != nil {
		log.Println("kvslib.NewTailServer() - Error in connecting to tail server:", err)
		return err
	}

	d.RpcClients.TailClient = tailClient

	for _, opId := range d.State.GetOpId {
		getArgs := d.State.OpIdToCalls[opId].Args
		// Invoke Get
		cbCall := d.RpcClients.TailClient.Go(
			"Server.Get",
			getArgs,
			nil,
			nil,
		)
		if cbCall.Error != nil {
			log.Println("kvslib.Get() - Error: ", cbCall.Error)
			return cbCall.Error
		}
	}
	return nil
}

func (d *KVS) NewHeadServer(serverArgs ServerArgs, reply *interface{}) error {
	// Reserve critical section
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	// Obtain tail server info
	d.Data.HeadServerInfo.ServerId = serverArgs.ServerId
	d.Data.HeadServerInfo.RemotePortIp = serverArgs.ServerIpPort

	headClient, err := util.GetRPCClient(d.Data.HeadServerInfo.LocalPortIp, serverArgs.ServerIpPort)
	if err != nil {
		log.Println("kvslib.NewHeadServer() - Error in connecting to head server:", err)
		return err
	}

	d.RpcClients.HeadClient = headClient

	for _, opId := range d.State.PutOpId {
		putArgs := d.State.OpIdToCalls[opId].Args
		// Invoke Get
		cbCall := d.RpcClients.HeadClient.Go(
			"Server.Put",
			putArgs,
			nil,
			nil,
		)
		if cbCall.Error != nil {
			log.Println("kvslib.Put() - Error: ", cbCall.Error)
			return cbCall.Error
		}
	}
	return nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	d.RpcClients.HeadClient.Close()
	d.RpcClients.TailClient.Close()
	d.RpcClients.CoordClient.Close()
	newKVS := NewKVS()
	*d = *newKVS
}
