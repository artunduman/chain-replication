package chainedkv

import (
	"errors"
	"net"
	"net/rpc"
	"strconv"
	"sync"

	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"cs.ubc.ca/cpsc416/a3/util"

	"github.com/DistributedClocks/tracing"
)

// tracing structs

type ServerStart struct {
	ServerId uint8
}

type ServerJoining struct {
	ServerId uint8
}

type NextServerJoining struct {
	NextServerId uint8
}

type NewJoinedSuccessor struct {
	NextServerId uint8
}

type ServerJoined struct {
	ServerId uint8
}

type ServerFailRecvd struct {
	FailedServerId uint8
}

type NewFailoverSuccessor struct {
	NewNextServerId uint8
}

type NewFailoverPredecessor struct {
	NewPrevServerId uint8
}

type ServerFailHandled struct {
	FailedServerId uint8
}

type PutRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwdRecvd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type GetRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
}

type GetResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

// end of tracing structs

type ServerConfig struct {
	ServerId          uint8
	CoordAddr         string
	ServerAddr        string
	ServerListenAddr  string
	ClientListenAddr  string
	TracingServerAddr string
	Secret            []byte
	TracingIdentity   string
}

type Server struct {
	Id         uint8
	NextServer *rpc.Client
	PrevServer *rpc.Client
	Tracer     *tracing.Tracer
	Trace      *tracing.Trace
	Coord      *rpc.Client
	KVS        map[string]string
	CurGId     uint64
	mu         sync.Mutex
}

type RegisterServerArgs struct {
	Id                uint8
	NextServerAddress string
	Token             tracing.TracingToken
}

type PutArgs struct {
	ClientId   string
	OpId       uint32
	Key        string
	Value      string
	GId        uint64
	ClientAddr string
	Token      tracing.TracingToken
}

type GetArgs struct {
	ClientId   string
	OpId       uint32
	Key        string
	ClientAddr string
	Token      tracing.TracingToken
}

type ReplyArgs struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
	Token tracing.TracingToken
}

type ServerArgs struct {
	ServerId     uint8
	ServerIpPort string
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string,
	serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	var coordJoinReply JoinReply
	var coordJoinedReply bool
	var serverRegReply tracing.TracingToken

	s.Tracer = strace
	s.Id = serverId
	s.NextServer = nil
	s.KVS = make(map[string]string)

	s.Trace = s.Tracer.CreateTrace()
	s.Trace.RecordAction(ServerStart{s.Id})

	err := rpc.Register(s)

	if err != nil {
		return err
	}

	tcpAddrClient, err := net.ResolveTCPAddr("tcp", clientListenAddr)

	if err != nil {
		return err
	}

	tcpAddrServer, err := net.ResolveTCPAddr("tcp", serverListenAddr)

	if err != nil {
		return err
	}

	clientListener, err := net.ListenTCP("tcp", tcpAddrClient)

	if err != nil {
		return err
	}

	serverListener, err := net.ListenTCP("tcp", tcpAddrServer)

	if err != nil {
		return err
	}

	s.Coord, err = util.GetRPCClient(serverAddr, coordAddr)

	if err != nil {
		return err
	}

	// Start listening for heartbeats
	ackIpPort, err := s.startFcheck(serverAddr)

	if err != nil {
		return err
	}

	// Join chain
	s.Trace.RecordAction(ServerJoining{s.Id})
	err = s.Coord.Call(
		"Coord.Join",
		JoinArgs{serverId, serverListenAddr, ackIpPort, s.Trace.GenerateToken()},
		&coordJoinReply,
	)

	if err != nil {
		return err
	}

	if coordJoinReply.PrevServerAddress == nil {
		s.PrevServer = nil
	} else {
		s.PrevServer, err = rpc.Dial("tcp", *coordJoinReply.PrevServerAddress)

		if err != nil {
			return err
		}

		err = s.PrevServer.Call(
			"Server.RegisterNextServer",
			RegisterServerArgs{s.Id, serverListenAddr, coordJoinReply.Token},
			&serverRegReply,
		)

		if err != nil {
			return err
		}

		s.Trace = s.Tracer.ReceiveToken(serverRegReply)
	}

	s.Trace.RecordAction(ServerJoined{s.Id})

	// Send joined to coord
	err = s.Coord.Call(
		"Coord.Joined",
		JoinedArgs{s.Id, s.Trace.GenerateToken()},
		&coordJoinedReply,
	)
	if err != nil {
		return err
	}

	go rpc.Accept(clientListener)
	rpc.Accept(serverListener)

	return nil
}

func (s *Server) RegisterNextServer(args RegisterServerArgs, reply *tracing.TracingToken) error {
	trace := s.Tracer.ReceiveToken(args.Token)

	nextServer, err := rpc.Dial("tcp", args.NextServerAddress)

	if err != nil {
		return err
	}

	trace.RecordAction(NextServerJoining{args.Id})
	s.NextServer = nextServer
	trace.RecordAction(NewJoinedSuccessor{args.Id})

	*reply = trace.GenerateToken()
	return nil
}

func (s *Server) isTail() bool {
	return s.NextServer == nil
}

func (s *Server) isHead() bool {
	return s.PrevServer == nil
}

func (s *Server) putFwd(trace *tracing.Trace, args PutArgs, reply *interface{}) error {
	var putReply interface{}

	trace.RecordAction(PutFwd{args.ClientId, args.OpId, args.GId, args.Key, args.Value})

	args.Token = trace.GenerateToken()
	err := s.NextServer.Call("Server.Put", args, &putReply)

	if err != nil {
		return err
	}

	return nil
}

func (s *Server) putTail(trace *tracing.Trace, args PutArgs, reply *interface{}) error {
	var replyArgs ReplyArgs
	var receivePutReply interface{}

	client, err := rpc.Dial("tcp", args.ClientAddr)

	if err != nil {
		return err
	}

	replyArgs.GId = args.GId
	replyArgs.OpId = args.OpId
	replyArgs.Key = args.Key
	replyArgs.Value = args.Value

	trace.RecordAction(PutResult{
		args.ClientId,
		args.OpId,
		args.GId,
		args.Key,
		args.Value,
	})

	replyArgs.Token = trace.GenerateToken()
	err = client.Call("KVS.ReceivePutResult", replyArgs, &receivePutReply)

	if err != nil {
		return err
	}

	return nil
}

func (s *Server) Put(args PutArgs, reply *interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	trace := s.Tracer.ReceiveToken(args.Token)

	s.KVS[args.Key] = args.Value

	if s.isHead() {
		trace.RecordAction(PutRecvd{args.ClientId, args.OpId, args.Key, args.Value})

		s.CurGId++
		args.GId = s.CurGId
		trace.RecordAction(PutOrdered{args.ClientId, args.OpId, args.GId, args.Key, args.Value})

		if s.isTail() {
			return s.putTail(trace, args, reply)
		} else {
			return s.putFwd(trace, args, reply)
		}
	}

	trace.RecordAction(PutFwdRecvd{args.ClientId, args.OpId, args.GId, args.Key, args.Value})

	if s.isTail() {
		return s.putTail(trace, args, reply)
	}

	return s.putFwd(trace, args, reply)
}

func (s *Server) Get(args GetArgs, reply *interface{}) error {
	var replyArgs ReplyArgs
	var receiveGetReply interface{}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isTail() {
		return errors.New("Server.Get: not tail")
	}

	trace := s.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(GetRecvd{args.ClientId, args.OpId, args.Key})

	client, err := rpc.Dial("tcp", args.ClientAddr)

	if err != nil {
		return err
	}

	replyArgs.GId = s.CurGId
	replyArgs.OpId = args.OpId
	replyArgs.Key = args.Key
	replyArgs.Value = s.KVS[args.Key]

	trace.RecordAction(GetResult{
		args.ClientId,
		args.OpId,
		s.CurGId,
		args.Key,
		s.KVS[args.Key],
	})

	replyArgs.Token = trace.GenerateToken()
	err = client.Call("KVS.ReceiveGetResult", replyArgs, &receiveGetReply)

	if err != nil {
		return err
	}

	return nil
}

func (s *Server) startFcheck(serverAddr string) (string, error) {
	serverAddrIp, _, err := net.SplitHostPort(serverAddr)

	if err != nil {
		return "", err
	}

	port, err := util.GetFreeUDPPort(serverAddrIp)

	if err != nil {
		return "", err
	}

	ackIpPort, err := net.ResolveUDPAddr(
		"udp",
		net.JoinHostPort(serverAddrIp, strconv.Itoa(port)),
	)

	if err != nil {
		return "", err
	}

	// Start fcheck ACK
	fchecker.Start(fchecker.StartStruct{
		AckLocalIPAckLocalPort: ackIpPort.String(),
	})

	return ackIpPort.String(), nil
}
