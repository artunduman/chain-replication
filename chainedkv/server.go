package chainedkv

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

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
	ServerServerAddr  string
	ServerListenAddr  string
	ClientListenAddr  string
	TracingServerAddr string
	Secret            []byte
	TracingIdentity   string
}

type Server struct {
	Id               uint8
	NextServer       *rpc.Client
	PrevServer       *rpc.Client
	Tracer           *tracing.Tracer
	Trace            *tracing.Trace
	Coord            *rpc.Client
	KVS              map[string]string
	NextGetGId       uint64
	NextPutGId       uint64
	mu               sync.Mutex
	ServerServerAddr string
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

type ServerFailArgs struct {
	FailedServerId uint8
	NewServerAddr  *string
	NewServerId    uint8
	Token          tracing.TracingToken
}

type UpdateGIdArgs struct {
	NextPutGId uint64
}

const PutGIdIncrement uint64 = 1024

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string,
	serverServerAddr string, serverListenAddr string, clientListenAddr string,
	strace *tracing.Tracer) error {

	var coordJoinReply JoinReply
	var coordJoinedReply interface{}
	var serverRegReply tracing.TracingToken

	s.Tracer = strace
	s.Id = serverId
	s.NextServer = nil
	s.KVS = make(map[string]string)
	s.ServerServerAddr = serverServerAddr

	s.Trace = s.Tracer.CreateTrace()
	s.Trace.RecordAction(ServerStart{s.Id})

	s.NextPutGId = PutGIdIncrement
	s.NextGetGId = 0

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

	defer clientListener.Close()

	serverListener, err := net.ListenTCP("tcp", tcpAddrServer)

	if err != nil {
		return err
	}

	defer serverListener.Close()

	s.Coord, err = util.GetRPCClient(serverAddr, coordAddr)

	if err != nil {
		return err
	}

	defer s.Coord.Close()

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

	s.Trace = s.Tracer.ReceiveToken(coordJoinReply.Token)

	if coordJoinReply.PrevServerAddress == nil {
		s.PrevServer = nil
	} else {
		s.PrevServer, err = util.GetRPCClient(serverServerAddr, *coordJoinReply.PrevServerAddress)

		if err != nil {
			return err
		}

		err = s.PrevServer.Call(
			"Server.RegisterNextServer",
			RegisterServerArgs{s.Id, serverListenAddr, s.Trace.GenerateToken()},
			&serverRegReply,
		)

		if err != nil {
			return err
		}

		s.Trace = s.Tracer.ReceiveToken(serverRegReply)
	}

	s.Trace.RecordAction(ServerJoined{s.Id})

	err = s.Coord.Call(
		"Coord.Joined",
		JoinedArgs{s.Id, s.Trace.GenerateToken()},
		&coordJoinedReply,
	)

	if err != nil {
		return err
	}

	go rpc.Accept(clientListener)
	go rpc.Accept(serverListener)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	return nil
}

func (s *Server) RegisterNextServer(args RegisterServerArgs, reply *tracing.TracingToken) error {
	trace := s.Tracer.ReceiveToken(args.Token)

	nextServer, err := util.SplitAndGetRPCClient(
		s.ServerServerAddr,
		args.NextServerAddress,
	)

	if err != nil {
		return err
	}

	trace.RecordAction(NextServerJoining{args.Id})
	s.NextServer = nextServer
	trace.RecordAction(NewJoinedSuccessor{args.Id})

	*reply = trace.GenerateToken()
	return nil
}

func (s *Server) ServerFailNewNextServer(args ServerFailArgs, reply *tracing.TracingToken) error {
	trace := s.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerFailRecvd{args.FailedServerId})

	s.mu.Lock()
	defer s.mu.Unlock()

	if args.NewServerAddr == nil {
		s.NextServer = nil
	} else {
		nextServer, _ := util.SplitAndGetRPCClient(
			s.ServerServerAddr,
			*args.NewServerAddr,
		)

		s.NextServer = nextServer
		trace.RecordAction(NewFailoverSuccessor{args.NewServerId})
	}

	trace.RecordAction(ServerFailHandled{args.FailedServerId})
	*reply = trace.GenerateToken()

	if s.isHead() && s.isTail() {
		s.NextGetGId = s.NextPutGId + 1
		s.NextPutGId += PutGIdIncrement
	}

	return nil
}

func (s *Server) ServerFailNewPrevServer(args ServerFailArgs, reply *tracing.TracingToken) error {
	trace := s.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerFailRecvd{args.FailedServerId})

	s.mu.Lock()
	defer s.mu.Unlock()

	if args.NewServerAddr == nil {
		s.PrevServer = nil
	} else {
		prevServer, _ := util.SplitAndGetRPCClient(
			s.ServerServerAddr,
			*args.NewServerAddr,
		)

		s.PrevServer = prevServer
		trace.RecordAction(NewFailoverPredecessor{args.NewServerId})
	}

	trace.RecordAction(ServerFailHandled{args.FailedServerId})
	*reply = trace.GenerateToken()

	if s.isHead() && s.isTail() {
		s.NextGetGId = s.NextPutGId + 1
		s.NextPutGId += PutGIdIncrement
	}

	return nil
}

func (s *Server) isTail() bool {
	return s.NextServer == nil
}

func (s *Server) isHead() bool {
	return s.PrevServer == nil
}

func (s *Server) putFwd(trace *tracing.Trace, args PutArgs, nextServer *rpc.Client) error {
	var reply interface{}

	trace.RecordAction(PutFwd{
		args.ClientId,
		args.OpId,
		args.GId,
		args.Key,
		args.Value,
	})

	args.Token = trace.GenerateToken()

	if nextServer != nil {
		err := nextServer.Call("Server.Put", args, &reply)

		if err != nil {
			return err
		}
	} else {
		return errors.New("Server.Put: next server is null")
	}

	return nil
}

func (s *Server) putTail(trace *tracing.Trace, args PutArgs) error {
	var replyArgs ReplyArgs
	var reply interface{}

	client, err := util.SplitAndGetRPCClient(
		s.ServerServerAddr,
		args.ClientAddr,
	)

	if err != nil {
		return err
	}

	defer client.Close()

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
	err = client.Call("KVS.ReceivePutResult", replyArgs, &reply)

	if err != nil {
		return err
	}

	return nil
}

func (s *Server) handlePut(trace *tracing.Trace, args PutArgs) error {
	var nextServer *rpc.Client
	var err error
	var reply interface{}

	if args.GId < s.NextPutGId {
		s.updateGId(UpdateGIdArgs{
			s.NextPutGId,
		}, &reply)

		return errors.New("Server.Put: GId not large enough")
	}

	s.NextGetGId = args.GId + 1
	s.NextPutGId = args.GId + PutGIdIncrement

	// Keep trying until server becomes tail
	for {
		if !s.isTail() {
			if s.NextServer != nil {
				nextServer = s.NextServer
			} else {
				nextServer = nil
			}
		}

		s.mu.Unlock()

		if s.isTail() {
			err = s.putTail(trace, args)
		} else {
			err = s.putFwd(trace, args, nextServer)
		}

		s.mu.Lock()

		if err == nil {
			return nil
		}

		if !s.isHead() {
			return err
		} else {
			time.Sleep(1 * time.Second)

			args.GId = s.NextPutGId

			s.NextGetGId = args.GId + 1
			s.NextPutGId = args.GId + PutGIdIncrement

			trace.RecordAction(PutOrdered{
				args.ClientId,
				args.OpId,
				args.GId,
				args.Key,
				args.Value,
			})
		}
	}
}

func (s *Server) Put(args PutArgs, reply *interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	trace := s.Tracer.ReceiveToken(args.Token)

	s.KVS[args.Key] = args.Value

	if s.isHead() {
		trace.RecordAction(PutRecvd{args.ClientId, args.OpId, args.Key, args.Value})

		args.GId = s.NextPutGId

		trace.RecordAction(PutOrdered{
			args.ClientId,
			args.OpId,
			args.GId,
			args.Key,
			args.Value,
		})

		return s.handlePut(trace, args)
	}

	trace.RecordAction(PutFwdRecvd{args.ClientId, args.OpId, args.GId, args.Key, args.Value})

	return s.handlePut(trace, args)
}

func (s *Server) Get(args GetArgs, reply *interface{}) error {
	var replyArgs ReplyArgs
	var rpcReply interface{}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isTail() {
		return errors.New("Server.Get: not tail")
	}

	trace := s.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(GetRecvd{args.ClientId, args.OpId, args.Key})

	client, err := util.SplitAndGetRPCClient(
		s.ServerServerAddr,
		args.ClientAddr,
	)

	if err != nil {
		return err
	}

	defer client.Close()

	replyArgs.GId = s.NextGetGId
	replyArgs.OpId = args.OpId
	replyArgs.Key = args.Key
	replyArgs.Value = s.KVS[args.Key]

	if s.NextGetGId == s.NextPutGId {
		s.updateGId(UpdateGIdArgs{
			s.NextPutGId + PutGIdIncrement,
		}, &rpcReply)
	}

	s.NextGetGId++

	trace.RecordAction(GetOrdered{
		args.ClientId,
		args.OpId,
		replyArgs.GId,
		args.Key,
	})

	trace.RecordAction(GetResult{
		args.ClientId,
		args.OpId,
		replyArgs.GId,
		args.Key,
		replyArgs.Value,
	})

	replyArgs.Token = trace.GenerateToken()
	err = client.Call("KVS.ReceiveGetResult", replyArgs, &rpcReply)

	if err != nil {
		return err
	}

	return nil
}

func (s *Server) UpdateGId(updateGIdArgs UpdateGIdArgs, reply *interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateGId(updateGIdArgs, reply)

	return nil
}

func (s *Server) updateGId(updateGIdArgs UpdateGIdArgs, reply *interface{}) {
	s.NextPutGId = updateGIdArgs.NextPutGId

	// Keep trying until server becomes head
	for !s.isHead() {
		if s.PrevServer != nil {
			err := s.PrevServer.Call(
				"Server.UpdateGId",
				UpdateGIdArgs{s.NextPutGId},
				&reply,
			)

			if err == nil {
				break
			}
		} else {
			s.mu.Unlock()
			s.mu.Lock()
		}
	}
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
