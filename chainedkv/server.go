package chainedkv

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"

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
	Coord      *rpc.Client
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	err := rpc.Register(s)
	if err != nil {
		log.Println("Server.Start: rpc.Register failed:", err)
		return err
	}
	tcpAddrClient, err := net.ResolveTCPAddr("tcp", clientListenAddr)
	tcpAddrServer, err := net.ResolveTCPAddr("tcp", serverListenAddr)
	if err != nil {
		log.Println("Server.Start: can't resolve address:", err)
		return err
	}
	clientListener, err := net.ListenTCP("tcp", tcpAddrClient)
	serverListener, err := net.ListenTCP("tcp", tcpAddrServer)
	if err != nil {
		log.Println("Server.Start: can't listen on address:", err)
		return err
	}
	go rpc.Accept(clientListener)
	go rpc.Accept(serverListener)

	client, err := util.GetRPCClient(serverAddr, coordAddr)
	if err != nil {
		log.Println("Server.Start: can't dial coordinator, make sure it's up:", err)
		return err
	}
	log.Printf("Server %d started...\n", serverId)

	// Join to coord
	log.Println("Server.Start: sending Join to coord")
	var reply JoinReply
	err = client.Call(
		"Coord.Join",
		JoinArgs{serverId, serverListenAddr, nil},
		&reply,
	) // TODO change token and change localhost in serverListenAddr to public ip
	if err != nil {
		log.Println("Server.Start: can't join to coordinator:", err)
		return err
	}
	log.Printf("Server.Start: Server %d joined the chain\n", serverId)
	s.Id = serverId
	s.NextServer = nil                 // I'm the tail, for now
	if reply.PrevServerAddress == "" { // TODO there is probably a better way to do this
		// I'm also the head!
		s.PrevServer = nil
	} else {
		s.PrevServer, err = rpc.Dial("tcp", reply.PrevServerAddress) // TODO possibly change to GetRPCClient depending on config updates on piazza
		if err != nil {
			log.Println("Server.Start: can't dial prev server:", err)
			return err
		}
		var replyRegister bool
		err := s.PrevServer.Call("Server.RegisterNextServer", serverListenAddr, &replyRegister)
		if err != nil {
			log.Println("Server.Start: can't register next server:", err)
			return err
		}
	}
	s.Tracer = strace
	s.Coord = client

	// Wait until OS interrupt
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("Server.Start: OS interrupt received, shutting down...")
	return nil
}

func (s *Server) RegisterNextServer(nextServerAddress string, reply *bool) error {
	log.Printf("Server.RegisterNextServer: Registering %s to server %d\n", nextServerAddress, s.Id)
	nextServer, err := rpc.Dial("tcp", nextServerAddress)
	if err != nil {
		log.Println("Server.RegisterNextServer: can't dial next server:", err)
		return err
	}
	s.NextServer = nextServer
	*reply = true
	return nil
}

func (s *Server) RegisterPrevServer(prevServerAddress string, reply *bool) error {
	log.Printf("Server.RegisterPrevServer: Registering %s to server %d\n", prevServerAddress, s.Id)
	prevServer, err := rpc.Dial("tcp", prevServerAddress)
	if err != nil {
		log.Println("Server.RegisterPrevServer: can't dial prev server:", err)
		return err
	}
	s.PrevServer = prevServer
	*reply = true
	return nil
}
func (s *Server) isTail() bool {
	return s.NextServer == nil
}

func (s *Server) isHead() bool {
	return s.PrevServer == nil
}

type PutArgs struct {
	Key        string
	Value      string
	ClientId   string
	GId        uint64
	ClientAddr string
	Token      *tracing.TracingToken
}

type PutReply struct {
	Value string
}

func (s *Server) Put(args PutArgs, reply *PutReply) error {
	log.Printf("Server.Put: Put %s:%s to server %d\n", args.Key, args.Value, s.Id)
	// TODO server magic here
	if s.isHead() {
		// TODO do ordering
	}
	if s.isTail() {
		// TODO Respond to client
	} else {
		err := s.NextServer.Call("Server.Put", args, reply) // Possibly .Go()?
		if err != nil {
			return err
		}
	}
	return nil
}

type GetArgs struct {
	Key   string
	GId   uint64
	Token *tracing.TracingToken
}

type GetReply struct {
	Value string
}

func (s *Server) Get(args GetArgs, reply *GetReply) error {
	if !s.isTail() {
		return errors.New("Server.Get: not tail")
	}
	// TODO Return the value here
	return nil
}
