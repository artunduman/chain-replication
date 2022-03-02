package chainedkv

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/DistributedClocks/tracing"
)

// Actions to be recorded by coord (as part of ctrace, ktrace, and strace):

type CoordStart struct {
}

type ServerFail struct {
	ServerId uint8
}

type ServerFailHandledRecvd struct {
	FailedServerId   uint8
	AdjacentServerId uint8
}

type NewChain struct {
	Chain []uint8
}

type AllServersJoined struct {
}

type HeadReqRecvd struct {
	ClientId string
}

type HeadRes struct {
	ClientId string
	ServerId uint8
}

type TailReqRecvd struct {
	ClientId string
}

type TailRes struct {
	ClientId string
	ServerId uint8
}

type ServerJoiningRecvd struct {
	ServerId uint8
}

type ServerJoinedRecvd struct {
	ServerId uint8
}

type CoordConfig struct {
	ClientAPIListenAddr string
	ServerAPIListenAddr string
	LostMsgsThresh      uint8
	NumServers          uint8
	TracingServerAddr   string
	Secret              []byte
	TracingIdentity     string
}

type ServerNode struct {
	remoteIpPort string
	client       *rpc.Client
}

type NodeRequest struct {
	ClientId string
	Token    tracing.TracingToken
}

type NodeResponse struct {
	ServerId     uint8
	ServerIpPort string
	Token        tracing.TracingToken
}

type ClientRequest struct {
	ClientId     string
	ClientIpPort string
}

type Coord struct {
	currChainLen      uint8
	discoveredServers map[uint8]*ServerNode
	cond              *sync.Cond
	tracer            *tracing.Tracer
	numServers        uint8
	lostMsgsThresh    uint8
}

func NewCoord() *Coord {
	return &Coord{}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8, ctrace *tracing.Tracer) error {
	c.discoveredServers = make(map[uint8]*ServerNode)
	c.currChainLen = 0
	c.cond = sync.NewCond(&sync.Mutex{})
	c.tracer = ctrace
	c.numServers = numServers
	c.lostMsgsThresh = lostMsgsThresh

	err := rpc.Register(c)
	if err != nil {
		log.Println("Coord.Start: rpc.Register failed:", err)
		return err
	}
	tcpAddrClient, err := net.ResolveTCPAddr("tcp", clientAPIListenAddr)
	tcpAddrServer, err := net.ResolveTCPAddr("tcp", serverAPIListenAddr)
	if err != nil {
		log.Println("Coord.Start: can't resolve TCP address:", err)
		return err
	}
	clientListener, err := net.ListenTCP("tcp", tcpAddrClient)
	serverListener, err := net.ListenTCP("tcp", tcpAddrServer)
	if err != nil {
		log.Println("Coord.Start: can't listen on TCP address:", err)
		return err
	}
	go rpc.Accept(clientListener)
	go rpc.Accept(serverListener)
	// Wait for interrupt
	c.tracer.CreateTrace().RecordAction(CoordStart{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("Coord stopped...")
	return nil
}

type JoinArgs struct {
	ServerId   uint8
	ServerAddr string
	Token      tracing.TracingToken
}

type JoinReply struct {
	PrevServerAddress *string
	Token             tracing.TracingToken
}

func (c *Coord) Join(args JoinArgs, reply *JoinReply) error {
	// Lock the critical section
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	trace := c.tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerJoiningRecvd{args.ServerId})

	client, err := rpc.Dial("tcp", args.ServerAddr)
	if err != nil {
		log.Println("Coord.Join: can't dial server:", err)
		return err // TODO make this more descriptive
	}
	c.discoveredServers[args.ServerId] = &ServerNode{remoteIpPort: args.ServerAddr, client: client}
	// Wait until the server is the next in the chain
	for c.currChainLen+1 != args.ServerId {
		c.cond.Wait()
	}

	expectedServerId := c.currChainLen + 1
	// Assert serverId == expected
	if args.ServerId != expectedServerId {
		log.Println("Coord.Join: serverId != expectedServerId")
		return errors.New("serverId:" + string(args.ServerId) + "!= expectedServerId:" + string(expectedServerId) + ", something went wrong")
	}

	if c.currChainLen == 0 {
		*reply = JoinReply{PrevServerAddress: nil, Token: trace.GenerateToken()}
	} else {
		prevServerId := c.currChainLen
		prevServerAddr := c.discoveredServers[prevServerId].remoteIpPort
		*reply = JoinReply{PrevServerAddress: &prevServerAddr, Token: trace.GenerateToken()}
	}
	c.cond.Broadcast()
	return nil
}

type JoinedArgs struct {
	ServerId uint8
	Token    tracing.TracingToken
}

func (c *Coord) Joined(args JoinedArgs, reply *bool) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	if args.ServerId != c.currChainLen+1 {
		log.Println("Coord.Joined: serverId != expectedServerId")
		return errors.New("serverId:" + string(args.ServerId) + "!= expectedServerId:" + string(c.currChainLen) + ", something went wrong")
	}

	trace := c.tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerJoinedRecvd{args.ServerId})

	// Only increment the chain length when it successfully joined
	c.currChainLen++
	// Unblock waiting join requests TODO should I unblock before getting the joined ack?
	c.cond.Broadcast()
	*reply = true
	return nil
}

func (c *Coord) GetHead(args NodeRequest, reply *NodeResponse) error {
	// TODO check if coord is ready
	reply.ServerId = 1 // TODO deterministically return server id
	reply.ServerIpPort = c.discoveredServers[reply.ServerId].remoteIpPort
	reply.Token = args.Token
	return nil
}

func (c *Coord) GetTail(args NodeRequest, reply *NodeResponse) error {
	// TODO check if coord is ready
	reply.ServerId = c.currChainLen // TODO deterministically return server id
	reply.ServerIpPort = c.discoveredServers[reply.ServerId].remoteIpPort
	reply.Token = args.Token
	return nil
}
