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

type Coord struct {
	currChainLen      uint8
	discoveredServers map[uint8]*ServerNode
	cond              *sync.Cond
}

func NewCoord() *Coord {
	return &Coord{}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8, ctrace *tracing.Tracer) error {
	c.discoveredServers = make(map[uint8]*ServerNode)
	c.currChainLen = 0
	c.cond = sync.NewCond(&sync.Mutex{})
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
	log.Println("Coord started...")
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
}

func (c *Coord) Join(args JoinArgs, reply *JoinReply) error {
	// Dial rpc for server
	log.Println("Coord.Join: received join from", args.ServerId)
	client, err := rpc.Dial("tcp", args.ServerAddr)
	if err != nil {
		log.Println("Coord.Join: can't dial server:", err)
		return err // TODO make this more descriptive
	}
	// Lock the critical section
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
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

	log.Println("Coord.Join: server id added to chain:", args.ServerId)
	c.currChainLen++

	if c.currChainLen == 1 {
		*reply = JoinReply{PrevServerAddress: nil}
	} else {
		prevServerId := c.currChainLen - 1
		prevServerAddr := c.discoveredServers[prevServerId].remoteIpPort
		*reply = JoinReply{PrevServerAddress: &prevServerAddr}
	}
	c.cond.Broadcast()
	return nil
}

type NodeRequest struct {
	ClientId string
	Token    tracing.TracingToken
}

func (c *Coord) GetHead(args NodeRequest, reply *string) error {
	// TODO check if coord is ready
	*reply = c.discoveredServers[1].remoteIpPort
	return nil
}

func (c *Coord) GetTail(args NodeRequest, reply *string) error {
	// TODO check if coord is ready
	*reply = c.discoveredServers[c.currChainLen].remoteIpPort
	return nil
}
