package chainedkv

import (
	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"errors"
	"log"
	"math/rand"
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
	serverId     uint8
	remoteIpPort string
	client       *rpc.Client
	ackIpPort    string
	joined       bool
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
	currChain         []uint8
	discoveredServers map[uint8]*ServerNode
	cond              *sync.Cond
	tracer            *tracing.Tracer
	numServers        uint8
	lostMsgsThresh    uint8
	localIp           string
}

func NewCoord() *Coord {
	return &Coord{}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8, ctrace *tracing.Tracer) error {
	c.discoveredServers = make(map[uint8]*ServerNode)
	c.currChain = make([]uint8, 0)
	c.cond = sync.NewCond(&sync.Mutex{})
	c.tracer = ctrace
	c.numServers = numServers
	c.lostMsgsThresh = lostMsgsThresh
	host, _, err := net.SplitHostPort(clientAPIListenAddr)
	if err != nil {
		return err
	}
	c.localIp = host

	err = rpc.Register(c)
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
	AckIpPort  string
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
		return err
	}
	c.discoveredServers[args.ServerId] = &ServerNode{
		serverId: args.ServerId, remoteIpPort: args.ServerAddr, client: client, ackIpPort: args.AckIpPort, joined: false,
	}
	// Wait until the server is the next in the chain
	for uint8(len(c.currChain)+1) != args.ServerId {
		c.cond.Wait()
	}

	expectedServerId := uint8(len(c.currChain) + 1)
	// Assert serverId == expected
	if args.ServerId != expectedServerId {
		log.Println("Coord.Join: serverId != expectedServerId")
		return errors.New("serverId:" + string(args.ServerId) + "!= expectedServerId:" + string(expectedServerId) + ", something went wrong")
	}

	if len(c.currChain) == 0 {
		*reply = JoinReply{PrevServerAddress: nil, Token: trace.GenerateToken()}
	} else {
		prevServerId := c.currChain[len(c.currChain)-1]
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

	if args.ServerId != uint8(len(c.currChain)+1) {
		log.Println("Coord.Joined: serverId != expectedServerId")
		return errors.New("serverId:" + string(args.ServerId) + "!= expectedServerId:" + string(rune(len(c.currChain))) + ", something went wrong")
	}

	trace := c.tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerJoinedRecvd{args.ServerId})

	// Only update the chain when it successfully joined
	c.currChain = append(c.currChain, args.ServerId)
	c.discoveredServers[args.ServerId].joined = true
	trace.RecordAction(NewChain{c.currChain})
	// Unblock waiting join requests
	c.cond.Broadcast()
	if uint8(len(c.currChain)) == c.numServers {
		trace.RecordAction(AllServersJoined{})
		// Start heartbeats
		nodes := make([]fchecker.Server, 0, len(c.discoveredServers))
		for _, node := range c.discoveredServers {
			nodes = append(nodes, fchecker.Server{
				ServerId: node.serverId,
				Addr:     node.ackIpPort,
			})
		}
		notifyFailureCh, err := startFcheck(
			c.localIp,
			nodes,
			c.lostMsgsThresh,
		)
		if err != nil {
			log.Println("Coord.Joined: startFcheck failed:", err)
			// Ignore for now, hopefully never fails
		}
		go c.checkFailure(notifyFailureCh)
	}
	*reply = true
	return nil
}

func (c *Coord) checkFailure(notifyFailureCh <-chan fchecker.FailureDetected) {
	for {
		failure := <-notifyFailureCh
		c.handleFailure(failure.ServerId)
	}
}

func (c *Coord) handleFailure(serverId uint8) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	trace := c.tracer.CreateTrace()
	trace.RecordAction(ServerFail{serverId})

	prevServerId, nextServerId, newChain := c.getPrevNextActiveServers(serverId)
	c.currChain = newChain
	c.discoveredServers[serverId].joined = false

	// Set addresses to send to servers
	var prevAddr *string
	var nextAddr *string
	if prevServerId == 0 {
		prevAddr = nil
	} else {
		cp := c.discoveredServers[prevServerId].remoteIpPort
		prevAddr = &cp
	}
	if nextServerId == 0 {
		nextAddr = nil
	} else {
		cp := c.discoveredServers[nextServerId].remoteIpPort
		nextAddr = &cp
	}

	// Send new servers
	var tokenRecvd tracing.TracingToken
	// Nit: parallelize
	if prevAddr != nil {
		prevClient := c.discoveredServers[prevServerId].client
		err := prevClient.Call("Server.ServerFailNewNextServer", ServerFailArgs{
			serverId,
			nextAddr,
			nextServerId,
			trace.GenerateToken(),
		}, &tokenRecvd)
		if err != nil {
			// Hopefully shouldn't happen
			log.Println("Coord.handleFailure: prevClient.Call failed:", err)
			return
		}
		trace = c.tracer.ReceiveToken(tokenRecvd)
		trace.RecordAction(ServerFailHandledRecvd{serverId, prevServerId})
	}
	if nextAddr != nil {
		nextClient := c.discoveredServers[nextServerId].client
		err := nextClient.Call("Server.ServerFailNewPrevServer", ServerFailArgs{
			serverId,
			prevAddr,
			prevServerId,
			trace.GenerateToken(),
		}, &tokenRecvd)
		if err != nil {
			log.Println("Coord.handleFailure: nextClient.Call failed:", err)
			return
		}
		trace = c.tracer.ReceiveToken(tokenRecvd)
		trace.RecordAction(ServerFailHandledRecvd{serverId, nextServerId})
	}
	trace.RecordAction(NewChain{c.currChain})
}

func (c *Coord) getPrevNextActiveServers(serverId uint8) (uint8, uint8, []uint8) {
	for i, id := range c.currChain {
		if id == serverId {
			newChain := append(c.currChain[:i], c.currChain[i+1:]...)
			if i == 0 {
				return 0, c.currChain[i+1], newChain
			} else if i == len(c.currChain)-1 {
				return c.currChain[i-1], 0, newChain
			} else {
				return c.currChain[i-1], c.currChain[i+1], newChain
			}
		}
	}
	return 0, 0, c.currChain
}

func (c *Coord) GetHead(args NodeRequest, reply *NodeResponse) error {
	// TODO check if coord is ready
	reply.ServerId = c.currChain[0] // TODO deterministically return server id
	reply.ServerIpPort = c.discoveredServers[reply.ServerId].remoteIpPort
	reply.Token = args.Token
	return nil
}

func (c *Coord) GetTail(args NodeRequest, reply *NodeResponse) error {
	// TODO check if coord is ready
	reply.ServerId = c.currChain[len(c.currChain)-1] // TODO deterministically return server id
	reply.ServerIpPort = c.discoveredServers[reply.ServerId].remoteIpPort
	reply.Token = args.Token
	return nil
}

func startFcheck(localIp string, remoteServers []fchecker.Server, lostMsgThresh uint8) (notifyCh <-chan fchecker.FailureDetected, err error) {
	// Convert servernode to fchecker.Server
	notifyCh, err = fchecker.Start(fchecker.StartStruct{
		AckLocalIPAckLocalPort:       "",
		EpochNonce:                   rand.Uint64(),
		HBeatLocalIP:                 localIp,
		HBeatRemoteIPHBeatRemotePort: remoteServers,
		LostMsgThresh:                lostMsgThresh,
	})
	if err != nil {
		log.Println("Coord.Start: can't start fchecker:", err)
		return nil, err
	}
	return notifyCh, nil
}
