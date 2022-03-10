package chainedkv

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"

	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"

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
	ServerId     uint8
	RemoteIpPort string
	Client       *rpc.Client
	AckIpPort    string
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

type JoinedArgs struct {
	ServerId uint8
	Token    tracing.TracingToken
}

type ClientRequest struct {
	ClientId     string
	ClientIpPort string
}

type Coord struct {
	CurrChain         []uint8
	DiscoveredServers map[uint8]*ServerNode
	Cond              *sync.Cond
	Tracer            *tracing.Tracer
	Trace             *tracing.Trace
	NumServers        uint8
	LostMsgsThresh    uint8
	LocalIp           string
}

func NewCoord() *Coord {
	return &Coord{}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string,
	LostMsgsThresh uint8, NumServers uint8, ctrace *tracing.Tracer) error {

	c.DiscoveredServers = make(map[uint8]*ServerNode)
	c.CurrChain = make([]uint8, 0)
	c.Cond = sync.NewCond(&sync.Mutex{})
	c.Tracer = ctrace
	c.NumServers = NumServers
	c.LostMsgsThresh = LostMsgsThresh

	c.Trace = c.Tracer.CreateTrace()
	c.Trace.RecordAction(CoordStart{})

	localIp, _, err := net.SplitHostPort(serverAPIListenAddr)

	if err != nil {
		return err
	}

	c.LocalIp = localIp

	err = rpc.Register(c)

	if err != nil {
		return err
	}

	tcpAddrClient, err := net.ResolveTCPAddr("tcp", clientAPIListenAddr)

	if err != nil {
		return err
	}

	tcpAddrServer, err := net.ResolveTCPAddr("tcp", serverAPIListenAddr)

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

	go rpc.Accept(clientListener)
	go rpc.Accept(serverListener)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	for _, ds := range c.DiscoveredServers {
		if ds.Client != nil {
			ds.Client.Close()
		}
	}

	return nil
}

func (c *Coord) Join(args JoinArgs, reply *JoinReply) error {
	c.Cond.L.Lock()
	defer c.Cond.L.Unlock()

	trace := c.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerJoiningRecvd{args.ServerId})

	client, err := rpc.Dial("tcp", args.ServerAddr)

	if err != nil {
		return err
	}

	c.DiscoveredServers[args.ServerId] = &ServerNode{
		ServerId:     args.ServerId,
		RemoteIpPort: args.ServerAddr,
		Client:       client,
		AckIpPort:    args.AckIpPort,
	}

	// Wait until the server is the next in the chain
	for uint8(len(c.CurrChain)+1) != args.ServerId {
		c.Cond.Wait()
	}

	if args.ServerId != uint8(len(c.CurrChain)+1) {
		return errors.New("expected different serverId")
	}

	if len(c.CurrChain) == 0 {
		*reply = JoinReply{
			PrevServerAddress: nil,
			Token:             trace.GenerateToken(),
		}
	} else {
		prevServerId := c.CurrChain[len(c.CurrChain)-1]
		*reply = JoinReply{
			PrevServerAddress: &c.DiscoveredServers[prevServerId].RemoteIpPort,
			Token:             trace.GenerateToken(),
		}
	}

	return nil
}

func (c *Coord) Joined(args JoinedArgs, reply *interface{}) error {
	c.Cond.L.Lock()
	defer c.Cond.L.Unlock()

	if args.ServerId != uint8(len(c.CurrChain)+1) {
		return errors.New("expected different serverId")
	}

	trace := c.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerJoinedRecvd{args.ServerId})

	c.CurrChain = append(c.CurrChain, args.ServerId)
	c.Trace.RecordAction(NewChain{c.CurrChain})

	c.Cond.Broadcast()

	if uint8(len(c.CurrChain)) == c.NumServers {
		c.Trace.RecordAction(AllServersJoined{})

		// Start fcheck, assume this does not fail
		notifyFailureCh, _ := c.startFcheck()
		go c.checkFailure(notifyFailureCh)
	}

	return nil
}

func (c *Coord) checkFailure(notifyFailureCh <-chan fchecker.FailureDetected) {
	for {
		failure := <-notifyFailureCh
		c.handleFailure(failure.ServerId)
	}
}

func (c *Coord) handleFailure(serverId uint8) {
	var prevAddr *string
	var nextAddr *string
	var tokenRecvd tracing.TracingToken

	c.Cond.L.Lock()
	defer c.Cond.L.Unlock()

	// One server can't fail, exit
	if len(c.CurrChain) == 1 {
		log.Fatalf("Last remaining server %d failed", serverId)
	}

	c.Trace.RecordAction(ServerFail{serverId})

	prevServerId, nextServerId, newChain := c.getPrevNextActiveServers(serverId)

	c.CurrChain = newChain
	c.NumServers = uint8(len(c.CurrChain))

	if prevServerId == 0 {
		prevAddr = nil
	} else {
		cp := c.DiscoveredServers[prevServerId].RemoteIpPort
		prevAddr = &cp
	}
	if nextServerId == 0 {
		nextAddr = nil
	} else {
		cp := c.DiscoveredServers[nextServerId].RemoteIpPort
		nextAddr = &cp
	}

	if prevAddr != nil {
		prevClient := c.DiscoveredServers[prevServerId].Client

		err := prevClient.Call("Server.ServerFailNewNextServer", ServerFailArgs{
			serverId,
			nextAddr,
			nextServerId,
			c.Trace.GenerateToken(),
		}, &tokenRecvd)

		if err == nil {
			c.Trace = c.Tracer.ReceiveToken(tokenRecvd)
			c.Trace.RecordAction(ServerFailHandledRecvd{serverId, prevServerId})
		}
	}

	if nextAddr != nil {
		nextClient := c.DiscoveredServers[nextServerId].Client

		err := nextClient.Call("Server.ServerFailNewPrevServer", ServerFailArgs{
			serverId,
			prevAddr,
			prevServerId,
			c.Trace.GenerateToken(),
		}, &tokenRecvd)

		if err == nil {
			c.Trace = c.Tracer.ReceiveToken(tokenRecvd)
			c.Trace.RecordAction(ServerFailHandledRecvd{serverId, nextServerId})
		}
	}

	c.Trace.RecordAction(NewChain{c.CurrChain})
}

// Pre: serverId is in the chain and chain has length > 1
func (c *Coord) getPrevNextActiveServers(serverId uint8) (uint8, uint8, []uint8) {
	for i, id := range c.CurrChain {
		if id == serverId {
			newChain := make([]uint8, len(c.CurrChain))
			copy(newChain, c.CurrChain)
			newChain = append(newChain[:i], newChain[i+1:]...)

			if i == 0 {
				return 0, c.CurrChain[i+1], newChain
			} else if i == len(c.CurrChain)-1 {
				return c.CurrChain[i-1], 0, newChain
			} else {
				return c.CurrChain[i-1], c.CurrChain[i+1], newChain
			}
		}
	}

	return 0, 0, c.CurrChain
}

func (c *Coord) GetHead(args NodeRequest, reply *NodeResponse) error {
	c.Cond.L.Lock()
	defer c.Cond.L.Unlock()

	trace := c.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(HeadReqRecvd{args.ClientId})

	// Wait until all servers have been added
	for uint8(len(c.CurrChain)) != c.NumServers {
		c.Cond.Wait()
	}

	reply.ServerId = c.CurrChain[0]
	reply.ServerIpPort = c.DiscoveredServers[reply.ServerId].RemoteIpPort

	trace.RecordAction(HeadRes{args.ClientId, reply.ServerId})
	reply.Token = trace.GenerateToken()

	return nil
}

func (c *Coord) GetTail(args NodeRequest, reply *NodeResponse) error {
	c.Cond.L.Lock()
	defer c.Cond.L.Unlock()

	trace := c.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(TailReqRecvd{args.ClientId})

	// Wait until all servers have been added
	for uint8(len(c.CurrChain)) != c.NumServers {
		c.Cond.Wait()
	}

	reply.ServerId = c.CurrChain[len(c.CurrChain)-1]
	reply.ServerIpPort = c.DiscoveredServers[reply.ServerId].RemoteIpPort

	trace.RecordAction(TailRes{args.ClientId, reply.ServerId})
	reply.Token = trace.GenerateToken()

	return nil
}

func (c *Coord) startFcheck() (<-chan fchecker.FailureDetected, error) {
	remoteServers := make([]fchecker.Server, 0, len(c.DiscoveredServers))

	for _, node := range c.DiscoveredServers {
		remoteServers = append(remoteServers, fchecker.Server{
			ServerId: node.ServerId,
			Addr:     node.AckIpPort,
		})
	}

	notifyCh, err := fchecker.Start(fchecker.StartStruct{
		AckLocalIPAckLocalPort:       "",
		EpochNonce:                   rand.Uint64(),
		HBeatLocalIP:                 c.LocalIp,
		HBeatRemoteIPHBeatRemotePort: remoteServers,
		LostMsgThresh:                c.LostMsgsThresh,
	})

	if err != nil {
		return nil, err
	}

	return notifyCh, nil
}
