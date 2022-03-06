package main

import (
	"log"
	"net"
	"os"
	"strconv"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config chainedkv.ServerConfig

	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <serverId>", os.Args[0])
	}
	serverIdInt, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Usage: %s <serverId>", os.Args[0])
	}
	serverId := uint8(serverIdInt)

	err = util.ReadJSONConfig("test/config/server_config.json", &config)
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}
	stracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: "server" + strconv.Itoa(serverIdInt),
		Secret:         config.Secret,
	})

	server := chainedkv.NewServer()

	serverHost, serverAddrBaseStr, _ := net.SplitHostPort(config.ServerAddr)
	_, serverServerAddrBaseStr, _ := net.SplitHostPort(config.ServerServerAddr)
	_, serverListenAddrBaseStr, _ := net.SplitHostPort(config.ServerListenAddr)
	_, clientListenAddrBaseStr, _ := net.SplitHostPort(config.ClientListenAddr)
	serverAddrBase, _ := strconv.Atoi(serverAddrBaseStr)
	serverServerAddrBase, _ := strconv.Atoi(serverServerAddrBaseStr)
	serverListenAddrBase, _ := strconv.Atoi(serverListenAddrBaseStr)
	clientListenAddrBase, _ := strconv.Atoi(clientListenAddrBaseStr)

	err = server.Start(
		serverId,
		config.CoordAddr,
		net.JoinHostPort(serverHost, strconv.Itoa(serverAddrBase+serverIdInt)),
		net.JoinHostPort(serverHost, strconv.Itoa(serverServerAddrBase+serverIdInt)),
		net.JoinHostPort(serverHost, strconv.Itoa(serverListenAddrBase+serverIdInt)),
		net.JoinHostPort(serverHost, strconv.Itoa(clientListenAddrBase+serverIdInt)),
		stracer,
	)
	if err != nil {
		log.Fatalf("Error starting server: %s", err)
	}
}
