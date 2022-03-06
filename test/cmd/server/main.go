// Used by integration.go

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

	serverHost, _, _ := net.SplitHostPort(config.ServerAddr)
	ports := make([]int, 4)
	for i := 0; i < 4; i++ {
		port, err := util.GetFreeTCPPort(serverHost)
		if err != nil {
			log.Fatalf("Error getting free port: %s", err)
		}
		ports[i] = port
	}
	err = server.Start(
		serverId,
		config.CoordAddr,
		net.JoinHostPort(serverHost, strconv.Itoa(ports[0])),
		net.JoinHostPort(serverHost, strconv.Itoa(ports[1])),
		net.JoinHostPort(serverHost, strconv.Itoa(ports[2])),
		net.JoinHostPort(serverHost, strconv.Itoa(ports[3])),
		stracer,
	)
	if err != nil {
		log.Fatalf("Error starting server: %s", err)
	}
}
