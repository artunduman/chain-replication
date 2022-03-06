// Used by integration.go

package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
	"log"
	"os"
	"strconv"
)

func main() {
	var config chainedkv.CoordConfig

	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <numServers>", os.Args[0])
	}
	numServersInt, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Usage: %s <numServers>", os.Args[0])
	}
	numServers := uint8(numServersInt)

	err = util.ReadJSONConfig("test/config/coord_config.json", &config)
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}
	ctracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	coord := chainedkv.Coord{}
	err = coord.Start(config.ClientAPIListenAddr, config.ServerAPIListenAddr, config.LostMsgsThresh, numServers, ctracer)
	if err != nil {
		log.Fatalf("Error starting coordinator: %s", err)
	}
}
