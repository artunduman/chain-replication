package main

import (
	"log"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config chainedkv.CoordConfig
	err := util.ReadJSONConfig("config/coord_config.json", &config)
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}
	ctracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	coord := chainedkv.NewCoord()
	coord.Start(config.ClientAPIListenAddr, config.ServerAPIListenAddr, config.LostMsgsThresh, config.NumServers, ctracer)
}
