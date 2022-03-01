package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
	"log"
)

func main() {
	var config chainedkv.ServerConfig
	err := util.ReadJSONConfig("test/config/server2_config.json", &config)
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}
	stracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	server := chainedkv.Server{}
	err = server.Start(config.ServerId, config.CoordAddr, config.ServerAddr, config.ServerListenAddr, config.ClientListenAddr, stracer)
	if err != nil {
		log.Fatalf("Error starting server: %s", err)
	}
}