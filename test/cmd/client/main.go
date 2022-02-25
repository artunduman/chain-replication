package main

import (
	"log"
	"time"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/kvslib"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config chainedkv.ClientConfig
	err := util.ReadJSONConfig("test/config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})

	client := kvslib.NewKVS()
	notifCh, err := client.Start(tracer, config.ClientID, config.CoordIPPort, config.LocalCoordIPPort, config.LocalHeadServerIPPort, config.LocalTailServerIPPort, config.ChCapacity)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	// Put a key-value pair
	op, err := client.Put(tracer, "clientID1", "key1", "value1")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err, op)

	time.Sleep(5)

	// Get a key's value
	op, err = client.Get(tracer, "clientID1", "key1")
	util.CheckErr(err, "Error getting value %v, opId: %v\b", err, op)

	for i := 0; i < 2; i++ {
		result := <-notifCh
		log.Println(result)
	}
	client.Stop()
}
