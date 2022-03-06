package main

import (
	"log"
	"strconv"
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

	for i := 0; i < 10; i++ {
		_, err := client.Put(tracer, "client1", strconv.Itoa(i), strconv.Itoa(i))
		if err != nil {
			log.Println("Error putting key-value pair: %v\n", err)
		}
	}

	for i := 0; i < 10; i++ {
		result := <-notifCh
		log.Println(result)
	}
	client.Stop()
	time.Sleep(time.Second)
	client.Stop()
}
