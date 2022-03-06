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

	for c := 0; c < 2; c++ {
		client := kvslib.NewKVS()
		notifCh, err := client.Start(tracer, config.ClientID, config.CoordIPPort, config.LocalCoordIPPort, config.LocalHeadServerIPPort, config.LocalTailServerIPPort, config.ChCapacity)
		if err != nil {
			log.Fatalf("Error starting client: %v\n", err)
		}

		for i := 0; i < 10; i++ {
			_, err := client.Put(tracer, "client"+strconv.Itoa(c), strconv.Itoa(i), strconv.Itoa(i))
			if err != nil {
				log.Printf("Error putting key-value pair: %v\n", err)
			}
		}

		for i := 0; i < 10; i++ {
			result := <-notifCh
			log.Println(result)
		}
		client.Stop()
		time.Sleep(time.Second)
	}
}
