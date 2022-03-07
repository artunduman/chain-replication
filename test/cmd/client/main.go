// Not used by integration.go

package main

import (
	"log"
	"net"
	"os"
	"strconv"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/kvslib"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config chainedkv.ClientConfig
	err := util.ReadJSONConfig("test/config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <clientId>", os.Args[0])
	}
	clientId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Usage: %s <clientId>", os.Args[0])
	}

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: "client" + strconv.Itoa(clientId),
		Secret:         config.Secret,
	})

	client := kvslib.NewKVS()
	clientHost, _, _ := net.SplitHostPort(config.LocalCoordIPPort)

	ports := make([]int, 3)
	for i := 0; i < 3; i++ {
		port, err := util.GetFreeTCPPort(clientHost)
		if err != nil {
			log.Fatal("Error getting free port: ", err)
		}
		ports[i] = port
	}

	notifCh, err := client.Start(
		tracer,
		"client"+strconv.Itoa(clientId),
		config.CoordIPPort,
		net.JoinHostPort(clientHost, strconv.Itoa(ports[0])),
		net.JoinHostPort(clientHost, strconv.Itoa(ports[1])),
		net.JoinHostPort(clientHost, strconv.Itoa(ports[2])),
		config.ChCapacity,
	)
	if err != nil {
		log.Fatalf("Error starting client: %v\n", err)
	}

	for i := 0; i < 20; i++ {
		_, err := client.Put(tracer, "client"+strconv.Itoa(clientId), strconv.Itoa(i), "value1")
		if err != nil {
			log.Println("Error putting key: ", err)
		}
		_, err = client.Get(tracer, "client"+strconv.Itoa(clientId), strconv.Itoa(i))
		if err != nil {
			log.Println("Error getting key: ", err)
		}
	}

	for i := 0; i < 40; i++ {
		result := <-notifCh
		log.Println(result)
	}
	client.Stop()
}
