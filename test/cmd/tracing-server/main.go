// Used by integration.go

package main

import (
	"github.com/DistributedClocks/tracing"
	"log"
)

func main() {
	tracingServer := tracing.NewTracingServerFromFile("test/config/tracing_server_config.json")

	err := tracingServer.Open()
	if err != nil {
		log.Fatal(err)
	}

	tracingServer.Accept() // serve requests forever
}
