package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/kvslib"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func build() {
	executeSync("make", "all")
}

func clean() {
	executeSync("make", "clean")
}

func setup(numServers int) map[string]*os.Process {
	processes := make(map[string]*os.Process)

	processes["tracing"] = startTracingServer()
	// Wait for it to start the server
	time.Sleep(time.Millisecond * 100)

	processes["coord"] = startCoord(numServers)
	time.Sleep(time.Millisecond * 100)

	// Join in reverse order
	for i := numServers; i > 0; i-- {
		processes["server"+strconv.Itoa(i)] = startServer(i)
	}
	return processes
}

func executeSync(command string, args ...string) {
	cmd := exec.Command(command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
}

func executeAsync(command string, args ...string) *os.Process {
	cmd := exec.Command(command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	return cmd.Process
}

func startTracingServer() *os.Process {
	return executeAsync("./test/bin/tracing")
}

func startCoord(numServers int) *os.Process {
	return executeAsync("./test/bin/coord", strconv.Itoa(numServers))
}

func startServer(serverId int) *os.Process {
	return executeAsync("./test/bin/server", strconv.Itoa(serverId))
}

func startClient(clientId int) (*kvslib.KVS, kvslib.NotifyChannel, *tracing.Tracer, string) {
	var config chainedkv.ClientConfig
	err := util.ReadJSONConfig("test/config/client_config.json", &config)
	if err != nil {
		log.Fatal("Error reading config file: ", err)
	}
	clientIdStr := "client" + strconv.Itoa(clientId)
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: clientIdStr,
		Secret:         config.Secret,
	})

	client := kvslib.NewKVS()
	clientHost, _, _ := net.SplitHostPort(config.LocalCoordIPPort)
	//_, localHeadBaseStr, _ := net.SplitHostPort(config.LocalHeadServerIPPort)
	//_, localTailBaseStr, _ := net.SplitHostPort(config.LocalTailServerIPPort)
	//
	//localCoordBase, _ := strconv.Atoi(localCoordBaseStr)
	//localHeadBase, _ := strconv.Atoi(localHeadBaseStr)
	//localTailBase, _ := strconv.Atoi(localTailBaseStr)

	ports := make([]int, 3)
	for i := 0; i < 3; i++ {
		port, err := util.GetFreeTCPPort(clientHost)
		if err != nil {
			log.Fatal("Error getting free port: ", err)
		}
		ports[i] = port
	}

	notifyCh, err := client.Start(
		tracer,
		clientIdStr,
		config.CoordIPPort,
		net.JoinHostPort(clientHost, strconv.Itoa(ports[0])),
		net.JoinHostPort(clientHost, strconv.Itoa(ports[1])),
		net.JoinHostPort(clientHost, strconv.Itoa(ports[2])),
		config.ChCapacity,
	)
	if err != nil {
		log.Fatal("Error starting client: ", err)
	}
	return client, notifyCh, tracer, "client" + strconv.Itoa(clientId)
}

func test1() {
	//Wait for servers to be up (easy case)
	time.Sleep(time.Millisecond * 1000)

	client, notifyCh, tracer, clientId := startClient(1)
	defer client.Stop()

	for i := 0; i < 10; i++ {
		_, err := client.Put(tracer, clientId, strconv.Itoa(i), strconv.Itoa(i))
		if err != nil {
			log.Fatal("Error putting key: ", err)
		}
	}

	for i := 0; i < 10; i++ {
		result := <-notifyCh
		log.Println(result)
	}
}

func test2() {
	// Don't wait for servers to be up, let coord handle it
	client, notifyCh, tracer, clientId := startClient(1)
	defer client.Stop()

	for i := 0; i < 10; i++ {
		_, err := client.Put(tracer, clientId, strconv.Itoa(i), strconv.Itoa(i))
		if err != nil {
			log.Fatal("Error putting key: ", err)
		}
	}

	for i := 0; i < 10; i++ {
		result := <-notifyCh
		log.Println(result)
	}
}

func test3() {
	// Test if server can continue after client crashes
}

func teardown(processes map[string]*os.Process, testIndex int) {
	for _, process := range processes {
		process.Kill()
	}
	executeSync(
		"mv", "./trace_output.log", "test/logs/tracing_"+strconv.Itoa(testIndex)+"_"+time.Now().String()+".log",
	)
	executeSync(
		"mv", "./shiviz_output.log", "test/logs/shiviz_"+strconv.Itoa(testIndex)+"_"+time.Now().String()+".log",
	)
}

func main() {
	build()
	defer clean()
	tests := []func(){
		test1,
	}
	for testIndex, test := range tests {
		runTest(test, testIndex)
	}
}

func runTest(test func(), testIndex int) {
	processes := setup(10)
	defer teardown(processes, testIndex)
	test()
}
