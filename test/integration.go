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
	executeSync("make", "-C", "./test/", "all")
}

func clean() {
	executeSync("make", "-C", "./test/", "clean")
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

func test0(processes map[string]*os.Process) {
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

func test1(processes map[string]*os.Process) {
	// Don't wait for servers to be up, let coord handle it
	client, notifyCh, tracer, clientId := startClient(2)
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

func test2(processes map[string]*os.Process) {
	// Kill head server before puts are acked
	client, notifyCh, tracer, clientId := startClient(3)
	defer client.Stop()

	for i := 0; i < 10; i++ {
		_, err := client.Put(tracer, clientId, strconv.Itoa(i), strconv.Itoa(i))
		if err != nil {
			log.Fatal("Error putting key: ", err)
		}
	}

	processes["server1"].Kill()
	for i := 0; i < 10; i++ {
		result := <-notifyCh
		log.Println(result)
	}
}

func test3(processes map[string]*os.Process) {
	// Kill two neighboring servers simultaneously
	// Wait for RTT to get calculated
	time.Sleep(time.Second * 2)
	processes["server2"].Kill()
	processes["server3"].Kill()
	// Wait for it to die
	processes["server3"].Wait()
	time.Sleep(time.Second * 3)
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
	tests := []func(map[string]*os.Process){
		test0,
		test1,
		test2,
		test3,
	}
	for testIndex, test := range tests {
		log.Println("Starting test:", testIndex)
		runTest(test, testIndex)
	}
}

func runTest(test func(map[string]*os.Process), testIndex int) {
	processes := setup(10)
	defer teardown(processes, testIndex)
	test(processes)
}
