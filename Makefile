.PHONY: client tracing clean all

all: server coord client tracing

server:
	go build -o test/bin/server ./test/cmd/server

coord:
	go build -o test/bin/coord ./test/cmd/coord

client:
	go build -o test/bin/client ./test/cmd/client

tracing:
	go build -o test/bin/tracing ./test/cmd/tracing-server

clean:
	rm -f test/bin/*
