.PHONY: all run test race bench clean

all: test run

run:
	go run cmd/gopulse/main.go

test:
	go test -v ./...

race:
	go test -v -race ./...

bench:
	go test -v -bench=. -benchmem ./...

clean:
	rm -rf bin/
