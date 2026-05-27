.PHONY: build test run clean docker-up docker-down

build:
	go build -o bin/broker ./cmd/broker

test:
	go test -v -race ./...

run: build
	BROKER_ID=1 PORT=8081 DATA_DIR=./data/1 NODES=1:localhost:8081 ./bin/broker

docker-up:
	docker-compose up --build -d

docker-down:
	docker-compose down -v

clean:
	rm -rf bin/ data/