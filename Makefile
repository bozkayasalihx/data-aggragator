build: 
	go build -o bin/aggragator
run: build
	bin/aggragator

reader: 
	go run ./pub/publisher.go