.PHONY: fmt lint test build check

fmt:
	goimports -w .
	gofmt -w .

lint:
	golangci-lint run ./...

test:
	go test -race ./...

build:
	go build ./...

check: fmt lint test
