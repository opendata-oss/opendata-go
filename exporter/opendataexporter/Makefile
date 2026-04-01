.PHONY: fmt lint test e2e build check

fmt:
	goimports -w .
	gofmt -w .

lint:
	golangci-lint run .

test:
	go test -race ./...

e2e:
	go test -tags=e2e -v ./e2e

build:
	go build ./...

check: fmt lint test
