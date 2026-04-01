.PHONY: fmt lint test e2e build check

LINT_CACHE_ROOT ?= /tmp/opendataexporter-lint

fmt:
	goimports -w .
	gofmt -w .

lint:
	mkdir -p $(LINT_CACHE_ROOT)/go-build $(LINT_CACHE_ROOT)/golangci-lint
	GOCACHE=$(LINT_CACHE_ROOT)/go-build GOLANGCI_LINT_CACHE=$(LINT_CACHE_ROOT)/golangci-lint golangci-lint run .

test:
	go test -race ./...

e2e:
	go test -tags=e2e -v ./e2e

build:
	go build ./...

check: fmt lint test
