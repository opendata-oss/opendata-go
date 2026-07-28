.PHONY: fmt lint test exporter-e2e logdb-integration compat-test build check

EXPORTER_MODULE := exporter/opendataexporter
ROOT_GO_FILES := $(shell find . -name '*.go' -not -path './$(EXPORTER_MODULE)/*' -not -path './.git/*')
ROOT_GO_PACKAGES := ./buffer ./logdb ./objstore ./test/compat-producer

# Format all Go files (equivalent to cargo fmt)
fmt:
	goimports -w $(ROOT_GO_FILES)
	gofmt -w $(ROOT_GO_FILES)
	$(MAKE) -C $(EXPORTER_MODULE) fmt

# Run linter (equivalent to cargo clippy -- -D warnings)
lint:
	golangci-lint run $(ROOT_GO_PACKAGES)
	$(MAKE) -C $(EXPORTER_MODULE) lint

# Run all tests
test:
	go test -race $(ROOT_GO_PACKAGES)
	$(MAKE) -C $(EXPORTER_MODULE) test

exporter-e2e:
	$(MAKE) -C $(EXPORTER_MODULE) e2e

# Integration tests for the logdb client against a real Log server.
# Requires docker, which starts ghcr.io/opendata-oss/log. Override the image
# with LOGDB_LOG_IMAGE, or point at an already-running server with
# LOGDB_BASE_URL to skip the container entirely.
logdb-integration:
	go test -tags=integration -count=1 -v ./logdb

compat-test:
	./scripts/compat-test.sh

# Build all packages
build:
	go build $(ROOT_GO_PACKAGES)
	$(MAKE) -C $(EXPORTER_MODULE) build

# Run all checks (fmt, lint, test)
check: fmt lint test
