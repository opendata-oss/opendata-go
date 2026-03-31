.PHONY: fmt lint test exporter-e2e compat-test build check

EXPORTER_MODULE := exporter/opendataexporter
ROOT_GO_FILES := $(shell find . -name '*.go' -not -path './$(EXPORTER_MODULE)/*' -not -path './.git/*')
ROOT_GO_PACKAGES := ./ingest ./objstore ./test/compat-producer

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

compat-test:
	./scripts/compat-test.sh

# Build all packages
build:
	go build $(ROOT_GO_PACKAGES)
	$(MAKE) -C $(EXPORTER_MODULE) build

# Run all checks (fmt, lint, test)
check: fmt lint test
