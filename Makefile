.PHONY: fmt lint test build check

# Format all Go files (equivalent to cargo fmt)
fmt:
	goimports -w .
	gofmt -w .

# Run linter (equivalent to cargo clippy -- -D warnings)
lint:
	golangci-lint run ./...

# Run all tests
test:
	go test -race ./...

# Build all packages
build:
	go build ./...

# Run all checks (fmt, lint, test)
check: fmt lint test
