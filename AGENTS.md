# AGENTS

## Scope

This repository contains a root Go module plus a nested exporter module at `exporter/opendataexporter`.

Key areas:
- `ingest`: batching, durability, manifest enqueueing
- `objstore`: object storage abstractions and implementations
- `exporter/opendataexporter`: OpenTelemetry Collector exporter module
- `test/compat-producer`: compatibility test helper

## Working Style

- Keep changes minimal and local to the affected package.
- Preserve existing package boundaries. `ingest` should stay reusable and should not take direct Collector dependencies.
- Prefer adding observability through small interfaces/hooks when instrumentation crosses package boundaries.
- Use ASCII unless the file already requires otherwise.

## Formatting

Run:

```sh
make fmt
```

Or for the exporter module only:

```sh
make -C exporter/opendataexporter fmt
```

## Tests

Run root tests:

```sh
make test
```

Run exporter module tests only:

```sh
make -C exporter/opendataexporter test
```

Run exporter e2e tests:

```sh
make exporter-e2e
```

Notes:
- Exporter e2e tests require Docker.
- The e2e path builds a custom collector binary and can take noticeable time.

## Lint

`golangci-lint` must be installed and available on `PATH`.

Working lint commands:

```sh
GOCACHE=/tmp/opendata-go-lint/root-go-build \
GOLANGCI_LINT_CACHE=/tmp/opendata-go-lint/root-golangci-lint \
golangci-lint run ./ingest ./objstore ./test/compat-producer
```

```sh
make -C exporter/opendataexporter lint
```

Important:
- The nested exporter module already uses temp caches in its `Makefile`.
- The repo-root `make lint` may still need the same temp-cache pattern if it fails with `no go files to analyze` or cache permission errors.

## Build

Run:

```sh
make build
```

Or for the exporter module only:

```sh
make -C exporter/opendataexporter build
```

## Repository Notes

- The exporter module is its own Go module with its own `go.mod`.
- Collector self-telemetry for `opendataexporter` is exposed through `service.telemetry.metrics`, not the normal service pipelines.
- For exporter self-metrics, Prometheus pull via the collector telemetry endpoint is the simplest validation path.
