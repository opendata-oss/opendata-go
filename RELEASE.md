# Release Process

This repository contains two Go modules:

- `github.com/opendata-oss/opendata-go`
- `github.com/opendata-oss/opendata-go/exporter/opendataexporter`

The exporter module depends on the root module, so releases must be tagged in a way that Go module resolution understands.

## Versioning

The modules are versioned independently.

- Root module tags use `vX.Y.Z`
- Exporter module tags use `exporter/opendataexporter/vA.B.C`

The exporter module's `go.mod` must require a released root module version, but it does not need to match the exporter module version.

## Release Order

1. Update code, changelog/docs, and dependency versions as needed.
2. If releasing the exporter module, confirm [`exporter/opendataexporter/go.mod`](exporter/opendataexporter/go.mod) requires an already-released root module version.
3. Merge to `main`.
4. Create and push the module tag you are releasing.

Example:

```bash
git tag v0.3.0
git push origin v0.3.0

git tag exporter/opendataexporter/v0.5.0
git push origin exporter/opendataexporter/v0.5.0
```

## What CI Does On Tag Push

When either release tag is pushed, GitHub Actions will:

- validate the tag format
- validate that the tagged module version matches the repository state
- run the relevant test suite for the tagged module
- create a GitHub Release for that tag

For exporter releases, CI also verifies that the root module version required by the exporter already exists as a root tag.

## Consumer Usage

Consumers of the exporter are expected to build their own OpenTelemetry Collector distributions and reference the exporter module version directly in their collector builder config.

Example:

```yaml
exporters:
  - gomod: github.com/opendata-oss/opendata-go/exporter/opendataexporter v0.5.0
    import: github.com/opendata-oss/opendata-go/exporter/opendataexporter
```

## Maintainer Checklist

- `make check`
- `make compat-test`
- `make exporter-e2e`
- if releasing the exporter, confirm [`exporter/opendataexporter/go.mod`](exporter/opendataexporter/go.mod) requires an already-tagged root module version
- push the root tag `vX.Y.Z` or exporter tag `exporter/opendataexporter/vA.B.C`
