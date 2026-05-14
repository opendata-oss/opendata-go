# otel-collector/ — custom OTel collector image (OpenData exporter bundled)

The Phase 8 cell gateway runs an OTel collector that needs the
in-repo `opendataexporter`. Stock `otelcol-contrib` doesn't include it;
this directory bakes a custom collector binary instead.

## Build locally

```
go install go.opentelemetry.io/collector/cmd/builder@v0.152.0
builder --config otel-collector/builder-config.yaml
# Output: otel-collector/_build/otelcol-opendata
```

## Build the image

```
docker build -f otel-collector/Dockerfile -t ghcr.io/opendata-oss/otel-collector:dev .
```

(From the repo root — the Dockerfile expects `otel-collector/` and
`exporter/opendataexporter/` to both be visible in the context.)

## CI

`.github/workflows/build-otel-collector.yml` is a `workflow_dispatch`
that mirrors the pattern in `build-image.yml` (opendata-contrib). Run
from the GitHub UI or:

```
gh workflow run "Build OTel collector image" \
  --ref odb-ht/3-producer-pipelining \
  -R opendata-oss/opendata-go
```

Output: `ghcr.io/opendata-oss/otel-collector:<safe-ref>-<sha>`.

## Versioning

`builder-config.yaml` pins:
- ocb / collector core: `v0.152.0` (matches what
  `exporter/opendataexporter/go.mod` already imports — keep them in
  lockstep when bumping)
- opendataexporter: in-tree via the `replaces` block

Bumping the core: edit `otelcol_version` and every `v0.152.0` in
`builder-config.yaml` together, plus the matching `v1.x` lines in
`exporter/opendataexporter/go.mod`.
