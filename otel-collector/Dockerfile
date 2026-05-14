# syntax=docker/dockerfile:1.7
#
# Two-stage build for the custom OpenData-flavored OTel collector.
# Image target: ghcr.io/opendata-oss/otel-collector:<sha>
#
# Build from repo root:
#   docker build -f otel-collector/Dockerfile -t ghcr.io/opendata-oss/otel-collector:dev .

FROM golang:1.26-bookworm AS builder
WORKDIR /src

# Pin to a known-good ocb release that matches the collector versions
# declared in builder-config.yaml. Mismatched ocb + component versions
# are the most common build failure for custom collectors.
ARG OCB_VERSION=v0.152.0
RUN go install go.opentelemetry.io/collector/cmd/builder@${OCB_VERSION}

COPY . /src
# Builder runs from the repo root; output_path in builder-config.yaml
# points at `./otel-collector/_build` so the binary lands there.
RUN /go/bin/builder --config otel-collector/builder-config.yaml
RUN cp /src/otel-collector/_build/otelcol-opendata /otelcol-opendata

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /otelcol-opendata /otelcol-opendata
EXPOSE 4317 4318 8888 13133
ENTRYPOINT ["/otelcol-opendata"]
CMD ["--config", "/etc/otelcol/config.yaml"]
