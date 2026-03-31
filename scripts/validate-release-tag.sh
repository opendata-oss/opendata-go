#!/usr/bin/env bash

set -euo pipefail

tag="${1:?tag is required}"

root_module="github.com/opendata-oss/opendata-go"
exporter_module="github.com/opendata-oss/opendata-go/exporter/opendataexporter"

root_declared_module="$(go list -m -f '{{.Path}}')"
exporter_declared_module="$(
  cd exporter/opendataexporter
  go list -m -f '{{.Path}}'
)"
exporter_root_requirement="$(
  cd exporter/opendataexporter
  go list -m -f '{{if eq .Path "'"${root_module}"'"}}{{.Version}}{{end}}' all | head -n 1
)"

if [[ "${root_declared_module}" != "${root_module}" ]]; then
  echo "root module path mismatch: expected ${root_module}, got ${root_declared_module}" >&2
  exit 1
fi

if [[ "${exporter_declared_module}" != "${exporter_module}" ]]; then
  echo "exporter module path mismatch: expected ${exporter_module}, got ${exporter_declared_module}" >&2
  exit 1
fi

case "${tag}" in
  v*)
    echo "validated root release tag ${tag}"
    ;;
  exporter/opendataexporter/v*)
    if [[ -z "${exporter_root_requirement}" ]]; then
      echo "exporter go.mod must require ${root_module}" >&2
      exit 1
    fi
    if ! git rev-parse -q --verify "refs/tags/${exporter_root_requirement}" >/dev/null; then
      echo "missing required root tag ${exporter_root_requirement}; tag the root dependency before releasing the exporter" >&2
      exit 1
    fi
    echo "validated exporter release tag ${tag}"
    ;;
  *)
    echo "unsupported release tag format: ${tag}" >&2
    exit 1
    ;;
esac
