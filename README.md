# OpenData Go

[![Discord](https://img.shields.io/badge/discord-join-7289DA?style=flat-square&logo=discord)](https://discord.gg/CsAQJ2AJGU)
[![GitHub License](https://img.shields.io/github/license/opendata-oss/opendata?style=flat-square)](LICENSE)

Go bindings for the [OpenData](https://github.com/opendata-oss/opendata) project.

## Packages

| Package | Description |
|---------|-------------|
| [`buffer`](buffer/) | Stateless buffering library |
| [`objstore`](objstore/) | Object storage abstraction with S3 and in-memory implementations. |

## Releases

This repository publishes Go modules by git tag rather than shipping prebuilt collector binaries.

- Root module tags use `vX.Y.Z`
- Exporter module tags use `exporter/opendataexporter/vX.Y.Z`

Release details are documented in [`RELEASE.md`](RELEASE.md).
