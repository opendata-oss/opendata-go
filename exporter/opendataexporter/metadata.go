package opendataexporter

import "go.opentelemetry.io/collector/component"

const (
	componentTypeStr = "opendata"

	// MetadataVersion is the version byte for OpenData queue metadata payloads.
	MetadataVersion uint8 = 1
	// SignalTypeMetrics identifies metrics payloads in OpenData queue metadata.
	SignalTypeMetrics uint8 = 1
	// PayloadEncodingOTLP identifies OTLP protobuf-encoded payloads.
	PayloadEncodingOTLP uint8 = 1
)

var componentType = component.MustNewType(componentTypeStr)

// EncodeMetadata encodes the OpenData queue metadata header.
func EncodeMetadata(signalType, encoding uint8) []byte {
	return []byte{MetadataVersion, signalType, encoding, 0}
}
