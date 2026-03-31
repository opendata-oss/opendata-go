package opendataexporter

import "go.opentelemetry.io/collector/component"

const (
	componentTypeStr          = "opendata"
	MetadataVersion     uint8 = 1
	SignalTypeMetrics   uint8 = 1
	PayloadEncodingOTLP uint8 = 1
)

var componentType = component.MustNewType(componentTypeStr)

// EncodeMetadata encodes the OpenData queue metadata header.
func EncodeMetadata(signalType, encoding uint8) []byte {
	return []byte{MetadataVersion, signalType, encoding, 0}
}
