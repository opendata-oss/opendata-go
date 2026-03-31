package opendataexporter

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	valid := &Config{
		ObjectStore: ObjectStoreConfig{
			Type:   objectStoreTypeS3,
			Bucket: "metrics-bucket",
			Region: "us-west-2",
		},
		DataPathPrefix: "ingest/otel/metrics/data",
		ManifestPath:   "ingest/otel/metrics/manifest",
		FlushInterval:  10 * time.Second,
		FlushSizeBytes: 1024,
		Compression:    compressionZstd,
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "valid"},
		{
			name: "missing object store type",
			mutate: func(cfg *Config) {
				cfg.ObjectStore.Type = ""
			},
			wantErr: true,
		},
		{
			name: "unsupported object store type",
			mutate: func(cfg *Config) {
				cfg.ObjectStore.Type = "gcs"
			},
			wantErr: true,
		},
		{
			name: "missing bucket",
			mutate: func(cfg *Config) {
				cfg.ObjectStore.Bucket = ""
			},
			wantErr: true,
		},
		{
			name: "missing region",
			mutate: func(cfg *Config) {
				cfg.ObjectStore.Region = ""
			},
			wantErr: true,
		},
		{
			name: "missing data path prefix",
			mutate: func(cfg *Config) {
				cfg.DataPathPrefix = ""
			},
			wantErr: true,
		},
		{
			name: "missing manifest path",
			mutate: func(cfg *Config) {
				cfg.ManifestPath = ""
			},
			wantErr: true,
		},
		{
			name: "invalid flush interval",
			mutate: func(cfg *Config) {
				cfg.FlushInterval = 0
			},
			wantErr: true,
		},
		{
			name: "invalid flush size",
			mutate: func(cfg *Config) {
				cfg.FlushSizeBytes = 0
			},
			wantErr: true,
		},
		{
			name: "invalid compression",
			mutate: func(cfg *Config) {
				cfg.Compression = "gzip"
			},
			wantErr: true,
		},
		{
			name: "none compression",
			mutate: func(cfg *Config) {
				cfg.Compression = compressionNone
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *valid
			cfg.ObjectStore = valid.ObjectStore
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}

			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
