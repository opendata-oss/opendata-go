// compat-producer writes test batches to an S3-compatible store using the Go
// ingestor, then exits. The Rust compat-consumer reads and verifies them.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/opendata-oss/opendata-go/ingest"
	"github.com/opendata-oss/opendata-go/objstore"
)

func main() {
	endpoint := envOrDefault("S3_ENDPOINT", "http://localhost:9000")
	bucket := envOrDefault("S3_BUCKET", "compat-test")
	accessKey := envOrDefault("AWS_ACCESS_KEY_ID", "minioadmin")
	secretKey := envOrDefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
	region := envOrDefault("AWS_REGION", "us-east-1")

	ctx := context.Background()

	cfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		log.Fatalf("failed to load aws config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})

	store := objstore.NewS3(client, bucket)

	config := ingest.IngestorConfig{
		DataPathPrefix:    "ingest",
		ManifestPath:      "ingest/manifest",
		FlushInterval:     24 * time.Hour,
		FlushSizeBytes:    64 * 1024 * 1024,
		MaxBufferedInputs: 1000,
		BatchCompression:  compressionFromEnv(),
	}

	ing := ingest.NewIngestor(store, config)

	// Write 3 separate batches with known data.
	batches := []struct {
		entries  [][]byte
		metadata []byte
	}{
		{
			entries:  [][]byte{[]byte("hello"), []byte("world")},
			metadata: []byte(`{"batch":1}`),
		},
		{
			entries:  [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")},
			metadata: []byte(`{"batch":2}`),
		},
		{
			entries:  [][]byte{[]byte("single-entry")},
			metadata: []byte(`{"batch":3}`),
		},
	}

	for i, b := range batches {
		if _, err := ing.Ingest(b.entries, b.metadata); err != nil {
			log.Fatalf("ingest batch %d failed: %v", i+1, err)
		}
		if err := ing.Flush(ctx); err != nil {
			log.Fatalf("flush batch %d failed: %v", i+1, err)
		}
		fmt.Printf("produced batch %d (%d entries)\n", i+1, len(b.entries))
	}

	if err := ing.Close(ctx); err != nil {
		log.Fatalf("close failed: %v", err)
	}

	fmt.Println("producer: done, 3 batches written")
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func compressionFromEnv() ingest.CompressionType {
	switch os.Getenv("BATCH_COMPRESSION") {
	case "zstd":
		return ingest.CompressionZstd
	default:
		return ingest.CompressionNone
	}
}
