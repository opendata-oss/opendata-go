// compat-consumer reads batches produced by the Go compat-producer and
// verifies the data matches expectations.
//
// It connects to an S3-compatible store (MinIO in CI), reads all queued batches
// via the Rust Collector, and asserts that the entries match what the Go
// producer wrote.

use std::env;
use std::process;

use bytes::Bytes;

use common::ObjectStoreConfig;
use common::storage::config::AwsObjectStoreConfig;
use ingest::{Collector, CollectorConfig};

#[tokio::main]
async fn main() {
    let region = env_or("AWS_REGION", "us-east-1");
    let bucket = env_or("S3_BUCKET", "compat-test");

    // The Rust object_store crate reads AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID,
    // and AWS_SECRET_ACCESS_KEY from the environment automatically via
    // AmazonS3Builder::from_env().
    let config = CollectorConfig {
        object_store: ObjectStoreConfig::Aws(AwsObjectStoreConfig { region, bucket }),
        manifest_path: "ingest/manifest".to_string(),
    };

    let collector = Collector::new(config).expect("failed to create collector");
    collector
        .initialize(None)
        .await
        .expect("failed to initialize collector");

    // Expected batches from the Go producer.
    let expected: Vec<(Vec<&str>, &str)> = vec![
        (vec!["hello", "world"], r#"{"batch":1}"#),
        (vec!["foo", "bar", "baz"], r#"{"batch":2}"#),
        (vec!["single-entry"], r#"{"batch":3}"#),
    ];

    let mut batch_idx = 0;
    loop {
        match collector.next_batch().await {
            Ok(Some(batch)) => {
                if batch_idx >= expected.len() {
                    eprintln!("ERROR: received more batches than expected");
                    process::exit(1);
                }

                let (ref expected_strs, expected_meta) = expected[batch_idx];
                let expected_entries: Vec<Bytes> =
                    expected_strs.iter().map(|s| Bytes::from(*s)).collect();

                if batch.entries != expected_entries {
                    eprintln!(
                        "ERROR: batch {} entries mismatch\n  expected: {:?}\n  actual:   {:?}",
                        batch_idx + 1,
                        expected_entries,
                        batch.entries
                    );
                    process::exit(1);
                }

                if batch.metadata.len() != 1 {
                    eprintln!(
                        "ERROR: batch {} expected 1 metadata range, got {}",
                        batch_idx + 1,
                        batch.metadata.len()
                    );
                    process::exit(1);
                }
                if batch.metadata[0].start_index != 0 {
                    eprintln!(
                        "ERROR: batch {} metadata start_index: expected 0, got {}",
                        batch_idx + 1,
                        batch.metadata[0].start_index
                    );
                    process::exit(1);
                }
                if batch.metadata[0].payload != Bytes::from(expected_meta) {
                    eprintln!(
                        "ERROR: batch {} metadata payload mismatch\n  expected: {:?}\n  actual:   {:?}",
                        batch_idx + 1,
                        expected_meta,
                        batch.metadata[0].payload
                    );
                    process::exit(1);
                }

                println!(
                    "consumed batch {} (seq={}, entries={}, location={})",
                    batch_idx + 1,
                    batch.sequence,
                    batch.entries.len(),
                    batch.location
                );

                collector
                    .ack(batch.sequence)
                    .await
                    .expect("failed to ack batch");

                batch_idx += 1;
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("ERROR: next_batch failed: {}", e);
                process::exit(1);
            }
        }
    }

    if batch_idx != expected.len() {
        eprintln!(
            "ERROR: expected {} batches, got {}",
            expected.len(),
            batch_idx
        );
        process::exit(1);
    }

    collector.close().await.expect("failed to close collector");
    println!("consumer: done, {} batches verified OK", batch_idx);
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
