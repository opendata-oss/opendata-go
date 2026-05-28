// Throughput/latency benchmarks for the producer and exporter paths.
//
// One binary, two modes selected by --mode:
//
//	--mode exporter:
//	    Drives Producer.Append + WriteHandle.Watcher.AwaitDurable in
//	    the path an OTel exporter would use. Stages: marshal,
//	    enqueue, await_durable.
//
//	--mode producer:
//	    Drives EncodeBatch + objstore.Put + manifest append (via
//	    Producer.Append/Flush) and decomposes per stage. Stages:
//	    encode, compress (no-op when compression=none), object_put,
//	    manifest_append.
//
// Both modes emit schema-v2 artifacts (metadata.json, results.json,
// raw/run-N.metrics.jsonl) under --output-dir.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/opendata-oss/opendata-go/buffer"
	"github.com/opendata-oss/opendata-go/objstore"
)

type args struct {
	mode                    string
	recordsPerBatch         int
	batches                 int
	recordBytes             int
	iterations              int
	seed                    int64
	outputDir               string
	unitID                  string
	changeSlug              string
	notes                   string
	flushSizeBytes          int
	encodeConcurrency       int
	uploadConcurrency       int
	manifestAppendBatchSize int
	// varied_param fields per the schema-v2 artifact contract:
	// {kind, name, baseline, candidate}.
	variedParamKind      string
	variedParamName      string
	variedParamBaseline  string
	variedParamCandidate string
}

func parseArgs() args {
	var a args
	flag.StringVar(&a.mode, "mode", "exporter", "exporter | producer")
	flag.IntVar(&a.recordsPerBatch, "records-per-batch", 1000, "records (entries) per Buffer batch")
	flag.IntVar(&a.batches, "batches", 100, "total batches in the workload")
	flag.IntVar(&a.recordBytes, "record-bytes", 320, "approx bytes per record")
	flag.IntVar(&a.iterations, "iterations", 5, "iterations for stat aggregation")
	flag.Int64Var(&a.seed, "seed", 42, "PRNG seed")
	flag.StringVar(&a.outputDir, "output-dir", "", "schema-v2 output dir (required)")
	flag.StringVar(&a.unitID, "unit-id", "", "1.1 or 1.2; defaults from --mode")
	flag.StringVar(&a.changeSlug, "change-slug", "baseline", "run-id slug")
	flag.StringVar(&a.notes, "notes", "", "free-text notes for metadata.json")
	flag.IntVar(&a.flushSizeBytes, "flush-size-bytes", 1, "Producer flush threshold; 1 forces per-Append flush")
	flag.IntVar(&a.encodeConcurrency, "encode-concurrency", 1, "EncodeConcurrency")
	flag.IntVar(&a.uploadConcurrency, "upload-concurrency", 1, "UploadConcurrency")
	flag.IntVar(&a.manifestAppendBatchSize, "manifest-append-batch-size", 1, "ManifestAppendBatchSize")
	flag.StringVar(&a.variedParamKind, "varied-param-kind", "", "metadata.varied_param.kind: config|code|schema|infra (required to populate varied_param)")
	flag.StringVar(&a.variedParamName, "varied-param-name", "", "metadata.varied_param.name: dotted path or short ident (e.g. ManifestAppendBatchSize)")
	flag.StringVar(&a.variedParamBaseline, "varied-param-baseline", "", "metadata.varied_param.baseline: the control value (e.g. 1)")
	flag.StringVar(&a.variedParamCandidate, "varied-param-candidate", "", "metadata.varied_param.candidate: the variant value (e.g. 16)")
	flag.Parse()
	if a.outputDir == "" {
		fmt.Fprintln(os.Stderr, "--output-dir is required")
		os.Exit(2)
	}
	if a.unitID == "" {
		switch a.mode {
		case "exporter":
			a.unitID = "1.1"
		case "producer":
			a.unitID = "1.2"
		default:
			fmt.Fprintf(os.Stderr, "unknown --mode %q\n", a.mode)
			os.Exit(2)
		}
	}
	return a
}

// =============================================================================
// Workload synthesis (deterministic).
// =============================================================================

func synthRecord(rng *rand.Rand, target int) []byte {
	buf := make([]byte, target)
	rng.Read(buf) //nolint:errcheck
	return buf
}

func synthBatch(seed int64, batchIdx int, recordsPerBatch, recordBytes int) [][]byte {
	// Use a smaller, deterministic mix that fits cleanly in int64.
	mix := int64(0x9E37_79B9)
	rng := rand.New(rand.NewSource(seed + int64(batchIdx)*mix))
	out := make([][]byte, recordsPerBatch)
	for i := range out {
		out[i] = synthRecord(rng, recordBytes)
	}
	return out
}

// =============================================================================
// Per-iteration result + stage breakdown.
// =============================================================================

type iterationResult struct {
	Iteration         int
	StartedUnixMs     int64
	ElapsedSeconds    float64
	RecordsProcessed  int
	BytesProcessed    int64
	StageObservations map[string][]float64 // stage name -> per-batch elapsed seconds
}

// =============================================================================
// Mode: exporter (1.1)
// =============================================================================

func runExporterIteration(ctx context.Context, iter int, a args) (*iterationResult, error) {
	store := objstore.NewInMemory()
	cfg := buffer.ProducerConfig{
		ManifestPath:            fmt.Sprintf("bench-1.1/iter-%d/manifest", iter),
		DataPathPrefix:          fmt.Sprintf("bench-1.1/iter-%d/data", iter),
		FlushInterval:           50 * time.Millisecond,
		FlushSizeBytes:          a.flushSizeBytes,
		MaxBufferedInputs:       1024,
		BatchCompression:        buffer.CompressionNone,
		EncodeConcurrency:       a.encodeConcurrency,
		UploadConcurrency:       a.uploadConcurrency,
		ManifestAppendBatchSize: a.manifestAppendBatchSize,
	}
	p := buffer.NewProducer(store, cfg)

	startedMs := time.Now().UnixMilli()
	startWall := time.Now()
	stages := map[string][]float64{
		"marshal":       {},
		"enqueue":       {},
		"await_durable": {},
	}

	type pending struct {
		watcher  *buffer.DurabilityWatcher
		stagedAt time.Time
	}
	pendings := make([]pending, 0, a.batches)
	totalRecords := 0
	totalBytes := int64(0)

	for batchIdx := 0; batchIdx < a.batches; batchIdx++ {
		// Stage 1: marshal — build [][]byte (the work an OTel exporter
		// does to hand bytes to the buffer producer).
		t0 := time.Now()
		entries := synthBatch(a.seed, batchIdx, a.recordsPerBatch, a.recordBytes)
		stages["marshal"] = append(stages["marshal"], time.Since(t0).Seconds())

		for _, e := range entries {
			totalBytes += int64(len(e))
		}
		totalRecords += len(entries)

		// Stage 2: enqueue — Producer.Append (returns when the message
		// is queued to the background batch writer). stagedAt is the
		// "exporter handed bytes off" timestamp; await_durable below
		// measures end-to-end durable latency from this point.
		stagedAt := time.Now()
		h, err := p.Append(entries, nil)
		if err != nil {
			return nil, fmt.Errorf("Append: %w", err)
		}
		stages["enqueue"] = append(stages["enqueue"], time.Since(stagedAt).Seconds())

		pendings = append(pendings, pending{watcher: h.Watcher, stagedAt: stagedAt})
	}

	// Stage 3: await_durable — end-to-end durable latency from the
	// per-batch stagedAt (Append-call moment) to durability resolution.
	// We do NOT pre-Flush here: with FlushSizeBytes=1, the producer's
	// background writer flushes each batch automatically; pre-Flushing
	// would race the timing window and report ~zero. Flush after the
	// loop catches any unflushed remainder (defensive; should be empty).
	for _, pn := range pendings {
		if err := pn.watcher.AwaitDurable(ctx); err != nil {
			return nil, fmt.Errorf("AwaitDurable: %w", err)
		}
		stages["await_durable"] = append(stages["await_durable"], time.Since(pn.stagedAt).Seconds())
	}
	if err := p.Flush(ctx); err != nil {
		return nil, fmt.Errorf("Flush: %w", err)
	}

	if err := p.Close(ctx); err != nil {
		return nil, fmt.Errorf("Close: %w", err)
	}
	elapsed := time.Since(startWall).Seconds()

	return &iterationResult{
		Iteration:         iter,
		StartedUnixMs:     startedMs,
		ElapsedSeconds:    elapsed,
		RecordsProcessed:  totalRecords,
		BytesProcessed:    totalBytes,
		StageObservations: stages,
	}, nil
}

// =============================================================================
// Mode: producer (1.2)
// =============================================================================

func runProducerIteration(ctx context.Context, iter int, a args) (*iterationResult, error) {
	// Run two passes: (a) individual stage measurements (encode +
	// object_put alone) on a sandbox store, then (b) a wall-clock
	// run through Producer.Append + Flush + AwaitDurable to pick up
	// manifest_cas_conflict_rate and the residual manifest_append
	// time.
	startedMs := time.Now().UnixMilli()

	// Pass (a): per-stage encode + object_put against an isolated store.
	sandbox := objstore.NewInMemory()
	stages := map[string][]float64{
		"encode":          {},
		"compress":        {},
		"object_put":      {},
		"manifest_append": {}, // populated below
		"append_total":    {},
	}
	startWall := time.Now()
	totalRecords := 0
	totalBytes := int64(0)

	for batchIdx := 0; batchIdx < a.batches; batchIdx++ {
		entries := synthBatch(a.seed, batchIdx, a.recordsPerBatch, a.recordBytes)
		for _, e := range entries {
			totalBytes += int64(len(e))
		}
		totalRecords += len(entries)

		t0 := time.Now()
		payload, err := buffer.EncodeBatch(entries, buffer.CompressionNone)
		if err != nil {
			return nil, fmt.Errorf("EncodeBatch: %w", err)
		}
		stages["encode"] = append(stages["encode"], time.Since(t0).Seconds())

		// `compress` stage: EncodeBatch with CompressionNone is a no-op
		// for compression. Recording 0 keeps the schema-required stage
		// present; future runs with batch_compression != none will time
		// EncodeBatch in two passes (raw vs compressed) to extract the
		// compression-only delta.
		stages["compress"] = append(stages["compress"], 0.0)

		t1 := time.Now()
		if err := sandbox.Put(ctx, fmt.Sprintf("bench-1.2/iter-%d/sandbox/batch-%08d", iter, batchIdx), payload); err != nil {
			return nil, fmt.Errorf("sandbox.Put: %w", err)
		}
		stages["object_put"] = append(stages["object_put"], time.Since(t1).Seconds())
	}
	sandboxElapsed := time.Since(startWall).Seconds()
	_ = sandboxElapsed

	// Pass (b): full Producer pipeline for manifest_append + conflict rate.
	full := objstore.NewInMemory()
	cfg := buffer.ProducerConfig{
		ManifestPath:            fmt.Sprintf("bench-1.2/iter-%d/manifest", iter),
		DataPathPrefix:          fmt.Sprintf("bench-1.2/iter-%d/data", iter),
		FlushInterval:           50 * time.Millisecond,
		FlushSizeBytes:          a.flushSizeBytes,
		MaxBufferedInputs:       1024,
		EncodeConcurrency:       a.encodeConcurrency,
		UploadConcurrency:       a.uploadConcurrency,
		ManifestAppendBatchSize: a.manifestAppendBatchSize,
		BatchCompression:        buffer.CompressionNone,
	}
	p := buffer.NewProducer(full, cfg)

	startWall = time.Now()
	pendings := make([]*buffer.DurabilityWatcher, 0, a.batches)
	for batchIdx := 0; batchIdx < a.batches; batchIdx++ {
		entries := synthBatch(a.seed, batchIdx, a.recordsPerBatch, a.recordBytes)
		t0 := time.Now()
		h, err := p.Append(entries, nil)
		if err != nil {
			return nil, fmt.Errorf("Append: %w", err)
		}
		stages["append_total"] = append(stages["append_total"], time.Since(t0).Seconds())
		pendings = append(pendings, h.Watcher)
	}
	if err := p.Flush(ctx); err != nil {
		return nil, fmt.Errorf("Flush: %w", err)
	}
	for _, w := range pendings {
		if err := w.AwaitDurable(ctx); err != nil {
			return nil, fmt.Errorf("AwaitDurable: %w", err)
		}
	}
	conflictRate := p.ConflictRate()
	if err := p.Close(ctx); err != nil {
		return nil, fmt.Errorf("Close: %w", err)
	}
	elapsed := time.Since(startWall).Seconds()

	// Approximate manifest_append per batch as
	// (full_pipeline_total - encode_median - object_put_median) / batches.
	// This double-counts encode+put cost from pass (a) but is the
	// closest the public API supports without instrumenting buffer.
	encodeMedian := median(stages["encode"])
	putMedian := median(stages["object_put"])
	perBatchTotal := elapsed / float64(a.batches)
	manifestAppendPerBatch := perBatchTotal - encodeMedian - putMedian
	if manifestAppendPerBatch < 0 {
		manifestAppendPerBatch = 0
	}
	for i := 0; i < a.batches; i++ {
		stages["manifest_append"] = append(stages["manifest_append"], manifestAppendPerBatch)
	}
	stages["_conflict_rate_const"] = []float64{conflictRate}

	return &iterationResult{
		Iteration:         iter,
		StartedUnixMs:     startedMs,
		ElapsedSeconds:    elapsed,
		RecordsProcessed:  totalRecords,
		BytesProcessed:    totalBytes,
		StageObservations: stages,
	}, nil
}

// =============================================================================
// Aggregation + schema-v2 emission.
// =============================================================================

type scalarAgg struct {
	Median float64 `json:"median"`
	P10    float64 `json:"p10"`
	P90    float64 `json:"p90"`
	N      int     `json:"n"`
}

func aggregate(samples []float64) scalarAgg {
	if len(samples) == 0 {
		return scalarAgg{}
	}
	s := append([]float64(nil), samples...)
	sort.Float64s(s)
	q := func(frac float64) float64 {
		idx := int(float64(len(s)-1) * frac)
		return s[idx]
	}
	return scalarAgg{Median: q(0.5), P10: q(0.1), P90: q(0.9), N: len(s)}
}

func median(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	s := append([]float64(nil), samples...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func percentile(samples []float64, frac float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	s := append([]float64(nil), samples...)
	sort.Float64s(s)
	idx := int(float64(len(s)-1) * frac)
	return s[idx]
}

func tsNowISO() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05Z")
}

func runIDSlug(slug string) string {
	return time.Now().UTC().Format("2006-01-02T1504") + "-" + slug
}

func gitInfo(repo string) map[string]any {
	rev := run("git", "-C", repo, "rev-parse", "HEAD")
	if len(rev) > 7 {
		rev = rev[:7]
	}
	branch := run("git", "-C", repo, "rev-parse", "--abbrev-ref", "HEAD")
	dirty := run("git", "-C", repo, "status", "--porcelain") != ""
	return map[string]any{
		"rev":    rev,
		"branch": branch,
		"dirty":  dirty,
	}
}

func run(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// =============================================================================
// Main.
// =============================================================================

type unitMeta struct {
	id          string
	title       string
	stages      []string
	scalarNames []string
}

var unitTable = map[string]unitMeta{
	"1.1": {
		id:          "1.1",
		title:       "Benchmark current Go exporter marshal + durable wait latency",
		stages:      []string{"marshal", "enqueue", "await_durable"},
		scalarNames: []string{"total_throughput_records_per_sec", "total_throughput_bytes_per_sec", "p99_end_to_end_durable_latency_ms"},
	},
	"1.2": {
		id:          "1.2",
		title:       "Benchmark current Go Buffer producer encode, S3/MinIO put, manifest append",
		stages:      []string{"encode", "compress", "object_put", "manifest_append"},
		scalarNames: []string{"total_throughput_records_per_sec", "manifest_cas_conflict_rate"},
	},
}

func main() {
	a := parseArgs()
	ctx := context.Background()

	unit, ok := unitTable[a.unitID]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown unit-id %q\n", a.unitID)
		os.Exit(2)
	}

	runDir := filepath.Join(a.outputDir, runIDSlug(a.changeSlug))
	rawDir := filepath.Join(runDir, "raw")
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	startedISO := tsNowISO()
	iters := make([]*iterationResult, 0, a.iterations)
	for i := 1; i <= a.iterations; i++ {
		fmt.Fprintf(os.Stderr, "iteration %d/%d: %d batches × %d records × %d bytes\n",
			i, a.iterations, a.batches, a.recordsPerBatch, a.recordBytes)
		var (
			r   *iterationResult
			err error
		)
		switch a.mode {
		case "exporter":
			r, err = runExporterIteration(ctx, i, a)
		case "producer":
			r, err = runProducerIteration(ctx, i, a)
		default:
			fmt.Fprintf(os.Stderr, "bad mode %q\n", a.mode)
			os.Exit(2)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "iter %d: %v\n", i, err)
			os.Exit(1)
		}

		// raw/run-N.metrics.jsonl
		rawPath := filepath.Join(rawDir, fmt.Sprintf("run-%d.metrics.jsonl", i))
		f, err := os.Create(rawPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create raw: %v\n", err)
			os.Exit(1)
		}
		enc := json.NewEncoder(f)
		for stage, samples := range r.StageObservations {
			for idx, secs := range samples {
				_ = enc.Encode(map[string]any{
					"ts_unix_ms": r.StartedUnixMs + int64(idx),
					"metric":     "stage." + stage + ".duration_seconds",
					"value":      secs,
					"labels":     map[string]any{"iteration": i, "batch_index": idx},
				})
			}
		}
		f.Close()

		iters = append(iters, r)
	}
	endedISO := tsNowISO()

	// Aggregate.
	thrRecsPerSec := make([]float64, len(iters))
	thrBytesPerSec := make([]float64, len(iters))
	durLatencyP99Ms := make([]float64, len(iters))
	for i, r := range iters {
		thrRecsPerSec[i] = float64(r.RecordsProcessed) / max(r.ElapsedSeconds, 1e-9)
		thrBytesPerSec[i] = float64(r.BytesProcessed) / max(r.ElapsedSeconds, 1e-9)
		// p99 await_durable latency (in ms) for 1.1; for 1.2 use append_total
		var s []float64
		if a.mode == "exporter" {
			s = r.StageObservations["await_durable"]
		} else {
			s = r.StageObservations["append_total"]
		}
		durLatencyP99Ms[i] = percentile(s, 0.99) * 1000
	}

	// Stage aggregates across all iterations.
	type stageOut struct {
		Name          string  `json:"name"`
		MedianMsPerOp float64 `json:"median_ms_per_op"`
		P10           float64 `json:"p10"`
		P90           float64 `json:"p90"`
		OpsPerSec     float64 `json:"ops_per_sec"`
	}
	allStages := []stageOut{}
	for _, name := range unit.stages {
		var all []float64
		for _, r := range iters {
			all = append(all, r.StageObservations[name]...)
		}
		allStages = append(allStages, stageOut{
			Name:          name,
			MedianMsPerOp: median(all) * 1000,
			P10:           percentile(all, 0.1) * 1000,
			P90:           percentile(all, 0.9) * 1000,
			OpsPerSec:     float64(len(all)) / sumOf(iterationElapsed(iters)),
		})
	}

	scalars := map[string]any{
		"total_throughput_records_per_sec": aggregate(thrRecsPerSec),
		"total_throughput_bytes_per_sec":   aggregate(thrBytesPerSec),
	}
	if a.mode == "exporter" {
		scalars["p99_end_to_end_durable_latency_ms"] = aggregate(durLatencyP99Ms)
	} else {
		// 1.2: report manifest CAS conflict rate (constant per iter)
		conflicts := []float64{}
		for _, r := range iters {
			conflicts = append(conflicts, r.StageObservations["_conflict_rate_const"]...)
		}
		scalars["manifest_cas_conflict_rate"] = aggregate(conflicts)
	}

	// Per-iteration json.
	perIter := []map[string]any{}
	for idx, r := range iters {
		stagesPerIter := []map[string]any{}
		for _, name := range unit.stages {
			stagesPerIter = append(stagesPerIter, map[string]any{
				"name":             name,
				"median_ms_per_op": median(r.StageObservations[name]) * 1000,
				"p10":              percentile(r.StageObservations[name], 0.1) * 1000,
				"p90":              percentile(r.StageObservations[name], 0.9) * 1000,
			})
		}
		perIter = append(perIter, map[string]any{
			"iteration": r.Iteration,
			"scalars": map[string]any{
				"iteration_elapsed_seconds":            r.ElapsedSeconds,
				"iteration_throughput_records_per_sec": thrRecsPerSec[idx],
				"iteration_throughput_bytes_per_sec":   thrBytesPerSec[idx],
				"iteration_records_processed":          float64(r.RecordsProcessed),
			},
			"stages":      stagesPerIter,
			"histograms":  map[string]any{},
			"raw_log":     fmt.Sprintf("raw/run-%d.metrics.jsonl", r.Iteration),
			"raw_metrics": fmt.Sprintf("raw/run-%d.metrics.jsonl", r.Iteration),
		})
	}

	results := map[string]any{
		"schema_version": 2,
		"scalars":        scalars,
		"stages":         allStages,
		"histograms":     map[string]any{},
		"iterations":     perIter,
	}
	if err := writeJSON(filepath.Join(runDir, "results.json"), results); err != nil {
		fmt.Fprintf(os.Stderr, "write results.json: %v\n", err)
		os.Exit(1)
	}

	// Workload + metadata.
	workload := map[string]any{
		"kind":              "buffer-synthetic-bytes",
		"records_per_batch": a.recordsPerBatch,
		"batches":           a.batches,
		"record_bytes":      a.recordBytes,
		"compression":       "none",
		"schema":            "buffer-bytes-v1",
		"generator":         "opendata-go cmd/bench --mode " + a.mode,
	}
	canonical, _ := json.Marshal(workload)
	canonicalPath := filepath.Join(rawDir, "workload.canonical.json")
	if err := os.WriteFile(canonicalPath, canonical, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write canonical: %v\n", err)
		os.Exit(1)
	}

	fingerprint := fmt.Sprintf("buffer-go-%drec-per-batch-%dbatches-%dbytes-no-compression",
		a.recordsPerBatch, a.batches, a.recordBytes)

	host := map[string]any{
		"machine":           os.Getenv("HOSTNAME"),
		"os":                runtime.GOOS + " " + runtime.GOARCH,
		"cpu_model":         os.Getenv("CPU_MODEL"),
		"cpu_count_logical": runtime.NumCPU(),
		"memory_gb":         -1,
		"container":         nil,
		"low_perturbation":  true,
	}

	repoPath := os.Getenv("OPENDATA_GO_REPO_PATH")
	if repoPath == "" {
		repoPath = "."
	}
	gitMap := map[string]any{
		"opendata":         nil,
		"opendata-go":      gitInfo(repoPath),
		"opendata-contrib": nil,
	}

	metadata := map[string]any{
		"schema_version": 2,
		"phase":          "phase01-baseline",
		"unit_id":        unit.id,
		"unit_title":     unit.title,
		"owner":          "Benchmark/Perf Implementor",
		"started_at":     startedISO,
		"ended_at":       endedISO,
		"experiment": map[string]any{
			"kind":       "ab",
			"dimensions": []string{},
			"fixed_controls": map[string]any{
				"records_per_batch": a.recordsPerBatch,
				"batches":           a.batches,
				"record_bytes":      a.recordBytes,
			},
			"matrix_file": nil,
		},
		"varied_param": variedParamObject(a),
		"git":          gitMap,
		"host":         host,
		"binary": map[string]any{
			"name":          "opendata-go-cmd-bench",
			"build_command": "go build -o bench ./cmd/bench",
			"build_rev":     "see git.opendata-go.rev",
			"build_flags":   "GOAMD64=v3 (recommend); default goarch otherwise",
		},
		"config": map[string]any{
			"runtime_yaml_path":   nil,
			"effective_yaml_hash": "n/a",
			"effective_yaml": map[string]any{
				"object_store":               "InMemory",
				"flush_size_bytes":           a.flushSizeBytes,
				"flush_interval_ms":          50,
				"max_buffered_inputs":        1024,
				"batch_compression":          "none",
				"encode_concurrency":         a.encodeConcurrency,
				"upload_concurrency":         a.uploadConcurrency,
				"manifest_append_batch_size": a.manifestAppendBatchSize,
			},
		},
		"services": map[string]any{
			"clickhouse":   nil,
			"object_store": map[string]any{"kind": "in-memory", "endpoint": nil, "region": nil, "bucket": nil, "container_image": nil},
			"iceberg":      nil,
		},
		"workload": map[string]any{
			"generator":               "opendata-go cmd/bench",
			"generator_rev":           "see git.opendata-go.rev",
			"seed":                    a.seed,
			"schema":                  "buffer-bytes-v1",
			"schema_hash":             "sha256:" + sha256Hex([]byte("buffer-bytes-v1")),
			"fingerprint":             fingerprint,
			"fingerprint_hash":        "sha256:" + sha256Hex(canonical),
			"canonical_path":          "raw/workload.canonical.json",
			"records_total":           a.recordsPerBatch * a.batches,
			"batches_total":           a.batches,
			"records_per_batch":       a.recordsPerBatch,
			"approx_bytes_per_record": a.recordBytes,
			"encoding":                "raw-bytes",
			"compression":             "none",
			"attribute_cardinality":   nil,
		},
		"iterations":   a.iterations,
		"baseline_run": nil,
		"notes":        a.notes,
	}
	if err := writeJSON(filepath.Join(runDir, "metadata.json"), metadata); err != nil {
		fmt.Fprintf(os.Stderr, "write metadata.json: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Wrote artifacts to %s\n", runDir)
}

// variedParamObject constructs the metadata.varied_param block per
// the schema-v2 artifact contract:
//
//	{ "kind": "config|code|schema|infra",
//	  "name": "<dotted path or short ident>",
//	  "baseline": <control value>,
//	  "candidate": <variant value> }
//
// Returns nil when any of the four fields is empty (the bench was a
// non-A/B run with no swept parameter, or the harness invocation
// did not pass the flags).
func variedParamObject(a args) any {
	if a.variedParamKind == "" || a.variedParamName == "" || a.variedParamBaseline == "" || a.variedParamCandidate == "" {
		return nil
	}
	return map[string]any{
		"kind":      a.variedParamKind,
		"name":      a.variedParamName,
		"baseline":  a.variedParamBaseline,
		"candidate": a.variedParamCandidate,
	}
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func sumOf(s []float64) float64 {
	t := 0.0
	for _, v := range s {
		t += v
	}
	return t
}

func iterationElapsed(rs []*iterationResult) []float64 {
	out := make([]float64, len(rs))
	for i, r := range rs {
		out[i] = r.ElapsedSeconds
	}
	return out
}
