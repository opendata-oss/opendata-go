//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	opendataexporter "github.com/opendata-oss/opendata-go/exporter/opendataexporter"
	"github.com/opendata-oss/opendata-go/ingest"
	"github.com/opendata-oss/opendata-go/objstore"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	awsRegion               = "us-east-1"
	minioAccessKey          = "test"
	minioSecretKey          = "testtesttest"
	minioImageEnv           = "MINIO_IMAGE"
	defaultMinIOImage       = "minio/minio:RELEASE.2025-09-07T16-13-09Z"
	collectorBuilderVersion = "v0.149.0"
)

func TestCollectorExportsMetricsToS3CompatibleStorage(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is required for e2e tests: %v", err)
	}
	if err := ensureDockerDaemon(); err != nil {
		t.Skipf("docker daemon is required for e2e tests: %v", err)
	}

	moduleDir := moduleRoot(t)
	repoRoot := filepath.Clean(filepath.Join(moduleDir, "..", ".."))
	buildDir := t.TempDir()

	minio := startMinIO(t)
	verifierClient := newS3Client(t, context.Background(), minio.apiEndpoint, true)
	bucket := fmt.Sprintf("opendata-exporter-e2e-%d", time.Now().UnixNano())
	createBucket(t, context.Background(), verifierClient, bucket)

	collectorBinary := buildCollector(t, moduleDir, repoRoot, buildDir)

	testCases := []struct {
		name        string
		compression string
	}{
		{name: "none", compression: "none"},
		{name: "zstd", compression: "zstd"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dataPathPrefix := fmt.Sprintf("ingest/otel/e2e/%s/data", tc.name)
			manifestPath := fmt.Sprintf("ingest/otel/e2e/%s/manifest", tc.name)
			receiverPort := freeTCPPort(t)
			telemetryPort := freeTCPPort(t)
			configPath := writeCollectorConfig(t, filepath.Join(buildDir, tc.name+".yaml"), collectorConfigTemplateData{
				ReceiverEndpoint: fmt.Sprintf("127.0.0.1:%d", receiverPort),
				TelemetryPort:    telemetryPort,
				S3Endpoint:       minio.exporterEndpoint,
				Bucket:           bucket,
				Region:           awsRegion,
				DataPathPrefix:   dataPathPrefix,
				ManifestPath:     manifestPath,
				Compression:      tc.compression,
			})

			collector := startCollector(t, collectorBinary, configPath, receiverPort, telemetryPort)
			defer collector.stop(t)

			original := testMetrics(tc.name)
			exportMetrics(t, receiverPort, original)
			verifyExportedMetrics(t, verifierClient, bucket, manifestPath, dataPathPrefix, original)
			verifyExporterSelfMetrics(t, telemetryPort)
		})
	}
}

type minioInstance struct {
	containerName    string
	apiEndpoint      string
	exporterEndpoint string
}

func startMinIO(t *testing.T) *minioInstance {
	t.Helper()

	port := freeTCPPort(t)
	containerName := fmt.Sprintf("opendata-exporter-e2e-%d", time.Now().UnixNano())
	image := os.Getenv(minioImageEnv)
	if image == "" {
		image = defaultMinIOImage
	}

	cmd := exec.Command("docker", "run", "-d", "--rm",
		"--name", containerName,
		"-e", "MINIO_ROOT_USER="+minioAccessKey,
		"-e", "MINIO_ROOT_PASSWORD="+minioSecretKey,
		"-e", "MINIO_DOMAIN=127.0.0.1.nip.io",
		"-p", fmt.Sprintf("%d:9000", port),
		image,
		"server", "/data",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("start minio: %v\n%s", err, output)
	}

	inst := &minioInstance{
		containerName:    containerName,
		apiEndpoint:      fmt.Sprintf("http://127.0.0.1:%d", port),
		exporterEndpoint: fmt.Sprintf("http://127.0.0.1.nip.io:%d", port),
	}

	t.Cleanup(func() {
		stopCmd := exec.Command("docker", "rm", "-f", containerName)
		_, _ = stopCmd.CombinedOutput()
	})

	waitForHTTPReady(t, inst.apiEndpoint+"/minio/health/live")
	return inst
}

func buildCollector(t *testing.T, moduleDir, repoRoot, workDir string) string {
	t.Helper()

	outputPath := filepath.Join(workDir, "collector-dist")
	if err := os.MkdirAll(outputPath, 0o755); err != nil {
		t.Fatalf("mkdir collector output: %v", err)
	}

	builderConfigPath := writeBuilderConfig(t, filepath.Join(workDir, "builder-config.yaml"), builderConfigTemplateData{
		OutputPath:        outputPath,
		ExporterModuleDir: moduleDir,
		RepoRootDir:       repoRoot,
	})

	cacheRoot, err := os.MkdirTemp("", "opendata-exporter-e2e-cache-*")
	if err != nil {
		t.Fatalf("create cache root: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(cacheRoot)
	})

	gomodCache := filepath.Join(cacheRoot, "gomodcache")
	gocache := filepath.Join(cacheRoot, "gocache")
	gopath := filepath.Join(cacheRoot, "gopath")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		"go", "run", "go.opentelemetry.io/collector/cmd/builder@"+collectorBuilderVersion,
		"--skip-strict-versioning",
		"--config="+builderConfigPath,
	)
	cmd.Dir = moduleDir
	cmd.Env = append(os.Environ(),
		"GO111MODULE=on",
		"GOMODCACHE="+gomodCache,
		"GOCACHE="+gocache,
		"GOPATH="+gopath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build collector: %v\n%s", err, output)
	}

	binaryPath := filepath.Join(outputPath, "otelcol-opendata-e2e")
	if _, err := os.Stat(binaryPath); err != nil {
		t.Fatalf("collector binary missing at %s: %v\n%s", binaryPath, err, output)
	}
	return binaryPath
}

type collectorProcess struct {
	cmd  *exec.Cmd
	logs bytes.Buffer
	done chan error
}

func startCollector(t *testing.T, binaryPath, configPath string, receiverPort, telemetryPort int) *collectorProcess {
	t.Helper()

	cmd := exec.Command(binaryPath, "--config", configPath)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+minioAccessKey,
		"AWS_SECRET_ACCESS_KEY="+minioSecretKey,
	)
	proc := &collectorProcess{
		cmd:  cmd,
		done: make(chan error, 1),
	}
	cmd.Stdout = &proc.logs
	cmd.Stderr = &proc.logs

	if err := cmd.Start(); err != nil {
		t.Fatalf("start collector: %v", err)
	}
	go func() {
		proc.done <- cmd.Wait()
	}()

	waitForCollectorReady(t, proc, receiverPort, telemetryPort)
	return proc
}

func (p *collectorProcess) stop(t *testing.T) {
	t.Helper()
	if p.cmd.Process == nil {
		return
	}

	_ = p.cmd.Process.Signal(os.Interrupt)

	select {
	case err := <-p.done:
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
			t.Fatalf("collector exit: %v\n%s", err, p.logs.String())
		}
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		err := <-p.done
		t.Fatalf("collector did not exit cleanly: %v\n%s", err, p.logs.String())
	}
}

func exportMetrics(t *testing.T, receiverPort int, md pmetric.Metrics) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		fmt.Sprintf("127.0.0.1:%d", receiverPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial collector OTLP receiver: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	client := pmetricotlp.NewGRPCClient(conn)
	if _, err := client.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md)); err != nil {
		t.Fatalf("export metrics: %v", err)
	}
}

func verifyExportedMetrics(t *testing.T, client *s3.Client, bucket, manifestPath, dataPathPrefix string, original pmetric.Metrics) {
	t.Helper()

	store := objstore.NewS3(client, bucket)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	manifestResult := waitForObject(t, ctx, store, manifestPath)

	entries, err := ingest.DecodeManifestEntries(manifestResult.Data)
	if err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 manifest entry, got %d", len(entries))
	}
	if len(entries[0].Metadata) != 1 {
		t.Fatalf("expected 1 metadata range, got %d", len(entries[0].Metadata))
	}
	if !strings.HasPrefix(entries[0].Location, dataPathPrefix+"/") {
		t.Fatalf("unexpected batch location %q", entries[0].Location)
	}

	wantMetadata := opendataexporter.EncodeMetadata(opendataexporter.SignalTypeMetrics, opendataexporter.PayloadEncodingOTLP)
	if got := entries[0].Metadata[0].Payload; !bytes.Equal(got, wantMetadata) {
		t.Fatalf("unexpected metadata payload: got %v want %v", got, wantMetadata)
	}

	batchResult := waitForObject(t, ctx, store, entries[0].Location)
	payloads, err := ingest.DecodeBatch(batchResult.Data)
	if err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 OTLP payload, got %d", len(payloads))
	}

	unmarshaler := &pmetric.ProtoUnmarshaler{}
	roundTripped, err := unmarshaler.UnmarshalMetrics(payloads[0])
	if err != nil {
		t.Fatalf("unmarshal round-tripped metrics: %v", err)
	}

	marshaler := &pmetric.ProtoMarshaler{}
	originalBytes, err := marshaler.MarshalMetrics(original)
	if err != nil {
		t.Fatalf("marshal original metrics: %v", err)
	}
	roundTripBytes, err := marshaler.MarshalMetrics(roundTripped)
	if err != nil {
		t.Fatalf("marshal round-tripped metrics: %v", err)
	}
	if !bytes.Equal(originalBytes, roundTripBytes) {
		t.Fatal("metrics payload round-trip mismatch")
	}
}

func waitForObject(t *testing.T, ctx context.Context, store objstore.ObjectStore, path string) objstore.GetResult {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	for {
		result, err := store.Get(ctx, path)
		if err == nil {
			return result
		}
		if !errors.Is(err, objstore.ErrNotFound) {
			t.Fatalf("get object %q: %v", path, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for object %q", path)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func waitForHTTPReady(t *testing.T, url string) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", url)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForCollectorReady(t *testing.T, proc *collectorProcess, receiverPort, telemetryPort int) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if grpcReady(receiverPort) && metricsReady(telemetryPort) {
			return
		}

		select {
		case waitErr := <-proc.done:
			t.Fatalf("collector exited before becoming ready: %v\n%s", waitErr, proc.logs.String())
		default:
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for collector readiness\n%s", proc.logs.String())
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func grpcReady(receiverPort int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		fmt.Sprintf("127.0.0.1:%d", receiverPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func metricsReady(telemetryPort int) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", telemetryPort))
	if err != nil {
		return false
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	return resp.StatusCode == http.StatusOK
}

func verifyExporterSelfMetrics(t *testing.T, telemetryPort int) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		body := fetchMetricsPage(t, telemetryPort)
		if metricValue(body, "opendataexporter_requests_total") > 0 &&
			metricValue(body, "opendataexporter_metrics_received_total") > 0 &&
			metricValue(body, "opendataexporter_data_points_received_total") > 0 &&
			metricValue(body, "opendataexporter_durable_wait_duration_seconds_count") > 0 &&
			metricValue(body, "opendataexporter_flush_duration_seconds_count") > 0 &&
			metricValue(body, "opendataexporter_manifest_enqueue_duration_seconds_count") > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for exporter self-metrics on :%d\n%s", telemetryPort, body)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func metricValue(body, name string) float64 {
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, name) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil || math.IsNaN(value) {
			continue
		}
		return value
	}
	return 0
}

func fetchMetricsPage(t *testing.T, telemetryPort int) string {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", telemetryPort))
	if err != nil {
		t.Fatalf("fetch collector telemetry metrics: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fetch collector telemetry metrics: unexpected status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read collector telemetry metrics: %v", err)
	}
	return string(body)
}

func createBucket(t *testing.T, ctx context.Context, client *s3.Client, bucket string) {
	t.Helper()

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}
}

func newS3Client(t *testing.T, ctx context.Context, endpoint string, usePathStyle bool) *s3.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(awsRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(minioAccessKey, minioSecretKey, "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = usePathStyle
	})
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate TCP port: %v", err)
	}
	defer func() {
		_ = ln.Close()
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func moduleRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, ".."))
}

func ensureDockerDaemon() error {
	cmd := exec.Command("docker", "info")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

type builderConfigTemplateData struct {
	OutputPath        string
	ExporterModuleDir string
	RepoRootDir       string
}

func writeBuilderConfig(t *testing.T, path string, data builderConfigTemplateData) string {
	t.Helper()
	return writeTemplate(t, filepath.Join("testdata", "builder-config.yaml.tmpl"), path, data)
}

type collectorConfigTemplateData struct {
	ReceiverEndpoint string
	TelemetryPort    int
	S3Endpoint       string
	Bucket           string
	Region           string
	DataPathPrefix   string
	ManifestPath     string
	Compression      string
}

func writeCollectorConfig(t *testing.T, path string, data collectorConfigTemplateData) string {
	t.Helper()
	return writeTemplate(t, filepath.Join("testdata", "collector-config.yaml.tmpl"), path, data)
}

func writeTemplate(t *testing.T, templatePath, outputPath string, data any) string {
	t.Helper()

	tmpl, err := template.ParseFiles(templatePath)
	if err != nil {
		t.Fatalf("parse template %s: %v", templatePath, err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create output file %s: %v", outputPath, err)
	}
	defer func() {
		_ = file.Close()
	}()

	if err := tmpl.Execute(file, data); err != nil {
		t.Fatalf("execute template %s: %v", templatePath, err)
	}
	return outputPath
}

func testMetrics(suffix string) pmetric.Metrics {
	md := pmetric.NewMetrics()

	resourceMetrics := md.ResourceMetrics().AppendEmpty()
	resourceMetrics.Resource().Attributes().PutStr("service.name", "opendata-exporter-e2e")
	resourceMetrics.Resource().Attributes().PutStr("test.case", suffix)

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("opendataexporter.e2e")

	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("requests.total")
	metric.SetDescription("collector e2e test metric")
	metric.SetUnit("1")
	metric.SetEmptyGauge()

	dp := metric.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("route", "/metrics")
	dp.SetIntValue(42)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1_700_000_000, 0)))

	return md
}
