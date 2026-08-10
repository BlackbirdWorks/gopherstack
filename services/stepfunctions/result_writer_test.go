package stepfunctions_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	s3pkg "github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// recordingHandler is a minimal slog.Handler that keeps every record it
// receives, so tests can assert on warnings emitted by backend code without
// asserting on stderr text.
type recordingHandler struct {
	records []slog.Record
	mu      sync.Mutex
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())

	return nil
}

func (h *recordingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(_ string) slog.Handler      { return h }

// findWarn returns the first WARN record whose message contains substr, or
// nil if none was emitted.
func (h *recordingHandler) findWarn(substr string) *slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := range h.records {
		if h.records[i].Level == slog.LevelWarn && strings.Contains(h.records[i].Message, substr) {
			rec := h.records[i]

			return &rec
		}
	}

	return nil
}

func recordAttrs(r *slog.Record) map[string]string {
	out := make(map[string]string, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		out[a.Key] = a.Value.String()

		return true
	})

	return out
}

// newLoggingBackend returns a stepfunctions backend whose execution
// goroutines log through a recordingHandler, so tests can inspect warnings
// emitted during state execution.
func newLoggingBackend(t *testing.T) (*stepfunctions.InMemoryBackend, *recordingHandler) {
	t.Helper()

	rh := &recordingHandler{}
	ctx := logger.Save(context.Background(), slog.New(rh))

	return stepfunctions.NewInMemoryBackendWithContext(ctx, "123456789012", "us-east-1"), rh
}

// resultWriterMapDef is a Map state over the whole input array with an
// optional ResultWriter clause spliced in, mirroring mapIterStateDef in
// map_runs_test.go.
func resultWriterMapDef(resultWriterClause string) string {
	return `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"ItemsPath": "$",
				"MaxConcurrency": 1,` + resultWriterClause + `
				"Iterator": {
					"StartAt": "P",
					"States": {"P": {"Type": "Pass", "End": true}}
				}
			}
		}
	}`
}

func newBucketBackedS3(t *testing.T, bucket string) *s3pkg.InMemoryBackend {
	t.Helper()

	bk := s3pkg.NewInMemoryBackend(&s3pkg.GzipCompressor{})
	_, err := bk.CreateBucket(context.Background(), &awss3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	return bk
}

func getS3ObjectBytes(t *testing.T, bk *s3pkg.InMemoryBackend, bucket, key string) []byte {
	t.Helper()

	out, err := bk.GetObject(context.Background(), &awss3.GetObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err)
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return data
}

func waitForTerminalExecution(t *testing.T, b *stepfunctions.InMemoryBackend, execARN string) *stepfunctions.Execution {
	t.Helper()

	require.Eventually(t, func() bool {
		d, err := b.DescribeExecution(execARN)

		return err == nil && d.Status != "RUNNING"
	}, 5*time.Second, 10*time.Millisecond)

	d, err := b.DescribeExecution(execARN)
	require.NoError(t, err)

	return d
}

type resultManifestFile struct {
	Key  string `json:"Key"`
	Size int    `json:"Size"`
}

type resultManifest struct {
	DestinationBucket string `json:"DestinationBucket"`
	MapRunArn         string `json:"MapRunArn"`
	ResultFiles       struct {
		Succeeded []resultManifestFile `json:"SUCCEEDED"`
		Failed    []resultManifestFile `json:"FAILED"`
		Pending   []resultManifestFile `json:"PENDING"`
	} `json:"ResultFiles"`
}

type mapExportOutput struct {
	MapRunArn           string `json:"MapRunArn"`
	ResultWriterDetails struct {
		Bucket string `json:"Bucket"`
		Key    string `json:"Key"`
	} `json:"ResultWriterDetails"`
}

func TestDistributedMapResultWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "writes results and manifest to wired s3 backend",
			fn: func(t *testing.T) {
				t.Helper()

				const bucket = "results-bucket"

				s3Bk := newBucketBackedS3(t, bucket)
				b := stepfunctions.NewInMemoryBackend()
				b.SetS3ResultWriter(stepfunctions.NewS3ResultWriterIntegration(s3Bk))

				def := resultWriterMapDef(
					`"ResultWriter": {"Resource":"arn:aws:states:::s3:putObject",` +
						`"Parameters":{"Bucket":"` + bucket + `","Prefix":"jobs"}},`,
				)

				sm, err := b.CreateStateMachine(context.Background(), "rw-sm", def, validRoleARN, "STANDARD")
				require.NoError(t, err)

				exec, err := b.StartExecution(sm.StateMachineArn, "rw-exec", `[1,2,3]`)
				require.NoError(t, err)

				d := waitForTerminalExecution(t, b, exec.ExecutionArn)
				require.Equal(t, "SUCCEEDED", d.Status, "cause=%s error=%s", d.Cause, d.Error)

				var out mapExportOutput
				require.NoError(t, json.Unmarshal([]byte(d.Output), &out))
				assert.Equal(t, bucket, out.ResultWriterDetails.Bucket)
				assert.True(t, strings.HasPrefix(out.ResultWriterDetails.Key, "jobs/"))
				assert.True(t, strings.HasSuffix(out.ResultWriterDetails.Key, "manifest.json"))
				assert.NotEmpty(t, out.MapRunArn)

				manifestBytes := getS3ObjectBytes(t, s3Bk, bucket, out.ResultWriterDetails.Key)

				var manifest resultManifest
				require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
				assert.Equal(t, bucket, manifest.DestinationBucket)
				assert.Equal(t, out.MapRunArn, manifest.MapRunArn)
				require.Len(t, manifest.ResultFiles.Succeeded, 1)
				assert.Empty(t, manifest.ResultFiles.Failed)
				assert.Empty(t, manifest.ResultFiles.Pending)
				assert.True(t, strings.HasSuffix(manifest.ResultFiles.Succeeded[0].Key, "SUCCEEDED_0.json"))
				assert.Positive(t, manifest.ResultFiles.Succeeded[0].Size)

				succeededBytes := getS3ObjectBytes(t, s3Bk, bucket, manifest.ResultFiles.Succeeded[0].Key)

				var records []map[string]any
				require.NoError(t, json.Unmarshal(succeededBytes, &records))
				require.Len(t, records, 3)
				assert.Equal(t, "SUCCEEDED", records[0]["Status"])
				assert.InDelta(t, 1, records[0]["Input"], 0)
				assert.InDelta(t, 1, records[0]["Output"], 0)

				runs, _, err := b.ListMapRuns(exec.ExecutionArn, "", 0)
				require.NoError(t, err)
				require.Len(t, runs, 1)
				assert.Equal(t, 3, runs[0].ItemCounts.ResultsWritten,
					"ResultsWritten should count items ResultWriter actually exported")
			},
		},
		{
			name: "resultwriter absent returns inline results unchanged",
			fn: func(t *testing.T) {
				t.Helper()

				b := stepfunctions.NewInMemoryBackend()

				sm, err := b.CreateStateMachine(
					context.Background(), "rw-absent-sm", resultWriterMapDef(""), validRoleARN, "STANDARD",
				)
				require.NoError(t, err)

				exec, err := b.StartExecution(sm.StateMachineArn, "rw-absent-exec", `[1,2,3]`)
				require.NoError(t, err)

				d := waitForTerminalExecution(t, b, exec.ExecutionArn)
				require.Equal(t, "SUCCEEDED", d.Status, "cause=%s error=%s", d.Cause, d.Error)

				var arr []float64
				require.NoError(t, json.Unmarshal([]byte(d.Output), &arr))
				assert.Equal(t, []float64{1, 2, 3}, arr)

				runs, _, err := b.ListMapRuns(exec.ExecutionArn, "", 0)
				require.NoError(t, err)
				require.Len(t, runs, 1)
				assert.Zero(t, runs[0].ItemCounts.ResultsWritten)
			},
		},
		{
			name: "resultwriter configured with no s3 backend wired degrades to inline",
			fn: func(t *testing.T) {
				t.Helper()

				b := stepfunctions.NewInMemoryBackend()
				// Deliberately never call b.SetS3ResultWriter.

				def := resultWriterMapDef(
					`"ResultWriter": {"Resource":"arn:aws:states:::s3:putObject",` +
						`"Parameters":{"Bucket":"nowhere-bucket"}},`,
				)

				sm, err := b.CreateStateMachine(context.Background(), "rw-nowired-sm", def, validRoleARN, "STANDARD")
				require.NoError(t, err)

				exec, err := b.StartExecution(sm.StateMachineArn, "rw-nowired-exec", `[1,2,3]`)
				require.NoError(t, err)

				d := waitForTerminalExecution(t, b, exec.ExecutionArn)
				require.Equal(t, "SUCCEEDED", d.Status,
					"a missing S3 writer must degrade to inline results, not fail the execution: cause=%s error=%s",
					d.Cause, d.Error)

				var arr []float64
				require.NoError(t, json.Unmarshal([]byte(d.Output), &arr))
				assert.Equal(t, []float64{1, 2, 3}, arr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t)
		})
	}
}

// TestDistributedMapResultWriterWarnLogs asserts on the actual warning
// records exportMapResults emits when a configured ResultWriter degrades
// silently in its effect (still SUCCEEDED, inline/default output) — checking
// the log is the only way to tell that case apart from a real export.
func TestDistributedMapResultWriterWarnLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "unwired s3 writer warns with state and bucket",
			fn: func(t *testing.T) {
				t.Helper()

				b, rh := newLoggingBackend(t)
				// Deliberately never call b.SetS3ResultWriter.

				def := resultWriterMapDef(
					`"ResultWriter": {"Resource":"arn:aws:states:::s3:putObject",` +
						`"Parameters":{"Bucket":"nowhere-bucket"}},`,
				)

				sm, err := b.CreateStateMachine(context.Background(), "rw-warn-sm", def, validRoleARN, "STANDARD")
				require.NoError(t, err)

				exec, err := b.StartExecution(sm.StateMachineArn, "rw-warn-exec", `[1,2,3]`)
				require.NoError(t, err)

				d := waitForTerminalExecution(t, b, exec.ExecutionArn)
				require.Equal(t, "SUCCEEDED", d.Status, "cause=%s error=%s", d.Cause, d.Error)

				rec := rh.findWarn("ResultWriter configured but export unavailable")
				require.NotNil(t, rec, "expected a warn log for the unwired ResultWriter fallback")

				attrs := recordAttrs(rec)
				assert.Equal(t, "M", attrs["state"])
				assert.Equal(t, "nowhere-bucket", attrs["bucket"])
			},
		},
		{
			name: "unsupported writerconfig warns with state and settings",
			fn: func(t *testing.T) {
				t.Helper()

				const bucket = "wc-bucket"

				s3Bk := newBucketBackedS3(t, bucket)
				b, rh := newLoggingBackend(t)
				b.SetS3ResultWriter(stepfunctions.NewS3ResultWriterIntegration(s3Bk))

				def := resultWriterMapDef(
					`"ResultWriter": {"Resource":"arn:aws:states:::s3:putObject",` +
						`"Parameters":{"Bucket":"` + bucket + `"},` +
						`"WriterConfig":{"Transformation":"COMPACT","OutputType":"JSONL"}},`,
				)

				sm, err := b.CreateStateMachine(context.Background(), "rw-wc-sm", def, validRoleARN, "STANDARD")
				require.NoError(t, err)

				exec, err := b.StartExecution(sm.StateMachineArn, "rw-wc-exec", `[1,2,3]`)
				require.NoError(t, err)

				d := waitForTerminalExecution(t, b, exec.ExecutionArn)
				require.Equal(t, "SUCCEEDED", d.Status, "cause=%s error=%s", d.Cause, d.Error)

				rec := rh.findWarn("ResultWriter WriterConfig not applied")
				require.NotNil(t, rec, "expected a warn log for the unapplied WriterConfig")

				attrs := recordAttrs(rec)
				assert.Equal(t, "M", attrs["state"])
				assert.Equal(t, "COMPACT", attrs["transformation"])
				assert.Equal(t, "JSONL", attrs["outputType"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t)
		})
	}
}
