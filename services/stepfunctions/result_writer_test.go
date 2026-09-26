package stepfunctions_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
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

// waitForTerminalExecution must run inside a synctest bubble.
func waitForTerminalExecution(t *testing.T, b *stepfunctions.InMemoryBackend, execARN string) *stepfunctions.Execution {
	t.Helper()

	synctest.Wait()

	d, err := b.DescribeExecution(execARN)
	require.NoError(t, err)
	require.NotEqual(t, "RUNNING", d.Status)

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
				// Default Transformation (NONE, since ResultWriter exports without a
				// WriterConfig) reports Input/Output as JSON-encoded strings, matching
				// a real child execution's DescribeExecution shape.
				assert.Equal(t, "1", records[0]["Input"])
				assert.Equal(t, "1", records[0]["Output"])
				assert.Equal(t, true, records[0]["InputDetails"].(map[string]any)["Included"])

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

				// Resource+Parameters with no WriterConfig defaults to Transformation
				// NONE, same as the writes-to-S3 case -- the missing S3 writer only
				// changes whether the formatted result is exported, not its shape.
				var records []map[string]any
				require.NoError(t, json.Unmarshal([]byte(d.Output), &records))
				require.Len(t, records, 3)
				assert.Equal(t, "SUCCEEDED", records[0]["Status"])
				assert.Equal(t, "1", records[0]["Input"])
				assert.Equal(t, "1", records[0]["Output"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, tt.fn)
		})
	}
}

// TestDistributedMapResultWriterWarnLogs asserts on the actual warning
// records exportMapResults emits when a configured ResultWriter degrades
// silently in its effect (still SUCCEEDED, inline/default output) — checking
// the log is the only way to tell that case apart from a real export.
// writerConfigStateMachineDef builds a 3-item Map+ResultWriter state
// machine whose Iterator always outputs the fixed array [10, 20] via a Pass
// state, regardless of the input item -- enough to exercise
// Transformation's array-handling (COMPACT keeps it nested, FLATTEN
// splices it) without needing per-item computed values. bucket=="" omits
// Resource/Parameters entirely (WriterConfig-only preview, no S3 export);
// transformation/outputType=="" omit that WriterConfig sub-field, letting
// AWS's documented defaults apply.
func writerConfigStateMachineDef(bucket, prefix, transformation, outputType string) string {
	var rwFields []string

	if bucket != "" {
		rwFields = append(rwFields,
			`"Resource":"arn:aws:states:::s3:putObject"`,
			`"Parameters":{"Bucket":"`+bucket+`","Prefix":"`+prefix+`"}`,
		)
	}

	var wcFields []string
	if transformation != "" {
		wcFields = append(wcFields, `"Transformation":"`+transformation+`"`)
	}

	if outputType != "" {
		wcFields = append(wcFields, `"OutputType":"`+outputType+`"`)
	}

	if len(wcFields) > 0 {
		rwFields = append(rwFields, `"WriterConfig":{`+strings.Join(wcFields, ",")+`}`)
	}

	return `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"ItemsPath": "$",
				"MaxConcurrency": 1,
				"ResultWriter": {` + strings.Join(rwFields, ",") + `},
				"Iterator": {
					"StartAt": "P",
					"States": {"P": {"Type": "Pass", "Result": [10, 20], "End": true}}
				}
			}
		}
	}`
}

// startWriterConfigExecution creates and starts def against a 3-item input
// through the real aws-sdk-go-v2 sfn client, waiting for it to leave
// RUNNING, and returns the terminal DescribeExecutionOutput.
func startWriterConfigExecution(
	t *testing.T, client *sfnsdk.Client, def, namePrefix string,
) *sfnsdk.DescribeExecutionOutput {
	t.Helper()

	ctx := t.Context()

	createSM, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(namePrefix + "-" + uuid.NewString()[:8]),
		Definition: aws.String(def),
		RoleArn:    aws.String(validRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createSM.StateMachineArn,
		Input:           aws.String(`[1,2,3]`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, dErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{ExecutionArn: startOut.ExecutionArn})

		return dErr == nil && d.Status != sfntypes.ExecutionStatusRunning
	}, 5*time.Second, 10*time.Millisecond, "execution should leave RUNNING")

	desc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{ExecutionArn: startOut.ExecutionArn})
	require.NoError(t, err)

	return desc
}

// assertTransformationEntries checks succeeded-file/preview entries against
// what each Transformation must produce for three items whose child output
// is always [10, 20] (AWS docs: input-output-resultwriter.html).
func assertTransformationEntries(t *testing.T, transformation string, raw []json.RawMessage) {
	t.Helper()

	switch transformation {
	case "FLATTEN":
		require.Len(t, raw, 6, "FLATTEN splices each [10,20] output into the outer array")

		want := []float64{10, 20, 10, 20, 10, 20}
		for i, r := range raw {
			var v float64
			require.NoError(t, json.Unmarshal(r, &v))
			assert.InDelta(t, want[i], v, 0)
		}
	case "COMPACT":
		require.Len(t, raw, 3)

		for _, r := range raw {
			var v []float64
			require.NoError(t, json.Unmarshal(r, &v))
			assert.Equal(t, []float64{10, 20}, v)
		}
	default: // NONE
		require.Len(t, raw, 3)

		for _, r := range raw {
			var rec map[string]any
			require.NoError(t, json.Unmarshal(r, &rec))
			assert.Equal(t, "SUCCEEDED", rec["Status"])
			assert.Equal(t, "[10,20]", rec["Output"], "NONE stringifies Output like a real DescribeExecution")
			assert.Equal(t, true, rec["InputDetails"].(map[string]any)["Included"])
		}
	}
}

// decodeResultEntries parses a SUCCEEDED_0.json/FAILED_0.json file's bytes
// into individual JSON values, per OutputType: JSON is one array, JSONL is
// one value per newline.
func decodeResultEntries(t *testing.T, data []byte, outputType string) []json.RawMessage {
	t.Helper()

	if outputType != "JSONL" {
		var arr []json.RawMessage
		require.NoError(t, json.Unmarshal(data, &arr))

		return arr
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	raw := make([]json.RawMessage, len(lines))

	for i, l := range lines {
		raw[i] = json.RawMessage(l)
	}

	return raw
}

// TestDistributedMapResultWriter_TransformationOutputType drives a real
// state machine through the SDK client for every Transformation x
// OutputType combination ResultWriter.WriterConfig documents
// (input-output-resultwriter.html), asserting the exact SUCCEEDED_0.json
// bytes written to the wired in-process S3 backend.
func TestDistributedMapResultWriter_TransformationOutputType(t *testing.T) {
	t.Parallel()

	transformations := []string{"NONE", "COMPACT", "FLATTEN"}
	outputTypes := []string{"JSON", "JSONL"}

	for _, transformation := range transformations {
		for _, outputType := range outputTypes {
			t.Run(transformation+"_"+outputType, func(t *testing.T) {
				t.Parallel()

				bucket := "wc-matrix-" + strings.ToLower(transformation+outputType)

				s3Bk := newBucketBackedS3(t, bucket)
				backend := stepfunctions.NewInMemoryBackend()
				backend.SetS3ResultWriter(stepfunctions.NewS3ResultWriterIntegration(s3Bk))

				client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))

				def := writerConfigStateMachineDef(bucket, "jobs", transformation, outputType)
				desc := startWriterConfigExecution(t, client, def, "wc-matrix")
				require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status,
					"cause=%s error=%s", aws.ToString(desc.Cause), aws.ToString(desc.Error))

				var out mapExportOutput
				require.NoError(t, json.Unmarshal([]byte(aws.ToString(desc.Output)), &out))
				require.Equal(t, bucket, out.ResultWriterDetails.Bucket)
				assert.True(t, strings.HasPrefix(out.ResultWriterDetails.Key, "jobs/"))
				assert.True(t, strings.HasSuffix(out.ResultWriterDetails.Key, "manifest.json"))

				manifestBytes := getS3ObjectBytes(t, s3Bk, bucket, out.ResultWriterDetails.Key)

				var manifest resultManifest
				require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
				require.Len(t, manifest.ResultFiles.Succeeded, 1)
				assert.Empty(t, manifest.ResultFiles.Failed)
				assert.True(t, strings.HasSuffix(manifest.ResultFiles.Succeeded[0].Key, "SUCCEEDED_0.json"))

				succeededBytes := getS3ObjectBytes(t, s3Bk, bucket, manifest.ResultFiles.Succeeded[0].Key)
				entries := decodeResultEntries(t, succeededBytes, outputType)
				assertTransformationEntries(t, transformation, entries)
			})
		}
	}
}

// TestDistributedMapResultWriter_FailedItemsKeepFullRecord proves that a
// FAILED item's exported record is always the full NONE-shaped record
// regardless of Transformation (AWS docs: "If a child workflow execution
// fails, Step Functions returns its execution result unchanged"), while
// SUCCEEDED items still honor COMPACT.
func TestDistributedMapResultWriter_FailedItemsKeepFullRecord(t *testing.T) {
	t.Parallel()

	const bucket = "wc-failed-bucket"

	s3Bk := newBucketBackedS3(t, bucket)
	backend := stepfunctions.NewInMemoryBackend()
	backend.SetS3ResultWriter(stepfunctions.NewS3ResultWriterIntegration(s3Bk))

	client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"ItemsPath": "$",
				"MaxConcurrency": 1,
				"ToleratedFailureCount": 1,
				"ResultWriter": {
					"Resource": "arn:aws:states:::s3:putObject",
					"Parameters": {"Bucket": "` + bucket + `", "Prefix": "jobs"},
					"WriterConfig": {"Transformation": "COMPACT"}
				},
				"Iterator": {
					"StartAt": "Check",
					"States": {
						"Check": {
							"Type": "Choice",
							"Choices": [{"Variable": "$", "NumericEquals": 2, "Next": "Boom"}],
							"Default": "OK"
						},
						"Boom": {"Type": "Fail", "Error": "States.TaskFailed", "Cause": "item 2 always fails"},
						"OK": {"Type": "Pass", "Result": [10, 20], "End": true}
					}
				}
			}
		}
	}`

	desc := startWriterConfigExecution(t, client, def, "wc-failed")
	require.Equal(
		t,
		sfntypes.ExecutionStatusSucceeded,
		desc.Status,
		"1 failure is within ToleratedFailureCount: cause=%s error=%s",
		aws.ToString(desc.Cause),
		aws.ToString(desc.Error),
	)

	var out mapExportOutput
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(desc.Output)), &out))

	manifestBytes := getS3ObjectBytes(t, s3Bk, bucket, out.ResultWriterDetails.Key)

	var manifest resultManifest
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	require.Len(t, manifest.ResultFiles.Succeeded, 1)
	require.Len(t, manifest.ResultFiles.Failed, 1)

	succeededBytes := getS3ObjectBytes(t, s3Bk, bucket, manifest.ResultFiles.Succeeded[0].Key)

	var succeeded [][]float64
	require.NoError(t, json.Unmarshal(succeededBytes, &succeeded))
	require.Len(t, succeeded, 2, "2 of 3 items succeed")
	assert.Equal(t, []float64{10, 20}, succeeded[0])

	failedBytes := getS3ObjectBytes(t, s3Bk, bucket, manifest.ResultFiles.Failed[0].Key)

	var failed []map[string]any
	require.NoError(t, json.Unmarshal(failedBytes, &failed))
	require.Len(t, failed, 1)
	assert.Equal(t, "FAILED", failed[0]["Status"])
	assert.Equal(t, "2", failed[0]["Input"])
	assert.Equal(t, "States.TaskFailed", failed[0]["Error"])
	assert.Equal(t, "item 2 always fails", failed[0]["Cause"])
	assert.Equal(t, "REDRIVABLE", failed[0]["RedriveStatus"])
}

// TestDistributedMapResultWriter_DistributedChildIdentity proves that a
// DISTRIBUTED Map's ResultWriter NONE records carry the real child
// execution's ExecutionArn/Name/StartDate, unlike an INLINE Map's (which
// has no such resource -- see TestDistributedMapResultWriter_
// TransformationOutputType's NONE case, which leaves them empty).
func TestDistributedMapResultWriter_DistributedChildIdentity(t *testing.T) {
	t.Parallel()

	const bucket = "wc-distributed-bucket"

	s3Bk := newBucketBackedS3(t, bucket)
	backend := stepfunctions.NewInMemoryBackend()
	backend.SetS3ResultWriter(stepfunctions.NewS3ResultWriterIntegration(s3Bk))

	client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"ItemsPath": "$",
				"MaxConcurrency": 1,
				"ResultWriter": {
					"Resource": "arn:aws:states:::s3:putObject",
					"Parameters": {"Bucket": "` + bucket + `", "Prefix": "jobs"}
				},
				"ItemProcessor": {
					"ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "STANDARD"},
					"StartAt": "P",
					"States": {"P": {"Type": "Pass", "Result": [10, 20], "End": true}}
				}
			}
		}
	}`

	desc := startWriterConfigExecution(t, client, def, "wc-distributed")
	require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status,
		"cause=%s error=%s", aws.ToString(desc.Cause), aws.ToString(desc.Error))

	var out mapExportOutput
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(desc.Output)), &out))

	manifestBytes := getS3ObjectBytes(t, s3Bk, bucket, out.ResultWriterDetails.Key)

	var manifest resultManifest
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	require.Len(t, manifest.ResultFiles.Succeeded, 1)

	succeededBytes := getS3ObjectBytes(t, s3Bk, bucket, manifest.ResultFiles.Succeeded[0].Key)

	var records []map[string]any
	require.NoError(t, json.Unmarshal(succeededBytes, &records))
	require.Len(t, records, 3)

	seen := map[string]bool{}

	for _, rec := range records {
		execArn, _ := rec["ExecutionArn"].(string)
		assert.NotEmpty(t, execArn, "a DISTRIBUTED Map item runs as a real child execution")
		assert.False(t, seen[execArn], "each child execution must have a unique ExecutionArn")
		seen[execArn] = true

		assert.NotEmpty(t, rec["Name"])
		assert.Positive(t, rec["StartDate"])
		assert.NotEmpty(t, rec["StateMachineArn"])
	}
}

// TestDistributedMapResultWriter_PreviewWithoutExport covers ResultWriter's
// WriterConfig-only shape (AWS docs' "Required field combinations": no
// Resource/Parameters means no S3 export, only a formatted state output).
func TestDistributedMapResultWriter_PreviewWithoutExport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		outputType string
	}{
		{name: "JSON", outputType: "JSON"},
		{name: "JSONL", outputType: "JSONL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackend()
			client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))

			def := writerConfigStateMachineDef("", "", "FLATTEN", tt.outputType)
			desc := startWriterConfigExecution(t, client, def, "wc-preview-"+strings.ToLower(tt.outputType))
			require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status,
				"cause=%s error=%s", aws.ToString(desc.Cause), aws.ToString(desc.Error))

			output := aws.ToString(desc.Output)

			if tt.outputType == "JSONL" {
				var jsonl string
				require.NoError(t, json.Unmarshal([]byte(output), &jsonl))

				lines := strings.Split(jsonl, "\n")
				require.Len(t, lines, 6)

				var v float64
				require.NoError(t, json.Unmarshal([]byte(lines[0]), &v))
				assert.InDelta(t, 10, v, 0)

				return
			}

			var arr []float64
			require.NoError(t, json.Unmarshal([]byte(output), &arr))
			assert.Equal(t, []float64{10, 20, 10, 20, 10, 20}, arr)
		})
	}
}

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, tt.fn)
		})
	}
}
