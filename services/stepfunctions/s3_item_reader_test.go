package stepfunctions_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// fakeS3Reader is a minimal asl.S3Reader stub for testing that
// InMemoryBackend.SetS3Reader actually reaches the ASL executor's Map state
// ItemReader path end-to-end.
//
// This locks a real, previously-undiscovered wiring gap: asl.Executor has
// always supported ItemReader (S3 CSV/JSON/JSONL decoding, unit-tested at
// the executor level with mocks), but nothing in services/stepfunctions/ --
// or cli.go -- ever called InMemoryBackend.SetS3Reader, so e.s3 was always
// nil in any real running process and every Distributed Map ItemReader
// failed with ErrS3ReaderNotConfigured. Mirrors the earlier ECS/Glue/
// EventBridge cli.go wiring gap fixed in a prior parity pass.
type fakeS3Reader struct {
	data []byte
}

func (f *fakeS3Reader) GetObjectBytes(_ context.Context, _, _ string) ([]byte, error) {
	return f.data, nil
}

// TestStartExecution_MapItemReader_S3 verifies that a Map state with an
// ItemReader resolves its items via the configured S3Reader once
// SetS3Reader has been called, and that the execution succeeds with one
// output entry per decoded item.
func TestStartExecution_MapItemReader_S3(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	b.SetS3Reader(&fakeS3Reader{data: []byte(`[{"n":1},{"n":2},{"n":3}]`)})

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"ItemReader": {
					"Resource": "arn:aws:states:::s3:getObject",
					"Parameters": {"Bucket": "test-bucket", "Key": "items.json"}
				},
				"ItemProcessor": {
					"StartAt": "P",
					"States": {"P": {"Type": "Pass", "End": true}}
				},
				"End": true
			}
		}
	}`

	sm, err := b.CreateStateMachine(context.Background(), "s3-itemreader-sm", def, validRoleARN, "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "s3-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		described, descErr := b.DescribeExecution(exec.ExecutionArn)

		return descErr == nil && described.Status != "RUNNING"
	}, 5*time.Second, 10*time.Millisecond)

	described, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	require.Equal(t, "SUCCEEDED", described.Status, "cause=%s error=%s", described.Cause, described.Error)

	var output []map[string]any
	require.NoError(t, json.Unmarshal([]byte(described.Output), &output))
	assert.Len(t, output, 3, "expected one output entry per S3-sourced item")
}

// TestStartExecution_MapItemReader_NoS3Reader verifies the documented
// failure mode when no S3Reader has been configured (matches this
// emulator's pre-existing, still-accurate ErrS3ReaderNotConfigured
// behavior) -- guards against a future regression silently reintroducing
// the wiring gap without a visible test failure.
func TestStartExecution_MapItemReader_NoS3Reader(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"ItemReader": {
					"Resource": "arn:aws:states:::s3:getObject",
					"Parameters": {"Bucket": "test-bucket", "Key": "items.json"}
				},
				"ItemProcessor": {
					"StartAt": "P",
					"States": {"P": {"Type": "Pass", "End": true}}
				},
				"End": true
			}
		}
	}`

	sm, err := b.CreateStateMachine(context.Background(), "no-s3-itemreader-sm", def, validRoleARN, "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "no-s3-exec", "{}")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		described, descErr := b.DescribeExecution(exec.ExecutionArn)

		return descErr == nil && described.Status != "RUNNING"
	}, 5*time.Second, 10*time.Millisecond)

	described, err := b.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", described.Status)
}
