package pipes_test

// Comprehensive tests for EventBridge Pipes improvements:
//   - Source filtering: only matching messages forwarded
//   - Enrichment: call tracking via metrics
//   - State transitions: STARTING/STOPPING intermediate states

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// fakeSQSReader is a simple in-process SQS reader for testing.
type fakeSQSReader struct {
	messages []*pipes.SQSMessage
	deleted  []string
}

func (f *fakeSQSReader) ReceivePipeMessages(_ string, maxMsgs int) ([]*pipes.SQSMessage, error) {
	n := min(len(f.messages), maxMsgs)

	out := f.messages[:n]
	f.messages = f.messages[n:]

	return out, nil
}

func (f *fakeSQSReader) DeletePipeMessages(_ string, handles []string) error {
	f.deleted = append(f.deleted, handles...)

	return nil
}

// fakeLambda records invocations.
type fakeLambda struct {
	calls int
}

func (f *fakeLambda) InvokeFunction(
	_ context.Context,
	_, _ string,
	_ []byte,
) ([]byte, int, error) {
	f.calls++

	return []byte(`{}`), 200, nil
}

func newPipeBackend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("000000000000", "us-east-1")
}

// TestPipeSourceFiltering verifies that messages not matching the filter are dropped.
func TestPipeSourceFiltering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		filterPattern string
		messages      []string
		wantDelivered int
	}{
		{
			name:          "all_messages_pass_no_filter",
			messages:      []string{`{"event":"a"}`, `{"event":"b"}`},
			filterPattern: "",
			wantDelivered: 2,
		},
		{
			name:          "filter_drops_non_matching",
			messages:      []string{`{"type":"order"}`, `{"type":"payment"}`, `{"type":"order"}`},
			filterPattern: "order",
			wantDelivered: 2,
		},
		{
			name:          "all_filtered_out",
			messages:      []string{`{"type":"payment"}`},
			filterPattern: "order",
			wantDelivered: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newPipeBackend()
			r := pipes.NewRunner(b)

			sqsReader := &fakeSQSReader{}
			lambda := &fakeLambda{}

			for i, body := range tt.messages {
				id := string(rune('a' + i))
				sqsReader.messages = append(sqsReader.messages, &pipes.SQSMessage{
					MessageID:     id,
					ReceiptHandle: "rh-" + id,
					Body:          body,
				})
			}

			r.SetSQSReader(sqsReader)
			r.SetLambdaInvoker(lambda)

			sourceParams := &pipes.SourceParameters{}
			if tt.filterPattern != "" {
				sourceParams.FilterCriteria = &pipes.FilterCriteria{
					Filters: []pipes.Filter{{Pattern: tt.filterPattern}},
				}
			}

			pipeName := "filter-pipe-" + tt.name
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             pipeName,
				Source:           "arn:aws:sqs:us-east-1:000000000000:queue",
				Target:           "arn:aws:lambda:us-east-1:000000000000:function:fn",
				DesiredState:     "RUNNING",
				SourceParameters: sourceParams,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, pipeName)

			pipes.PollAllPipesOnce(context.Background(), r)

			if tt.wantDelivered > 0 {
				assert.Positive(t, lambda.calls, "expected Lambda to be invoked")
				assert.Len(t, sqsReader.deleted, tt.wantDelivered, "expected deleted messages count")
			} else {
				assert.Equal(t, 0, lambda.calls, "expected Lambda not to be invoked when all filtered out")
				assert.Empty(t, sqsReader.deleted)
			}
		})
	}
}

// TestPipeEnrichmentTracking verifies that enrichment invocations are counted.
func TestPipeEnrichmentTracking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		enrichmentARN string
		msgCount      int
		wantCount     int64
	}{
		{
			name:          "enrichment_counted_when_configured",
			enrichmentARN: "arn:aws:lambda:us-east-1:000000000000:function:enricher",
			msgCount:      2,
			wantCount:     1, // one poll = one enrichment call
		},
		{
			name:          "no_enrichment_no_count",
			enrichmentARN: "",
			msgCount:      2,
			wantCount:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newPipeBackend()
			r := pipes.NewRunner(b)

			sqsReader := &fakeSQSReader{}
			lambda := &fakeLambda{}

			for i := range tt.msgCount {
				id := string(rune('a' + i))
				sqsReader.messages = append(sqsReader.messages, &pipes.SQSMessage{
					MessageID:     id,
					ReceiptHandle: "rh-" + id,
					Body:          `{"type":"event"}`,
				})
			}

			r.SetSQSReader(sqsReader)
			r.SetLambdaInvoker(lambda)

			pipeName := "enrich-pipe-" + tt.name
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         pipeName,
				Source:       "arn:aws:sqs:us-east-1:000000000000:queue",
				Target:       "arn:aws:lambda:us-east-1:000000000000:function:fn",
				Enrichment:   tt.enrichmentARN,
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, pipeName)

			pipes.PollAllPipesOnce(context.Background(), r)

			count := b.GetEnrichmentCallCount(context.Background(), pipeName)
			assert.Equal(t, tt.wantCount, count, "enrichment call count mismatch")
		})
	}
}

// TestPipeStateTransitions verifies STARTING and STOPPING intermediate states.
func TestPipeStateTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		initialState      string
		action            string // "start" or "stop"
		wantImmediate     string // state immediately after the call
		wantEventualFinal string // state after async transition completes
	}{
		{
			name:              "stop_pipe_goes_through_stopping",
			initialState:      "RUNNING",
			action:            "stop",
			wantImmediate:     "STOPPING",
			wantEventualFinal: "STOPPED",
		},
		{
			name:              "start_stopped_pipe_goes_through_starting",
			initialState:      "STOPPED",
			action:            "start",
			wantImmediate:     "STARTING",
			wantEventualFinal: "RUNNING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newPipeBackend()
			pipeName := "transition-" + tt.name

			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         pipeName,
				Source:       "arn:aws:sqs:us-east-1:000000000000:queue",
				Target:       "arn:aws:lambda:us-east-1:000000000000:function:fn",
				DesiredState: tt.initialState,
			})
			require.NoError(t, err)

			// Perform the action.
			var result *pipes.Pipe
			switch tt.action {
			case "stop":
				result, err = b.StopPipe(context.Background(), pipeName)
			case "start":
				result, err = b.StartPipe(context.Background(), pipeName)
			}
			require.NoError(t, err)

			// Verify intermediate state in the synchronous return value.
			assert.Equal(t, tt.wantImmediate, result.CurrentState,
				"expected intermediate state %q", tt.wantImmediate)

			// Wait for the async transition to complete.
			require.Eventually(t, func() bool {
				p, e := b.GetPipe(context.Background(), pipeName)

				return e == nil && p.CurrentState == tt.wantEventualFinal
			}, 2*time.Second, 10*time.Millisecond,
				"timed out waiting for pipe to reach %q", tt.wantEventualFinal)
		})
	}
}

// TestPipeStateTransitions_DoubleStart verifies that starting an already-RUNNING pipe errors.
func TestPipeStateTransitions_DoubleStart(t *testing.T) {
	t.Parallel()

	b := newPipeBackend()
	_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:         "double-start",
		Source:       "arn:aws:sqs:us-east-1:000000000000:queue",
		Target:       "arn:aws:lambda:us-east-1:000000000000:function:fn",
		DesiredState: "RUNNING",
	})
	require.NoError(t, err)

	// Should fail since pipe is already RUNNING.
	_, err = b.StartPipe(context.Background(), "double-start")
	require.Error(t, err)
	require.ErrorIs(t, err, pipes.ErrValidation)
}
