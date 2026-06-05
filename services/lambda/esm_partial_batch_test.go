package lambda_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestLambda_SQS_PartialBatchFailure verifies that when Lambda returns a
// batchItemFailures list, only the messages NOT in that list are deleted.
// Messages whose IDs appear in batchItemFailures must remain visible (for
// retry / DLQ processing), matching the AWS ReportBatchItemFailures behaviour.
func TestLambda_SQS_PartialBatchFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		messages         []*lambda.SQSMessage
		response         []byte
		wantDeleted      []string
		wantNotDeleted   []string
		functionRespType string
	}{
		{
			name: "AllSucceed_AllDeleted",
			messages: []*lambda.SQSMessage{
				{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "a"},
				{MessageID: "msg-2", ReceiptHandle: "rh-2", Body: "b"},
			},
			// nil response → no failures → delete everything
			response:         nil,
			wantDeleted:      []string{"rh-1", "rh-2"},
			wantNotDeleted:   nil,
			functionRespType: "ReportBatchItemFailures",
		},
		{
			name: "OneFailure_FailedMessageKept",
			messages: []*lambda.SQSMessage{
				{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "ok"},
				{MessageID: "msg-2", ReceiptHandle: "rh-2", Body: "fail"},
			},
			response: mustMarshal(map[string]any{
				"batchItemFailures": []map[string]any{
					{"itemIdentifier": "msg-2"},
				},
			}),
			wantDeleted:      []string{"rh-1"},
			wantNotDeleted:   []string{"rh-2"},
			functionRespType: "ReportBatchItemFailures",
		},
		{
			name: "AllFail_NothingDeleted",
			messages: []*lambda.SQSMessage{
				{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "a"},
				{MessageID: "msg-2", ReceiptHandle: "rh-2", Body: "b"},
			},
			response: mustMarshal(map[string]any{
				"batchItemFailures": []map[string]any{
					{"itemIdentifier": "msg-1"},
					{"itemIdentifier": "msg-2"},
				},
			}),
			wantDeleted:    nil,
			wantNotDeleted: []string{"rh-1", "rh-2"},
			functionRespType: "ReportBatchItemFailures",
		},
		{
			name: "WithoutReportBatchType_EmptyBatchItemFailures_AllDeleted",
			messages: []*lambda.SQSMessage{
				{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "a"},
				{MessageID: "msg-2", ReceiptHandle: "rh-2", Body: "b"},
			},
			// Without ReportBatchItemFailures function response type,
			// the batchItemFailures list is ignored and all messages deleted.
			response: mustMarshal(map[string]any{
				"batchItemFailures": []map[string]any{
					{"itemIdentifier": "msg-2"},
				},
			}),
			wantDeleted:      []string{"rh-1", "rh-2"},
			wantNotDeleted:   nil,
			functionRespType: "", // not set
		},
		{
			name: "EmptyBatchItemFailures_AllDeleted",
			messages: []*lambda.SQSMessage{
				{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "a"},
			},
			response: mustMarshal(map[string]any{
				"batchItemFailures": []map[string]any{},
			}),
			wantDeleted:      []string{"rh-1"},
			wantNotDeleted:   nil,
			functionRespType: "ReportBatchItemFailures",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			queueARN := "arn:aws:sqs:us-east-1:000000000000:partial-batch-queue"
			fnName := "partial-batch-fn"

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: fnName}))

			var fnRespTypes []string
			if tt.functionRespType != "" {
				fnRespTypes = []string{tt.functionRespType}
			}

			_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN:        queueARN,
				FunctionName:          fnName,
				BatchSize:             10,
				Enabled:               true,
				FunctionResponseTypes: fnRespTypes,
			})
			require.NoError(t, err)

			resp := tt.response

			reader := &fakeSQSReader{messages: tt.messages}
			poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
			poller.SetSQSReader(reader)
			lambda.SetSQSInvoker(poller, func(_ context.Context, _ string) ([]byte, error) {
				return resp, nil
			})

			lambda.PollOnce(t.Context(), poller)

			reader.mu.Lock()
			deleted := reader.deletedIDs
			reader.mu.Unlock()

			for _, wantDel := range tt.wantDeleted {
				assert.Contains(t, deleted, wantDel, "expected %q to be deleted", wantDel)
			}

			for _, wantKept := range tt.wantNotDeleted {
				assert.NotContains(t, deleted, wantKept, "expected %q to NOT be deleted (it failed)", wantKept)
			}
		})
	}
}

// TestLambda_ESM_FunctionResponseTypes_StoredAndRetrieved verifies that
// FunctionResponseTypes is persisted on the ESM and returned by Get/List.
func TestLambda_ESM_FunctionResponseTypes_StoredAndRetrieved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		responseTypes []string
	}{
		{
			name:          "ReportBatchItemFailures",
			responseTypes: []string{"ReportBatchItemFailures"},
		},
		{
			name:          "EmptyList",
			responseTypes: []string{},
		},
		{
			name:          "NilList",
			responseTypes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "resp-types-fn"}))

			created, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN:        "arn:aws:sqs:us-east-1:000000000000:resp-types-queue",
				FunctionName:          "resp-types-fn",
				Enabled:               true,
				FunctionResponseTypes: tt.responseTypes,
			})
			require.NoError(t, err)

			got, getErr := backend.GetEventSourceMapping(created.UUID)
			require.NoError(t, getErr)

			if len(tt.responseTypes) == 0 {
				assert.Empty(t, got.FunctionResponseTypes)
			} else {
				assert.Equal(t, tt.responseTypes, got.FunctionResponseTypes)
			}
		})
	}
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return b
}

// ensure fakeSQSReader is only declared once across test files; reuse the one from esm_test.go.
var _ sync.Mutex
