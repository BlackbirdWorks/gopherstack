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

// TestLambda_IsDynamoDBStreamARN tests the DynamoDB stream ARN detector.
func TestLambda_IsDynamoDBStreamARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		expected bool
	}{
		{
			name:     "valid DDB stream ARN",
			arn:      "arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
			expected: true,
		},
		{
			name:     "DDB table ARN without stream",
			arn:      "arn:aws:dynamodb:us-east-1:000000000000:table/my-table",
			expected: false,
		},
		{
			name:     "Kinesis ARN",
			arn:      "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			expected: false,
		},
		{
			name:     "SQS ARN",
			arn:      "arn:aws:sqs:us-east-1:000000000000:my-queue",
			expected: false,
		},
		{
			name:     "empty string",
			arn:      "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := lambda.IsDynamoDBStreamARN(tt.arn)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// fakeDDBStreamsReader is a test DynamoDBStreamsReader for unit tests.
type fakeDDBStreamsReader struct {
	getIterErr   error
	getRecErr    error
	records      []lambda.DynamoDBStreamRecord
	iterCalls    int
	recordsCalls int
	mu           sync.Mutex
}

func (f *fakeDDBStreamsReader) GetStreamShardIterator(_, _ string) (string, error) {
	f.mu.Lock()
	f.iterCalls++
	f.mu.Unlock()

	if f.getIterErr != nil {
		return "", f.getIterErr
	}

	return "ddb-test-iter:0", nil
}

func (f *fakeDDBStreamsReader) GetStreamRecords(_ string, _ int) ([]lambda.DynamoDBStreamRecord, string, error) {
	f.mu.Lock()
	f.recordsCalls++
	recs := f.records
	f.records = nil // clear on first read
	f.mu.Unlock()

	if f.getRecErr != nil {
		return nil, "", f.getRecErr
	}

	return recs, "ddb-test-iter:999", nil
}

// TestLambda_DDB_Poller_SkipsWhenNoReader tests that poll skips DDB ARNs when no reader is set.
func TestLambda_DDB_Poller_SkipsWhenNoReader(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN:   "arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
		FunctionName:     "my-function",
		StartingPosition: "TRIM_HORIZON",
		Enabled:          true,
	})
	require.NoError(t, err)

	// Create poller with no DDB reader (only Kinesis reader)
	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})

	// Should not panic
	lambda.PollOnce(t.Context(), poller)
}

// TestLambda_DDB_Poller_GetIteratorError tests graceful handling of GetStreamShardIterator errors.
func TestLambda_DDB_Poller_GetIteratorError(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN:   "arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
		FunctionName:     "my-function",
		StartingPosition: "TRIM_HORIZON",
		Enabled:          true,
	})
	require.NoError(t, err)

	reader := &fakeDDBStreamsReader{getIterErr: assert.AnError}
	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	lambda.SetDynamoDBStreamsReaderOnPoller(poller, reader)

	// Should not panic on error
	lambda.PollOnce(t.Context(), poller)

	reader.mu.Lock()
	calls := reader.iterCalls
	reader.mu.Unlock()

	assert.Equal(t, 1, calls)
}

// TestLambda_DDB_Poller_GetRecordsError tests graceful handling of GetStreamRecords errors.
func TestLambda_DDB_Poller_GetRecordsError(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN:   "arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
		FunctionName:     "my-function",
		StartingPosition: "TRIM_HORIZON",
		Enabled:          true,
	})
	require.NoError(t, err)

	reader := &fakeDDBStreamsReader{getRecErr: assert.AnError}
	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	lambda.SetDynamoDBStreamsReaderOnPoller(poller, reader)

	// Poll once to get iterator
	lambda.PollOnce(t.Context(), poller)

	reader.mu.Lock()
	recCalls := reader.recordsCalls
	reader.mu.Unlock()

	assert.Equal(t, 1, recCalls)
}

// TestLambda_DDB_Poller_InvokesLambdaWithRecords tests that the poller invokes Lambda
// with a correctly-formatted DynamoDB stream event when records are available.
func TestLambda_DDB_Poller_InvokesLambdaWithRecords(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	streamARN := "arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000"

	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN:   streamARN,
		FunctionName:     "arn:aws:lambda:us-east-1:000000000000:function:my-function",
		StartingPosition: "TRIM_HORIZON",
		BatchSize:        10,
		Enabled:          true,
	})
	require.NoError(t, err)

	testRecords := []lambda.DynamoDBStreamRecord{
		{
			EventID:        "event-1",
			EventName:      "INSERT",
			SequenceNumber: "000000000000000000001",
			NewImage:       map[string]any{"pk": map[string]any{"S": "hello"}},
		},
		{
			EventID:        "event-2",
			EventName:      "MODIFY",
			SequenceNumber: "000000000000000000002",
			NewImage:       map[string]any{"pk": map[string]any{"S": "world"}},
			OldImage:       map[string]any{"pk": map[string]any{"S": "old"}},
		},
	}

	reader := &fakeDDBStreamsReader{records: testRecords}

	var mu sync.Mutex
	var capturedPayloads [][]byte

	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	lambda.SetDynamoDBStreamsReaderOnPoller(poller, reader)
	lambda.SetDDBInvoker(poller, func(_ context.Context, _ string, payload []byte) error {
		mu.Lock()
		capturedPayloads = append(capturedPayloads, payload)
		mu.Unlock()

		return nil
	})

	lambda.PollOnce(t.Context(), poller)

	mu.Lock()
	payloads := capturedPayloads
	mu.Unlock()

	require.Len(t, payloads, 1, "expected one invocation with a batch of records")

	var event struct {
		Records []struct {
			Dynamodb struct {
				NewImage       map[string]any `json:"NewImage"`
				OldImage       map[string]any `json:"OldImage"`
				SequenceNumber string         `json:"SequenceNumber"`
			} `json:"dynamodb"`
			EventID      string `json:"eventID"`
			EventName    string `json:"eventName"`
			EventSource  string `json:"eventSource"`
			EventVersion string `json:"eventVersion"`
			AWSRegion    string `json:"awsRegion"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(payloads[0], &event))

	require.Len(t, event.Records, 2)
	assert.Equal(t, "event-1", event.Records[0].EventID)
	assert.Equal(t, "INSERT", event.Records[0].EventName)
	assert.Equal(t, "aws:dynamodb", event.Records[0].EventSource)
	assert.Equal(t, "1.1", event.Records[0].EventVersion)
	assert.Equal(t, "us-east-1", event.Records[0].AWSRegion)
	assert.Equal(t, "000000000000000000001", event.Records[0].Dynamodb.SequenceNumber)
	assert.NotNil(t, event.Records[0].Dynamodb.NewImage)

	assert.Equal(t, "event-2", event.Records[1].EventID)
	assert.Equal(t, "MODIFY", event.Records[1].EventName)
	assert.NotNil(t, event.Records[1].Dynamodb.OldImage)
}

// TestLambda_DDB_Poller_NoRecords tests that no Lambda invocation occurs when there are no records.
func TestLambda_DDB_Poller_NoRecords(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN:   "arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
		FunctionName:     "my-function",
		StartingPosition: "TRIM_HORIZON",
		Enabled:          true,
	})
	require.NoError(t, err)

	reader := &fakeDDBStreamsReader{records: nil}

	var invoked bool

	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	lambda.SetDynamoDBStreamsReaderOnPoller(poller, reader)
	lambda.SetDDBInvoker(poller, func(_ context.Context, _ string, _ []byte) error {
		invoked = true

		return nil
	})

	lambda.PollOnce(t.Context(), poller)

	assert.False(t, invoked, "Lambda should not be invoked when there are no records")
}

// TestLambda_DDB_Poller_DisabledMappingSkipped tests that disabled DDB ESMs are not polled.
func TestLambda_DDB_Poller_DisabledMappingSkipped(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN:   "arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
		FunctionName:     "my-function",
		StartingPosition: "TRIM_HORIZON",
		Enabled:          false,
	})
	require.NoError(t, err)

	reader := &fakeDDBStreamsReader{records: []lambda.DynamoDBStreamRecord{
		{EventID: "e1", EventName: "INSERT"},
	}}

	var invoked bool

	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	lambda.SetDynamoDBStreamsReaderOnPoller(poller, reader)
	lambda.SetDDBInvoker(poller, func(_ context.Context, _ string, _ []byte) error {
		invoked = true

		return nil
	})

	lambda.PollOnce(t.Context(), poller)

	assert.False(t, invoked, "Lambda should not be invoked for disabled ESM")

	reader.mu.Lock()
	recCalls := reader.recordsCalls
	reader.mu.Unlock()

	assert.Equal(t, 0, recCalls, "GetStreamRecords should not be called for disabled ESM")
}

// TestLambda_ESM_ShardIteratorCleanup tests that shard iterator entries for deleted ESMs
// are swept on the next poll cycle, preventing unbounded map growth.
func TestLambda_ESM_ShardIteratorCleanup(t *testing.T) {
	t.Parallel()

	const streamARN = "arn:aws:dynamodb:us-east-1:000000000000:table/cleanup-table/stream/2024-01-01T00:00:00.000"

	_, backend := newRealHandler(t)

	esm, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN:   streamARN,
		FunctionName:     "cleanup-fn",
		StartingPosition: "TRIM_HORIZON",
		Enabled:          true,
	})
	require.NoError(t, err)

	reader := &fakeDDBStreamsReader{
		records: []lambda.DynamoDBStreamRecord{
			{EventID: "e1", EventName: "INSERT"},
		},
	}

	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	lambda.SetDynamoDBStreamsReaderOnPoller(poller, reader)
	lambda.SetDDBInvoker(poller, func(_ context.Context, _ string, _ []byte) error { return nil })

	// First poll: a shard iterator entry should be stored.
	lambda.PollOnce(t.Context(), poller)
	assert.Equal(t, 1, lambda.ShardIteratorsLen(poller), "iterator should exist after first poll")

	// Delete the ESM.
	_, delErr := backend.DeleteEventSourceMapping(esm.UUID)
	require.NoError(t, delErr)

	// Second poll: the stale iterator entry should be swept.
	lambda.PollOnce(t.Context(), poller)
	assert.Equal(t, 0, lambda.ShardIteratorsLen(poller), "iterator should be swept after ESM deletion")
}
