package lambda_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- sweepESMs: marks degraded ESMs (parity fix) ----

func TestSweepESMs_MarksDegradedESM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		wantLastProcessing string
		createFunction     bool
	}{
		{
			name:               "function_exists_no_degradation",
			createFunction:     true,
			wantLastProcessing: "No records processed",
		},
		{
			name:               "function_missing_marks_problem",
			createFunction:     false,
			wantLastProcessing: "PROBLEM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newParityBackend(t)

			if tt.createFunction {
				require.NoError(t, bk.CreateFunction(makeMinimalFunction("esm-fn")))
			}

			esm, err := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
				FunctionName:   "esm-fn",
				Enabled:        true,
			})
			require.NoError(t, err)

			j := lambda.NewJanitor(bk, lambda.DefaultSettings())
			lambda.SweepESMsForTest(context.Background(), j)

			got, err := bk.GetEventSourceMapping(esm.UUID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantLastProcessing, got.LastProcessingResult)
		})
	}
}

// TestSweepESMs_QualifiedFunctionARN_NotMarkedDegraded proves the
// parity-sweep-3 fix to esmFunctionName/functionNameFromARN: an ESM created
// against a qualified function ARN (a common real pattern — point the
// trigger at a specific published version/alias rather than $LATEST) must
// not be treated as pointing at a missing function.
//
// Before the fix, esmFunctionName took only the LAST colon-separated ARN
// segment, so "arn:...:function:qual-esm-fn:1" was stored as
// FunctionARN=".../function:1" — the qualifier "1" mistaken for the function
// name. The janitor's existence check then looked up a nonexistent function
// literally named "1" and permanently marked the mapping "PROBLEM".
func TestSweepESMs_QualifiedFunctionARN_NotMarkedDegraded(t *testing.T) {
	t.Parallel()

	bk := newParityBackend(t)
	require.NoError(t, bk.CreateFunction(makeMinimalFunction("qual-esm-fn")))

	_, err := bk.PublishVersion("qual-esm-fn", "")
	require.NoError(t, err)

	esm, err := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
		FunctionName:   "arn:aws:lambda:us-east-1:123456789012:function:qual-esm-fn:1",
		Enabled:        true,
	})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:qual-esm-fn:1", esm.FunctionARN,
		"FunctionARN must preserve the name:qualifier suffix, matching real Lambda's echo")

	name, qualifier := lambda.FunctionNameAndQualifierFromARN(esm.FunctionARN)
	assert.Equal(t, "qual-esm-fn", name)
	assert.Equal(t, "1", qualifier)

	j := lambda.NewJanitor(bk, lambda.DefaultSettings())
	lambda.SweepESMsForTest(context.Background(), j)

	got, err := bk.GetEventSourceMapping(esm.UUID)
	require.NoError(t, err)
	assert.NotEqual(t, "PROBLEM", got.LastProcessingResult,
		"a qualified-ARN ESM's function must resolve correctly, not be treated as missing")
}

func TestSweepESMs_SkipsDisabledESMs(t *testing.T) {
	t.Parallel()

	bk := newParityBackend(t)

	esm, err := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
		FunctionName:   "missing-fn",
		Enabled:        false,
	})
	require.NoError(t, err)

	j := lambda.NewJanitor(bk, lambda.DefaultSettings())
	lambda.SweepESMsForTest(context.Background(), j)

	got, err := bk.GetEventSourceMapping(esm.UUID)
	require.NoError(t, err)
	assert.NotEqual(t, "PROBLEM", got.LastProcessingResult,
		"disabled ESM must not be health-checked")
}

func TestLambda_SQSRegionFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "us-east-1", arn: "arn:aws:sqs:us-east-1:000000000000:q", want: "us-east-1"},
		{name: "eu-west-2", arn: "arn:aws:sqs:eu-west-2:123456789012:my-queue", want: "eu-west-2"},
		{name: "not an sqs arn", arn: "arn:aws:sns:us-east-1:000000000000:t", want: ""},
		{name: "malformed", arn: "not-an-arn", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, lambda.SQSRegionFromARN(tt.arn))
		})
	}
}

func TestLambda_SplitByRecordAge(t *testing.T) {
	t.Parallel()

	nowMs := time.Now().UnixMilli()
	oldMs := time.Now().Add(-2 * time.Hour).UnixMilli()

	tests := []struct {
		name        string
		msgs        []*lambda.SQSMessage
		maxAge      int
		wantKept    int
		wantExpired int
	}{
		{
			name:        "no age limit keeps all",
			maxAge:      0,
			msgs:        []*lambda.SQSMessage{{MessageID: "1", SentTimestampMillis: oldMs}},
			wantKept:    1,
			wantExpired: 0,
		},
		{
			name:   "old message expired",
			maxAge: 60,
			msgs: []*lambda.SQSMessage{
				{MessageID: "1", SentTimestampMillis: oldMs},
				{MessageID: "2", SentTimestampMillis: nowMs},
			},
			wantKept:    1,
			wantExpired: 1,
		},
		{
			name:        "unknown timestamp kept",
			maxAge:      60,
			msgs:        []*lambda.SQSMessage{{MessageID: "1", SentTimestampMillis: 0}},
			wantKept:    1,
			wantExpired: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kept, expired := lambda.SplitByRecordAge(tt.msgs, tt.maxAge)
			assert.Len(t, kept, tt.wantKept)
			assert.Len(t, expired, tt.wantExpired)
		})
	}
}

func TestLambda_SplitByFilter(t *testing.T) {
	t.Parallel()

	criteria := fc(`{"body":{"type":["order"]}}`)

	msgs := []*lambda.SQSMessage{
		{MessageID: "1", ReceiptHandle: "rh-1", Body: `{"type":"order"}`},
		{MessageID: "2", ReceiptHandle: "rh-2", Body: `{"type":"invoice"}`},
		{MessageID: "3", ReceiptHandle: "rh-3", Body: `{"type":"order"}`},
	}

	matched, filtered := lambda.SplitByFilter(criteria, msgs)
	assert.Len(t, matched, 2)
	assert.Len(t, filtered, 1)
	assert.Equal(t, "2", filtered[0].MessageID)

	// Nil criteria keeps every message.
	allMatched, none := lambda.SplitByFilter(nil, msgs)
	assert.Len(t, allMatched, 3)
	assert.Empty(t, none)
}

func TestLambda_BuildSQSEventPayload(t *testing.T) {
	t.Parallel()

	msgs := []*lambda.SQSMessage{
		{
			MessageID:              "m1",
			ReceiptHandle:          "rh1",
			Body:                   "hello",
			MD5OfBody:              "abc",
			MD5OfMessageAttributes: "def",
			Attributes:             map[string]string{"SenderId": "AIDA"},
			MessageAttributes: map[string]lambda.SQSMessageAttribute{
				"priority": {DataType: "String", StringValue: "high"},
			},
		},
	}

	payload, err := lambda.BuildSQSEventPayload("eu-west-1", "arn:aws:sqs:eu-west-1:000000000000:q", msgs)
	require.NoError(t, err)

	var event struct {
		Records []struct {
			AWSRegion              string                                `json:"awsRegion"`
			MD5OfMessageAttributes string                                `json:"md5OfMessageAttributes"`
			MessageAttributes      map[string]lambda.SQSMessageAttribute `json:"messageAttributes"`
			EventSourceARN         string                                `json:"eventSourceARN"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(payload, &event))
	require.Len(t, event.Records, 1)

	rec := event.Records[0]
	assert.Equal(t, "eu-west-1", rec.AWSRegion)
	assert.Equal(t, "def", rec.MD5OfMessageAttributes)
	assert.Equal(t, "high", rec.MessageAttributes["priority"].StringValue)
	assert.Equal(t, "arn:aws:sqs:eu-west-1:000000000000:q", rec.EventSourceARN)

	// messageAttributes is always present (empty object) when none supplied.
	empty, err := lambda.BuildSQSEventPayload("us-east-1", "arn:aws:sqs:us-east-1:0:q",
		[]*lambda.SQSMessage{{MessageID: "x"}})
	require.NoError(t, err)
	assert.Contains(t, string(empty), `"messageAttributes":{}`)
}

func TestLambda_AccumulateSQSBatch(t *testing.T) {
	t.Parallel()

	t.Run("no window flushes immediately", func(t *testing.T) {
		t.Parallel()

		p := lambda.NewEventSourcePoller(nil, &fakeKinesisReader{})
		m := &lambda.EventSourceMapping{UUID: "u1", MaximumBatchingWindowInSeconds: 0, BatchSize: 10}
		batch, flush := lambda.AccumulateSQSBatch(p, m, []*lambda.SQSMessage{{MessageID: "1"}})
		assert.True(t, flush)
		assert.Len(t, batch, 1)
	})

	t.Run("window holds partial batch then flushes on full", func(t *testing.T) {
		t.Parallel()

		p := lambda.NewEventSourcePoller(nil, &fakeKinesisReader{})
		m := &lambda.EventSourceMapping{UUID: "u2", MaximumBatchingWindowInSeconds: 300, BatchSize: 3}

		// First partial batch is held.
		batch, flush := lambda.AccumulateSQSBatch(p, m, []*lambda.SQSMessage{{MessageID: "1"}})
		assert.False(t, flush)
		assert.Empty(t, batch)

		// Duplicate message id is de-duplicated (still under batch size).
		batch, flush = lambda.AccumulateSQSBatch(p, m, []*lambda.SQSMessage{{MessageID: "1", ReceiptHandle: "rh-new"}})
		assert.False(t, flush)
		assert.Empty(t, batch)

		// Reaching batch size flushes the accumulated batch.
		batch, flush = lambda.AccumulateSQSBatch(p, m,
			[]*lambda.SQSMessage{{MessageID: "2"}, {MessageID: "3"}})
		assert.True(t, flush)
		assert.Len(t, batch, 3)
	})

	t.Run("window elapsed flushes partial batch", func(t *testing.T) {
		t.Parallel()

		p := lambda.NewEventSourcePoller(nil, &fakeKinesisReader{})
		m := &lambda.EventSourceMapping{UUID: "u3", MaximumBatchingWindowInSeconds: 0, BatchSize: 10}
		// A zero window with buffering disabled flushes immediately; emulate an
		// elapsed window by using a tiny window and sleeping.
		m.MaximumBatchingWindowInSeconds = 1
		_, flush := lambda.AccumulateSQSBatch(p, m, []*lambda.SQSMessage{{MessageID: "1"}})
		assert.False(t, flush)

		time.Sleep(1100 * time.Millisecond)

		batch, flush := lambda.AccumulateSQSBatch(p, m, nil)
		assert.True(t, flush)
		assert.Len(t, batch, 1)
	})
}

func TestLambda_Poller_SQS_FilterCriteriaDropsUnmatched(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		bisect bool
	}{
		{name: "plain", bisect: false},
		{name: "bisect_on_error", bisect: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			queueARN := "arn:aws:sqs:us-west-2:000000000000:filter-queue"
			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "filter-fn"}))

			_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN:             queueARN,
				FunctionName:               "filter-fn",
				BatchSize:                  10,
				Enabled:                    true,
				BisectBatchOnFunctionError: tt.bisect,
				FilterCriteria: &lambda.FilterCriteria{
					Filters: []lambda.Filter{{Pattern: `{"body":{"type":["order"]}}`}},
				},
			})
			require.NoError(t, err)

			reader := &fakeSQSReader{
				messages: []*lambda.SQSMessage{
					{MessageID: "match", ReceiptHandle: "rh-match", Body: `{"type":"order"}`},
					{MessageID: "drop", ReceiptHandle: "rh-drop", Body: `{"type":"invoice"}`},
				},
			}

			poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
			poller.SetSQSReader(reader)
			// Force the Lambda invocation to fail so only FILTER-dropped messages are
			// deleted; the matched message must remain for redelivery.
			lambda.SetSQSInvoker(poller, func(_ context.Context, _ string) ([]byte, error) {
				return nil, forcedError{}
			})

			lambda.PollOnce(t.Context(), poller)

			reader.mu.Lock()
			deleted := reader.deletedIDs
			reader.mu.Unlock()

			assert.Contains(t, deleted, "rh-drop", "filtered-out message must be deleted")
			assert.NotContains(t, deleted, "rh-match", "matched message must NOT be deleted after invoke failure")
		})
	}
}
