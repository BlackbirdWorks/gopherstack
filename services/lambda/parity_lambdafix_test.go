package lambda_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestLambda_ClassifyFunctionError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		result  []byte
		isError bool
	}{
		{
			name:    "runtime error endpoint is Unhandled",
			isError: true,
			result:  []byte(`{"errorMessage":"boom","errorType":"ValueError"}`),
			want:    "Unhandled",
		},
		{
			name:    "error-shaped response payload is Handled",
			isError: false,
			result:  []byte(`{"errorMessage":"caught","errorType":"MyError"}`),
			want:    "Handled",
		},
		{
			name:    "normal response has no function error",
			isError: false,
			result:  []byte(`{"answer":42}`),
			want:    "",
		},
		{
			name:    "empty response has no function error",
			isError: false,
			result:  nil,
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, lambda.ClassifyFunctionError(tt.isError, tt.result))
		})
	}
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

func TestLambda_FunctionURL_CORSPreflight(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	lambda.SetFunctionURLConfigForTest(backend, "cors-fn", &lambda.FunctionURLConfig{
		AuthType: "NONE",
		Cors: &lambda.FunctionURLCors{
			AllowOrigins: []string{"https://example.com"},
			AllowMethods: []string{"GET", "POST"},
			AllowHeaders: []string{"content-type"},
			MaxAge:       600,
		},
	})

	handler := lambda.BuildFunctionURLHandler(backend, "cors-fn")

	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	rec := httptest.NewRecorder()
	handler(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET,POST", rec.Header().Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "content-type", rec.Header().Get("Access-Control-Allow-Headers"))
	assert.Equal(t, "600", rec.Header().Get("Access-Control-Max-Age"))
}

func TestLambda_FunctionURL_AWSIAMRejectsUnsigned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		authValue string
	}{
		{name: "missing signature", authValue: ""},
		{
			name:      "malformed signature",
			authValue: "AWS4-HMAC-SHA256 Credential=bad, SignedHeaders=host, Signature=deadbeef",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, backend)

			lambda.SetFunctionURLConfigForTest(backend, "iam-fn", &lambda.FunctionURLConfig{AuthType: "AWS_IAM"})

			handler := lambda.BuildFunctionURLHandler(backend, "iam-fn")

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.authValue != "" {
				req.Header.Set("Authorization", tt.authValue)
			}

			rec := httptest.NewRecorder()
			handler(rec, req)

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Contains(t, rec.Body.String(), "Forbidden")
		})
	}
}

func TestLambda_FunctionURL_EventPayloadEnrichment(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	req := httptest.NewRequest(http.MethodGet, "/path?foo=bar&baz=qux", nil)
	req.Header.Set("Cookie", "session=abc; theme=dark")

	payload, err := lambda.BuildURLEventPayload(backend, req)
	require.NoError(t, err)

	var event struct {
		QueryStringParameters map[string]string `json:"queryStringParameters"`
		Headers               map[string]string `json:"headers"`
		Cookies               []string          `json:"cookies"`
	}
	require.NoError(t, json.Unmarshal(payload, &event))

	assert.ElementsMatch(t, []string{"session=abc", "theme=dark"}, event.Cookies)
	assert.Equal(t, "bar", event.QueryStringParameters["foo"])
	assert.Equal(t, "qux", event.QueryStringParameters["baz"])
	// Cookie header is omitted from headers (surfaced via cookies instead).
	assert.NotContains(t, event.Headers, "cookie")
}

// fakeAsyncDelivery records delivery calls for assertion.
type fakeAsyncDelivery struct {
	calls []deliveryCall
	mu    sync.Mutex
}

type deliveryCall struct {
	attrs   map[string]string
	target  string
	payload []byte
}

func (f *fakeAsyncDelivery) DeliverToTarget(
	_ context.Context, target string, payload []byte, attrs map[string]string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, deliveryCall{target: target, payload: payload, attrs: attrs})

	return nil
}

func (f *fakeAsyncDelivery) targets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	for i, c := range f.calls {
		out[i] = c.target
	}

	return out
}

func TestLambda_AsyncDestinationDelivery(t *testing.T) {
	t.Parallel()

	const (
		dlqARN     = "arn:aws:sqs:us-east-1:000000000000:dlq"
		successARN = "arn:aws:sns:us-east-1:000000000000:on-success"
		failureARN = "arn:aws:sqs:us-east-1:000000000000:on-failure"
	)

	tests := []struct {
		name        string
		wantTargets []string
		success     bool
	}{
		{
			name:        "failure routes to DLQ and OnFailure",
			success:     false,
			wantTargets: []string{dlqARN, failureARN},
		},
		{
			name:        "success routes to OnSuccess only",
			success:     true,
			wantTargets: []string{successARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, backend)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName:     "async-fn",
				FunctionArn:      "arn:aws:lambda:us-east-1:000000000000:function:async-fn",
				DeadLetterConfig: &lambda.DeadLetterConfig{TargetArn: dlqARN},
			}))

			_, err := backend.PutFunctionEventInvokeConfig("async-fn", &lambda.PutFunctionEventInvokeConfigInput{
				DestinationConfig: &lambda.DestinationConfig{
					OnSuccess: &lambda.Destination{Destination: successARN},
					OnFailure: &lambda.Destination{Destination: failureARN},
				},
			})
			require.NoError(t, err)

			fake := &fakeAsyncDelivery{}
			backend.SetAsyncDestinationDelivery(fake)

			lambda.DispatchAsyncOutcomeForTest(context.Background(), backend, lambda.AsyncOutcomeForTest{
				FunctionName:    "async-fn",
				RequestID:       "req-1",
				RequestPayload:  []byte(`{"in":1}`),
				ResponsePayload: []byte(`{"out":2}`),
				InvokeCount:     3,
				StatusCode:      200,
				Success:         tt.success,
			})

			assert.ElementsMatch(t, tt.wantTargets, fake.targets())
		})
	}
}

func TestLambda_ProvisionedConcurrencyLifecycle(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "pc-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:pc-fn",
	}))

	backend.SetProvisionedConcurrencyDelay(200 * time.Millisecond)

	cfg, err := backend.PutProvisionedConcurrencyConfig("pc-fn", "1", 5)
	require.NoError(t, err)
	assert.Equal(t, "IN_PROGRESS", cfg.Status)
	assert.Equal(t, 0, cfg.AvailableProvisionedConcurrentExecutions)

	require.Eventually(t, func() bool {
		got, getErr := backend.GetProvisionedConcurrencyConfig("pc-fn", "1")

		return getErr == nil && got.Status == "READY" && got.AvailableProvisionedConcurrentExecutions == 5
	}, 3*time.Second, 20*time.Millisecond)
}

func TestLambda_ProvisionedConcurrency_NoDelayReadyImmediately(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "pc-fn2"}))

	cfg, err := backend.PutProvisionedConcurrencyConfig("pc-fn2", "1", 3)
	require.NoError(t, err)
	assert.Equal(t, "READY", cfg.Status)
	assert.Equal(t, 3, cfg.AvailableProvisionedConcurrentExecutions)
}

func TestLambda_FunctionLifecycle_PendingToActive(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	backend.SetActivationDelay(200 * time.Millisecond)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "life-fn",
		State:        lambda.FunctionStateActive, // handler default; backend overrides to Pending
	}))

	assert.Equal(t, lambda.FunctionStatePending, lambda.GetFunctionStateForTest(backend, "life-fn"))

	require.Eventually(t, func() bool {
		return lambda.GetFunctionStateForTest(backend, "life-fn") == lambda.FunctionStateActive
	}, 3*time.Second, 20*time.Millisecond)
}

func TestLambda_FunctionLifecycle_NoDelayActiveImmediately(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "life-fn2",
		State:        lambda.FunctionStateActive,
	}))

	assert.Equal(t, lambda.FunctionStateActive, lambda.GetFunctionStateForTest(backend, "life-fn2"))
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

// forcedError is a trivial error used to force invocation failures in tests.
type forcedError struct{}

func (forcedError) Error() string { return "forced failure" }
