package lambda_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestESMMetrics_SQS(t *testing.T) {
	t.Parallel()

	failResp := mustMarshal(map[string]any{
		"batchItemFailures": []map[string]any{{"itemIdentifier": "m2"}},
	})
	msgs := func() []*lambda.SQSMessage {
		return []*lambda.SQSMessage{
			{MessageID: "m1", ReceiptHandle: "r1", Body: `{"k":"keep"}`},
			{MessageID: "m2", ReceiptHandle: "r2", Body: `{"k":"keep"}`},
			{MessageID: "m3", ReceiptHandle: "r3", Body: `{"k":"drop"}`},
		}
	}

	tests := []struct {
		want    map[string]float64
		name    string
		groups  []string
		resp    []byte
		filter  bool
		wantAny bool
	}{
		{
			name:    "event_count_with_partial_failure",
			groups:  []string{"EventCount"},
			resp:    failResp,
			filter:  true,
			wantAny: true,
			want: map[string]float64{
				"PolledEventCount": 3, "FilteredOutEventCount": 1,
				"InvokedEventCount": 2, "FailedInvokeEventCount": 1, "DeletedEventCount": 1,
			},
		},
		{name: "group_not_enabled", groups: []string{}, resp: nil, wantAny: false},
		{name: "no_metrics_config", groups: nil, resp: nil, wantAny: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)
			rec := &pointRecorder{}
			backend.SetMetricEmitter(rec)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "metrics-fn"}))

			in := &lambda.CreateEventSourceMappingInput{
				EventSourceARN:        "arn:aws:sqs:us-east-1:000000000000:metrics-queue",
				FunctionName:          "metrics-fn",
				BatchSize:             10,
				Enabled:               true,
				FunctionResponseTypes: []string{"ReportBatchItemFailures"},
			}
			if tt.groups != nil {
				in.MetricsConfig = &lambda.ESMMetricsConfig{Metrics: tt.groups}
			}

			if tt.filter {
				in.FilterCriteria = &lambda.FilterCriteria{
					Filters: []lambda.Filter{{Pattern: `{"body":{"k":["keep"]}}`}},
				}
			}

			esm, err := backend.CreateEventSourceMapping(in)
			require.NoError(t, err)

			poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
			poller.SetSQSReader(&fakeSQSReader{messages: msgs()})
			lambda.SetSQSInvoker(poller, func(context.Context, string) ([]byte, error) { return tt.resp, nil })
			lambda.PollOnce(t.Context(), poller)

			if !tt.wantAny {
				rec.mu.Lock()
				defer rec.mu.Unlock()
				assert.Empty(t, rec.points)

				return
			}

			for name, v := range tt.want {
				pts := rec.named(name)
				require.Len(t, pts, 1, name)
				assert.InDelta(t, v, pts[0].Value, 0, name)
				assert.Equal(t, "AWS/Lambda", pts[0].Namespace)
				require.Len(t, pts[0].Dimensions, 1)
				assert.Equal(t, "EventSourceMappingUUID", pts[0].Dimensions[0].Name)
				assert.Equal(t, esm.UUID, pts[0].Dimensions[0].Value)
			}
		})
	}
}
