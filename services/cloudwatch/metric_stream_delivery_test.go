package cloudwatch_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFirehosePutter captures PutRecordBatch calls for assertions.
type mockFirehosePutter struct {
	calls []firehosePutCall
	mu    sync.Mutex
}

type firehosePutCall struct {
	streamName string
	records    [][]byte
}

func (m *mockFirehosePutter) PutRecordBatch(
	_ context.Context, streamName string, records [][]byte,
) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, firehosePutCall{streamName: streamName, records: records})

	return len(records), nil
}

func (m *mockFirehosePutter) captured() []firehosePutCall {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]firehosePutCall, len(m.calls))
	copy(out, m.calls)

	return out
}

// TestMetricStream_DeliversToFirehose verifies that a running metric stream
// with OutputFormat=json actually delivers matched PutMetricData records to
// the wired Firehose backend, in the real CloudWatch Metric Streams JSON
// record format, instead of only advancing LastUpdateDate.
func TestMetricStream_DeliversToFirehose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		streamForm     string
		putForm        string
		wantMetricName string
		wantDelivered  bool
	}{
		{
			name: "matching_namespace_delivered",
			streamForm: "Action=PutMetricStream&Name=stream-a" +
				"&FirehoseArn=arn:aws:firehose:us-east-1:123:deliverystream/ds-a" +
				"&RoleArn=arn:aws:iam::123:role/r&OutputFormat=json",
			putForm: "Action=PutMetricData&Namespace=AWS/EC2" +
				"&MetricData.member.1.MetricName=CPUUtilization" +
				"&MetricData.member.1.Value=42",
			wantDelivered:  true,
			wantMetricName: "CPUUtilization",
		},
		{
			name: "excluded_namespace_not_delivered",
			streamForm: "Action=PutMetricStream&Name=stream-b" +
				"&FirehoseArn=arn:aws:firehose:us-east-1:123:deliverystream/ds-b" +
				"&RoleArn=arn:aws:iam::123:role/r&OutputFormat=json" +
				"&ExcludeFilters.member.1.Namespace=AWS/EC2",
			putForm: "Action=PutMetricData&Namespace=AWS/EC2" +
				"&MetricData.member.1.MetricName=CPUUtilization" +
				"&MetricData.member.1.Value=42",
			wantDelivered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newCWHandlerWithBackend()
			fh := &mockFirehosePutter{}
			backend.SetFirehosePutter(fh)

			rec := postForm(t, h, tt.streamForm)
			require.Equal(t, 200, rec.Code, rec.Body.String())

			rec = postForm(t, h, tt.putForm)
			require.Equal(t, 200, rec.Code, rec.Body.String())

			if !tt.wantDelivered {
				require.Never(t, func() bool {
					return len(fh.captured()) > 0
				}, 200*time.Millisecond, 20*time.Millisecond)

				return
			}

			require.Eventually(t, func() bool {
				return len(fh.captured()) > 0
			}, 2*time.Second, 20*time.Millisecond)

			calls := fh.captured()
			require.Len(t, calls, 1)
			require.Len(t, calls[0].records, 1)

			var rec2 map[string]any
			require.NoError(t, json.Unmarshal(calls[0].records[0], &rec2))
			assert.Equal(t, tt.wantMetricName, rec2["metric_name"])
			assert.Equal(t, "AWS/EC2", rec2["namespace"])
			value := rec2["value"].(map[string]any)
			assert.InEpsilon(t, 42.0, value["max"], 0.0001)
			assert.InEpsilon(t, 42.0, value["min"], 0.0001)
		})
	}
}

// TestMetricStream_NoFirehoseWired verifies that PutMetricData still succeeds
// and stream state still advances when no Firehose backend is wired -- a
// documented no-op, matching the cli.go-wiring-deferred pattern used
// elsewhere in this codebase, not a silent failure of the write itself.
func TestMetricStream_NoFirehoseWired(t *testing.T) {
	t.Parallel()

	h, backend := newCWHandlerWithBackend()

	rec := postForm(t, h, "Action=PutMetricStream&Name=stream-c"+
		"&FirehoseArn=arn:aws:firehose:us-east-1:123:deliverystream/ds-c"+
		"&RoleArn=arn:aws:iam::123:role/r&OutputFormat=json")
	require.Equal(t, 200, rec.Code, rec.Body.String())

	rec = postForm(t, h, "Action=PutMetricData&Namespace=AWS/EC2"+
		"&MetricData.member.1.MetricName=CPUUtilization"+
		"&MetricData.member.1.Value=42")
	require.Equal(t, 200, rec.Code, rec.Body.String())

	s, err := backend.GetMetricStream("stream-c")
	require.NoError(t, err)
	assert.False(t, s.LastUpdateDate.IsZero())
}
