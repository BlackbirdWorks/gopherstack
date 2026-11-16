package cloudwatchlogs_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const (
	subPolicyDoc = `{"DestinationArn":"arn:aws:lambda:us-east-1:123456789012:function:sink","FilterPattern":"ERROR"}`
	xformDoc     = `[{"addKeys":{"entries":[{"key":"k","value":"v"}]}}]`
	indexDoc     = `{"Fields":["RequestId"]}`
)

type sinkRecorder struct {
	dests []string
	mu    sync.Mutex
}

func (s *sinkRecorder) record(dest string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dests = append(s.dests, dest)
}

func (s *sinkRecorder) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.dests...)
}

func newPolicyBackend(t *testing.T, groups ...string) *cloudwatchlogs.InMemoryBackend {
	t.Helper()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	t.Cleanup(b.Close)

	for _, g := range groups {
		_, err := b.CreateLogGroup(t.Context(), g, "", "")
		require.NoError(t, err)
		_, err = b.CreateLogStream(t.Context(), g, "s")
		require.NoError(t, err)
	}

	return b
}

func putEvent(t *testing.T, b *cloudwatchlogs.InMemoryBackend, group, msg string) {
	t.Helper()

	_, err := b.PutLogEvents(t.Context(), group, "s", "", []cloudwatchlogs.InputLogEvent{
		{Message: msg, Timestamp: time.Now().UnixMilli()},
	})
	require.NoError(t, err)
	b.Drain()
}

func TestAccountSubscriptionFilterPolicyAppliesToSelectedGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		criteria  string
		message   string
		delivered int
	}{
		{name: "no_criteria", message: "ERROR boom", delivered: 1},
		{name: "not_in_other", criteria: `LogGroupName NOT IN ["other"]`, message: "ERROR boom", delivered: 1},
		{name: "not_in_group", criteria: `LogGroupName NOT IN ["app", "x"]`, message: "ERROR boom", delivered: 0},
		{name: "pattern_mismatch", message: "all fine", delivered: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newPolicyBackend(t, "app")
			rec := &sinkRecorder{}
			b.SetSubscriptionDeliverer(cloudwatchlogs.SubscriptionDelivererFunc(
				func(_ context.Context, dest string, _ []byte) error {
					rec.record(dest)

					return nil
				}))

			_, err := b.PutAccountPolicy("sub", "SUBSCRIPTION_FILTER_POLICY", subPolicyDoc, "ALL", tt.criteria)
			require.NoError(t, err)

			putEvent(t, b, "app", tt.message)
			assert.Len(t, rec.list(), tt.delivered)
		})
	}
}

func TestAccountTransformerPolicySelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		criteria string
		group    string
		want     bool
	}{
		{name: "prefix_match", criteria: `LogGroupNamePrefix="/aws/lambda"`, group: "/aws/lambda/fn", want: true},
		{name: "prefix_miss", criteria: `LogGroupNamePrefix="/aws/lambda"`, group: "/other/fn", want: false},
		{name: "no_criteria", group: "/any", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newPolicyBackend(t)
			_, err := b.PutAccountPolicy("x", "TRANSFORMER_POLICY", xformDoc, "ALL", tt.criteria)
			require.NoError(t, err)

			procs, ok := b.TransformerProcessors(tt.group)
			assert.Equal(t, tt.want, ok)
			assert.Equal(t, tt.want, len(procs) == 1)
		})
	}
}

func TestAccountFieldIndexPolicySelection(t *testing.T) {
	t.Parallel()

	b := newPolicyBackend(t)
	_, err := b.PutAccountPolicy("idx", "FIELD_INDEX_POLICY", indexDoc, "ALL", `LogGroupNamePrefix="lambda"`)
	require.NoError(t, err)

	got, _ := b.DescribeIndexPolicies([]string{"lambda-a", "web-b"}, "", 0)
	require.Len(t, got, 1)
	assert.Equal(t, "lambda-a", got[0].LogGroupIdentifier)
}

type memSink struct {
	objects map[string]string
	mu      sync.Mutex
}

func (m *memSink) PutObject(_ context.Context, bucket, key string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objects[bucket+"/"+key] = string(body)

	return nil
}

func TestDataProtectionAuditFindingsDelivery(t *testing.T) {
	t.Parallel()

	const identifier = `["arn:aws:dataprotection::aws:data-identifier/EmailAddress"]`

	policy := func(dest string) string {
		return `{"Name":"p","Version":"2021-06-01","Statement":[{"Sid":"a","DataIdentifier":` + identifier +
			`,"Operation":{"Audit":{"FindingsDestination":` + dest + `}}}]}`
	}

	tests := []struct {
		check func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, sink *memSink, fh *sinkRecorder)
		name  string
		dest  string
	}{
		{
			name: "log_group", dest: `{"CloudWatchLogs":{"LogGroup":"audit"}}`,
			check: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, _ *memSink, _ *sinkRecorder) {
				t.Helper()

				ev, _, _, err := b.GetLogEvents(
					t.Context(),
					"audit",
					"DataProtectionAuditFindings",
					nil,
					nil,
					10,
					"",
					true,
				)
				require.NoError(t, err)
				require.Len(t, ev, 1)
				assert.Contains(t, ev[0].Message, `"name":"EmailAddress"`)
				assert.Contains(t, ev[0].Message, `"start":9,"end":23`)
				assert.Contains(t, ev[0].Message, `"resourceArn":"arn:aws:logs:us-east-1:123456789012:log-group:app:*"`)
			},
		},
		{
			name: "s3_bucket", dest: `{"S3":{"Bucket":"findings"}}`,
			check: func(t *testing.T, _ *cloudwatchlogs.InMemoryBackend, sink *memSink, _ *sinkRecorder) {
				t.Helper()

				sink.mu.Lock()
				defer sink.mu.Unlock()

				require.Len(t, sink.objects, 1)

				for k, v := range sink.objects {
					assert.True(t, strings.HasPrefix(k, "findings/DataProtectionAudit/"), k)
					assert.Contains(t, v, `"name":"EmailAddress"`)
				}
			},
		},
		{
			name: "firehose", dest: `{"Firehose":{"DeliveryStream":"ds"}}`,
			check: func(t *testing.T, _ *cloudwatchlogs.InMemoryBackend, _ *memSink, fh *sinkRecorder) {
				t.Helper()

				assert.Equal(t, []string{"arn:aws:firehose:us-east-1:123456789012:deliverystream/ds"}, fh.list())
			},
		},
		{
			name: "all_destinations",
			dest: `{"CloudWatchLogs":{"LogGroup":"audit"},"Firehose":{"DeliveryStream":"ds"},"S3":{"Bucket":"findings"}}`,
			check: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, sink *memSink, fh *sinkRecorder) {
				t.Helper()

				ev, _, _, err := b.GetLogEvents(
					t.Context(),
					"audit",
					"DataProtectionAuditFindings",
					nil,
					nil,
					10,
					"",
					true,
				)
				require.NoError(t, err)
				assert.Len(t, ev, 1)

				sink.mu.Lock()
				defer sink.mu.Unlock()

				assert.Len(t, sink.objects, 1)
				assert.Len(t, fh.list(), 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newPolicyBackend(t, "app", "audit")
			sink := &memSink{objects: map[string]string{}}
			fh := &sinkRecorder{}

			b.SetExportSink(sink)
			b.SetSubscriptionDeliverer(cloudwatchlogs.SubscriptionDelivererFunc(
				func(_ context.Context, dest string, _ []byte) error {
					fh.record(dest)

					return nil
				}))

			require.NoError(t, b.PutDataProtectionPolicy("app", policy(tt.dest)))

			putEvent(t, b, "app", "mail me bob@example.com")
			b.Drain()

			tt.check(t, b, sink, fh)
		})
	}
}
