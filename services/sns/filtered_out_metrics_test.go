package sns_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

func TestPublish_FilteredOutMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs   map[string]sns.MessageAttribute
		name    string
		scope   string
		message string
		want    string
	}{
		{
			name:    "mismatched_attribute",
			attrs:   map[string]sns.MessageAttribute{"k": {DataType: "String", StringValue: "other"}},
			message: "m",
			want:    "NumberOfNotificationsFilteredOut-MessageAttributes",
		},
		{name: "no_attributes", message: "m", want: "NumberOfNotificationsFilteredOut-NoMessageAttributes"},
		{
			name:    "invalid_array_attribute",
			attrs:   map[string]sns.MessageAttribute{"k": {DataType: "String.Array", StringValue: "[oops"}},
			message: "m",
			want:    "NumberOfNotificationsFilteredOut-InvalidAttributes",
		},
		{
			name:    "body_mismatch",
			scope:   "MessageBody",
			message: `{"k":"other"}`,
			want:    "NumberOfNotificationsFilteredOut-MessageBody",
		},
		{
			name:    "invalid_body",
			scope:   "MessageBody",
			message: "not json",
			want:    "NumberOfNotificationsFilteredOut-InvalidMessageBody",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			var (
				mu  sync.Mutex
				got = map[string]float64{}
			)

			b.SetMetricEmitter(cwmetric.EmitterFunc(func(p cwmetric.Point) error {
				mu.Lock()
				defer mu.Unlock()

				got[p.Name] += p.Value

				return nil
			}))

			topic, err := b.CreateTopic("t", nil)
			require.NoError(t, err)

			sub, err := b.Subscribe(topic.TopicArn, "email", "a@example.com", `{"k":["want"]}`)
			require.NoError(t, err)

			if tt.scope != "" {
				require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicyScope", tt.scope))
			}

			_, err = b.Publish(topic.TopicArn, tt.message, "", "", tt.attrs)
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()

			assert.InDelta(t, 1, got["NumberOfNotificationsFilteredOut"], 0)
			assert.InDelta(t, 1, got[tt.want], 0)
		})
	}
}
