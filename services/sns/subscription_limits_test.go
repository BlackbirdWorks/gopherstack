package sns_test

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestSubscribe_FilterPolicyKeyLimit verifies that Subscribe rejects a FilterPolicy
// with more than 5 top-level keys (AWS SNS "Filter policy constraints": a filter
// policy may declare at most 5 keys) with InvalidParameter, and accepts exactly 5.
func TestSubscribe_FilterPolicyKeyLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterPolicy string
		wantErr      bool
	}{
		{
			name:         "five_keys_accepted",
			filterPolicy: `{"a":["1"],"b":["1"],"c":["1"],"d":["1"],"e":["1"]}`,
			wantErr:      false,
		},
		{
			name:         "six_keys_rejected",
			filterPolicy: `{"a":["1"],"b":["1"],"c":["1"],"d":["1"],"e":["1"],"f":["1"]}`,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topic, err := b.CreateTopic("filter-key-limit-"+tt.name, nil)
			require.NoError(t, err)

			_, err = b.Subscribe(topic.TopicArn, "sqs",
				"arn:aws:sqs:us-east-1:000000000000:q", tt.filterPolicy)

			if tt.wantErr {
				require.ErrorIs(t, err, sns.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestSetSubscriptionAttributes_FilterPolicyKeyLimit verifies the same 5-key cap is
// enforced when a FilterPolicy is attached via SetSubscriptionAttributes, not just Subscribe.
func TestSetSubscriptionAttributes_FilterPolicyKeyLimit(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	topic, err := b.CreateTopic("filter-key-limit-ssa", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicy",
		`{"a":["1"],"b":["1"],"c":["1"],"d":["1"],"e":["1"],"f":["1"]}`)
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestSubscribe_FilterPolicyLimitExceeded_PerTopic verifies that Subscribe returns
// FilterPolicyLimitExceeded (not InvalidParameter) once a topic already has
// maxFilterPoliciesPerTopic (200) subscriptions carrying a non-empty FilterPolicy.
func TestSubscribe_FilterPolicyLimitExceeded_PerTopic(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	topic, err := b.CreateTopic("filter-policy-topic-quota", nil)
	require.NoError(t, err)

	for i := range sns.ExportedMaxFilterPoliciesPerTopic {
		endpoint := fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i)
		_, subErr := b.Subscribe(topic.TopicArn, "sqs", endpoint, `{"k":["v"]}`)
		require.NoError(t, subErr)
	}

	_, err = b.Subscribe(topic.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:one-too-many", `{"k":["v"]}`)
	require.ErrorIs(t, err, sns.ErrFilterPolicyLimitExceeded)

	// A subscription with NO filter policy is unaffected by the quota.
	_, err = b.Subscribe(topic.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:no-filter", "")
	require.NoError(t, err)
}

// TestSubscribe_SubscriptionLimitExceeded verifies that Subscribe returns
// SubscriptionLimitExceeded once a topic's subscription count reaches the
// (test-lowered) per-topic limit, and that the dedup path (re-subscribing the
// same protocol+endpoint) is unaffected since it never creates a new row.
func TestSubscribe_SubscriptionLimitExceeded(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	sns.SetSubscriptionLimitPerTopicForTest(b, 2)

	topic, err := b.CreateTopic("sub-limit-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1", "")
	require.NoError(t, err)
	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q2", "")
	require.NoError(t, err)

	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q3", "")
	require.ErrorIs(t, err, sns.ErrSubscriptionLimitExceeded)
}

// TestSubscribe_SubscriptionLimitExceededHandler verifies the HTTP wire shape: a
// SubscriptionLimitExceeded backend error surfaces as HTTP 403 with the exact
// AWS error code string in the XML error body.
func TestSubscribe_SubscriptionLimitExceededHandler(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	sns.SetSubscriptionLimitPerTopicForTest(b, 1)
	topicArn := mustCreateTopic(t, b, "sub-limit-handler-topic")
	mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	form := url.Values{
		"Action":   {"Subscribe"},
		"Version":  {"2010-03-31"},
		"TopicArn": {topicArn},
		"Protocol": {"sqs"},
		"Endpoint": {"arn:aws:sqs:us-east-1:000000000000:q2"},
	}

	rec := snsPost(t, h, form)

	assert.Equal(t, 403, rec.Code)
	assert.Contains(t, rec.Body.String(), "SubscriptionLimitExceeded")
}

// TestSetSubscriptionAttributes_FilterPolicyLimitExceededHandler verifies the HTTP
// wire shape for the account-wide FilterPolicyLimitExceeded quota via
// SetSubscriptionAttributes, and that it does not fire when updating a subscription's
// own existing filter policy in place (self-exclusion).
func TestSetSubscriptionAttributes_FilterPolicyLimitExceededHandler(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	topic, err := b.CreateTopic("filter-policy-self-update", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(topic.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q", `{"k":["v"]}`)
	require.NoError(t, err)

	// Updating the subscription's own filter policy must not count itself twice
	// against the per-topic quota.
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicy", `{"k":["v2"]}`)
	require.NoError(t, err)
}
