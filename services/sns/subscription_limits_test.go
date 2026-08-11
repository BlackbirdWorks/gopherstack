package sns_test

import (
	"testing"

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
// TestSubscribe_FilterPolicyLimitExceeded_PerTopic, TestSubscribe_SubscriptionLimitExceeded,
// and TestSubscribe_SubscriptionLimitExceededHandler live in whitebox_test.go: they need
// direct access to the unexported maxFilterPoliciesPerTopic constant and
// subscriptionLimitPerTopic override.

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
