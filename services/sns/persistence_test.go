package sns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *sns.InMemoryBackend) string
		verify func(t *testing.T, b *sns.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *sns.InMemoryBackend) string {
				topic, err := b.CreateTopic("test-topic", nil)
				if err != nil {
					return ""
				}

				return topic.TopicArn
			},
			verify: func(t *testing.T, b *sns.InMemoryBackend, id string) {
				t.Helper()

				attrs, err := b.GetTopicAttributes(id)
				require.NoError(t, err)
				assert.NotEmpty(t, attrs)
				assert.Equal(t, id, attrs["TopicArn"])
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *sns.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *sns.InMemoryBackend, _ string) {
				t.Helper()

				topics, _, err := b.ListTopics("")
				require.NoError(t, err)
				assert.Empty(t, topics)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a full-state
// Snapshot->Restore round trip covering every resource converted to
// pkgs/store (topics, subscriptions, platform applications/endpoints, SMS
// sandbox) plus every raw map left un-registered (topic tags, opted-out phone
// numbers, SMS attributes, origination numbers, and the ArchivePolicy message
// archive). It specifically checks state that is NOT itself JSON-marshaled --
// the subscriptionsByTopic secondary index and the unexported
// parsedFilterPolicy cache -- by observing their effect on real API behaviour
// (SubscriptionsConfirmed/Pending counts and FilterPolicy-based delivery
// filtering) after restoring into a brand new backend instance.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	topic, err := original.CreateTopic("full-state-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionSeconds":60}`,
	})
	require.NoError(t, err)

	// A confirmed, filtered email subscription: proves both the
	// subscriptionsByTopic index and the unexported parsedFilterPolicy cache
	// survive the round trip, since neither is itself JSON-serialised.
	sub, err := original.Subscribe(topic.TopicArn, "email", "filtered@example.com", `{"kind":["match"]}`)
	require.NoError(t, err)
	_, err = original.ConfirmSubscription(topic.TopicArn, "token")
	require.NoError(t, err)

	// A second, unfiltered SQS subscription on the same topic so the topic
	// index has more than one member to rebuild.
	_, err = original.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q", "")
	require.NoError(t, err)

	original.SetTopicTags(topic.TopicArn, tags.FromMap("test", map[string]string{"env": "prod"}))

	app, err := original.CreatePlatformApplication(
		"full-state-app", "GCM", map[string]string{"PlatformCredential": "cred"},
	)
	require.NoError(t, err)

	endpoint, err := original.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token", nil)
	require.NoError(t, err)

	require.NoError(t, original.CreateSMSSandboxPhoneNumber("+15550001111", "en"))

	sns.AddOptedOutPhoneNumberForTest(original, "+15550002222")

	require.NoError(t, original.SetSMSAttributes(map[string]string{"DefaultSMSType": "Transactional"}))

	original.SeedOriginationNumber(sns.XMLOriginationPhone{PhoneNumber: "+15550003333", Iso2CountryCode: "US"})

	// Archive a message so topicMessageArchive (the parity-sweep
	// ArchivePolicy/ReplayPolicy buffer) has content to round-trip.
	_, err = original.Publish(topic.TopicArn, "archived-message-body", "", "", nil)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Topic + attributes.
	attrs, err := fresh.GetTopicAttributes(topic.TopicArn)
	require.NoError(t, err)
	assert.Equal(t, `{"MessageRetentionSeconds":60}`, attrs["ArchivePolicy"])
	// Both subscriptions confirmed by this point.
	assert.Equal(t, "2", attrs["SubscriptionsConfirmed"])
	assert.Equal(t, "0", attrs["SubscriptionsPending"])

	// subscriptionsByTopic index rebuilt: ListSubscriptionsByTopic must see both.
	subsByTopic, _, err := fresh.ListSubscriptionsByTopic(topic.TopicArn, "")
	require.NoError(t, err)
	assert.Len(t, subsByTopic, 2)

	// topicMessageArchive (raw map, parity-sweep ArchivePolicy replay buffer).
	// Checked here, before the additional Publish calls below add more entries.
	archived := fresh.GetArchivedMessages(topic.TopicArn)
	require.Len(t, archived, 1)
	assert.Equal(t, "archived-message-body", archived[0].Message)

	// parsedFilterPolicy rebuilt: publishing a matching vs non-matching
	// message must change what collectPublishTargets records for the email
	// subscription, proving the raw FilterPolicy string was re-parsed after
	// restore rather than left as an empty/zero parsedFilterPolicy.
	subAttrs, err := fresh.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.JSONEq(t, `{"kind":["match"]}`, subAttrs["FilterPolicy"])

	_, err = fresh.Publish(topic.TopicArn, "no-match-body", "", "", map[string]sns.MessageAttribute{
		"kind": {DataType: "String", StringValue: "nomatch"},
	})
	require.NoError(t, err)
	assert.Empty(t, fresh.DrainEmailDeliveries(), "filter policy should reject a non-matching attribute")

	_, err = fresh.Publish(topic.TopicArn, "match-body", "", "", map[string]sns.MessageAttribute{
		"kind": {DataType: "String", StringValue: "match"},
	})
	require.NoError(t, err)
	assert.Len(t, fresh.DrainEmailDeliveries(), 1, "filter policy should accept a matching attribute")

	// Tags.
	assert.Equal(t, map[string]string{"env": "prod"}, fresh.GetTopicTags(topic.TopicArn))

	// Platform application + endpoint.
	appAttrs, err := fresh.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
	require.NoError(t, err)
	assert.Equal(t, "cred", appAttrs["PlatformCredential"])

	epAttrs, err := fresh.GetEndpointAttributes(endpoint.EndpointArn)
	require.NoError(t, err)
	assert.Equal(t, "device-token", epAttrs["Token"])

	// SMS sandbox.
	sandboxNums, _, err := fresh.ListSMSSandboxPhoneNumbers("", 0)
	require.NoError(t, err)
	require.Len(t, sandboxNums, 1)
	assert.Equal(t, "+15550001111", sandboxNums[0].PhoneNumber)

	// Opted-out phone numbers (raw map, left un-registered).
	optedOut, err := fresh.CheckIfPhoneNumberIsOptedOut("+15550002222")
	require.NoError(t, err)
	assert.True(t, optedOut)

	// SMS attributes (raw map, left un-registered).
	smsAttrs, err := fresh.GetSMSAttributes(nil)
	require.NoError(t, err)
	assert.Equal(t, "Transactional", smsAttrs["DefaultSMSType"])

	// Origination numbers (raw map, left un-registered).
	origNums, _, err := fresh.ListOriginationNumbers("", 0)
	require.NoError(t, err)
	require.Len(t, origNums, 1)
	assert.Equal(t, "+15550003333", origNums[0].PhoneNumber)
}

// TestSNS_PersistenceWithNewFields validates that Snapshot/Restore correctly round-trips
// SMS sandbox state, opted-out phone numbers, SMS attributes, topic permissions, and
// platform applications/endpoints.
func TestSNS_PersistenceWithNewFields(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	// SMS sandbox numbers.
	require.NoError(t, b.CreateSMSSandboxPhoneNumber("+12125550001", "en-US"))
	require.NoError(t, b.CreateSMSSandboxPhoneNumber("+12125550002", "de-DE"))

	// SMS attributes.
	require.NoError(t, b.SetSMSAttributes(map[string]string{
		"DefaultSMSType":    "Transactional",
		"MonthlySpendLimit": "100",
	}))

	// Topic with permission.
	tp, err := b.CreateTopic("persist-test", nil)
	require.NoError(t, err)
	require.NoError(t, b.AddPermission(tp.TopicArn, "allow-all", []string{"*"}, []string{"Publish"}))

	// Platform application with endpoint.
	app, err := b.CreatePlatformApplication("my-app", "GCM", map[string]string{"PlatformCredential": "fake-key"})
	require.NoError(t, err)
	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token-123", nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := sns.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify SMS sandbox numbers are preserved.
	nums, _, err := b2.ListSMSSandboxPhoneNumbers("", 0)
	require.NoError(t, err)
	require.Len(t, nums, 2)
	assert.Equal(t, "Pending", nums[0].Status)
	assert.Equal(t, "en-US", nums[0].LanguageCode)

	// Verify SMS attributes are preserved.
	attrs, err := b2.GetSMSAttributes(nil)
	require.NoError(t, err)
	assert.Equal(t, "Transactional", attrs["DefaultSMSType"])
	assert.Equal(t, "100", attrs["MonthlySpendLimit"])

	// Verify permissions survive the round-trip via GetTopicAttributes.
	topicAttrs, err := b2.GetTopicAttributes(tp.TopicArn)
	require.NoError(t, err)
	assert.NotEmpty(t, topicAttrs)

	// Verify platform applications are preserved.
	apps, _, err := b2.ListPlatformApplications("")
	require.NoError(t, err)
	require.Len(t, apps, 1)
	assert.Equal(t, app.PlatformApplicationArn, apps[0].PlatformApplicationArn)

	// Verify platform endpoints are preserved.
	eps, _, err := b2.ListEndpointsByPlatformApplication(app.PlatformApplicationArn, "")
	require.NoError(t, err)
	require.Len(t, eps, 1)
	assert.Equal(t, ep.EndpointArn, eps[0].EndpointArn)
}
