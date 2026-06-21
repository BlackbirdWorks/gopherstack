package cloudwatchlogs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *cloudwatchlogs.InMemoryBackend) string
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *cloudwatchlogs.InMemoryBackend) string {
				_, err := b.CreateLogGroup(context.Background(), "test-group", "", "")
				if err != nil {
					return ""
				}

				return "test-group"
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, id string) {
				t.Helper()

				groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 100)
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Equal(t, id, groups[0].LogGroupName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *cloudwatchlogs.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, _ string) {
				t.Helper()

				groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 100)
				require.NoError(t, err)
				assert.Empty(t, groups)
			},
		},
		{
			name: "round_trip_preserves_subscription_filters",
			setup: func(b *cloudwatchlogs.InMemoryBackend) string {
				_, err := b.CreateLogGroup(context.Background(), "sub-grp", "", "")
				if err != nil {
					return ""
				}
				_ = b.PutSubscriptionFilter(
					context.Background(),
					"sub-grp", "my-filter", "ERROR",
					"arn:aws:lambda:us-east-1:123456789012:function:target",
					"", "",
				)

				return "sub-grp"
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, id string) {
				t.Helper()

				filters, _, err := b.DescribeSubscriptionFilters(context.Background(), id, "", "", 100)
				require.NoError(t, err)
				require.Len(t, filters, 1)
				assert.Equal(t, "my-filter", filters[0].FilterName)
				assert.Equal(t, "ERROR", filters[0].FilterPattern)
				assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:target", filters[0].DestinationArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestHandler_SnapshotRestore_PreservesTags(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	_, err := b.CreateLogGroup(context.Background(), "tagged-group", "", "")
	require.NoError(t, err)

	h := cloudwatchlogs.NewHandler(b)

	// Set tags on the log group via the handler so the tag-serialization path is exercised.
	h.SetTagsForTest("tagged-group", map[string]string{"env": "prod", "team": "ops"})

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore into a fresh handler.
	b2 := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h2 := cloudwatchlogs.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Log group should be present in the restored backend.
	groups, _, gErr := b2.DescribeLogGroups(context.Background(), "", "", 100)
	require.NoError(t, gErr)
	require.Len(t, groups, 1)
	assert.Equal(t, "tagged-group", groups[0].LogGroupName)

	// Tags should have been restored.
	restoredTags := h2.GetTagsForTest("tagged-group")
	assert.Equal(t, "prod", restoredTags["env"])
	assert.Equal(t, "ops", restoredTags["team"])
}

func TestHandler_SnapshotRestore_StaleTagsCleared(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	_, err := b.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)

	// Original handler has a tag.
	h := cloudwatchlogs.NewHandler(b)
	h.SetTagsForTest("g", map[string]string{"stale": "yes"})

	// Snapshot a second handler that has no tags.
	b2 := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	_, err = b2.CreateLogGroup(context.Background(), "g", "", "")
	require.NoError(t, err)
	h2 := cloudwatchlogs.NewHandler(b2)
	snap := h2.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore the snapshot into h — stale tags should be cleared.
	require.NoError(t, h.Restore(t.Context(), snap))
	restoredTags := h.GetTagsForTest("g")
	assert.Empty(t, restoredTags)
}

func TestHandler_SnapshotRestore_InvalidData(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := cloudwatchlogs.NewHandler(b)
	err := h.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_SnapshotRestore_PreservesRetention(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	_, err := b.CreateLogGroup(context.Background(), "ret-grp", "", "")
	require.NoError(t, err)
	require.NoError(t, b.SetRetentionPolicy(context.Background(), "ret-grp", func() *int32 {
		v := int32(14)

		return &v
	}()))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	groups, _, err := b2.DescribeLogGroups(context.Background(), "", "", 100)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.NotNil(t, groups[0].RetentionInDays)
	assert.Equal(t, int32(14), *groups[0].RetentionInDays)
}

func TestInMemoryBackend_SnapshotRestore_CompletenessMapsSurvive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "resource_policy_survives",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("my-policy", `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies()
				require.Len(t, policies, 1)
				assert.Equal(t, "my-policy", policies[0].PolicyName)
			},
		},
		{
			name: "delivery_destination_survives",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliveryDestination("my-dest", "arn:aws:s3:::bucket", "JSON", nil)
				require.NoError(t, err)
				err = b.PutDeliveryDestinationPolicy("my-dest", `{"Statement":[]}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				got, err := b.GetDeliveryDestination("my-dest")
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:s3:::bucket", got.TargetArn)
				policy, err := b.GetDeliveryDestinationPolicy("my-dest")
				require.NoError(t, err)
				assert.Contains(t, policy, "Statement")
			},
		},
		{
			name: "delivery_source_survives",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliverySource("my-src", "APPLICATION_LOGS", []string{"arn:aws:ec2:::i-1"}, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				got, err := b.GetDeliverySource("my-src")
				require.NoError(t, err)
				assert.Equal(t, "APPLICATION_LOGS", got.LogType)
			},
		},
		{
			name: "destination_survives",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("my-dest", "arn:aws:kinesis:::stream/s", "arn:aws:iam:::role/r")
				require.NoError(t, err)
				err = b.PutDestinationPolicy("my-dest", `{"Statement":[]}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dests := b.DescribeDestinations("")
				require.Len(t, dests, 1)
				assert.Equal(t, "my-dest", dests[0].DestinationName)
				assert.Contains(t, dests[0].AccessPolicy, "Statement")
			},
		},
		{
			name: "index_policy_survives",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIndexPolicy("/aws/lambda/fn", `{"fields":["@message"]}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeIndexPolicies()
				require.Len(t, policies, 1)
				assert.Equal(t, "/aws/lambda/fn", policies[0].LogGroupIdentifier)
			},
		},
		{
			name: "transformer_survives",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutTransformer("/aws/lambda/fn", []map[string]any{{"parseJSON": map[string]any{}}})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tr, err := b.GetTransformer("/aws/lambda/fn")
				require.NoError(t, err)
				require.Len(t, tr.Processors, 1)
			},
		},
		{
			name: "integration_survives",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIntegration("my-opensearch", "OPENSEARCH")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				ig, err := b.GetIntegration("my-opensearch")
				require.NoError(t, err)
				assert.Equal(t, "OPENSEARCH", ig.Type)
			},
		},
		{
			name: "deletion_protected_survives",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("/aws/lambda/fn", true)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.True(t, b.IsLogGroupDeletionProtected("/aws/lambda/fn"))
			},
		},
		{
			name: "metric_filters_survive",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "/grp", "", "")
				require.NoError(t, err)
				err = b.PutMetricFilter(context.Background(), "/grp", "my-filter", "ERROR",
					[]cloudwatchlogs.MetricTransformation{{
						MetricName: "ErrCount", MetricNamespace: "App", MetricValue: "1",
					}})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				filters, _, err := b.DescribeMetricFilters(context.Background(), "/grp", "", "", "", "", 50)
				require.NoError(t, err)
				require.Len(t, filters, 1)
				assert.Equal(t, "my-filter", filters[0].FilterName)
			},
		},
		{
			name: "query_definitions_survive",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutQueryDefinition("my-query", "fields @message | limit 20", "", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				defs, _, err := b.DescribeQueryDefinitions("my-query", 100, "")
				require.NoError(t, err)
				require.Len(t, defs, 1)
				assert.Equal(t, "my-query", defs[0].Name)
			},
		},
		{
			name: "data_protection_policies_survive",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutDataProtectionPolicy("/grp", `{"Name":"protect"}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				doc, err := b.GetDataProtectionPolicy("/grp")
				require.NoError(t, err)
				assert.Contains(t, doc, "protect")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			t.Cleanup(func() { original.Close() })

			tt.setup(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			t.Cleanup(func() { fresh.Close() })
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh)
		})
	}
}
