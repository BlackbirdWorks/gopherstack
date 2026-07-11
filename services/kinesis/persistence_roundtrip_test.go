package kinesis //nolint:testpackage // reuses ctxRegion (isolation_test.go) for direct region control.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip exercises every kind
// of state the Phase 3.3 pkgs/store conversion touches in a single pass:
// multiple streams across multiple regions, inline shard records, inline
// consumers, tags, resource policies, and the account-level on-demand stream
// count limit. It proves store.Registry.SnapshotAll/RestoreAll round-trips
// the flat streams Table (keyed by region/name, see streamKey) exactly like
// the region-nested map it replaced, and that the raw (non-Table)
// resourcePolicies map still persists alongside it.
func TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	original := NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	require.NoError(t, original.CreateStream(ctxEast, &CreateStreamInput{
		StreamName: "alpha",
		ShardCount: 2,
	}))
	require.NoError(t, original.CreateStream(ctxWest, &CreateStreamInput{
		StreamName: "beta",
		ShardCount: 1,
	}))

	// Inline shard records (hot path, stays inline per Stream -- not decomposed).
	_, err := original.PutRecord(ctxEast, &PutRecordInput{
		StreamName: "alpha", PartitionKey: "pk", Data: []byte("east-data"),
	})
	require.NoError(t, err)
	_, err = original.PutRecord(ctxWest, &PutRecordInput{
		StreamName: "beta", PartitionKey: "pk", Data: []byte("west-data"),
	})
	require.NoError(t, err)

	alphaDesc, err := original.DescribeStream(ctxEast, &DescribeStreamInput{StreamName: "alpha"})
	require.NoError(t, err)
	betaDesc, err := original.DescribeStream(ctxWest, &DescribeStreamInput{StreamName: "beta"})
	require.NoError(t, err)

	// Inline consumers (Stream.Consumers) on the east stream.
	_, err = original.RegisterStreamConsumer(ctxEast, &RegisterStreamConsumerInput{
		StreamARN: alphaDesc.StreamARN, ConsumerName: "fanout",
	})
	require.NoError(t, err)

	// Tags on the west stream.
	require.NoError(t, original.TagResource(ctxWest, &TagResourceInput{
		ResourceARN: betaDesc.StreamARN,
		Tags:        map[string]string{"env": "test"},
	}))

	// Resource policy (raw, non-Table map) keyed by ARN.
	require.NoError(t, original.PutResourcePolicy(ctx, &PutResourcePolicyInput{
		ResourceARN: alphaDesc.StreamARN,
		Policy:      `{"Version":"2012-10-17"}`,
	}))

	// Account-level setting persisted alongside the streams table.
	require.NoError(t, original.UpdateAccountSettings(ctx, &UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: 42,
	}))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Both streams, across both regions, survived under the flat Table.
	assert.Equal(t, 2, fresh.StreamCount())
	all := fresh.ListAll(ctx)
	assert.Len(t, all, 2)

	// Region isolation preserved: each region's ListStreams sees only its own.
	eastList, err := fresh.ListStreams(ctxEast, &ListStreamsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha"}, eastList.StreamNames)

	westList, err := fresh.ListStreams(ctxWest, &ListStreamsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"beta"}, westList.StreamNames)

	// Inline shard records survived, keyed to the correct region's stream.
	alphaAfter, err := fresh.DescribeStream(ctxEast, &DescribeStreamInput{StreamName: "alpha"})
	require.NoError(t, err)
	assert.Len(t, alphaAfter.Shards, 2)
	assert.Contains(t, alphaAfter.StreamARN, "us-east-1")

	betaAfter, err := fresh.DescribeStream(ctxWest, &DescribeStreamInput{StreamName: "beta"})
	require.NoError(t, err)
	assert.Len(t, betaAfter.Shards, 1)
	assert.Contains(t, betaAfter.StreamARN, "us-west-2")

	eastIt, err := fresh.GetShardIterator(ctxEast, &GetShardIteratorInput{
		StreamName: "alpha", ShardID: "shardId-000000000000", ShardIteratorType: iteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	eastRecs, err := fresh.GetRecords(ctx, &GetRecordsInput{ShardIterator: eastIt.ShardIterator})
	require.NoError(t, err)
	require.Len(t, eastRecs.Records, 1)
	assert.Equal(t, []byte("east-data"), eastRecs.Records[0].Data)

	westIt, err := fresh.GetShardIterator(ctxWest, &GetShardIteratorInput{
		StreamName: "beta", ShardID: "shardId-000000000000", ShardIteratorType: iteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	westRecs, err := fresh.GetRecords(ctx, &GetRecordsInput{ShardIterator: westIt.ShardIterator})
	require.NoError(t, err)
	require.Len(t, westRecs.Records, 1)
	assert.Equal(t, []byte("west-data"), westRecs.Records[0].Data)

	// Inline consumer survived (ARN-routed, region-independent of ctx).
	consumerOut, err := fresh.DescribeStreamConsumer(ctx, &DescribeStreamConsumerInput{
		StreamARN: alphaDesc.StreamARN, ConsumerName: "fanout",
	})
	require.NoError(t, err)
	assert.Equal(t, "fanout", consumerOut.ConsumerDescription.ConsumerName)

	// Tags survived.
	tagsOut, err := fresh.ListTagsForResource(ctx, &ListTagsForResourceInput{ResourceARN: betaDesc.StreamARN})
	require.NoError(t, err)
	assert.Equal(t, "test", tagsOut.Tags["env"])

	// Resource policy (raw map, not a store.Table) survived.
	policyOut, err := fresh.GetResourcePolicy(ctx, &GetResourcePolicyInput{ResourceARN: alphaDesc.StreamARN})
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policyOut.Policy)

	// Account setting persisted alongside the streams table.
	settingsOut, err := fresh.DescribeAccountSettings(ctx)
	require.NoError(t, err)
	assert.Equal(t, 42, settingsOut.OnDemandStreamCountLimit)
}

// TestInMemoryBackend_Restore_IncompatibleVersion_ResetsEmpty verifies the
// snapshot version guard: a snapshot whose "version" field does not match the
// backend's current kinesisSnapshotVersion is never partially decoded as the
// current shape. Restore must discard it, reset to empty, and return nil
// (not an error) -- this is the same recoverable-condition contract sqs's
// pilot conversion established for its own version guard.
func TestInMemoryBackend_Restore_IncompatibleVersion_ResetsEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	original := NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, original.CreateStream(ctx, &CreateStreamInput{StreamName: "s", ShardCount: 1}))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Corrupt just the version field to simulate a snapshot from an
	// incompatible (older/newer) build.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snap, &raw))
	bumped, err := json.Marshal(kinesisSnapshotVersion + 1)
	require.NoError(t, err)
	raw["version"] = bumped
	mutated, err := json.Marshal(raw)
	require.NoError(t, err)

	fresh := NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), mutated))

	assert.Equal(t, 0, fresh.StreamCount())
	out, err := fresh.ListStreams(ctx, &ListStreamsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.StreamNames)
}
