package kinesisanalytics //nolint:testpackage // needs kaCtxRegion and the unexported snapshot version const.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip exercises every kind
// of state the Phase 3.3 pkgs/store conversion touches in a single pass:
// multiple applications across multiple regions, every sub-resource kind
// (CloudWatch logging option, input, output, reference data source), tags,
// and the account/region/next-ID scalars persisted alongside the apps Table.
// It proves store.Registry.SnapshotAll/RestoreAll round-trips the flat apps
// Table (keyed by "region#name", see applicationKey) exactly like the
// region-nested map it replaced, and that the byRegion/byARN indexes rebuild
// correctly on Restore.
func TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	ctxEast := kaCtxRegion("us-east-1")
	ctxWest := kaCtxRegion("us-west-2")

	original := NewInMemoryBackend("us-east-1", "000000000000")

	eastApp, err := original.CreateApplication(
		ctxEast, "alpha", "east desc", "SELECT 1", "role-arn",
		[]InputDescription{{
			NamePrefix:       "in",
			InputParallelism: &InputParallelism{Count: 1},
			KinesisStreamsInputDescription: &KinesisStreamsInputDesc{
				ResourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/east-in",
				RoleARN:     "role-arn",
			},
		}},
		[]OutputDescription{{
			Name: "out",
			KinesisStreamsOutputDescription: &KinesisStreamsOutputDesc{
				ResourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/east-out",
				RoleARN:     "role-arn",
			},
			DestinationSchema: &DestinationSchemaDesc{RecordFormatType: recordFormatJSON},
		}},
		[]CloudWatchLoggingOptionDesc{{
			LogStreamARN: "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
			RoleARN:      "role-arn",
		}},
		map[string]string{"env": "east"},
	)
	require.NoError(t, err)

	require.NoError(t, original.AddApplicationReferenceDataSource(
		ctxEast, "alpha", eastApp.ApplicationVersionID,
		ReferenceDataSourceDescription{
			TableName: "ref-table",
			S3ReferenceDataSourceDescription: &S3ReferenceDataSourceDesc{
				BucketARN: "arn:aws:s3:::bucket", FileKey: "key", RoleARN: "role-arn",
			},
		},
	))

	westApp, err := original.CreateApplication(
		ctxWest, "alpha", "west desc", "SELECT 2", "", nil, nil, nil, map[string]string{"env": "west"},
	)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("us-east-1", "000000000000")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Both same-named applications, across both regions, survived under the flat Table.
	assert.Equal(t, 2, ApplicationCount(fresh))

	// Region isolation preserved: each region's ListApplications sees only its own.
	eastList, _, err := fresh.ListApplications(ctxEast, "", 0)
	require.NoError(t, err)
	require.Len(t, eastList, 1)
	assert.Equal(t, "SELECT 1", eastList[0].ApplicationCode)

	westList, _, err := fresh.ListApplications(ctxWest, "", 0)
	require.NoError(t, err)
	require.Len(t, westList, 1)
	assert.Equal(t, "SELECT 2", westList[0].ApplicationCode)

	// Sub-resources (inputs/outputs/CWL options/reference data sources) survived.
	eastAfter, err := fresh.DescribeApplication(ctxEast, "alpha")
	require.NoError(t, err)
	require.Len(t, eastAfter.Inputs, 1)
	assert.Equal(t, "in", eastAfter.Inputs[0].NamePrefix)
	require.Len(t, eastAfter.Outputs, 1)
	assert.Equal(t, "out", eastAfter.Outputs[0].Name)
	require.Len(t, eastAfter.CloudWatchLoggingOptions, 1)
	require.Len(t, eastAfter.ReferenceDataSources, 1)
	assert.Equal(t, "ref-table", eastAfter.ReferenceDataSources[0].TableName)
	assert.Contains(t, eastAfter.ApplicationARN, "us-east-1")

	westAfter, err := fresh.DescribeApplication(ctxWest, "alpha")
	require.NoError(t, err)
	assert.Contains(t, westAfter.ApplicationARN, "us-west-2")
	assert.NotEqual(t, eastAfter.ApplicationARN, westAfter.ApplicationARN)

	// Tags survived.
	eastTags, err := fresh.ListTagsForResource(context.Background(), eastAfter.ApplicationARN)
	require.NoError(t, err)
	assert.Equal(t, "east", eastTags["env"])

	// byARN index rebuilt: TagResource/UntagResource work via ARN post-restore.
	// (ListTagsForResource/TagResource validate that the ARN's embedded region
	// matches the request's region, so the west ARN needs ctxWest here.)
	require.NoError(t, fresh.TagResource(ctxWest, westApp.ApplicationARN, map[string]string{"new": "tag"}))
	westTags, err := fresh.ListTagsForResource(ctxWest, westApp.ApplicationARN)
	require.NoError(t, err)
	assert.Equal(t, "tag", westTags["new"])
	assert.Equal(t, "west", westTags["env"])

	// NextID counter survived: a newly created sub-resource on the restored
	// backend must not collide with the ID handed out (pre-snapshot) to the
	// application's existing CloudWatch logging option.
	preSnapshotOptionID := eastAfter.CloudWatchLoggingOptions[0].CloudWatchLoggingOptionID
	require.NoError(t, fresh.AddApplicationCloudWatchLoggingOption(
		ctxEast, "alpha", eastAfter.ApplicationVersionID,
		CloudWatchLoggingOptionDesc{
			LogStreamARN: "arn:aws:logs:us-east-1:000000000000:log-group:g2:log-stream:s2",
			RoleARN:      "role-arn",
		},
	))
	eastFinal, err := fresh.DescribeApplication(ctxEast, "alpha")
	require.NoError(t, err)
	require.Len(t, eastFinal.CloudWatchLoggingOptions, 2)
	newOptionID := eastFinal.CloudWatchLoggingOptions[1].CloudWatchLoggingOptionID
	assert.NotEmpty(t, newOptionID)
	assert.NotEqual(t, preSnapshotOptionID, newOptionID)
}

// TestInMemoryBackend_Restore_IncompatibleVersion_ResetsEmpty verifies the
// snapshot version guard: a snapshot whose "version" field does not match the
// backend's current kinesisanalyticsSnapshotVersion is never partially
// decoded as the current shape. Restore must discard it, reset to empty, and
// return nil (not an error) -- the same recoverable-condition contract
// established by services/kinesis and services/sqs for their own version
// guards.
func TestInMemoryBackend_Restore_IncompatibleVersion_ResetsEmpty(t *testing.T) {
	t.Parallel()

	ctx := kaCtxRegion("us-east-1")

	original := NewInMemoryBackend("us-east-1", "000000000000")
	_, err := original.CreateApplication(ctx, "app", "", "", "", nil, nil, nil, nil)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Corrupt just the version field to simulate a snapshot from an
	// incompatible (older/newer) build.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snap, &raw))
	bumped, err := json.Marshal(kinesisanalyticsSnapshotVersion + 1)
	require.NoError(t, err)
	raw["version"] = bumped
	mutated, err := json.Marshal(raw)
	require.NoError(t, err)

	fresh := NewInMemoryBackend("us-east-1", "000000000000")
	require.NoError(t, fresh.Restore(t.Context(), mutated))

	assert.Equal(t, 0, ApplicationCount(fresh))
	out, _, err := fresh.ListApplications(ctx, "", 0)
	require.NoError(t, err)
	assert.Empty(t, out)
}

// TestInMemoryBackend_Restore_AbsentVersion_ResetsEmpty verifies that a
// pre-Phase-3.3 snapshot (no "version" field at all, decoding as Version 0)
// is treated the same as any other incompatible version: discarded, not
// partially decoded.
func TestInMemoryBackend_Restore_AbsentVersion_ResetsEmpty(t *testing.T) {
	t.Parallel()

	fresh := NewInMemoryBackend("us-east-1", "000000000000")

	legacy := []byte(`{"apps":{"us-east-1":{"legacy-app":{"ApplicationName":"legacy-app"}}},"next_id":7}`)
	require.NoError(t, fresh.Restore(t.Context(), legacy))

	assert.Equal(t, 0, ApplicationCount(fresh))
}

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error (not silently discarded like a version mismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("us-east-1", "000000000000")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}
