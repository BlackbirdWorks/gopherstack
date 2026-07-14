package kinesisanalyticsv2 //nolint:testpackage // needs ctxRegion's unexported region context key

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// persistenceFixtureIDs carries every identifier newPersistenceTestBackend
// creates, so the round-trip assertions in
// Test_InMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore without re-deriving names.
type persistenceFixtureIDs struct {
	appName        string
	appARN         string
	otherRegionApp string
	snapshotName   string
	cwlOptionID    string
	inputID        string
	outputID       string
	referenceID    string
	vpcID          string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (applications, snapshots) and every secondary store.Index
// derived from them (byRegion/byARN on applications, byApp on snapshots),
// plus every sub-resource nested inside Application (tags,
// CloudWatchLoggingOptionDescs, InputDescriptions with a processing config,
// OutputDescriptions, ReferenceDataSourceDescriptions,
// VpcConfigurationDescriptions), so a Snapshot from it exercises the entire
// persisted surface of the backend. A second application is created in a
// different region under the same name to exercise the region#name
// composite key and the byRegion index distinguishing same-named
// applications across regions.
func newPersistenceTestBackend(t *testing.T) (*InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	app, err := b.CreateApplication(
		ctx, "app1", "FLINK-1_18", "arn:aws:iam::000000000000:role/svc", "desc", "STREAMING",
		[]Tag{{Key: "env", Value: "test"}},
	)
	require.NoError(t, err)

	// currentVersionID 0 skips the optimistic-concurrency check (see
	// checkAndBumpVersion), so every Add* call below can use the same app
	// snapshot without re-fetching between calls.
	const skipVersionCheck = 0

	require.NoError(t, b.AddApplicationCloudWatchLoggingOption(
		ctx, app.ApplicationName, skipVersionCheck,
		"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s", "arn:aws:iam::000000000000:role/logs",
	))

	require.NoError(t, b.AddApplicationInput(ctx, app.ApplicationName, skipVersionCheck, InputDescription{
		NamePrefix: "in1",
		KinesisStreamsInputDescription: &KinesisStreamsInputDesc{
			ResourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/in1",
		},
	}))

	refreshed, err := b.DescribeApplication(ctx, app.ApplicationName)
	require.NoError(t, err)
	require.Len(t, refreshed.InputDescriptions, 1)
	inputID := refreshed.InputDescriptions[0].InputID

	require.NoError(t, b.AddApplicationInputProcessingConfiguration(
		ctx, app.ApplicationName, skipVersionCheck, inputID,
		&InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: "arn:aws:lambda:us-east-1:000000000000:function:proc",
			},
		},
	))

	require.NoError(t, b.AddApplicationOutput(ctx, app.ApplicationName, skipVersionCheck, OutputDescription{
		Name: "out1",
		KinesisStreamsOutputDescription: &KinesisStreamsOutputDesc{
			ResourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/out1",
		},
	}))

	require.NoError(t, b.AddApplicationReferenceDataSource(
		ctx, app.ApplicationName, skipVersionCheck, ReferenceDataSourceDescription{
			TableName: "ref1",
			S3ReferenceDataSourceDescription: &S3ReferenceDataSourceDesc{
				BucketARN: "arn:aws:s3:::bucket", FileKey: "key",
			},
		},
	))

	require.NoError(t, b.AddApplicationVpcConfiguration(
		ctx, app.ApplicationName, skipVersionCheck, VpcConfigurationDescription{
			VpcID:            "vpc-1",
			SubnetIDs:        []string{"subnet-1"},
			SecurityGroupIDs: []string{"sg-1"},
		},
	))

	_, err = b.StartApplication(ctx, app.ApplicationName)
	require.NoError(t, err)

	snap, err := b.CreateApplicationSnapshot(ctx, app.ApplicationName, "snap1")
	require.NoError(t, err)

	// A same-named application in a different region: must round-trip as a
	// distinct entry under the region#name composite key, not collide with
	// (or overwrite) app1 in us-east-1.
	_, err = b.CreateApplication(ctxRegion("eu-west-1"), "app1", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	final, err := b.DescribeApplication(ctx, app.ApplicationName)
	require.NoError(t, err)
	require.Len(t, final.CloudWatchLoggingOptionDescs, 1)
	require.Len(t, final.OutputDescriptions, 1)
	require.Len(t, final.ReferenceDataSourceDescriptions, 1)
	require.Len(t, final.VpcConfigurationDescriptions, 1)

	return b, persistenceFixtureIDs{
		appName:        final.ApplicationName,
		appARN:         final.ApplicationARN,
		otherRegionApp: "app1",
		snapshotName:   snap.SnapshotName,
		cwlOptionID:    final.CloudWatchLoggingOptionDescs[0].CloudWatchLoggingOptionID,
		inputID:        inputID,
		outputID:       final.OutputDescriptions[0].OutputID,
		referenceID:    final.ReferenceDataSourceDescriptions[0].ReferenceID,
		vpcID:          final.VpcConfigurationDescriptions[0].VpcConfigurationID,
	}
}

// Test_InMemoryBackend_SnapshotRestore_FullState round-trips both
// store.Tables (applications, snapshots), every secondary store.Index
// derived from them (byRegion/byARN on applications, byApp on snapshots),
// and every json:"-" sub-field carried through the ephemeral DTO registry
// (persistence.go) through Snapshot -> Restore into a fresh backend.
// operations and versions are deliberately not asserted here: neither is
// persisted before or after this conversion (see the InMemoryBackend doc
// comment in backend.go).
func Test_InMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 2, ApplicationCount(fresh))
	assert.Equal(t, 1, SnapshotCount(fresh))

	// applications table: every sub-field survives the DTO round trip.
	app, err := fresh.DescribeApplication(t.Context(), ids.appName)
	require.NoError(t, err)
	assert.Equal(t, ids.appName, app.ApplicationName)
	assert.Equal(t, ids.appARN, app.ApplicationARN)
	assert.Equal(t, "FLINK-1_18", app.RuntimeEnvironment)
	assert.Equal(t, ApplicationStatusRunning, app.ApplicationStatus)
	require.Len(t, app.Tags, 1)
	assert.Equal(t, Tag{Key: "env", Value: "test"}, app.Tags[0])
	require.Len(t, app.CloudWatchLoggingOptionDescs, 1)
	assert.Equal(t, ids.cwlOptionID, app.CloudWatchLoggingOptionDescs[0].CloudWatchLoggingOptionID)
	require.Len(t, app.InputDescriptions, 1)
	assert.Equal(t, ids.inputID, app.InputDescriptions[0].InputID)
	require.NotNil(t, app.InputDescriptions[0].InputProcessingConfigurationDescription)
	assert.Equal(t,
		"arn:aws:lambda:us-east-1:000000000000:function:proc",
		app.InputDescriptions[0].InputProcessingConfigurationDescription.InputLambdaProcessor.ResourceARN,
	)
	require.Len(t, app.OutputDescriptions, 1)
	assert.Equal(t, ids.outputID, app.OutputDescriptions[0].OutputID)
	require.Len(t, app.ReferenceDataSourceDescriptions, 1)
	assert.Equal(t, ids.referenceID, app.ReferenceDataSourceDescriptions[0].ReferenceID)
	require.Len(t, app.VpcConfigurationDescriptions, 1)
	assert.Equal(t, ids.vpcID, app.VpcConfigurationDescriptions[0].VpcConfigurationID)

	// applicationsByRegion index: ListApplications only returns the
	// us-east-1 application, not the eu-west-1 one.
	list, _ := fresh.ListApplications(t.Context(), "")
	require.Len(t, list, 1)
	assert.Equal(t, ids.appName, list[0].ApplicationName)

	// region#name composite key: the same-named eu-west-1 application
	// round-tripped as a distinct entry, not a collision with us-east-1.
	otherApp, err := fresh.DescribeApplication(ctxRegion("eu-west-1"), ids.otherRegionApp)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", otherApp.Region)

	// applicationsByARN index (via TagResource -> findByARN).
	tags, err := fresh.ListTagsForResource(t.Context(), ids.appARN)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].Key)

	// snapshots table + snapshotsByApp index.
	snapDetail, err := fresh.DescribeApplicationSnapshot(t.Context(), ids.appName, ids.snapshotName)
	require.NoError(t, err)
	assert.Equal(t, ids.snapshotName, snapDetail.SnapshotName)
	assert.Equal(t, "READY", snapDetail.SnapshotStatus)

	snapList, _, err := fresh.ListApplicationSnapshots(t.Context(), ids.appName, "")
	require.NoError(t, err)
	require.Len(t, snapList, 1)
	assert.Equal(t, ids.snapshotName, snapList[0].SnapshotName)

	// Sanity: account/region carried through construction (not persisted;
	// NewInMemoryBackend always takes them as constructor args -- see
	// backendSnapshot's doc comment in persistence.go).
	assert.Equal(t, "us-east-1", fresh.Region())
	assert.Equal(t, "000000000000", fresh.AccountID())
}

// Test_InMemoryBackend_SnapshotRestore_VersionMismatch verifies that a
// snapshot whose version doesn't match the current backend (including the
// pre-Phase-3.3 format, which decodes with Version == 0) is discarded
// cleanly rather than partially decoded: the backend resets to empty state
// and Restore returns no error.
func Test_InMemoryBackend_SnapshotRestore_VersionMismatch(t *testing.T) {
	t.Parallel()

	b, ids := newPersistenceTestBackend(t)

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Zero(t, ApplicationCount(b))
	assert.Zero(t, SnapshotCount(b))

	_, err = b.DescribeApplication(t.Context(), ids.appName)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = b.DescribeApplicationSnapshot(t.Context(), ids.appName, ids.snapshotName)
	require.ErrorIs(t, err, ErrNotFound)

	// A pre-Phase-3.3 snapshot (no version field at all) decodes as
	// Version == 0, which must be treated the same as any other mismatch.
	b2, _ := newPersistenceTestBackend(t)
	err = b2.Restore(t.Context(), []byte(`{"tables":{}}`))
	require.NoError(t, err)
	assert.Zero(t, ApplicationCount(b2))
}

// Test_InMemoryBackend_SnapshotRestore_InvalidData verifies malformed JSON
// surfaces as an error rather than being silently discarded (that path is
// reserved for a syntactically valid but version-mismatched snapshot; see
// Test_InMemoryBackend_SnapshotRestore_VersionMismatch).
func Test_InMemoryBackend_SnapshotRestore_InvalidData(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// Test_Handler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (already wired before this conversion, unlike some
// other Phase 3.3 services -- see persistence.go).
func Test_Handler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, ids := newPersistenceTestBackend(t)
	h := NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := NewHandler(NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	app, err := h2.Backend.DescribeApplication(t.Context(), ids.appName)
	require.NoError(t, err)
	assert.Equal(t, ids.appName, app.ApplicationName)
}
