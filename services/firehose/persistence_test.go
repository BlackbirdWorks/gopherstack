package firehose_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *firehose.InMemoryBackend) string
		verify func(t *testing.T, b *firehose.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *firehose.InMemoryBackend) string {
				stream, err := b.CreateDeliveryStream(
					context.TODO(),
					firehose.CreateDeliveryStreamInput{Name: "test-stream"},
				)
				if err != nil {
					return ""
				}

				return stream.Name
			},
			verify: func(t *testing.T, b *firehose.InMemoryBackend, id string) {
				t.Helper()

				stream, err := b.DescribeDeliveryStream(context.TODO(), id)
				require.NoError(t, err)
				assert.Equal(t, id, stream.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *firehose.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *firehose.InMemoryBackend, _ string) {
				t.Helper()

				streams := b.ListDeliveryStreams(context.TODO())
				assert.Empty(t, streams)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestRestore_ClosesExistingTagsBeforeReplace verifies that Restore closes Tags on
// streams that are being replaced, preventing Prometheus registry leaks.
func TestRestore_ClosesExistingTagsBeforeReplace(t *testing.T) {
	t.Parallel()

	// Populate a backend with two tagged streams.
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "old-stream"})
	require.NoError(t, err)
	require.NoError(t, b.TagDeliveryStream(context.TODO(), "old-stream", map[string]string{"gen": "old"}))

	// Build a snapshot that contains a different set of streams.
	newBackend := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err = newBackend.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "new-stream"})
	require.NoError(t, err)
	snap := newBackend.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore onto the populated backend — must not panic and must close old Tags.
	require.NoError(t, b.Restore(t.Context(), snap))

	// Only the new stream should be visible now.
	names := b.ListDeliveryStreams(context.TODO())
	assert.Equal(t, []string{"new-stream"}, names)
}

// TestRestore_RecalculatesBufferSizeBytes verifies that bufferSizeBytes is correctly
// recomputed after a snapshot/restore cycle, so that size-based flush thresholds
// fire correctly on the restored backend.
func TestRestore_RecalculatesBufferSizeBytes(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}

	// Create a stream and buffer records without triggering a flush.
	original := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	original.SetS3Backend(s3mock)
	_, err := original.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "restore-size-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::restore-bucket",
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 300,
			},
		},
	})
	require.NoError(t, err)

	// Put 900 KB — below threshold so no flush yet.
	require.NoError(t, original.PutRecord(context.TODO(), "restore-size-stream", make([]byte, 900*1024)))
	assert.Empty(t, s3mock.calls)

	// Snapshot and restore onto a fresh backend.
	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	restored.SetS3Backend(s3mock)
	require.NoError(t, restored.Restore(t.Context(), snap))

	// Adding another 200 KB should now push over the 1 MB threshold and flush.
	require.NoError(t, restored.PutRecord(context.TODO(), "restore-size-stream", make([]byte, 200*1024)))
	assert.Len(t, s3mock.calls, 1, "size-based flush should fire after restore")
}

// TestInMemoryBackend_SnapshotRestore_FullState is the Phase 3.3 datalayer conversion's
// full-state round-trip: it exercises every field path the streams store.Table (see
// store_setup.go) carries through Snapshot/Restore, plus the two same-named streams in
// different regions that only the "region|name" composite key (rather than the old
// map[region]map[name]*DeliveryStream nesting) can keep isolated from each other.
// AccountID/Region and the streams table are the only backend state persistence.go
// carries (see backendSnapshot); pollerCancel/sortedNamesCache/pendingFlush are ephemeral
// caches never persisted, before or after this conversion.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := firehose.NewInMemoryBackend("111122223333", "us-east-1")

	// A fully-populated stream in the backend's default region: destination, tags,
	// buffered (unflushed) records, and a delivery-failure metric.
	_, err := original.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "shared-name",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::east-bucket",
			Prefix:    "east/",
		},
	})
	require.NoError(t, err)
	require.NoError(t, original.TagDeliveryStream(
		context.TODO(), "shared-name", map[string]string{"env": "prod", "region": "east"},
	))
	require.NoError(t, original.PutRecord(context.TODO(), "shared-name", []byte("east-record")))

	// A second stream with the SAME name, seeded directly into a different region via
	// AddStreamInternal (the only public way to place a stream outside the backend's
	// default region without driving the HTTP layer's per-request region header). Table's
	// composite key must keep this fully distinct from "shared-name" above.
	original.AddStreamInternal(&firehose.DeliveryStream{
		Name:               "shared-name",
		ARN:                "arn:aws:firehose:us-west-2:111122223333:deliverystream/shared-name",
		Region:             "us-west-2",
		AccountID:          "111122223333",
		Status:             "ACTIVE",
		DeliveryStreamType: "DirectPut",
		VersionID:          "1",
	})

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := firehose.NewInMemoryBackend("111122223333", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	// The default-region stream round-trips with its destination, tags and buffered record.
	east, err := restored.DescribeDeliveryStream(context.TODO(), "shared-name")
	require.NoError(t, err)
	require.NotNil(t, east.S3Destination)
	assert.Equal(t, "arn:aws:s3:::east-bucket", east.S3Destination.BucketARN)
	assert.Equal(t, "east/", east.S3Destination.Prefix)

	tagList, err := restored.ListTagsForDeliveryStream(context.TODO(), "shared-name")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "region": "east"}, tagList)

	// The us-west-2 entry survived under its own composite key, isolated from the
	// same-named us-east-1 entry (StreamFailedRecords returns -1 only when the
	// region|name key is absent).
	assert.NotEqual(t, int64(-1), firehose.StreamFailedRecords(restored, "us-west-2", "shared-name"))
	assert.Equal(t, 2, firehose.StreamCount(restored), "both regions' streams must survive the round trip")
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that Restore discards (rather than
// partially decodes) a snapshot whose version does not match the current
// firehoseSnapshotVersion -- both an explicit future/incompatible version and the
// version-absent shape a pre-Phase-3.3 snapshot would produce -- resetting the backend to
// empty instead of erroring.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, snap map[string]any)
		name   string
	}{
		{
			name: "future_version",
			mutate: func(t *testing.T, snap map[string]any) {
				t.Helper()
				snap["version"] = 999
			},
		},
		{
			name: "absent_version_pre_phase_3_3_shape",
			mutate: func(t *testing.T, snap map[string]any) {
				t.Helper()
				delete(snap, "version")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := source.CreateDeliveryStream(
				context.TODO(), firehose.CreateDeliveryStreamInput{Name: "source-stream"},
			)
			require.NoError(t, err)

			raw := source.Snapshot(t.Context())
			require.NotNil(t, raw)

			var generic map[string]any
			require.NoError(t, json.Unmarshal(raw, &generic))
			tt.mutate(t, generic)
			mutated, err := json.Marshal(generic)
			require.NoError(t, err)

			// Target already holds unrelated state, which the version-mismatch path must
			// discard (registry.ResetAll) rather than merge with the incompatible snapshot.
			target := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			_, err = target.CreateDeliveryStream(
				context.TODO(), firehose.CreateDeliveryStreamInput{Name: "pre-existing"},
			)
			require.NoError(t, err)

			require.NoError(t, target.Restore(t.Context(), mutated))

			assert.Empty(t, target.ListDeliveryStreams(context.TODO()))
			assert.Equal(t, 0, firehose.StreamCount(target))
		})
	}
}

// TestInMemoryBackend_RestoreV1MSKReadFromTimestampDiscarded proves
// gopherstack-hjdd's fix: a v1 snapshot holding
// MSKSourceDescription.ReadFromTimestamp in the pre-d83f4b5d3 string shape
// must be discarded cleanly now that firehoseSnapshotVersion is 2, rather
// than erroring Restore outright when the registered "streams" table's
// float64-typed field can't decode a JSON string.
func TestInMemoryBackend_RestoreV1MSKReadFromTimestampDiscarded(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")

	v1Snapshot := []byte(`{
		"version": 1,
		"tables": {
			"streams": [{
				"name": "old-stream",
				"arn": "arn:aws:firehose:us-east-1:000000000000:deliverystream/old-stream",
				"status": "ACTIVE",
				"accountID": "000000000000",
				"region": "us-east-1",
				"source": {
					"MSKSourceDescription": {
						"MSKClusterARN": "arn:aws:kafka:us-east-1:000000000000:cluster/c1",
						"TopicName": "topic-1",
						"ReadFromTimestamp": "2024-01-01T00:00:00Z"
					}
				}
			}]
		}
	}`)

	require.NoError(t, b.Restore(t.Context(), v1Snapshot),
		"a v1 snapshot must be discarded via the version guard, not error out of RestoreAll")

	_, err := b.DescribeDeliveryStream(t.Context(), "old-stream")
	require.ErrorIs(t, err, firehose.ErrNotFound,
		"incompatible-version snapshot must reset to empty, not partially decode")
}
