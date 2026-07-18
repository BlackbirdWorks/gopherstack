package kinesis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *kinesis.InMemoryBackend) string
		verify func(t *testing.T, b *kinesis.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *kinesis.InMemoryBackend) string {
				err := b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
					StreamName: "test-stream",
					ShardCount: 1,
				})
				if err != nil {
					return ""
				}

				return "test-stream"
			},
			verify: func(t *testing.T, b *kinesis.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: id})
				require.NoError(t, err)
				assert.Equal(t, id, out.StreamName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *kinesis.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *kinesis.InMemoryBackend, _ string) {
				t.Helper()

				out, err := b.ListStreams(context.Background(), &kinesis.ListStreamsInput{})
				require.NoError(t, err)
				assert.Empty(t, out.StreamNames)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

// TestInMemoryBackend_OnDemandStreamCountLimit_SurvivesRestore verifies that
// UpdateAccountSettings' OnDemandStreamCountLimit is part of the persisted
// snapshot and not lost across a restart, and that a snapshot taken before
// any UpdateAccountSettings call restores the AWS default limit.
func TestInMemoryBackend_OnDemandStreamCountLimit_SurvivesRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		configure func(t *testing.T, b *kinesis.InMemoryBackend)
		name      string
		wantLimit int
	}{
		{
			name: "custom_limit_persists",
			configure: func(t *testing.T, b *kinesis.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.UpdateAccountSettings(context.Background(), &kinesis.UpdateAccountSettingsInput{
					OnDemandStreamCountLimit: 25,
				}))
			},
			wantLimit: 25,
		},
		{
			name:      "default_limit_when_unset",
			configure: func(_ *testing.T, _ *kinesis.InMemoryBackend) {},
			wantLimit: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			tt.configure(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			restored := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, restored.Restore(t.Context(), snap))

			out, err := restored.DescribeAccountSettings(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tt.wantLimit, out.OnDemandStreamCountLimit)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestSnapshot_EmptyShardRecords_NoNull verifies that an empty shard serialises to []
// (not null) so that snapshots remain stable after restore.
func TestSnapshot_EmptyShardRecords_NoNull(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "empty-shard-stream"}),
	)

	snap := bk.Snapshot(t.Context())
	require.NotNil(t, snap)
	// The JSON must not contain a null records field.
	assert.NotContains(t, string(snap), `"records":null`)
	assert.Contains(t, string(snap), `"records":[]`)
}

// TestSnapshot_RestoreClearsOldPointers verifies that restoring a smaller snapshot
// into a backend that previously held records does not retain stale pointers.
func TestSnapshot_RestoreClearsOldPointers(t *testing.T) {
	t.Parallel()

	// Create a backend with records in it.
	bk := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "ptr-stream"}))

	for range 5 {
		_, err := bk.PutRecord(context.Background(), &kinesis.PutRecordInput{
			StreamName:   "ptr-stream",
			PartitionKey: "pk",
			Data:         []byte("data"),
		})
		require.NoError(t, err)
	}

	// Take a snapshot of the populated state.
	snap := bk.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Now restore into the same backend (simulating an in-place restore).
	require.NoError(t, bk.Restore(t.Context(), snap))

	desc, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "ptr-stream"})
	require.NoError(t, err)
	assert.Len(t, desc.Shards, 1)
}

// TestPersistenceRoundTrip_HandlerTags verifies Snapshot/Restore preserves state.
func TestPersistenceRoundTrip_HandlerTags(t *testing.T) {
	t.Parallel()

	b1 := kinesis.NewInMemoryBackend()

	require.NoError(t, b1.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "persist-stream",
		ShardCount: 2,
		Region:     "us-east-1",
		AccountID:  "123456789012",
	}))
	require.NoError(t, b1.PutResourcePolicy(context.Background(), &kinesis.PutResourcePolicyInput{
		ResourceARN: "arn:aws:kinesis:us-east-1:123:stream/other",
		Policy:      `{"Version":"2012-10-17"}`,
	}))

	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := kinesis.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.StreamCount())
	assert.Equal(t, 1, b2.ResourcePolicyCount())
}

// TestTags_SurvivePersistenceRestore proves that stream tags applied
// via every tag-mutating operation (inline on CreateStream, the legacy
// AddTagsToStream API, and the ARN-based TagResource API) are stored on the
// backend's persisted stream.Tags field and therefore survive a
// Snapshot/Restore round-trip. Previously, CreateStream/AddTagsToStream/
// RemoveTagsFromStream/ListTagsForStream/ListTagsForResource operated on a
// handler-local map that was never included in the persisted snapshot, so
// those tags silently vanished on every restart (a real data-loss bug, not a
// "looks-wrong-but-correct" case) even though the stream itself and its
// shards/records persisted correctly.
func TestTags_SurvivePersistenceRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
		apply      func(t *testing.T, h *kinesis.Handler, streamName, streamARN string)
		wantKey    string
		wantValue  string
	}{
		{
			name:       "tag_from_create",
			streamName: "persist-tags-create",
			apply: func(t *testing.T, h *kinesis.Handler, streamName, _ string) {
				t.Helper()
				rec := doRequest(t, h, "CreateStream", map[string]any{
					"StreamName": streamName,
					"ShardCount": 1,
					"Tags":       map[string]any{"owner": "create"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantKey:   "owner",
			wantValue: "create",
		},
		{
			name:       "tag_from_add_tags_to_stream",
			streamName: "persist-tags-add",
			apply: func(t *testing.T, h *kinesis.Handler, streamName, _ string) {
				t.Helper()
				require.Equal(t, http.StatusOK, doRequest(t, h, "CreateStream", map[string]any{
					"StreamName": streamName,
					"ShardCount": 1,
				}).Code)
				rec := doRequest(t, h, "AddTagsToStream", map[string]any{
					"StreamName": streamName,
					"Tags":       map[string]string{"owner": "add-tags"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantKey:   "owner",
			wantValue: "add-tags",
		},
		{
			name:       "tag_from_tag_resource",
			streamName: "persist-tags-tagresource",
			apply: func(t *testing.T, h *kinesis.Handler, streamName, streamARN string) {
				t.Helper()
				require.Equal(t, http.StatusOK, doRequest(t, h, "CreateStream", map[string]any{
					"StreamName": streamName,
					"ShardCount": 1,
				}).Code)
				rec := doRequest(t, h, "TagResource", map[string]any{
					"ResourceARN": streamARN,
					"Tags":        map[string]string{"owner": "tag-resource"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantKey:   "owner",
			wantValue: "tag-resource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			h := kinesis.NewHandler(backend)

			// TagResource needs the ARN up front; build it the same way the
			// backend does so the sub-test can address the stream before it exists.
			streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/" + tt.streamName
			tt.apply(t, h, tt.streamName, streamARN)

			// Simulate a process restart: snapshot the backend, then restore
			// into a brand-new backend/handler pair (no shared state).
			snap := backend.Snapshot(t.Context())
			require.NotNil(t, snap)

			restoredBackend := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, restoredBackend.Restore(t.Context(), snap))
			restoredHandler := kinesis.NewHandler(restoredBackend)

			rec := doRequest(t, restoredHandler, "ListTagsForResource", map[string]any{
				"ResourceARN": streamARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			found := false
			for _, kv := range resp.Tags {
				if kv.Key == tt.wantKey {
					assert.Equal(t, tt.wantValue, kv.Value)
					found = true
				}
			}
			assert.True(t, found, "expected tag %q to survive persistence restore, got %+v", tt.wantKey, resp.Tags)
		})
	}
}
