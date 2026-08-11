package kms

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tagsMapLen(h *Handler) int {
	h.tagsMu.RLock("test.tagsMapLen")
	defer h.tagsMu.RUnlock()

	return len(h.tags)
}

func sweepJanitor(ctx context.Context, h *Handler) {
	if h.janitor == nil {
		return
	}

	h.janitor.SweepOnce(ctx)
}

// TestTagsLeak_PurgeKey verifies that permanently deleting a key (via
// ScheduleKeyDeletion's pending window elapsing and a janitor sweep) removes
// its entry from Handler.tags, the handler-level side map of *tags.Tags
// keyed by KeyID. Handler.tags lives entirely outside InMemoryBackend, so
// unlike every other per-key index the janitor already cascade-cleans
// (aliases, grants, lastUsage), nothing removes a tags entry unless the
// janitor is explicitly wired to do so via Janitor.OnKeyPurged (set in
// Handler.WithJanitor). Since KMS key IDs are UUIDs that are never reused, an
// unfixed regression here leaks one *tags.Tags (and the lockmetrics/
// Prometheus registration it owns) per tagged key for the remaining
// lifetime of the process.
func TestTagsLeak_PurgeKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tagOnly []bool // per-key: whether to tag that key before purging it
	}{
		{name: "single_tagged_key_purged", tagOnly: []bool{true}},
		{name: "untagged_key_purged_no_error", tagOnly: []bool{false}},
		{name: "tagged_key_purged_leaves_untouched_key_alone", tagOnly: []bool{true, false}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			h := NewHandler(NewInMemoryBackend())
			h.WithJanitor(time.Hour)

			mem, ok := h.Backend.(*InMemoryBackend)
			require.True(t, ok)

			keyIDs := make([]string, len(tt.tagOnly))

			for i, tagged := range tt.tagOnly {
				out, err := mem.CreateKey(ctx, &CreateKeyInput{Description: "tags-leak-test"})
				require.NoError(t, err)
				keyIDs[i] = out.KeyMetadata.KeyID

				if tagged {
					h.setTags(keyIDs[i], map[string]string{"env": "test"})
					assert.NotEmpty(t, h.getTags(keyIDs[i]), "tag must be set before purge")
				}

				_, err = mem.ScheduleKeyDeletion(ctx, &ScheduleKeyDeletionInput{
					KeyID:               keyIDs[i],
					PendingWindowInDays: 7,
				})
				require.NoError(t, err)
				mem.SetDeletionDateForTest(keyIDs[i], time.Now().Add(-time.Second))
			}

			sweepJanitor(ctx, h)

			assert.Equal(t, 0, KeyCount(mem), "all keys must be purged")
			assert.Equal(t, 0, tagsMapLen(h),
				"Handler.tags must not retain entries for permanently purged keys")

			for _, kid := range keyIDs {
				assert.Empty(t, h.getTags(kid), "purged key %s must report no tags", kid)
			}
		})
	}
}
