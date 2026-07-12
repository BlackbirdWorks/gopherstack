package kms_test

// handler_tags_persistence_test.go — regression for Handler-level KMS resource tags
// being silently dropped across a Snapshot/Restore cycle.
//
// KMS tags are NOT part of InMemoryBackend's own state: CreateKey/TagResource apply them
// to a side map (Handler.tags, keyed by KeyID) rather than embedding them in the Key
// struct the way most other gopherstack services embed *tags.Tags directly in their
// resource type (see .claude/memories/pkgs-catalog.md's tags entry). Before this fix,
// Handler.Snapshot delegated straight to Backend.Snapshot and never serialized
// Handler.tags at all, so a gopherstack process restart with persistence enabled would
// silently lose every KMS key's tags -- even though ListResourceTags kept returning them
// correctly within a single running process. That is exactly the class of bug that
// produces perpetual `terraform plan` drift on the `tags` attribute of aws_kms_key after
// a restart.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestHandlerSnapshotRestore_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	out, err := backend.CreateKey(ctx, &kms.CreateKeyInput{Description: "tag-persist"})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	h.SetTags(keyID, map[string]string{"Environment": "prod", "Team": "platform"})

	snap := h.Snapshot(ctx)
	require.NotEmpty(t, snap)

	backend2 := kms.NewInMemoryBackend()
	h2 := kms.NewHandler(backend2)
	require.NoError(t, h2.Restore(ctx, snap))

	// Backend state (the key itself) round-trips as before.
	desc, err := backend2.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, "tag-persist", desc.KeyMetadata.Description)

	// Tags must now also round-trip.
	assert.Equal(t, map[string]string{"Environment": "prod", "Team": "platform"}, h2.GetTags(keyID))
}

func TestHandlerSnapshotRestore_EmptyTagsOmitted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	_, err := backend.CreateKey(ctx, &kms.CreateKeyInput{Description: "no-tags"})
	require.NoError(t, err)

	snap := h.Snapshot(ctx)
	require.NotEmpty(t, snap)

	backend2 := kms.NewInMemoryBackend()
	h2 := kms.NewHandler(backend2)
	require.NoError(t, h2.Restore(ctx, snap))

	out, err := backend2.ListKeys(ctx, &kms.ListKeysInput{})
	require.NoError(t, err)
	require.Len(t, out.Keys, 1)
	assert.Empty(t, h2.GetTags(out.Keys[0].KeyID))
}

// TestHandlerRestore_LegacyBackendOnlySnapshot verifies that a "legacy" snapshot --
// raw backend bytes with no handlerSnapshot wrapper, i.e. exactly what Handler.Snapshot
// produced before tags were persisted -- still restores backend state cleanly instead of
// erroring out. This matters for any gopherstack deployment upgrading across this fix
// with an existing on-disk snapshot.
func TestHandlerRestore_LegacyBackendOnlySnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := kms.NewInMemoryBackend()

	out, err := backend.CreateKey(ctx, &kms.CreateKeyInput{Description: "legacy"})
	require.NoError(t, err)

	legacy := backend.Snapshot(ctx)
	require.NotEmpty(t, legacy)

	var probe map[string]any
	require.NoError(t, json.Unmarshal(legacy, &probe))
	_, hasHandlerFormat := probe["handlerFormat"]
	require.False(t, hasHandlerFormat, "precondition: a raw backend snapshot has no handlerFormat marker")

	backend2 := kms.NewInMemoryBackend()
	h2 := kms.NewHandler(backend2)
	require.NoError(t, h2.Restore(ctx, legacy))

	desc, err := backend2.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: out.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Equal(t, "legacy", desc.KeyMetadata.Description)
}
