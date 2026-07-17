package resourcegroupstaggingapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestBackendReset verifies that Reset() clears only dynamic per-test state (report
// state) while preserving providers, taggers, and untaggers -- these are wired once at
// server startup and must survive resets, otherwise cross-service tagging integration
// breaks after /_gopherstack/reset.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{}
	})
	b.RegisterARNTagger(func(_ context.Context, _ string, _ map[string]string) (bool, error) { return false, nil })
	b.RegisterARNUntagger(func(_ context.Context, _ string, _ []string) (bool, error) { return false, nil })
	resourcegroupstaggingapi.AddReportStateInternal(b, "SUCCEEDED", "s3://bucket/path", "2025-01-01T00:00:00Z")

	require.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b))
	require.Equal(t, 1, resourcegroupstaggingapi.TaggerCount(b))
	require.Equal(t, 1, resourcegroupstaggingapi.UntaggerCount(b))
	require.True(t, resourcegroupstaggingapi.HasReportState(b))

	b.Reset()

	assert.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b), "providers must survive Reset()")
	assert.Equal(t, 1, resourcegroupstaggingapi.TaggerCount(b), "taggers must survive Reset()")
	assert.Equal(t, 1, resourcegroupstaggingapi.UntaggerCount(b), "untaggers must survive Reset()")
	assert.False(t, resourcegroupstaggingapi.HasReportState(b), "reportState must be cleared by Reset()")
}

func TestAccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := resourcegroupstaggingapi.NewInMemoryBackend("123456789012", "eu-west-1")

	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &resourcegroupstaggingapi.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrNilAppContext)
}

// TestSnapshotRestore_ProvidersClearedTaggersCleared verifies that providers, taggers,
// and untaggers -- runtime callbacks that cannot be serialized -- are always cleared by
// Restore, regardless of what was registered on the original backend.
func TestSnapshotRestore_ProvidersClearedTaggersCleared(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource { return nil })
	b.RegisterARNTagger(func(_ context.Context, _ string, _ map[string]string) (bool, error) { return false, nil })
	b.RegisterARNUntagger(func(_ context.Context, _ string, _ []string) (bool, error) { return false, nil })

	require.Equal(t, 1, resourcegroupstaggingapi.ProviderCount(b))
	require.Equal(t, 1, resourcegroupstaggingapi.TaggerCount(b))
	require.Equal(t, 1, resourcegroupstaggingapi.UntaggerCount(b))

	snap := b.Snapshot(t.Context())

	b2 := newBackend(t)
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Providers, taggers, and untaggers are runtime callbacks; they cannot be serialized.
	assert.Equal(t, 0, resourcegroupstaggingapi.ProviderCount(b2))
	assert.Equal(t, 0, resourcegroupstaggingapi.TaggerCount(b2))
	assert.Equal(t, 0, resourcegroupstaggingapi.UntaggerCount(b2))
}
