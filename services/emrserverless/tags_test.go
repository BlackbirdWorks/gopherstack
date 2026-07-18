package emrserverless_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// --- findTagsByARN uncovered branches (jobRunARNs key exists but maps don't) ---

func TestFindTagsByARN_AfterReset(t *testing.T) {
	t.Parallel()

	// After a snapshot round-trip with nil job run sub-map, ensureMaps should fix it.
	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("tag-arn-app", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)

	jr, err := b.StartJobRun(app.ApplicationID, "arn:aws:iam::000000000000:role/r", "run1", "", nil)
	require.NoError(t, err)

	// Verify tags can be retrieved via ARN.
	tags, err := b.ListTagsForResource(jr.Arn)
	require.NoError(t, err)
	assert.NotNil(t, tags)

	// Snapshot and restore preserves ARN index.
	snap := b.Snapshot(t.Context())
	b2 := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	tags2, err := b2.ListTagsForResource(jr.Arn)
	require.NoError(t, err)
	assert.NotNil(t, tags2)
}

// --- ListTagsForResource returns stable result ---

func TestListTagsForResource_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	app, err := b.CreateApplication("tags-empty-app", "SPARK", "emr-6.6.0", "", nil)
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(app.Arn)
	require.NoError(t, err)
	assert.NotNil(t, tags)
	assert.Empty(t, tags)
}

// TestTagResource_HandlesNilSeededTags verifies TagResource does not panic
// when the resource was seeded with a nil Tags map, and that the tag write
// succeeds and is subsequently visible.
func TestTagResource_HandlesNilSeededTags(t *testing.T) {
	t.Parallel()

	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	appID := "seed-nil-tags"
	now := time.Now().UTC()

	app := &emrserverless.Application{
		ApplicationID: appID,
		Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID,
		Name:          "nil-tags-app",
		Type:          "SPARK",
		State:         emrserverless.ApplicationStateCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          nil, // intentionally nil
	}
	b.AddApplicationInternal(app)

	// TagResource must not panic even when seeded with nil Tags.
	err := b.TagResource(app.Arn, map[string]string{"k": "v"})
	require.NoError(t, err)

	got, err := b.ListTagsForResource(app.Arn)
	require.NoError(t, err)
	assert.Equal(t, "v", got["k"])
}
