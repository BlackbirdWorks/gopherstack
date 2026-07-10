package comprehend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// newFullPersistenceTestBackend creates a backend with one populated entry
// in every store.Table (jobs, resources, iterations -- see
// services/comprehend/store_setup.go) plus every raw map left
// un-converted (tags, policies, policyRevisions), so a Snapshot from it
// exercises the entire persisted surface of the backend.
func newFullPersistenceTestBackend(t *testing.T) *comprehend.InMemoryBackend {
	t.Helper()

	b := comprehend.NewInMemoryBackend("123456789012", "us-east-1")

	// jobs table.
	job, err := b.StartJob("entities-detection-job", "full-job", map[string]any{
		"LanguageCode": "en",
	})
	require.NoError(t, err)

	// resources table (a flywheel, so StartFlywheelIteration below has
	// something to reference) plus tags (raw map).
	resource, err := b.CreateResource(
		"flywheel", "full-flywheel", "",
		map[string]any{},
		[]comprehend.Tag{{Key: "env", Value: "test"}},
	)
	require.NoError(t, err)

	// iterations table.
	_, err = b.StartFlywheelIteration(resource.Arn)
	require.NoError(t, err)

	// policies + policyRevisions raw maps.
	_, err = b.PutResourcePolicy(resource.Arn, `{"Version":"2012-10-17"}`, "")
	require.NoError(t, err)

	t.Logf("seeded job %q resource %q", job.JobID, resource.Arn)

	return b
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (jobs, resources, iterations) and every raw map left
// un-converted (tags, policies, policyRevisions) through Snapshot ->
// Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newFullPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := comprehend.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// jobs table.
	jobs := fresh.ListJobs("entities-detection-job")
	require.Len(t, jobs, 1)
	assert.Equal(t, "full-job", jobs[0].JobName)

	// resources table.
	resources := fresh.ListResources("flywheel")
	require.Len(t, resources, 1)
	flywheelArn := resources[0].Arn
	assert.Equal(t, "full-flywheel", resources[0].Name)

	// iterations table.
	iterations := fresh.ListFlywheelIterations(flywheelArn)
	require.Len(t, iterations, 1)

	// tags (raw map).
	tags, err := fresh.ListTags(flywheelArn)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, comprehend.Tag{Key: "env", Value: "test"}, tags[0])

	// policies + policyRevisions (raw maps).
	policy, revision, err := fresh.GetResourcePolicy(flywheelArn)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policy)
	assert.NotEmpty(t, revision)

	// AccountID/Region carried through the snapshot.
	assert.Equal(t, "us-east-1", fresh.Region())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0 since Comprehend had no
// persistence at all before Phase 3.3) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newFullPersistenceTestBackend(t)

	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListJobs("entities-detection-job"))
	assert.Empty(t, b.ListResources("flywheel"))

	_, _, err = b.GetResourcePolicy("arn:aws:comprehend:us-east-1:123456789012:flywheel/full-flywheel")
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero verifies that a
// pre-Phase-3.3 snapshot (no "version"/"tables" fields at all, since
// Comprehend had no Snapshot/Restore before this rollout) decodes with
// Version == 0, which mismatches comprehendSnapshotVersion and is discarded
// the same way any other incompatible version is -- never partially decoded
// under some hypothetical old flat-map shape.
func TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero(t *testing.T) {
	t.Parallel()

	oldFormatSnap := `{"jobs":{},"resources":{},"iterations":{},"tags":{}}`

	b := comprehend.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte(oldFormatSnap))
	require.NoError(t, err)

	assert.Empty(t, b.ListJobs("entities-detection-job"))
	assert.Empty(t, b.ListResources("flywheel"))
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on). Before Phase 3.3, Handler had no Snapshot/Restore at all, so
// this delegation is new: see persistence.go's doc comment on Handler.Snapshot.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend := newFullPersistenceTestBackend(t)
	h := comprehend.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := comprehend.NewHandler(comprehend.NewInMemoryBackend("000000000000", "us-west-2"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	jobs := h2.Backend.ListJobs("entities-detection-job")
	require.Len(t, jobs, 1)
	assert.Equal(t, "full-job", jobs[0].JobName)
}
