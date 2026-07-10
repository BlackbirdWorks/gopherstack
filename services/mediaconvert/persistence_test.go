package mediaconvert_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateQueue("seed-queue", "", "", "", nil)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, mediaconvert.QueueCount(b))
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all (the pre-Phase-3.3 shape: plain
// resource maps keyed directly by "queues", "jobs", etc., no "tables"/
// "version" wrapper) decodes with Version == 0, which mismatches
// mediaconvertSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateQueue("seed-queue", "", "", "", nil)
	require.NoError(t, err)

	oldShape := `{"queues":{"q1":{"name":"q1","arn":"arn:old","pricingPlan":"ON_DEMAND","status":"ACTIVE"}},` +
		`"accountID":"000000000000","region":"us-east-1"}`
	err = b.Restore(t.Context(), []byte(oldShape))
	require.NoError(t, err)

	assert.Equal(t, 0, mediaconvert.QueueCount(b))
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (queues incl. the byARN secondary index, jobTemplates,
// jobs, presets, plus the "dirty" tokenIndex table), every raw map left
// un-converted (tags, certificates), and the scalar policy field.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := mediaconvert.NewInMemoryBackend("111122223333", "us-west-2")

	rp := &mediaconvert.ReservationPlan{Status: "ACTIVE", Commitment: "ONE_YEAR", ReservedSlots: 3}
	overrides := map[string]any{"engine": map[string]any{"version": "2"}}
	queue, err := original.CreateQueueFull(
		"queue-1", "primary queue", "RESERVED", "ACTIVE",
		map[string]string{"team": "media"}, 5, rp, overrides,
	)
	require.NoError(t, err)

	// A second queue exists purely to prove the byARN secondary index
	// survives Restore: jobs below are attached to it by ARN, not name.
	queue2, err := original.CreateQueue("queue-2", "secondary queue", "", "", nil)
	require.NoError(t, err)

	jobTemplate, err := original.CreateJobTemplate(
		"template-1", "desc", "category-a", "queue-1", 10, map[string]any{"k": "v"}, map[string]string{"env": "prod"},
	)
	require.NoError(t, err)

	preset, err := original.CreatePreset("preset-1", "desc", "category-b", map[string]any{"p": 1}, nil)
	require.NoError(t, err)

	job, err := original.CreateJobFull(
		"arn:aws:iam::111122223333:role/mc-role", "queue-1", "template-1",
		map[string]any{"s": "v"}, map[string]string{"owner": "team"}, map[string]string{"m": "v"},
		"JOB", "req-token-1", "ENABLED", "2017-08-29", 7,
		[]mediaconvert.HopDestination{{Queue: "queue-2", WaitMinutes: 5}},
	)
	require.NoError(t, err)

	require.NoError(t, original.AssociateCertificate("arn:aws:acm:us-west-2:111122223333:cert/abc"))
	original.PutPolicy("ALLOWED", "ALLOWED", "DISALLOWED")
	original.TagResource(queue.Arn, map[string]string{"extra": "tag"})

	require.Equal(t, 1, mediaconvert.TokenIndexSize(original))

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := mediaconvert.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Scalars.
	assert.Equal(t, "111122223333", fresh.AccountID())
	assert.Equal(t, "us-west-2", fresh.Region())

	// Direct tables.
	gotQueue, err := fresh.GetQueue(queue.Name)
	require.NoError(t, err)
	assert.Equal(t, "primary queue", gotQueue.Description)
	assert.Equal(t, "RESERVED", gotQueue.PricingPlan)
	assert.Equal(t, "media", gotQueue.Tags["team"])
	require.NotNil(t, gotQueue.ReservationPlan)
	assert.Equal(t, 3, gotQueue.ReservationPlan.ReservedSlots)
	require.NotNil(t, gotQueue.ServiceOverrides)

	gotJobTemplate, err := fresh.GetJobTemplate(jobTemplate.Name)
	require.NoError(t, err)
	assert.Equal(t, "category-a", gotJobTemplate.Category)
	assert.Equal(t, 10, gotJobTemplate.Priority)

	gotPreset, err := fresh.GetPreset(preset.Name)
	require.NoError(t, err)
	assert.Equal(t, "category-b", gotPreset.Category)

	gotJob, err := fresh.GetJob(job.ID)
	require.NoError(t, err)
	assert.Equal(t, "req-token-1", gotJob.ClientRequestToken)
	require.Len(t, gotJob.HopDestinations, 1)
	assert.Equal(t, "queue-2", gotJob.HopDestinations[0].Queue)

	// byARN secondary index on the queues table: resolving a queue by ARN
	// (rather than name) only works if AddIndex's registered index was
	// correctly repopulated by Table.Restore, not left empty.
	jobByArn, err := fresh.CreateJob(
		"arn:aws:iam::111122223333:role/mc-role", queue2.Arn, "", nil, nil, nil, "",
	)
	require.NoError(t, err)
	assert.Equal(t, queue2.Arn, jobByArn.QueueArn)

	// Deleting the queue afterward must also clear the restored index entry
	// -- proves Table.Delete (not just Table.Restore) still maintains the
	// index correctly on a backend that came from Restore.
	require.NoError(t, fresh.DeleteQueue(queue2.Name))
	_, err = fresh.CreateJob("arn:aws:iam::111122223333:role/mc-role", queue2.Arn, "", nil, nil, nil, "")
	require.ErrorIs(t, err, mediaconvert.ErrNotFound)

	// Plain (un-converted) maps/scalars that were persisted before Phase 3.3.
	assert.Equal(t, 1, mediaconvert.CertificateCount(fresh))

	pol, err := fresh.GetPolicy()
	require.NoError(t, err)
	assert.Equal(t, "ALLOWED", pol.HTTPInputs)
	assert.Equal(t, "DISALLOWED", pol.S3Inputs)

	tags := fresh.GetTags(queue.Arn)
	assert.Equal(t, "tag", tags["extra"])

	// "Dirty" tokenIndex table: the identity (token) key set survives the
	// round trip via the DTO registry, matching pre-Phase-3.3 behavior where
	// the map's key set (but not its unexported-field values) survived a
	// Snapshot/Restore round trip.
	assert.Equal(t, 1, mediaconvert.TokenIndexSize(fresh))
}

// TestHandler_SnapshotRestoreDelegate verifies the Handler-level Snapshot and
// Restore -- which cli.go's setupPersistence actually calls, since it
// type-asserts the service.Registerable value returned by Provider.Init
// (the Handler, not InMemoryBackend) -- correctly delegate to the backend.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))

	_, err := h.Backend.CreateQueue("delegate-queue", "", "", "", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, mediaconvert.QueueCount(h2.Backend.(*mediaconvert.InMemoryBackend)))
}
