package cloudcontrol_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	persistTestAccountID = "111122223333"
	persistTestRegion    = "us-west-2"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend(persistTestAccountID, persistTestRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend(persistTestAccountID, persistTestRegion)
	_, err := b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"seed"}`, "")
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListAllResources())
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches cloudcontrolSnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. This also covers
// the pre-Phase-3.3 on-disk shape (CloudControl had no persistence at all
// before this conversion, so there is no legacy shape to actually collide
// with -- any input lacking "version" hits this same path).
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend(persistTestAccountID, persistTestRegion)
	_, err := b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"seed"}`, "")
	require.NoError(t, err)

	err = b.Restore(t.Context(), []byte(`{"resources":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListAllResources())
}

// persistSeed is every resource/event created by
// TestInMemoryBackend_SnapshotRestore_FullState, kept together so the
// post-restore assertions can refer back to the original tokens.
type persistSeed struct {
	createLogGroup *cloudcontrol.ProgressEvent
	updateLogGroup *cloudcontrol.ProgressEvent
	createQueue    *cloudcontrol.ProgressEvent
	deleteQueue    *cloudcontrol.ProgressEvent
}

// seedPersistenceState populates b with one resource of each shape this test
// cares about: a resource later updated in place, a resource left untouched,
// a resource created then deleted (so only its ProgressEvent survives, not
// the resource itself), a client-token-idempotent creation, and a
// synthetically injected IN_PROGRESS event (AddProgressEvent, the only way
// to reach that status -- see backend.go). Factored out of the test function
// to keep it under the repo's complexity limits.
func seedPersistenceState(t *testing.T, b *cloudcontrol.InMemoryBackend) persistSeed {
	t.Helper()

	createLogGroup, err := b.CreateResource(
		"AWS::Logs::LogGroup", `{"LogGroupName":"snap-grp"}`, "client-token-1",
	)
	require.NoError(t, err)

	_, err = b.CreateResource("AWS::S3::Bucket", `{"BucketName":"snap-bucket"}`, "")
	require.NoError(t, err)

	updateLogGroup, err := b.UpdateResource(
		"AWS::Logs::LogGroup", "snap-grp", `[{"op":"replace","path":"/RetentionInDays","value":30}]`, "",
	)
	require.NoError(t, err)

	createQueue, err := b.CreateResource("AWS::SQS::Queue", `{"QueueName":"to-delete"}`, "")
	require.NoError(t, err)

	deleteQueue, err := b.DeleteResource("AWS::SQS::Queue", "to-delete", "")
	require.NoError(t, err)

	b.AddProgressEvent(&cloudcontrol.ProgressEvent{
		RequestToken:    "in-progress-token",
		TypeName:        "AWS::EC2::Instance",
		Identifier:      "i-1234",
		Operation:       "CREATE",
		OperationStatus: "IN_PROGRESS",
	})

	return persistSeed{
		createLogGroup: createLogGroup,
		updateLogGroup: updateLogGroup,
		createQueue:    createQueue,
		deleteQueue:    deleteQueue,
	}
}

// assertResourcesRestored checks the "resources" table (a direct store.Table
// keyed by Resource.TypeName+"/"+Resource.Identifier) round-tripped exactly:
// the updated, untouched, and deleted resources all land in their expected
// post-restore state.
func assertResourcesRestored(t *testing.T, b *cloudcontrol.InMemoryBackend) {
	t.Helper()

	logGroup, err := b.GetResource("AWS::Logs::LogGroup", "snap-grp")
	require.NoError(t, err)
	assert.Contains(t, logGroup.Properties, "RetentionInDays")

	_, err = b.GetResource("AWS::S3::Bucket", "snap-bucket")
	require.NoError(t, err)

	_, err = b.GetResource("AWS::SQS::Queue", "to-delete")
	require.ErrorIs(t, err, cloudcontrol.ErrNotFound)

	assert.Len(t, b.ListAllResources(), 2)
}

// assertRequestsRestored checks the "requests" table (a direct store.Table
// keyed by ProgressEvent.RequestToken) round-tripped every event, including
// the terminal ones (CREATE/UPDATE/DELETE) and the synthetically injected
// IN_PROGRESS one.
func assertRequestsRestored(t *testing.T, b *cloudcontrol.InMemoryBackend, seed persistSeed) {
	t.Helper()

	for _, token := range []string{
		seed.createLogGroup.RequestToken,
		seed.updateLogGroup.RequestToken,
		seed.createQueue.RequestToken,
		seed.deleteQueue.RequestToken,
		"in-progress-token",
	} {
		_, err := b.GetResourceRequestStatus(token)
		require.NoError(t, err, "request token %s must survive restore", token)
	}

	events, _, err := b.ListResourceRequests(nil, 0, "")
	require.NoError(t, err)
	// createLogGroup, createBucket (untracked in persistSeed but still an
	// event), updateLogGroup, createQueue, deleteQueue, in-progress-token.
	assert.Len(t, events, 6)
}

// assertClientTokensRestored checks the raw (non-store.Table) clientTokens
// map round-tripped: replaying the original CreateResource call with the
// same ClientToken must still return the cached ProgressEvent from before
// the restore, not create a duplicate resource or a new request.
func assertClientTokensRestored(t *testing.T, b *cloudcontrol.InMemoryBackend, seed persistSeed) {
	t.Helper()

	replay, err := b.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"snap-grp"}`, "client-token-1")
	require.NoError(t, err)
	assert.Equal(t, seed.createLogGroup.RequestToken, replay.RequestToken)
	assert.Len(t, b.ListAllResources(), 2, "idempotent replay must not create a duplicate resource")
}

// TestInMemoryBackend_SnapshotRestore_FullState is the full-state
// Snapshot -> Restore round trip: every store.Table (resources, requests)
// and the one raw-left persisted map (clientTokens), plus accountID/region,
// must come back byte-for-byte equivalent in the observable behaviour they
// drive.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	b := cloudcontrol.NewInMemoryBackend(persistTestAccountID, persistTestRegion)
	seed := seedPersistenceState(t, b)

	data := b.Snapshot(t.Context())
	require.NotEmpty(t, data)

	// Restore into a fresh backend constructed with different
	// accountID/region, so a passing Region() assertion below proves Restore
	// actually overwrote the constructor defaults rather than coincidentally
	// matching them.
	b2 := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, persistTestRegion, b2.Region())
	assertResourcesRestored(t, b2)
	assertRequestsRestored(t, b2, seed)
	assertClientTokensRestored(t, b2, seed)
}

// TestHandler_SnapshotRestore verifies Handler.Snapshot/Handler.Restore
// delegate to the backend (the dead-wiring fix: before Phase 3.3 neither
// Handler nor InMemoryBackend implemented persistence.Persistable at all, so
// cli.go's generic setupPersistence never picked CloudControl up).
func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateResource("AWS::Logs::LogGroup", `{"LogGroupName":"handler-grp"}`, "")
	require.NoError(t, err)

	data := h.Snapshot(context.Background())
	require.NotEmpty(t, data)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(context.Background(), data))

	_, err = h2.Backend.GetResource("AWS::Logs::LogGroup", "handler-grp")
	require.NoError(t, err)
}
