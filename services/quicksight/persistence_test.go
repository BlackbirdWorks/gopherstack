package quicksight_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a service.Registerable
// (the *Handler returned by Provider.Init) in the persistence.Manager only if
// that Handler itself satisfies Snapshot(ctx)/Restore(ctx, []byte);
// InMemoryBackend implementing the same two methods (exercised directly by
// TestQuickSight_Phase3_3_StoreRoundTrip) is not enough on its own, since
// Handler.Backend is the StorageBackend interface and does not promote them.
// Mirrors services/securityhub's Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	_, err := backend.CreateGroup("000000000000", "default", "handler-group", "desc")
	require.NoError(t, err)

	data := h.Snapshot(ctx)
	require.NotEmpty(t, data)

	restoredBackend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	restoredHandler := quicksight.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	assert.Equal(t, 1, quicksight.GroupCount(restoredBackend))

	got, err := restoredBackend.DescribeGroup("000000000000", "default", "handler-group")
	require.NoError(t, err)
	assert.Equal(t, "handler-group", got.GroupName)
}
