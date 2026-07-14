package pinpoint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a service.Registerable
// (the *Handler returned by Provider.Init) in the persistence.Manager only if
// that Handler itself satisfies Snapshot(ctx)/Restore(ctx, []byte);
// InMemoryBackend implementing the same two methods (exercised directly by
// TestRefinement1_SnapshotRestore) is not enough on its own, since
// Handler.Backend is the StorageBackend interface and does not promote them.
// Mirrors services/securityhub's Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	h := pinpoint.NewHandler(backend)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	app, err := backend.CreateApp("us-east-1", "123456789012", "handler-app", map[string]string{"env": "test"})
	require.NoError(t, err)

	data := h.Snapshot(ctx)
	require.NotEmpty(t, data)

	restoredBackend := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	restoredHandler := pinpoint.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	assert.Equal(t, 1, pinpoint.AppCount(restoredBackend))

	got, err := restoredBackend.GetApp(app.ID)
	require.NoError(t, err)
	assert.Equal(t, "handler-app", got.Name)
}
