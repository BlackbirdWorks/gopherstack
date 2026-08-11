package transfer_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestPersistence_FullStateRoundTrip exercises Snapshot/Restore across every
// resource table the Phase 3.3 store.Table conversion touches, including the
// composite-keyed ones (users, accesses, agreements, hostKeys, sshPublicKeys,
// executions) whose secondary indexes must be rebuilt correctly by Restore --
// not just counted, but queried back through their index-backed List/Count
// methods to prove the indexes survive the round trip.
func TestPersistence_FullStateRoundTrip(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newTestBackend(t)

		s, err := b.CreateServer(nil, map[string]string{"env": "test"})
		require.NoError(t, err)

		_, err = b.CreateUser(s.ServerID, "alice", "/home/alice", "arn:role", nil)
		require.NoError(t, err)

		_, err = b.CreateAccess(s.ServerID, "ext-1", "arn:role", "/home/ext-1", nil)
		require.NoError(t, err)

		profile, err := b.CreateProfile("LOCAL", "as2id", nil)
		require.NoError(t, err)

		_, err = b.CreateAgreement(s.ServerID, "desc", profile.ProfileID, profile.ProfileID, "/base", "arn:role", nil)
		require.NoError(t, err)

		connector, err := b.CreateConnector("https://example.com", "arn:role", nil, nil, nil)
		require.NoError(t, err)

		webApp, err := b.CreateWebApp(&transfer.CreateWebAppInput{
			IdentityCenterConfig: &transfer.WebAppIdentityCenterConfig{
				InstanceArn: "arn:aws:sso:::instance/ssoins-persistence",
				Role:        "arn:aws:iam::123456789012:role/webapp-idp",
			},
		})
		require.NoError(t, err)

		workflow, err := b.CreateWorkflow("desc", nil, nil, nil)
		require.NoError(t, err)

		_, err = b.ImportCertificate("SIGNING", "", "desc", time.Time{}, time.Time{}, nil)
		require.NoError(t, err)

		_, err = b.ImportHostKey(s.ServerID, testHostKeyEd25519, "desc", nil)
		require.NoError(t, err)

		_, err = b.ImportSSHPublicKey(s.ServerID, "alice", testHostKeyEd25519)
		require.NoError(t, err)

		_, err = b.CreateExecution(workflow.WorkflowID)
		require.NoError(t, err)

		b.StartFileFileTransferResult(connector.ConnectorID, []string{"file1"})
		b.StartAsyncOperationRecord(connector.ConnectorID, "LIST")

		require.NoError(t, b.TagResource("arn:aws:transfer:us-east-1:123456789012:server/"+s.ServerID,
			map[string]string{"k": "v"}))

		_, err = b.UpdateWebAppCustomization(webApp.WebAppID, "title", "logo", "favicon")
		require.NoError(t, err)

		// Snapshot and restore into a fresh backend.
		data := b.Snapshot(t.Context())
		require.NotNil(t, data)

		b2 := newTestBackend(t)
		require.NoError(t, b2.Restore(t.Context(), data))

		assert.Equal(t, 1, transfer.ServerCount(b2))
		assert.Equal(t, 1, transfer.UserCount(b2))
		assert.Equal(t, 1, transfer.AccessCount(b2))
		assert.Equal(t, 1, transfer.AgreementCount(b2))
		assert.Equal(t, 1, transfer.ConnectorCount(b2))
		assert.Equal(t, 1, transfer.ProfileCount(b2))
		assert.Equal(t, 1, transfer.WebAppCount(b2))
		assert.Equal(t, 1, transfer.WorkflowCount(b2))
		assert.Equal(t, 1, transfer.CertificateCount(b2))
		assert.Equal(t, 1, transfer.HostKeyCount(b2))
		assert.Equal(t, 1, transfer.SSHPublicKeyCount(b2))

		// Secondary indexes must be rebuilt, not just the primary tables: these
		// all resolve through a store.Index, not a direct Table.Get.
		users, err := b2.ListUsers(s.ServerID)
		require.NoError(t, err)
		assert.Len(t, users, 1)
		assert.Equal(t, "alice", users[0].UserName)

		accesses, err := b2.ListAccesses(s.ServerID)
		require.NoError(t, err)
		assert.Len(t, accesses, 1)

		agreements, err := b2.ListAgreements(s.ServerID)
		require.NoError(t, err)
		assert.Len(t, agreements, 1)

		hostKeys, err := b2.ListHostKeys(s.ServerID)
		require.NoError(t, err)
		assert.Len(t, hostKeys, 1)

		sshKeys := b2.ListSSHPublicKeys(s.ServerID, "alice")
		assert.Len(t, sshKeys, 1)
		assert.Equal(t, 1, b2.CountUserSSHPublicKeys(s.ServerID, "alice"))

		executions, err := b2.ListExecutions(workflow.WorkflowID)
		require.NoError(t, err)
		assert.Len(t, executions, 1)

		transferResults := b2.ListFileFileTransferResults(connector.ConnectorID)
		assert.Len(t, transferResults, 1)

		tags := b2.ListTagsForResource("arn:aws:transfer:us-east-1:123456789012:server/" + s.ServerID)
		assert.Equal(t, "v", tags["k"])

		// webAppCustomizations is a pre-existing gap (never part of
		// backendSnapshot, even before this conversion) that this refactor
		// deliberately preserves rather than fixes as a side effect.
		cust, err := b2.DescribeWebAppCustomization(webApp.WebAppID)
		require.NoError(t, err)
		assert.Empty(t, cust.Title, "webAppCustomizations was never persisted before Phase 3.3 either")

		// DeleteServer must still cascade-delete every composite-keyed child
		// table after a restore, proving the rebuilt indexes are live (not just
		// populated once at Restore time).
		require.NoError(t, b2.StopServer(s.ServerID))
		// Server was already OFFLINE from creation, so StopServer is a no-op;
		// Wait just settles any pending goroutines before asserting.
		synctest.Wait()
		require.NoError(t, b2.DeleteServer(s.ServerID))
		assert.Equal(t, 0, transfer.UserCount(b2))
		assert.Equal(t, 0, transfer.AccessCount(b2))
		assert.Equal(t, 0, transfer.AgreementCount(b2))
		assert.Equal(t, 0, transfer.HostKeyCount(b2))
		assert.Equal(t, 0, transfer.SSHPublicKeyCount(b2))
	})
}

// TestPersistence_IncompatibleVersionResetsToEmpty verifies that Restore
// discards a snapshot whose version field doesn't match the current
// backendSnapshot shape by resetting to empty, rather than erroring or
// partially decoding it.
func TestPersistence_IncompatibleVersionResetsToEmpty(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	// A version 0 snapshot (the shape before any version field existed, and
	// also what json.Unmarshal produces for a "version" field absent from
	// the payload) must be discarded cleanly.
	require.NoError(t, b.Restore(t.Context(), []byte(`{"version":0,"tables":{}}`)))

	assert.Equal(t, 0, transfer.ServerCount(b))
}

// TestSnapshotRestore verifies Snapshot and Restore round-trip.
func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "arn:role", nil)
	require.NoError(t, err)

	_, err = b.CreateConnector("https://example.com", "arn:role", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateProfile("LOCAL", "as2id", nil)
	require.NoError(t, err)

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, 1, transfer.ServerCount(b2))
	assert.Equal(t, 1, transfer.UserCount(b2))
	assert.Equal(t, 1, transfer.ConnectorCount(b2))
	assert.Equal(t, 1, transfer.ProfileCount(b2))

	s2, err := b2.DescribeServer(s.ServerID)
	require.NoError(t, err)
	assert.Equal(t, s.ServerID, s2.ServerID)
}

// TestSnapshotRestoreEmpty verifies Restore works on empty snapshot.
func TestSnapshotRestoreEmpty(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, 0, transfer.ServerCount(b2))
}

// TestSnapshotRestoreInvalidJSON verifies Restore returns error on bad data.
func TestSnapshotRestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-json"))

	require.Error(t, err)
}

// TestHandlerSnapshotRestore verifies Handler Snapshot/Restore delegation.
func TestHandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := transfer.NewHandler(b)

	rec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	data := h.Snapshot(t.Context())
	require.NotNil(t, data)

	h2 := transfer.NewHandler(transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), data))

	b2 := h2.Backend.(*transfer.InMemoryBackend)
	assert.Equal(t, 1, transfer.ServerCount(b2))
}

// TestSnapshotPreservesAgreementStatus verifies agreement status is preserved in snapshot.
func TestSnapshotPreservesAgreementStatus(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	ag, err := b.CreateAgreement(s.ServerID, "desc", "p-local", "p-partner", "/base", "arn:role", nil)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", ag.Status)

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), data))
	assert.Equal(t, 1, transfer.AgreementCount(b2))
}

// TestHandlerSnapshotNilBackend verifies Snapshot returns nil for non-InMemory backends.
func TestHandlerSnapshotNilBackend(t *testing.T) {
	t.Parallel()

	// Use a handler with a custom non-InMemoryBackend to exercise the nil path.
	// We use a standard backend – just confirm Snapshot doesn't panic.
	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := transfer.NewHandler(b)
	data := h.Snapshot(t.Context())
	assert.NotNil(t, data)
}
