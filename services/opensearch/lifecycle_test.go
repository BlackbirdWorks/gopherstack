package opensearch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// newLifecycleBackend returns a backend with a long processing delay so
// lifecycle windows are observable within a test.
func newLifecycleBackend(t *testing.T) *opensearch.InMemoryBackend {
	t.Helper()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.SetProcessingDelay(time.Hour)

	return b
}

// TestDomainProcessingWindow_Create asserts that after CreateDomain a domain is
// observably in a Creating/Processing window, and settles to Active once the
// window elapses. This is the SDK-waiter-observable transition parity.md flags.
func TestDomainProcessingWindow_Create(t *testing.T) {
	t.Parallel()

	b := newLifecycleBackend(t)

	created, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "proc-create"})
	require.NoError(t, err)

	// During the window: Processing true, DomainProcessingStatus == Creating.
	assert.True(t, created.Created, "Created must be true")
	proc, upgrade, dps := opensearch.DomainProcessingState(created)
	assert.True(t, proc, "domain must be Processing during the create window")
	assert.False(t, upgrade)
	assert.Equal(t, "Creating", dps)

	// DescribeDomain observes the same in-flight state.
	desc, err := b.DescribeDomain("proc-create")
	require.NoError(t, err)
	proc, _, dps = opensearch.DomainProcessingState(desc)
	assert.True(t, proc)
	assert.Equal(t, "Creating", dps)

	// After the window elapses the domain settles to Active.
	opensearch.ExpireDomainProcessing(b, "proc-create")
	settled, err := b.DescribeDomain("proc-create")
	require.NoError(t, err)
	proc, upgrade, dps = opensearch.DomainProcessingState(settled)
	assert.False(t, proc, "domain must settle out of Processing")
	assert.False(t, upgrade)
	assert.Equal(t, "Active", dps)
}

// TestDomainProcessingWindow_UpdateUpgrade covers the Modifying and
// UpgradingEngineVersion windows.
func TestDomainProcessingWindow_UpdateUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(t *testing.T, b *opensearch.InMemoryBackend)
		name    string
		wantDPS string
		upgrade bool
	}{
		{
			name: "update_config_modifying",
			mutate: func(t *testing.T, b *opensearch.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateDomainConfig("lc", opensearch.UpdateDomainConfigInput{
					AccessPolicies: "{}",
				})
				require.NoError(t, err)
			},
			wantDPS: "Modifying",
			upgrade: false,
		},
		{
			name: "upgrade_engine_version",
			mutate: func(t *testing.T, b *opensearch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.UpgradeDomain("lc", "OpenSearch_2.13"))
			},
			wantDPS: "UpgradingEngineVersion",
			upgrade: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newLifecycleBackend(t)
			_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "lc"})
			require.NoError(t, err)
			// Settle the create window so the mutation window is unambiguous.
			opensearch.ExpireDomainProcessing(b, "lc")

			tt.mutate(t, b)

			desc, err := b.DescribeDomain("lc")
			require.NoError(t, err)
			proc, upgrade, dps := opensearch.DomainProcessingState(desc)
			assert.True(t, proc, "must be Processing during the %s window", tt.name)
			assert.Equal(t, tt.upgrade, upgrade)
			assert.Equal(t, tt.wantDPS, dps)

			// Settles back to Active.
			opensearch.ExpireDomainProcessing(b, "lc")
			settled, err := b.DescribeDomain("lc")
			require.NoError(t, err)
			proc, _, dps = opensearch.DomainProcessingState(settled)
			assert.False(t, proc)
			assert.Equal(t, "Active", dps)
		})
	}
}

// TestDomainDeleteWindow asserts a domain is observably Deleting before it is
// finally removed, and that the fast (zero-delay) path removes it immediately.
func TestDomainDeleteWindow(t *testing.T) {
	t.Parallel()

	t.Run("observable_deleting_window", func(t *testing.T) {
		t.Parallel()

		b := newLifecycleBackend(t)
		_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "del"})
		require.NoError(t, err)
		opensearch.ExpireDomainProcessing(b, "del")

		deleted, err := b.DeleteDomain("del")
		require.NoError(t, err)
		assert.True(t, deleted.Deleted, "delete response must report Deleted")

		// Still describable, in Deleting state, during the window.
		desc, err := b.DescribeDomain("del")
		require.NoError(t, err)
		assert.True(t, desc.Deleted)
		proc, _, dps := opensearch.DomainProcessingState(desc)
		assert.True(t, proc)
		assert.Equal(t, "Deleting", dps)
		assert.Contains(t, b.ListDomainNames(), "del")

		// Once the window elapses the domain is gone.
		opensearch.ExpireDomainProcessing(b, "del")
		_, err = b.DescribeDomain("del")
		require.ErrorIs(t, err, opensearch.ErrDomainNotFound)
		assert.NotContains(t, b.ListDomainNames(), "del")
	})

	t.Run("fast_path_removes_immediately", func(t *testing.T) {
		t.Parallel()

		b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
		_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "fast"})
		require.NoError(t, err)

		deleted, err := b.DeleteDomain("fast")
		require.NoError(t, err)
		assert.True(t, deleted.Deleted)

		_, err = b.DescribeDomain("fast")
		require.ErrorIs(t, err, opensearch.ErrDomainNotFound)
	})
}

// TestServiceSoftwareUpdate_RealState asserts StartServiceSoftwareUpdate records
// mutable state and CancelServiceSoftwareUpdate acts on it (or rejects when no
// update is scheduled), rather than returning a canned envelope.
func TestServiceSoftwareUpdate_RealState(t *testing.T) {
	t.Parallel()

	t.Run("start_then_cancel", func(t *testing.T) {
		t.Parallel()

		b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
		_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "ssw"})
		require.NoError(t, err)

		started, err := b.StartServiceSoftwareUpdate("ssw", "")
		require.NoError(t, err)
		assert.Equal(t, "PENDING_UPDATE", started.UpdateStatus)
		assert.Equal(t, "PENDING_UPDATE", opensearch.DomainServiceSoftwareStatus(b, "ssw"))

		cancelled, err := b.CancelServiceSoftwareUpdate("ssw")
		require.NoError(t, err)
		assert.Equal(t, "ELIGIBLE", cancelled.UpdateStatus)
		assert.False(t, cancelled.Cancellable)
		assert.Equal(t, "ELIGIBLE", opensearch.DomainServiceSoftwareStatus(b, "ssw"))
	})

	t.Run("cancel_without_pending_update_errors", func(t *testing.T) {
		t.Parallel()

		b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
		_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "nossw"})
		require.NoError(t, err)

		_, err = b.CancelServiceSoftwareUpdate("nossw")
		require.Error(t, err)
		assert.ErrorIs(t, err, opensearch.ErrValidation)
	})

	t.Run("cancel_twice_errors", func(t *testing.T) {
		t.Parallel()

		b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
		_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "twice"})
		require.NoError(t, err)
		_, err = b.StartServiceSoftwareUpdate("twice", "")
		require.NoError(t, err)
		_, err = b.CancelServiceSoftwareUpdate("twice")
		require.NoError(t, err)

		// The second cancel is no longer valid: nothing is PENDING_UPDATE.
		_, err = b.CancelServiceSoftwareUpdate("twice")
		require.ErrorIs(t, err, opensearch.ErrValidation)
	})
}

// TestServerlessCollectionCreatingWindow asserts collections transition
// CREATING → ACTIVE rather than reporting ACTIVE instantly.
func TestServerlessCollectionCreatingWindow(t *testing.T) {
	t.Parallel()

	t.Run("creating_then_active", func(t *testing.T) {
		t.Parallel()

		b := newLifecycleBackend(t)
		coll, err := b.CreateServerlessCollection("c1", "SEARCH", "", "", nil)
		require.NoError(t, err)
		assert.Equal(t, "CREATING", coll.Status)

		got := b.BatchGetServerlessCollections(nil, []string{"c1"})
		require.Len(t, got, 1)
		assert.Equal(t, "CREATING", got[0].Status)

		opensearch.ExpireCollectionStatus(b, coll.ID)
		got = b.BatchGetServerlessCollections(nil, []string{"c1"})
		require.Len(t, got, 1)
		assert.Equal(t, "ACTIVE", got[0].Status)
	})

	t.Run("fast_path_active_immediately", func(t *testing.T) {
		t.Parallel()

		b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
		coll, err := b.CreateServerlessCollection("c2", "SEARCH", "", "", nil)
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", coll.Status)
	})

	t.Run("delete_window_then_gone", func(t *testing.T) {
		t.Parallel()

		b := newLifecycleBackend(t)
		coll, err := b.CreateServerlessCollection("c3", "SEARCH", "", "", nil)
		require.NoError(t, err)

		del, err := b.DeleteServerlessCollection(coll.ID)
		require.NoError(t, err)
		assert.Equal(t, "DELETING", del.Status)
		require.Len(t, b.BatchGetServerlessCollections(nil, []string{"c3"}), 1)

		opensearch.ExpireCollectionStatus(b, coll.ID)
		assert.Empty(t, b.BatchGetServerlessCollections(nil, []string{"c3"}))
	})
}

// TestConnectionAndVpcDeleteWindows asserts outbound connections and VPC
// endpoints enter an observable DELETING window before removal.
func TestConnectionAndVpcDeleteWindows(t *testing.T) {
	t.Parallel()

	t.Run("outbound_connection", func(t *testing.T) {
		t.Parallel()

		b := newLifecycleBackend(t)
		conn, err := b.CreateOutboundConnection("alias", nil, nil)
		require.NoError(t, err)

		del, err := b.DeleteOutboundConnection(conn.ConnectionID)
		require.NoError(t, err)
		assert.Equal(t, "DELETING", del.Status)
		require.Len(t, b.DescribeOutboundConnections(), 1)

		opensearch.ExpireOutboundConnection(b, conn.ConnectionID)
		assert.Empty(t, b.DescribeOutboundConnections())
	})

	t.Run("vpc_endpoint", func(t *testing.T) {
		t.Parallel()

		b := newLifecycleBackend(t)
		ep, err := b.CreateVpcEndpoint("arn:aws:es:us-east-1:123456789012:domain/x", nil)
		require.NoError(t, err)

		del, err := b.DeleteVpcEndpoint(ep.VpcEndpointID)
		require.NoError(t, err)
		assert.Equal(t, "DELETING", del.Status)
		require.Len(t, b.ListVpcEndpoints(), 1)

		opensearch.ExpireVpcEndpoint(b, ep.VpcEndpointID)
		assert.Empty(t, b.ListVpcEndpoints())
	})
}
