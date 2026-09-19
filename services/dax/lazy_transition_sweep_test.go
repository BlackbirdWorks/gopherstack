package dax_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// past the modeled 1s clusterTransitionDelay, strictly greater so two
// timers due at the same fake instant never race on fire order.
const pastTransitionDeadline = 2 * time.Second

// TestClusterTransitionPromotesAfterDeadline proves the goroutine-free
// replacement (gopherstack-1x2u0): a cluster/node reports its transient
// status right after the triggering call and its terminal status once
// sweepClusterTransitionsLocked's deadline has passed, with no background
// goroutine involved.
func TestClusterTransitionPromotesAfterDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		trigger    func(t *testing.T, b *dax.InMemoryBackend) string
		wantBefore string
		wantAfter  string
		checkAfter func(t *testing.T, b *dax.InMemoryBackend, clusterName string)
		name       string
	}{
		{
			name: "create settles at available",
			trigger: func(t *testing.T, b *dax.InMemoryBackend) string {
				t.Helper()
				c, err := b.CreateCluster(validCreateInput("sweep-create"))
				require.NoError(t, err)

				return c.ClusterName
			},
			wantBefore: dax.StatusCreating,
			wantAfter:  dax.StatusAvailable,
		},
		{
			name: "increase settles at available",
			trigger: func(t *testing.T, b *dax.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateCluster(validCreateInput("sweep-increase"))
				require.NoError(t, err)
				dax.SetClusterAvailableForTest(b, "sweep-increase")

				_, err = b.IncreaseReplicationFactor(dax.IncreaseReplicationFactorInput{
					ClusterName:          "sweep-increase",
					NewReplicationFactor: 2,
				})
				require.NoError(t, err)

				return "sweep-increase"
			},
			wantBefore: dax.StatusModifying,
			wantAfter:  dax.StatusAvailable,
		},
		{
			name: "decrease settles at available and clears NodeIDsToRemove",
			trigger: func(t *testing.T, b *dax.InMemoryBackend) string {
				t.Helper()
				in := validCreateInput("sweep-decrease")
				in.ReplicationFactor = 2
				_, err := b.CreateCluster(in)
				require.NoError(t, err)
				dax.SetClusterAvailableForTest(b, "sweep-decrease")

				_, err = b.DecreaseReplicationFactor(dax.DecreaseReplicationFactorInput{
					ClusterName:          "sweep-decrease",
					NewReplicationFactor: 1,
				})
				require.NoError(t, err)

				return "sweep-decrease"
			},
			wantBefore: dax.StatusModifying,
			wantAfter:  dax.StatusAvailable,
			checkAfter: func(t *testing.T, b *dax.InMemoryBackend, clusterName string) {
				t.Helper()
				clusters, _, err := b.DescribeClusters([]string{clusterName}, 0, "")
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Empty(t, clusters[0].NodeIDsToRemove)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newTestBackend()
				clusterName := tt.trigger(t, b)

				clusters, _, err := b.DescribeClusters([]string{clusterName}, 0, "")
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, tt.wantBefore, clusters[0].Status)

				time.Sleep(pastTransitionDeadline)

				clusters, _, err = b.DescribeClusters([]string{clusterName}, 0, "")
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, tt.wantAfter, clusters[0].Status)

				if tt.checkAfter != nil {
					tt.checkAfter(t, b, clusterName)
				}
			})
		})
	}
}

// TestDeleteClusterRemovedAfterDeadline proves DeleteCluster's lazy
// counterpart to the removed goroutine: the cluster stays visible as
// "deleting" until its deadline passes, then sweepClusterTransitionsLocked
// actually removes it (and its tags) on the next op, matching real AWS
// rather than the old goroutine's own timing.
func TestDeleteClusterRemovedAfterDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newTestBackend()

		created, err := b.CreateCluster(validCreateInput("sweep-delete"))
		require.NoError(t, err)
		clusterARN := created.ClusterArn
		dax.SetClusterAvailableForTest(b, "sweep-delete")

		deleted, err := b.DeleteCluster("sweep-delete")
		require.NoError(t, err)
		assert.Equal(t, dax.StatusDeleting, deleted.Status)

		clusters, _, err := b.DescribeClusters([]string{"sweep-delete"}, 0, "")
		require.NoError(t, err)
		require.Len(t, clusters, 1)
		assert.Equal(t, dax.StatusDeleting, clusters[0].Status)

		time.Sleep(pastTransitionDeadline)

		_, _, err = b.DescribeClusters([]string{"sweep-delete"}, 0, "")
		require.ErrorIs(t, err, dax.ErrClusterNotFound)

		tags, _, err := b.ListTags(clusterARN, "")
		require.Error(t, err, "tags for a fully-deleted cluster's ARN must be gone too")
		assert.Empty(t, tags)
	})
}

// TestNodeRebootPromotesAfterDeadline proves RebootNode's node-level
// transition (RebootDeadline) resolves the same way the cluster-level one
// does.
func TestNodeRebootPromotesAfterDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newTestBackend()

		created, err := b.CreateCluster(validCreateInput("sweep-reboot"))
		require.NoError(t, err)
		dax.SetClusterAvailableForTest(b, "sweep-reboot")
		nodeID := created.Nodes[0].NodeID

		_, err = b.RebootNode("sweep-reboot", nodeID)
		require.NoError(t, err)

		clusters, _, err := b.DescribeClusters([]string{"sweep-reboot"}, 0, "")
		require.NoError(t, err)
		require.Len(t, clusters, 1)
		require.Len(t, clusters[0].Nodes, 1)
		assert.Equal(t, dax.StatusRebooting, clusters[0].Nodes[0].NodeStatus)

		time.Sleep(pastTransitionDeadline)

		clusters, _, err = b.DescribeClusters([]string{"sweep-reboot"}, 0, "")
		require.NoError(t, err)
		require.Len(t, clusters, 1)
		require.Len(t, clusters[0].Nodes, 1)
		assert.Equal(t, dax.StatusAvailable, clusters[0].Nodes[0].NodeStatus)
	})
}

// TestMutationDuringTransitionRejected proves a mutation attempted while a
// cluster is mid-transition still gets the same InvalidClusterStateFault it
// got before this pass replaced the async goroutines with a lazy sweep --
// the observable precondition behavior is unchanged, only the mechanism
// advancing the cluster past it is.
func TestMutationDuringTransitionRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(b *dax.InMemoryBackend, clusterName string) error
		name   string
	}{
		{
			name: "IncreaseReplicationFactor while creating",
			mutate: func(b *dax.InMemoryBackend, clusterName string) error {
				_, err := b.IncreaseReplicationFactor(dax.IncreaseReplicationFactorInput{
					ClusterName:          clusterName,
					NewReplicationFactor: 2,
				})

				return err
			},
		},
		{
			name: "DecreaseReplicationFactor while creating",
			mutate: func(b *dax.InMemoryBackend, clusterName string) error {
				_, err := b.DecreaseReplicationFactor(dax.DecreaseReplicationFactorInput{
					ClusterName:          clusterName,
					NewReplicationFactor: 1,
				})

				return err
			},
		},
		{
			name: "RebootNode while creating",
			mutate: func(b *dax.InMemoryBackend, clusterName string) error {
				_, err := b.RebootNode(clusterName, clusterName+"-0000")

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.CreateCluster(validCreateInput("mid-transition"))
			require.NoError(t, err)

			err = tt.mutate(b, "mid-transition")
			require.Error(t, err)
			assert.ErrorIs(t, err, dax.ErrInvalidClusterState)
		})
	}
}

// TestSnapshotRestoreMidTransitionPromotesAfterDeadline proves a cluster
// snapshotted mid-transition keeps its TransitionDeadline across Restore
// (persistence.go no longer force-promotes it with a recovery goroutine)
// and still promotes correctly once real time -- spanning the snapshot and
// the restore -- catches up to that deadline.
func TestSnapshotRestoreMidTransitionPromotesAfterDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		original := newTestBackend()
		_, err := original.CreateCluster(validCreateInput("mid-snap"))
		require.NoError(t, err)

		clusters, _, err := original.DescribeClusters([]string{"mid-snap"}, 0, "")
		require.NoError(t, err)
		require.Len(t, clusters, 1)
		require.Equal(t, dax.StatusCreating, clusters[0].Status, "snapshot must capture the cluster mid-transition")

		snap := original.Snapshot(t.Context())
		require.NotEmpty(t, snap)

		time.Sleep(pastTransitionDeadline)

		fresh := newTestBackend()
		require.NoError(t, fresh.Restore(t.Context(), snap))

		clusters, _, err = fresh.DescribeClusters([]string{"mid-snap"}, 0, "")
		require.NoError(t, err)
		require.Len(t, clusters, 1)
		assert.Equal(t, dax.StatusAvailable, clusters[0].Status,
			"restore must promote a cluster whose deadline already passed by wall-clock time")
	})
}
