package dax_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- IncreaseReplicationFactor: AZ assignment ----

func TestIncreaseReplicationFactorAZAssignment(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, not performance-critical
		azs           []string
		wantAZsForNew []string
		name          string
		initialFactor int
		newFactor     int
	}{
		{
			name:          "no existing nodes, AZs assigned in order",
			initialFactor: 1,
			newFactor:     3,
			azs:           []string{"us-east-1a", "us-east-1b"},
			wantAZsForNew: []string{"us-east-1a", "us-east-1b"},
		},
		{
			name:          "existing nodes present, new AZs assigned by offset not raw index",
			initialFactor: 2,
			newFactor:     5,
			azs:           []string{"us-east-1a", "us-east-1b", "us-east-1c"},
			wantAZsForNew: []string{"us-east-1a", "us-east-1b", "us-east-1c"},
		},
		{
			name:          "fewer AZs than new nodes uses default for remainder",
			initialFactor: 1,
			newFactor:     4,
			azs:           []string{"us-east-1b"},
			wantAZsForNew: []string{"us-east-1b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			_, err := b.CreateCluster(dax.CreateClusterInput{
				ClusterName:       "az-test",
				NodeType:          "dax.r5.large",
				IamRoleArn:        "arn:aws:iam::123456789012:role/DAXRole",
				ReplicationFactor: tt.initialFactor,
			})
			require.NoError(t, err)

			out, err := b.IncreaseReplicationFactor(dax.IncreaseReplicationFactorInput{
				ClusterName:          "az-test",
				NewReplicationFactor: tt.newFactor,
				AvailabilityZones:    tt.azs,
			})
			require.NoError(t, err)
			require.Len(t, out.Nodes, tt.newFactor)

			newNodes := out.Nodes[tt.initialFactor:]
			for i, wantAZ := range tt.wantAZsForNew {
				assert.Equal(t, wantAZ, newNodes[i].AvailabilityZone,
					"new node[%d] AZ mismatch", i)
			}
		})
	}
}

// ---- RebootNode: empty NodeId returns ErrInvalidParameterValue ----

func TestRebootNodeEmptyNodeID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		nodeID      string
	}{
		{name: "empty nodeID", nodeID: "", errSentinel: dax.ErrInvalidParameterValue},
		{name: "nonexistent nodeID", nodeID: "no-such-node", errSentinel: dax.ErrNodeNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateCluster(validCreateInput("reboot-test"))
			require.NoError(t, err)

			_, err = b.RebootNode("reboot-test", tt.nodeID)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.errSentinel)
		})
	}
}

// ---- RebootNode: node returns to available after recovery ----

func TestRebootNodeRecovery(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.CreateCluster(dax.CreateClusterInput{
		ClusterName:       "recovery-test",
		NodeType:          "dax.r5.large",
		IamRoleArn:        "arn:aws:iam::123456789012:role/DAXRole",
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	// Fetch the node ID.
	clusters, _, err := b.DescribeClusters([]string{"recovery-test"}, 0, "")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	require.Len(t, clusters[0].Nodes, 1)
	nodeID := clusters[0].Nodes[0].NodeID

	// Initiate reboot.
	out, err := b.RebootNode("recovery-test", nodeID)
	require.NoError(t, err)
	require.Len(t, out.Nodes, 1)
	assert.Equal(t, "rebooting", out.Nodes[0].NodeStatus)

	// Wait for recovery goroutine (sleeps 1s).
	time.Sleep(2 * time.Second)

	// Node should be back to available.
	clusters, _, err = b.DescribeClusters([]string{"recovery-test"}, 0, "")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	require.Len(t, clusters[0].Nodes, 1)
	assert.Equal(t, "available", clusters[0].Nodes[0].NodeStatus)
}

// ---- UpdateParameterGroup: marks dependent clusters pending-reboot ----

func TestUpdateParameterGroupMarksPendingReboot(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.CreateParameterGroup("custom-pg", "test group")
	require.NoError(t, err)

	_, err = b.CreateCluster(dax.CreateClusterInput{
		ClusterName:        "pg-cluster",
		NodeType:           "dax.r5.large",
		IamRoleArn:         "arn:aws:iam::123456789012:role/DAXRole",
		ReplicationFactor:  2,
		ParameterGroupName: "custom-pg",
	})
	require.NoError(t, err)

	// Cluster not using this PG should not be affected.
	_, err = b.CreateCluster(validCreateInput("other-cluster"))
	require.NoError(t, err)

	_, err = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
		ParameterGroupName: "custom-pg",
		ParameterNameValues: []dax.ParameterNameValue{
			{ParameterName: "query-ttl-millis", ParameterValue: "600000"},
		},
	})
	require.NoError(t, err)

	clusters, _, err := b.DescribeClusters([]string{"pg-cluster"}, 0, "")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	pgStatus := clusters[0].ParameterGroup
	assert.Equal(t, "pending-reboot", pgStatus.ParameterApplyStatus)
	assert.Len(t, pgStatus.NodeIDsToReboot, 2, "both nodes should be listed for reboot")

	// Other cluster should remain in-sync.
	other, _, err := b.DescribeClusters([]string{"other-cluster"}, 0, "")
	require.NoError(t, err)
	require.Len(t, other, 1)
	assert.Equal(t, "in-sync", other[0].ParameterGroup.ParameterApplyStatus)
}
