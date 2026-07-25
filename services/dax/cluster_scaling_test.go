package dax_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- IncreaseReplicationFactor ----

func TestIncreaseReplicationFactor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *dax.InMemoryBackend)
		check   func(t *testing.T, c *dax.Cluster)
		name    string
		input   dax.IncreaseReplicationFactorInput
		wantErr bool
	}{
		{
			name: "increase from 1 to 3",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("grow"))
			},
			input: dax.IncreaseReplicationFactorInput{
				ClusterName:          "grow",
				NewReplicationFactor: 3,
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, 3, c.TotalNodes)
				assert.Len(t, c.Nodes, 3)
			},
		},
		{
			name:    "cluster not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			input:   dax.IncreaseReplicationFactorInput{ClusterName: "no-such", NewReplicationFactor: 2},
			wantErr: true,
		},
		{
			name: "factor exceeds max",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("x"))
			},
			input:   dax.IncreaseReplicationFactorInput{ClusterName: "x", NewReplicationFactor: 11},
			wantErr: true,
		},
		{
			name: "factor not greater than current",
			setup: func(b *dax.InMemoryBackend) {
				in := validCreateInput("x")
				in.ReplicationFactor = 3
				_, _ = b.CreateCluster(in)
			},
			input:   dax.IncreaseReplicationFactorInput{ClusterName: "x", NewReplicationFactor: 3},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			c, err := b.IncreaseReplicationFactor(tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

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

// ---- DecreaseReplicationFactor ----

func TestDecreaseReplicationFactor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *dax.InMemoryBackend)
		check   func(t *testing.T, c *dax.Cluster)
		name    string
		input   dax.DecreaseReplicationFactorInput
		wantErr bool
	}{
		{
			name: "decrease from 3 to 1",
			setup: func(b *dax.InMemoryBackend) {
				in := validCreateInput("shrink")
				in.ReplicationFactor = 3
				_, _ = b.CreateCluster(in)
			},
			input: dax.DecreaseReplicationFactorInput{
				ClusterName:          "shrink",
				NewReplicationFactor: 1,
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Equal(t, 1, c.TotalNodes)
				assert.Len(t, c.Nodes, 1)
				// NodeIDsToRemove (types.Cluster.NodeIdsToRemove) surfaces the nodes
				// this decrease is removing while the cluster is transiently "modifying".
				assert.ElementsMatch(t, []string{"shrink-0001", "shrink-0002"}, c.NodeIDsToRemove)
			},
		},
		{
			name: "decrease with specific node IDs",
			setup: func(b *dax.InMemoryBackend) {
				in := validCreateInput("specific")
				in.ReplicationFactor = 3
				_, _ = b.CreateCluster(in)
			},
			input: dax.DecreaseReplicationFactorInput{
				ClusterName:          "specific",
				NewReplicationFactor: 1,
				NodeIDsToRemove:      []string{"specific-0001", "specific-0002"},
			},
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				assert.Len(t, c.Nodes, 1)
				assert.Equal(t, "specific-0000", c.Nodes[0].NodeID)
				assert.ElementsMatch(t, []string{"specific-0001", "specific-0002"}, c.NodeIDsToRemove)
			},
		},
		{
			name:    "cluster not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			input:   dax.DecreaseReplicationFactorInput{ClusterName: "no-such", NewReplicationFactor: 1},
			wantErr: true,
		},
		{
			name: "factor not less than current",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("x"))
			},
			input:   dax.DecreaseReplicationFactorInput{ClusterName: "x", NewReplicationFactor: 1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			c, err := b.DecreaseReplicationFactor(tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, c)
			}
		})
	}
}

// ---- DecreaseReplicationFactor: NodeIDsToRemove count validation ----

func TestDecreaseReplicationFactorNodeIDsCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		nodeIDs     []string
		newFactor   int
		wantErr     bool
	}{
		{
			name:      "no node IDs uses tail removal",
			nodeIDs:   nil,
			newFactor: 1,
			wantErr:   false,
		},
		{
			name:        "wrong count rejected",
			nodeIDs:     []string{"valid-name-0000"},
			newFactor:   1,
			wantErr:     true,
			errSentinel: dax.ErrInvalidParameterCombination,
		},
		{
			name:      "correct count accepted",
			nodeIDs:   []string{"valid-name-0001", "valid-name-0002"},
			newFactor: 1,
			wantErr:   false,
		},
		{
			name:        "nonexistent node ID rejected",
			nodeIDs:     []string{"nonexistent-9999", "nonexistent-8888"},
			newFactor:   1,
			wantErr:     true,
			errSentinel: dax.ErrNodeNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateCluster(dax.CreateClusterInput{
				ClusterName:       "valid-name",
				NodeType:          "dax.r5.large",
				IamRoleArn:        "arn:aws:iam::123456789012:role/DAXRole",
				ReplicationFactor: 3,
			})
			require.NoError(t, err)

			_, err = b.DecreaseReplicationFactor(dax.DecreaseReplicationFactorInput{
				ClusterName:          "valid-name",
				NewReplicationFactor: tt.newFactor,
				NodeIDsToRemove:      tt.nodeIDs,
			})

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- RebootNode ----

func TestRebootNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *dax.InMemoryBackend)
		check       func(t *testing.T, c *dax.Cluster)
		name        string
		clusterName string
		nodeID      string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("reboot-me"))
			},
			clusterName: "reboot-me",
			nodeID:      "reboot-me-0000",
			check: func(t *testing.T, c *dax.Cluster) {
				t.Helper()
				require.Len(t, c.Nodes, 1)
				assert.Equal(t, dax.StatusRebooting, c.Nodes[0].NodeStatus)
			},
		},
		{
			name:        "cluster not found",
			setup:       func(_ *dax.InMemoryBackend) {},
			clusterName: "no-such",
			nodeID:      "node-0000",
			wantErr:     true,
		},
		{
			name: "node not found",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateCluster(validCreateInput("exist"))
			},
			clusterName: "exist",
			nodeID:      "no-such-node",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			c, err := b.RebootNode(tt.clusterName, tt.nodeID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, c)
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

// ---- DecreaseReplicationFactor: NodeIDsToRemove is transient ----

func TestDecreaseReplicationFactorNodeIDsToRemoveClearsOnRecovery(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	in := validCreateInput("shrink-transient")
	in.ReplicationFactor = 3
	_, err := b.CreateCluster(in)
	require.NoError(t, err)

	out, err := b.DecreaseReplicationFactor(dax.DecreaseReplicationFactorInput{
		ClusterName:          "shrink-transient",
		NewReplicationFactor: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, "modifying", out.Status)
	assert.NotEmpty(t, out.NodeIDsToRemove, "NodeIDsToRemove should be populated while the decrease is in flight")

	// Wait for the async recovery goroutine (sleeps 1s) to bring the cluster
	// back to "available" and clear the transient removal list.
	time.Sleep(2 * time.Second)

	clusters, _, err := b.DescribeClusters([]string{"shrink-transient"}, 0, "")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "available", clusters[0].Status)
	assert.Empty(t, clusters[0].NodeIDsToRemove)
}
