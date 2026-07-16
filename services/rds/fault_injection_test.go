package rds_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRDSBackend_FISFaultCleanedOnClusterDelete(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(
		"fault-cluster",
		"aurora-postgresql",
		"admin",
		"pass",
		"",
		0,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	// Inject an expired fault (simulates an active fault entry).
	b.InjectExpiredFaultForTest("fault-cluster")

	// Deleting the cluster should remove the fault entry.
	_, err = b.DeleteDBCluster("fault-cluster")
	require.NoError(t, err)

	// After deletion the fault map should not contain the entry.
	active := b.IsClusterFailoverActive("fault-cluster")
	assert.False(t, active)
}
