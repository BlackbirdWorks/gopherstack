package rds_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestDescribeDBClusters_RacesWithClusterMemberWriters guards cloneDBClusterMutableSlices.
// Each case exercises an in-place DBClusterMembers writer that used to share its backing array with a live cluster.
func TestDescribeDBClusters_RacesWithClusterMemberWriters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *rds.InMemoryBackend)
		mutate func(b *rds.InMemoryBackend, i int)
		name   string
	}{
		{
			name: "failover",
			setup: func(t *testing.T, b *rds.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDBInstance("race-writer", "aurora-mysql", "db.r5.large", "", "", "", 20,
					rds.DBInstanceOptions{DBClusterIdentifier: "race-cluster"})
				require.NoError(t, err)
				_, err = b.CreateDBInstance("race-reader", "aurora-mysql", "db.r5.large", "", "", "", 20,
					rds.DBInstanceOptions{DBClusterIdentifier: "race-cluster"})
				require.NoError(t, err)
			},
			mutate: func(b *rds.InMemoryBackend, i int) {
				target := "race-writer"
				if i%2 == 0 {
					target = "race-reader"
				}

				_, _ = b.FailoverDBCluster("race-cluster", target)
			},
		},
		{
			// DeleteDBInstance compacts DBClusterMembers with slices.DeleteFunc, a second in-place writer.
			// Deleting the non-last of two freshly added members forces DeleteFunc to shift-write the survivor.
			name: "delete_instance",
			mutate: func(b *rds.InMemoryBackend, i int) {
				first := fmt.Sprintf("race-tmp-a-%d", i)
				second := fmt.Sprintf("race-tmp-b-%d", i)

				_, _ = b.CreateDBInstance(first, "aurora-mysql", "db.r5.large", "", "", "", 20,
					rds.DBInstanceOptions{DBClusterIdentifier: "race-cluster"})
				_, _ = b.CreateDBInstance(second, "aurora-mysql", "db.r5.large", "", "", "", 20,
					rds.DBInstanceOptions{DBClusterIdentifier: "race-cluster"})
				_, _ = b.DeleteDBInstanceWithOptions(first, true, "", true)
				_, _ = b.DeleteDBInstanceWithOptions(second, true, "", true)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			_, err := b.CreateDBCluster("race-cluster", "aurora-mysql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			const iterations = 2000

			var wg sync.WaitGroup

			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					clusters, describeErr := b.DescribeDBClusters("race-cluster")
					if describeErr != nil {
						continue
					}

					for _, c := range clusters {
						for _, m := range c.DBClusterMembers {
							_ = m.IsClusterWriter
						}
					}
				}
			}()

			go func() {
				defer wg.Done()

				for i := range iterations {
					tt.mutate(b, i)
				}
			}()

			wg.Wait()
		})
	}
}
