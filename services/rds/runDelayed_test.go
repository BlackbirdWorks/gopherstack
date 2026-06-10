package rds_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// instanceTransitionDelay in the backend; mirror as a local lower bound for
// timing assertions. The backend uses 250ms.
const transitionDelay = 250 * time.Millisecond

// TestRebootDBClusterDelayedTransition exercises the delayed lifecycle goroutine
// scheduled by RebootDBCluster via runDelayed. It verifies both that the
// transition still fires after the delay and that Close cancels in-flight
// transitions promptly without mutating state after shutdown (the leak fix).
func TestRebootDBClusterDelayedTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantStatus    string
		closeEarly    bool
		wantFastClose bool
	}{
		{
			name:       "transition fires after delay",
			closeEarly: false,
			wantStatus: "available",
		},
		{
			name:          "close cancels in-flight transition",
			closeEarly:    true,
			wantStatus:    "rebooting",
			wantFastClose: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("123456789012", "us-east-1")

			_, err := b.CreateDBCluster(
				"my-cluster",
				"aurora-mysql",
				"admin",
				"",
				"",
				0,
				nil,
				rds.DBClusterOptions{},
			)
			require.NoError(t, err)

			_, err = b.RebootDBCluster("my-cluster")
			require.NoError(t, err)

			if tt.closeEarly {
				// Close immediately, before the transition delay elapses. The
				// delayed goroutine must observe stopCh and return without
				// mutating state. Close must block only briefly on b.wg.Wait().
				start := time.Now()
				b.Close()
				elapsed := time.Since(start)

				if tt.wantFastClose {
					require.Less(t, elapsed, transitionDelay,
						"Close should not wait out the full transition delay")
				}

				clusters, derr := b.DescribeDBClusters("my-cluster")
				require.NoError(t, derr)
				require.Equal(t, tt.wantStatus, clusters[0].Status)

				return
			}

			// Wait for the delayed transition to fire, then verify the status
			// and a clean Close afterward.
			require.Eventually(t, func() bool {
				clusters, derr := b.DescribeDBClusters("my-cluster")
				if derr != nil || len(clusters) == 0 {
					return false
				}

				return clusters[0].Status == tt.wantStatus
			}, 2*time.Second, 10*time.Millisecond)

			b.Close()
		})
	}
}

// TestCloseIdempotent verifies Close can be called more than once without
// panicking on a double-close of stopCh (sync.Once guard).
func TestCloseIdempotent(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", "us-east-1")

	require.NotPanics(t, func() {
		b.Close()
		b.Close()
	})
}
