package rds_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

func newFISRDSHandler() *rds.Handler {
	return rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))
}

func TestRDS_FISActions(t *testing.T) {
	t.Parallel()

	h := newFISRDSHandler()
	actions := h.FISActions()

	ids := make([]string, len(actions))
	for i, a := range actions {
		ids[i] = a.ActionID
	}

	assert.Contains(t, ids, "aws:rds:reboot-db-instances")
	assert.Contains(t, ids, "aws:rds:failover-db-cluster")
}

func TestRDS_FISActions_TargetTypes(t *testing.T) {
	t.Parallel()

	h := newFISRDSHandler()

	tests := []struct {
		name       string
		actionID   string
		targetType string
	}{
		{
			name:       "reboot_instances_target_type",
			actionID:   "aws:rds:reboot-db-instances",
			targetType: "aws:rds:db",
		},
		{
			name:       "failover_cluster_target_type",
			actionID:   "aws:rds:failover-db-cluster",
			targetType: "aws:rds:cluster",
		},
	}

	actions := h.FISActions()
	actionMap := make(map[string]service.FISActionDefinition, len(actions))
	for _, a := range actions {
		actionMap[a.ActionID] = a
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, ok := actionMap[tt.actionID]
			require.True(t, ok, "action %s not found", tt.actionID)
			assert.Equal(t, tt.targetType, a.TargetType)
		})
	}
}

func TestRDS_ExecuteFISAction_RebootInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []string
		wantErr bool
	}{
		{
			name:    "known_instance",
			targets: []string{"arn:aws:rds:us-east-1:000000000000:db/test-instance"},
			wantErr: false,
		},
		{
			name:    "unknown_instance_returns_error",
			targets: []string{"arn:aws:rds:us-east-1:000000000000:db/nonexistent"},
			wantErr: true,
		},
		{
			name:    "no_targets",
			targets: []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newFISRDSHandler()

			// Create a test instance if needed.
			if len(tt.targets) > 0 && !tt.wantErr {
				id := rdsIDFromARNForTest(tt.targets[0])
				_, err := h.Backend.CreateDBInstance(id, "mysql", "db.t3.micro", "testdb", "admin", "", 20)
				require.NoError(t, err)
			}

			err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
				ActionID: "aws:rds:reboot-db-instances",
				Targets:  tt.targets,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRDS_ExecuteFISAction_FailoverDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		targets  []string
		duration time.Duration
		wantErr  bool
	}{
		{
			name:    "single_cluster_no_duration",
			targets: []string{"arn:aws:rds:us-east-1:000000000000:cluster/my-cluster"},
			wantErr: false,
		},
		{
			name:     "single_cluster_with_duration",
			targets:  []string{"arn:aws:rds:us-east-1:000000000000:cluster/timed-cluster"},
			duration: 100 * time.Millisecond,
			wantErr:  false,
		},
		{
			name:    "no_targets",
			targets: []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newFISRDSHandler()

			err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
				ActionID: "aws:rds:failover-db-cluster",
				Targets:  tt.targets,
				Duration: tt.duration,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// For clusters with non-zero duration, the fault should eventually clear.
			if tt.duration > 0 && len(tt.targets) > 0 {
				time.Sleep(tt.duration + 50*time.Millisecond)

				id := rdsIDFromARNForTest(tt.targets[0])
				assert.False(t, h.Backend.IsClusterFailoverActive(id),
					"failover fault should have expired after duration")
			}
		})
	}
}

func TestRDS_ExecuteFISAction_Unknown(t *testing.T) {
	t.Parallel()

	h := newFISRDSHandler()

	err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:rds:unknown-action",
		Targets:  []string{"some-target"},
	})

	require.NoError(t, err)
}

func TestRDS_FISActions_FailoverHasDurationParam(t *testing.T) {
	t.Parallel()

	h := newFISRDSHandler()

	actions := h.FISActions()

	for _, a := range actions {
		if a.ActionID == "aws:rds:failover-db-cluster" {
			paramNames := make([]string, len(a.Parameters))
			for i, p := range a.Parameters {
				paramNames[i] = p.Name
			}

			assert.Contains(t, paramNames, "duration")

			return
		}
	}

	t.Fatal("aws:rds:failover-db-cluster action not found")
}

func TestRDS_ExecuteFISAction_FailoverDBCluster_CtxCancel(t *testing.T) {
	t.Parallel()

	h := newFISRDSHandler()

	ctx, cancel := context.WithCancel(context.Background())

	const clusterTarget = "arn:aws:rds:us-east-1:000000000000:cluster/cancel-cluster"

	// Activate indefinite fault (dur==0).
	err := h.ExecuteFISAction(ctx, service.FISActionExecution{
		ActionID: "aws:rds:failover-db-cluster",
		Targets:  []string{clusterTarget},
		Duration: 0,
	})
	require.NoError(t, err)

	assert.True(t, h.Backend.IsClusterFailoverActive("cancel-cluster"), "fault should be active")

	// Cancel ctx (simulates StopExperiment).
	cancel()

	// Fault should clear promptly.
	require.Eventually(t, func() bool {
		return !h.Backend.IsClusterFailoverActive("cancel-cluster")
	}, 2*time.Second, 20*time.Millisecond, "fault should clear after ctx cancel")
}

func TestRDS_IsClusterFailoverActive_LazyEviction(t *testing.T) {
	t.Parallel()

	h := newFISRDSHandler()

	const clusterTarget = "arn:aws:rds:us-east-1:000000000000:cluster/lazy-evict-cluster"

	// Activate with a very short duration.
	err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:rds:failover-db-cluster",
		Targets:  []string{clusterTarget},
		Duration: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	// Wait for expiry.
	time.Sleep(50 * time.Millisecond)

	// IsClusterFailoverActive should return false and lazily remove the entry.
	assert.False(t, h.Backend.IsClusterFailoverActive("lazy-evict-cluster"))
}

// rdsIDFromARNForTest extracts the resource ID from an RDS ARN for test helpers.
func rdsIDFromARNForTest(arnOrID string) string {
	for i := len(arnOrID) - 1; i >= 0; i-- {
		if arnOrID[i] == '/' {
			return arnOrID[i+1:]
		}
	}

	return arnOrID
}
