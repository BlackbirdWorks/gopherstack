package emr_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

func TestEMR_Janitor_SweepsTerminatedClusters(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow("sweep-test", "emr-6.0.0", nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.TerminateJobFlows([]string{cluster.ID}))

	janitor := emr.NewJanitor(b, 10*time.Millisecond, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go janitor.Run(ctx)

	// Wait until the cluster is swept from the backend.
	require.Eventually(t, func() bool {
		_, descErr := b.DescribeCluster(cluster.ID)

		return descErr != nil
	}, 2*time.Second, 20*time.Millisecond, "terminated cluster should be swept")
}

func TestEMR_Janitor_ActiveClusterNotSwept(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow("active-test", "emr-6.0.0", nil, nil)
	require.NoError(t, err)

	// Do NOT terminate the cluster — it should never be swept.
	janitor := emr.NewJanitor(b, 10*time.Millisecond, 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	go janitor.Run(ctx)

	// Wait for the janitor context to expire (several ticks), then verify cluster still exists.
	<-ctx.Done()

	_, err = b.DescribeCluster(cluster.ID)
	require.NoError(t, err, "active cluster must not be swept")
}

func TestEMR_Janitor_RecentlyTerminatedNotSwept(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow("recent-terminated", "emr-6.0.0", nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.TerminateJobFlows([]string{cluster.ID}))

	// Use a very long TTL so the cluster should not be swept.
	janitor := emr.NewJanitor(b, 10*time.Millisecond, 24*time.Hour)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	go janitor.Run(ctx)

	<-ctx.Done()

	// Cluster should still be reachable with TERMINATED state.
	c, err := b.DescribeCluster(cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, emr.StateTerminated, c.Status.State)
}

func TestEMR_Handler_WithJanitor_StartWorker(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	h := emr.NewHandler(b).WithJanitor(10*time.Millisecond, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}

func TestEMR_Handler_StartWorker_NoJanitor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	err := h.StartWorker(t.Context())
	require.NoError(t, err)
}

func TestEMR_Backend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		createClusters int
		wantAfterReset int
	}{
		{
			name:           "reset clears all clusters",
			createClusters: 3,
			wantAfterReset: 0,
		},
		{
			name:           "reset on empty backend is a no-op",
			createClusters: 0,
			wantAfterReset: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emr.NewInMemoryBackend(testAccountID, testRegion)

			for range tt.createClusters {
				_, err := b.RunJobFlow("cluster", "emr-6.0.0", nil, nil)
				require.NoError(t, err)
			}

			b.Reset()

			clusters := b.ListClusters()
			assert.Len(t, clusters, tt.wantAfterReset)
		})
	}
}

func TestEMR_Handler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "reset-cluster"})
	require.Equal(t, 200, createRec.Code)

	h.Reset()

	listRec := doEMRRequest(t, h, "ListClusters", map[string]any{})
	require.Equal(t, 200, listRec.Code)

	var out struct {
		Clusters []any `json:"Clusters"`
	}

	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.Empty(t, out.Clusters)
}
