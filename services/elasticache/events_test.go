package elasticache_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func TestDescribeEvents_RecordsOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		clusterID       string
		wantSourceID    string
		wantSourceType  string
		wantMsgContains string
	}{
		{
			name:            "create_cluster_records_event",
			clusterID:       "evt-cluster",
			wantSourceID:    "evt-cluster",
			wantSourceType:  "cache-cluster",
			wantMsgContains: "created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1", nil)

			_, err := backend.CreateCluster(context.Background(), tt.clusterID, "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			p, err := backend.DescribeEvents(context.Background(), "", "", "", time.Time{}, time.Time{}, 0, 0)
			require.NoError(t, err)
			require.NotEmpty(t, p.Data)

			found := false
			for _, e := range p.Data {
				if e.SourceIdentifier == tt.wantSourceID && e.SourceType == tt.wantSourceType {
					assert.Contains(t, e.Message, tt.wantMsgContains)
					found = true
				}
			}
			assert.True(t, found, "expected event not found in DescribeEvents results")
		})
	}
}

func TestBackend_Events_AfterMultipleOps(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(context.Background(), "event-cl", "redis", "cache.t3.micro", "", "", "", 1, 0)
	require.NoError(t, err)

	_, err = b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "event-rg", Description: "event test",
	})
	require.NoError(t, err)

	p, err := b.DescribeEvents(context.Background(), "", "", "", time.Time{}, time.Time{}, 0, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 2)
}
