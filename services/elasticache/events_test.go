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

// TestDescribeEvents_DefaultsToLastHour verifies the documented default
// (api_op_DescribeEvents.go: "By default, only the events occurring within
// the last hour are returned; however, you can retrieve up to 14 days' worth
// of events if necessary"). Omitting Duration/StartTime/EndTime must narrow
// to the last hour, not widen to every event ever recorded.
func TestDescribeEvents_DefaultsToLastHour(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newFakeClock()
	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	b.SetClock(clock.now)

	_, err := b.CreateCluster(ctx, "old-cluster", "redis", "cache.t3.micro", 0)
	require.NoError(t, err)

	clock.advance(90 * time.Minute)

	_, err = b.CreateCluster(ctx, "recent-cluster", "redis", "cache.t3.micro", 0)
	require.NoError(t, err)

	p, err := b.DescribeEvents(ctx, "", "", "", time.Time{}, time.Time{}, 0, 0)
	require.NoError(t, err)

	var sawOld, sawRecent bool

	for _, e := range p.Data {
		switch e.SourceIdentifier {
		case "old-cluster":
			sawOld = true
		case "recent-cluster":
			sawRecent = true
		}
	}

	assert.False(t, sawOld, "an event from 90 minutes ago must not appear under the default last-hour window")
	assert.True(t, sawRecent, "an event from just now must appear under the default last-hour window")
}

// TestDescribeEvents_DurationIsMinutes verifies Duration's documented unit
// (api_op_DescribeEvents.go: "The number of minutes worth of events to
// retrieve"). Duration=1 must retrieve the last minute, not the last second.
func TestDescribeEvents_DurationIsMinutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newFakeClock()
	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	b.SetClock(clock.now)

	_, err := b.CreateCluster(ctx, "dur-cluster", "redis", "cache.t3.micro", 0)
	require.NoError(t, err)

	clock.advance(30 * time.Second)

	p, err := b.DescribeEvents(ctx, "", "", "", time.Time{}, time.Time{}, 1, 0)
	require.NoError(t, err)

	found := false

	for _, e := range p.Data {
		if e.SourceIdentifier == "dur-cluster" {
			found = true
		}
	}

	assert.True(t, found, "Duration=1 (one minute) must still include an event from 30 seconds ago")
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
