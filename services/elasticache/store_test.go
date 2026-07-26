package elasticache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_all_state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1", nil)

			_, err := backend.CreateCluster(context.Background(), "reset-cluster", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			_, err = backend.CreateReplicationGroup(context.Background(), "reset-rg", "test")
			require.NoError(t, err)

			backend.Reset()

			_, err = backend.DescribeClusters(context.Background(), "reset-cluster", "", 0, false)
			require.ErrorIs(t, err, elasticache.ErrClusterNotFound)

			_, err = backend.DescribeReplicationGroups(context.Background(), "reset-rg", "", 0)
			require.ErrorIs(t, err, elasticache.ErrReplicationGroupNotFound)

			p, err := backend.DescribeEvents(context.Background(), "", "", "", time.Time{}, time.Time{}, 0, 0)
			require.NoError(t, err)
			assert.Empty(t, p.Data)

			_ = tt.name
		})
	}
}

func TestBackend_Reset_ClearsAll(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateClusterWithOptions(context.Background(), "reset-cl", "redis", "cache.t3.micro", "", "", "", 1, 0)
	require.NoError(t, err)

	_, err = b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "reset-rg", Description: "for reset",
	})
	require.NoError(t, err)

	_, err = b.CreateUser(context.Background(), "reset-user", "reset-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	b.Reset()

	// All resources should be gone.
	p1, err := b.DescribeClusters(context.Background(), "", "", 0, false)
	require.NoError(t, err)
	assert.Empty(t, p1.Data)

	p2, err := b.DescribeReplicationGroups(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p2.Data)

	p3, err := b.DescribeUsers(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p3.Data)
}

// TestBackend_ConcurrentDescribeNoRace is a regression test for a class of
// data races where a Describe* method -- holding only b.mu.RLock() -- lazily
// initialised a per-region resource table (e.g. b.clusters[region] = ...)
// exactly like the Create*/Modify* methods that hold the full b.mu.Lock().
// Two RLock-holding readers could both observe a nil entry for a region no
// writer has ever touched and concurrently write to the same outer map,
// which is a data race on a plain Go map regardless of the RWMutex, since
// RLock does not serialize readers against each other.
//
// Each case hammers one resource family's Describe* call, from many workers,
// against a single never-before-written region on a fresh backend. Run with
// `go test -race` to catch a regression of this class; it must pass cleanly
// with no race detected.
func TestBackend_ConcurrentDescribeNoRace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(b *elasticache.InMemoryBackend, ctx context.Context) error
		name string
	}{
		{name: "clusters", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeClusters(ctx, "", "", 0, false)

			return err
		}},
		{name: "replication_groups", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeReplicationGroups(ctx, "", "", 0)

			return err
		}},
		{name: "parameter_groups", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeParameterGroups(ctx, "", "", 0)

			return err
		}},
		{name: "subnet_groups", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeSubnetGroups(ctx, "", "", 0)

			return err
		}},
		{name: "snapshots", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeSnapshots(ctx, "", "", "", "", "", 0)

			return err
		}},
		{name: "cache_security_groups", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeCacheSecurityGroups(ctx, "", "", 0)

			return err
		}},
		{name: "serverless_caches", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeServerlessCaches(ctx, "", "", 0)

			return err
		}},
		{name: "serverless_cache_snapshots", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeServerlessCacheSnapshots(ctx, "", "", "", 0)

			return err
		}},
		{name: "users", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeUsers(ctx, "", "", 0)

			return err
		}},
		{name: "user_groups", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeUserGroups(ctx, "", "", 0)

			return err
		}},
		{name: "reserved_cache_nodes", call: func(b *elasticache.InMemoryBackend, ctx context.Context) error {
			_, err := b.DescribeReservedCacheNodes(ctx, "", "", "", "", 0)

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// A fresh backend whose default region has never been written by
			// any Create*/Modify* call, so the very first Describe* call is
			// the one that would lazily create the per-region entry.
			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			ctx := context.Background()

			const workers = 16
			const opsPerWorker = 25

			var wg sync.WaitGroup
			for range workers {
				wg.Go(func() {
					for range opsPerWorker {
						_ = tt.call(backend, ctx)
					}
				})
			}
			wg.Wait()
		})
	}
}
