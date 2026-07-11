package elasticache

import (
	"context"
	"testing"
)

func TestRegionIsolation_Clusters(t *testing.T) {
	b := NewInMemoryBackend("us-east-1", "123456789012", "standard", nil)

	ctxEast := context.WithValue(context.Background(), regionContextKey{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKey{}, "us-west-2")

	_, err := b.CreateCluster(ctxEast, "my-cluster", "redis", "cache.t3.micro", 6379)
	if err != nil {
		t.Fatalf("create cluster east: %v", err)
	}

	eastClusters, err := b.DescribeClusters(ctxEast, "my-cluster", "", 100, false)
	if err != nil {
		t.Fatalf("describe clusters east: %v", err)
	}
	if len(eastClusters.Data) != 1 {
		t.Fatalf("expected 1 cluster in us-east-1, got %d", len(eastClusters.Data))
	}

	westClusters, err := b.DescribeClusters(ctxWest, "", "", 100, false)
	if err != nil {
		t.Fatalf("describe clusters west: %v", err)
	}
	if len(westClusters.Data) != 0 {
		t.Fatalf("expected 0 clusters in us-west-2, got %d", len(westClusters.Data))
	}
}

func TestRegionIsolation_ReplicationGroups(t *testing.T) {
	b := NewInMemoryBackend("us-east-1", "123456789012", "standard", nil)

	ctxEast := context.WithValue(context.Background(), regionContextKey{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKey{}, "us-west-2")

	opts := ReplicationGroupCreateOpts{
		ID:            "my-rg",
		Description:   "test rg",
		CacheNodeType: "cache.t3.micro",
		Engine:        "redis",
		EngineVersion: "7.0",
	}

	_, err := b.CreateReplicationGroupFull(ctxEast, opts)
	if err != nil {
		t.Fatalf("create rg east: %v", err)
	}

	eastRGs, err := b.DescribeReplicationGroups(ctxEast, "my-rg", "", 100)
	if err != nil {
		t.Fatalf("describe rg east: %v", err)
	}
	if len(eastRGs.Data) != 1 {
		t.Fatalf("expected 1 rg in us-east-1, got %d", len(eastRGs.Data))
	}

	westRGs, err := b.DescribeReplicationGroups(ctxWest, "", "", 100)
	if err != nil {
		t.Fatalf("describe rg west: %v", err)
	}
	if len(westRGs.Data) != 0 {
		t.Fatalf("expected 0 rgs in us-west-2, got %d", len(westRGs.Data))
	}
}

// TestSnapshotRestore_CrossRegion verifies a Phase 3.3 per-region
// store.Table conversion doesn't collapse or drop a second region's data:
// clusters in two distinct regions must both survive a Snapshot/Restore
// round trip into a fresh backend, each still isolated to its own region.
// This test lives in the internal package (unlike persistence_test.go) so it
// can use the unexported regionContextKey directly, the same way the
// TestRegionIsolation_* tests above do.
func TestSnapshotRestore_CrossRegion(t *testing.T) {
	original := NewInMemoryBackend("redis", "123456789012", "us-east-1", nil)

	ctxEast := context.WithValue(context.Background(), regionContextKey{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKey{}, "us-west-2")

	if _, err := original.CreateCluster(ctxEast, "cr-cluster-east", "redis", "cache.t3.micro", 6379); err != nil {
		t.Fatalf("create cluster east: %v", err)
	}
	if _, err := original.CreateCluster(ctxWest, "cr-cluster-west", "redis", "cache.t3.micro", 6379); err != nil {
		t.Fatalf("create cluster west: %v", err)
	}

	snap := original.Snapshot(context.Background())
	if snap == nil {
		t.Fatal("snapshot returned nil")
	}

	fresh := NewInMemoryBackend("redis", "123456789012", "us-east-1", nil)
	if err := fresh.Restore(context.Background(), snap); err != nil {
		t.Fatalf("restore: %v", err)
	}

	eastAfter, err := fresh.DescribeClusters(ctxEast, "", "", 100, false)
	if err != nil {
		t.Fatalf("describe clusters east after restore: %v", err)
	}
	if len(eastAfter.Data) != 1 || eastAfter.Data[0].ClusterID != "cr-cluster-east" {
		t.Fatalf("expected exactly cr-cluster-east in us-east-1 after restore, got %+v", eastAfter.Data)
	}

	westAfter, err := fresh.DescribeClusters(ctxWest, "", "", 100, false)
	if err != nil {
		t.Fatalf("describe clusters west after restore: %v", err)
	}
	if len(westAfter.Data) != 1 || westAfter.Data[0].ClusterID != "cr-cluster-west" {
		t.Fatalf("expected exactly cr-cluster-west in us-west-2 after restore, got %+v", westAfter.Data)
	}
}

// TestSnapshotRestore_CacheSecurityGroupIngress verifies the one map Phase
// 3.3 deliberately left un-converted (cacheSecurityGroupIngress is
// slice-valued, not map[string]*T, so store.Table cannot represent it)
// still round-trips through Snapshot/Restore. Unlike the exported-API checks
// in persistence_test.go, ingress data has no public Describe surface, so
// this reaches into the unexported field directly from within the package.
func TestSnapshotRestore_CacheSecurityGroupIngress(t *testing.T) {
	original := NewInMemoryBackend("redis", "123456789012", "us-east-1", nil)

	ctx := context.Background()
	if _, err := original.CreateCacheSecurityGroup(ctx, "ingress-sg", "test"); err != nil {
		t.Fatalf("create cache security group: %v", err)
	}
	if _, err := original.AuthorizeCacheSecurityGroupIngress(ctx, "ingress-sg", "peer-sg", "999999999999"); err != nil {
		t.Fatalf("authorize ingress: %v", err)
	}

	snap := original.Snapshot(ctx)
	if snap == nil {
		t.Fatal("snapshot returned nil")
	}

	fresh := NewInMemoryBackend("redis", "123456789012", "us-east-1", nil)
	if err := fresh.Restore(ctx, snap); err != nil {
		t.Fatalf("restore: %v", err)
	}

	ingress := fresh.cacheSecurityGroupIngressStore(fresh.region)["ingress-sg"]
	if len(ingress) != 1 || ingress[0].EC2SecurityGroupName != "peer-sg" {
		t.Fatalf("expected 1 ingress entry for peer-sg to survive restore, got %+v", ingress)
	}
}

// TestReset_RegisteredTableSurvivesRepeatedCalls guards against a
// regression where Reset accidentally re-registers the
// globalReplicationGroups table (store.Register panics on a duplicate name)
// instead of clearing it in place via b.registry.ResetAll().
func TestReset_RegisteredTableSurvivesRepeatedCalls(t *testing.T) {
	b := NewInMemoryBackend("redis", "123456789012", "us-east-1", nil)

	ctx := context.Background()

	rg, err := b.CreateReplicationGroup(ctx, "reset-rg", "test")
	if err != nil {
		t.Fatalf("create replication group: %v", err)
	}
	_, createErr := b.CreateGlobalReplicationGroup(ctx, "reset-grg", "test", rg.ReplicationGroupID)
	if createErr != nil {
		t.Fatalf("create global replication group: %v", createErr)
	}

	b.Reset()
	b.Reset()

	grgs, err := b.DescribeGlobalReplicationGroups(ctx, "", "", 100)
	if err != nil {
		t.Fatalf("describe global replication groups after reset: %v", err)
	}
	if len(grgs.Data) != 0 {
		t.Fatalf("expected 0 global replication groups after Reset, got %d", len(grgs.Data))
	}

	_, createErr2 := b.CreateGlobalReplicationGroup(ctx, "post-reset-grg", "test", "")
	if createErr2 != nil {
		t.Fatalf("create global replication group after reset: %v", createErr2)
	}
}
