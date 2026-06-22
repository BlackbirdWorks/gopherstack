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
