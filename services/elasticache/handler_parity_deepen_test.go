package elasticache_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// newTestStackSeeded creates a test stack backed by the given pre-configured backend.
func newTestStackSeeded(t *testing.T, b *elasticache.InMemoryBackend) *elasticachesdk.Client {
	t.Helper()

	handler := elasticache.NewHandler(b)

	e := echo.New()
	registry := service.NewRegistry()
	_ = registry.Register(handler)
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return elasticachesdk.NewFromConfig(cfg, func(o *elasticachesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// ----------------------------------------
// DescribeSnapshots SnapshotSource filter (AWS parity)
//
// Real AWS: SnapshotSource="system" returns only automated snapshots;
// "user" returns only manual ones. The filter values differ from the
// stored field values ("automated"/"manual").
// ----------------------------------------

func TestHandler_DescribeSnapshots_SnapshotSource_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend)
		name           string
		snapshotSource string
		wantNames      []string
		wantCount      int
	}{
		{
			name:           "no_filter_returns_all",
			snapshotSource: "",
			wantCount:      2,
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("snap-src-rg"),
					ReplicationGroupDescription: aws.String("snap source test"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					ReplicationGroupId: aws.String("snap-src-rg"),
					SnapshotName:       aws.String("manual-snap"),
				})
				require.NoError(t, err)
				elasticache.AddSnapshotInternal(b, "auto-snap", "snap-src-rg", "automated")
			},
		},
		{
			name:           "user_filter_returns_only_manual",
			snapshotSource: "user",
			wantCount:      1,
			wantNames:      []string{"manual-snap"},
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("snap-src-rg"),
					ReplicationGroupDescription: aws.String("snap source test"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					ReplicationGroupId: aws.String("snap-src-rg"),
					SnapshotName:       aws.String("manual-snap"),
				})
				require.NoError(t, err)
				elasticache.AddSnapshotInternal(b, "auto-snap", "snap-src-rg", "automated")
			},
		},
		{
			name:           "system_filter_returns_only_automated",
			snapshotSource: "system",
			wantCount:      1,
			wantNames:      []string{"auto-snap"},
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("snap-src-rg"),
					ReplicationGroupDescription: aws.String("snap source test"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					ReplicationGroupId: aws.String("snap-src-rg"),
					SnapshotName:       aws.String("manual-snap"),
				})
				require.NoError(t, err)
				elasticache.AddSnapshotInternal(b, "auto-snap", "snap-src-rg", "automated")
			},
		},
		{
			name:           "system_filter_empty_when_no_automated",
			snapshotSource: "system",
			wantCount:      0,
			wantNames:      []string{},
			setup: func(t *testing.T, client *elasticachesdk.Client, _ *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("snap-src-rg"),
					ReplicationGroupDescription: aws.String("snap source test"),
				})
				require.NoError(t, err)
				_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
					ReplicationGroupId: aws.String("snap-src-rg"),
					SnapshotName:       aws.String("only-manual"),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			client := newTestStackSeeded(t, b)

			if tt.setup != nil {
				tt.setup(t, client, b)
			}

			var src *string
			if tt.snapshotSource != "" {
				src = aws.String(tt.snapshotSource)
			}

			out, err := client.DescribeSnapshots(t.Context(), &elasticachesdk.DescribeSnapshotsInput{
				SnapshotSource: src,
			})
			require.NoError(t, err)
			assert.Len(t, out.Snapshots, tt.wantCount)

			if len(tt.wantNames) > 0 {
				names := make([]string, 0, len(out.Snapshots))
				for _, s := range out.Snapshots {
					names = append(names, aws.ToString(s.SnapshotName))
				}
				assert.ElementsMatch(t, tt.wantNames, names)
			}
		})
	}
}

// ----------------------------------------
// Backend: DescribeSnapshots SnapshotSource filter (unit)
// ----------------------------------------

func TestBackend_DescribeSnapshots_SnapshotSource_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		snapshotSource string
		wantCount      int
	}{
		{name: "no_filter", snapshotSource: "", wantCount: 2},
		{name: "user", snapshotSource: "user", wantCount: 1},
		{name: "system", snapshotSource: "system", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			elasticache.AddSnapshotInternal(b, "snap-manual", "rg-x", "manual")
			elasticache.AddSnapshotInternal(b, "snap-auto", "rg-x", "automated")

			p, err := b.DescribeSnapshots(context.Background(), "", "", "", tt.snapshotSource, "", 0)
			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)
		})
	}
}

// ----------------------------------------
// DescribeCacheClusters ShowCacheClustersNotInReplicationGroups (AWS parity)
//
// Real AWS: when true, only clusters NOT belonging to a replication group
// are returned — i.e. standalone Memcached and single-node Redis clusters.
// ----------------------------------------

func TestHandler_DescribeCacheClusters_ShowCacheClustersNotInReplicationGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend)
		name    string
		wantIDs []string
		notInRG bool
		wantAll bool
	}{
		{
			name:    "false_or_omitted_returns_all",
			notInRG: false,
			wantAll: true,
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("standalone-cl"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				// Seed a cluster that belongs to a RG.
				elasticache.AddClusterInRGInternal(b, "rg-member-cl", "some-rg")
			},
		},
		{
			name:    "true_returns_only_standalone_clusters",
			notInRG: true,
			wantIDs: []string{"standalone-cl"},
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("standalone-cl"),
					Engine:         aws.String("redis"),
				})
				require.NoError(t, err)
				elasticache.AddClusterInRGInternal(b, "rg-member-cl", "some-rg")
			},
		},
		{
			name:    "true_returns_multiple_standalone",
			notInRG: true,
			wantIDs: []string{"sa-1", "sa-2"},
			setup: func(t *testing.T, client *elasticachesdk.Client, b *elasticache.InMemoryBackend) {
				t.Helper()
				for _, id := range []string{"sa-1", "sa-2"} {
					_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
						CacheClusterId: aws.String(id),
						Engine:         aws.String("redis"),
					})
					require.NoError(t, err)
				}
				// Seed RG-member clusters — they must be excluded.
				elasticache.AddClusterInRGInternal(b, "rg-member-1", "rg-x")
				elasticache.AddClusterInRGInternal(b, "rg-member-2", "rg-x")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			client := newTestStackSeeded(t, b)

			if tt.setup != nil {
				tt.setup(t, client, b)
			}

			out, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
				ShowCacheClustersNotInReplicationGroups: aws.Bool(tt.notInRG),
			})
			require.NoError(t, err)

			if tt.wantAll {
				assert.NotEmpty(t, out.CacheClusters)

				return
			}

			ids := make([]string, 0, len(out.CacheClusters))
			for _, cl := range out.CacheClusters {
				ids = append(ids, aws.ToString(cl.CacheClusterId))
			}
			assert.ElementsMatch(t, tt.wantIDs, ids)
		})
	}
}

// ----------------------------------------
// Backend: DescribeClusters notInRG filter (unit)
// ----------------------------------------

func TestBackend_DescribeClusters_NotInRG_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
		notInRG   bool
	}{
		{name: "no_filter_returns_all", notInRG: false, wantCount: 3},
		{name: "not_in_rg_excludes_rg_members", notInRG: true, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

			// One standalone cluster.
			_, err := b.CreateCluster(context.Background(), "standalone", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			// Two clusters with ReplicationGroupID set (simulate RG membership).
			elasticache.AddClusterInRGInternal(b, "rg-member-1", "test-rg")
			elasticache.AddClusterInRGInternal(b, "rg-member-2", "test-rg")

			p, err := b.DescribeClusters(context.Background(), "", "", 0, tt.notInRG)
			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)
		})
	}
}
