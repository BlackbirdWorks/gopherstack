package elasticache_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// mockDNSRegistrar is a simple in-memory DNSRegistrar for testing.
type mockDNSRegistrar struct {
	registered   map[string]bool
	deregistered map[string]bool
	mu           sync.Mutex
}

func newMockDNS() *mockDNSRegistrar {
	return &mockDNSRegistrar{
		registered:   make(map[string]bool),
		deregistered: make(map[string]bool),
	}
}

func (m *mockDNSRegistrar) Register(hostname string) {
	m.mu.Lock()
	m.registered[hostname] = true
	m.mu.Unlock()
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	m.mu.Lock()
	m.deregistered[hostname] = true
	m.mu.Unlock()
}

func TestCreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantPrefix string
		wantSuffix string
		withDNS    bool
	}{
		{
			name:       "dns_registration",
			withDNS:    true,
			wantPrefix: "my-cache.",
			wantSuffix: ".us-east-1.cache.amazonaws.com",
		},
		{
			name:       "no_dns_still_works",
			withDNS:    false,
			wantSuffix: ".cache.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var dns *mockDNSRegistrar
			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")

			if tt.withDNS {
				dns = newMockDNS()
				backend.SetDNSRegistrar(dns)
			}

			cluster, err := backend.CreateCluster("my-cache", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			if tt.wantPrefix != "" {
				assert.True(t, strings.HasPrefix(cluster.Endpoint, tt.wantPrefix))
			}
			assert.True(t, strings.HasSuffix(cluster.Endpoint, tt.wantSuffix))

			if tt.withDNS {
				assert.True(t, dns.registered[cluster.Endpoint], "hostname should be registered with DNS")
			}
		})
	}
}

func TestDeleteCluster_DNSDeregistration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "deregisters_on_delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dns := newMockDNS()
			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")
			backend.SetDNSRegistrar(dns)

			cluster, err := backend.CreateCluster("my-cache", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			endpoint := cluster.Endpoint

			err = backend.DeleteCluster("my-cache")
			require.NoError(t, err)

			assert.True(t, dns.deregistered[endpoint], "hostname should be deregistered from DNS on delete")
		})
	}
}

func TestCreateClusterWithOptions_AtomicNoLeak(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		name           string
		paramGroupName string
	}{
		{
			name:           "param_group_not_found_no_leak",
			paramGroupName: "nonexistent-pg",
			wantErr:        elasticache.ErrParameterGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "123456789012", "us-east-1")

			_, err := backend.CreateClusterWithOptions(
				"my-cache",
				"redis",
				"cache.t3.micro",
				tt.paramGroupName,
				"",
				"",
				0,
			)
			require.ErrorIs(t, err, tt.wantErr)

			_, descErr := backend.DescribeClusters("my-cache", "", 0)
			require.ErrorIs(t, descErr, elasticache.ErrClusterNotFound)
		})
	}
}

func TestCreateClusterWithOptions_FamilyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		name           string
		engine         string
		paramGroupName string
	}{
		{
			name:           "redis_cluster_with_memcached_param_group",
			engine:         "redis",
			paramGroupName: "default.memcached1.6",
			wantErr:        elasticache.ErrInvalidParameterGroupFamily,
		},
		{
			name:           "memcached_cluster_with_redis_param_group",
			engine:         "memcached",
			paramGroupName: "default.redis7",
			wantErr:        elasticache.ErrInvalidParameterGroupFamily,
		},
		{
			name:           "redis_cluster_with_redis_param_group",
			engine:         "redis",
			paramGroupName: "default.redis7",
			wantErr:        nil,
		},
		{
			name:           "memcached_cluster_with_memcached_param_group",
			engine:         "memcached",
			paramGroupName: "default.memcached1.6",
			wantErr:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")

			_, err := backend.CreateClusterWithOptions(
				"my-cache",
				tt.engine,
				"cache.t3.micro",
				tt.paramGroupName,
				"",
				"",
				0,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestListTagsForResource_NilTagsSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_tags_returns_empty_map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")

			snap := backend.Snapshot()
			require.NotNil(t, snap)

			backend2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")
			require.NoError(t, backend2.Restore(snap))

			_, err := backend2.CreateCluster("nil-tags-cluster", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			p, err := backend2.DescribeClusters("nil-tags-cluster", "", 0)
			require.NoError(t, err)

			clusterARN := p.Data[0].ARN
			result, err := backend2.ListTagsForResource(clusterARN)
			require.NoError(t, err)
			assert.NotNil(t, result)
		})
	}
}

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

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")

			_, err := backend.CreateCluster(tt.clusterID, "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			p, err := backend.DescribeEvents("", "", "", 0)
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

func TestFailoverReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		rgID    string
	}{
		{
			name:    "failover_existing_group",
			rgID:    "my-rg",
			wantErr: nil,
		},
		{
			name:    "failover_missing_group",
			rgID:    "nonexistent",
			wantErr: elasticache.ErrReplicationGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")

			if tt.wantErr == nil {
				_, err := backend.CreateReplicationGroup(tt.rgID, "test rg")
				require.NoError(t, err)
			}

			rg, err := backend.FailoverReplicationGroup(tt.rgID, "0001")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "available", rg.Status)

			p, evErr := backend.DescribeEvents(tt.rgID, "replication-group", "", 0)
			require.NoError(t, evErr)

			found := false
			for _, e := range p.Data {
				if e.Message == "failover completed" {
					found = true
				}
			}
			assert.True(t, found, "expected failover event not found")
		})
	}
}

func TestAddRemoveTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags         map[string]string
		wantAfterAdd    map[string]string
		wantAfterRemove map[string]string
		name            string
		removeTags      []string
	}{
		{
			name:            "add_and_remove_tags",
			addTags:         map[string]string{"env": "prod", "team": "platform"},
			removeTags:      []string{"team"},
			wantAfterAdd:    map[string]string{"env": "prod", "team": "platform"},
			wantAfterRemove: map[string]string{"env": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")

			c, err := backend.CreateCluster("tag-cluster", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			err = backend.AddTagsToResource(c.ARN, tt.addTags)
			require.NoError(t, err)

			got, err := backend.ListTagsForResource(c.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAfterAdd, got)

			err = backend.RemoveTagsFromResource(c.ARN, tt.removeTags)
			require.NoError(t, err)

			got, err = backend.ListTagsForResource(c.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAfterRemove, got)
		})
	}
}

func TestModifyCluster_ScalesAndEngineVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
		wantVersion   string
		numCacheNodes int
		wantNodes     int
	}{
		{
			name:          "scale_and_engine_version",
			numCacheNodes: 3,
			engineVersion: "7.2.0",
			wantNodes:     3,
			wantVersion:   "7.2.0",
		},
		{
			name:          "engine_version_only",
			numCacheNodes: 0,
			engineVersion: "6.2.0",
			wantNodes:     1,
			wantVersion:   "6.2.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")

			_, err := backend.CreateCluster("mod-cluster", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			modified, err := backend.ModifyCluster("mod-cluster", "", "", tt.engineVersion, "", "", tt.numCacheNodes)
			require.NoError(t, err)

			assert.Equal(t, tt.wantVersion, modified.EngineVersion)
			assert.Equal(t, tt.wantNodes, modified.NumCacheNodes)
		})
	}
}

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

			backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "123456789012", "us-east-1")

			_, err := backend.CreateCluster("reset-cluster", "redis", "cache.t3.micro", 0)
			require.NoError(t, err)

			_, err = backend.CreateReplicationGroup("reset-rg", "test")
			require.NoError(t, err)

			backend.Reset()

			_, err = backend.DescribeClusters("reset-cluster", "", 0)
			require.ErrorIs(t, err, elasticache.ErrClusterNotFound)

			_, err = backend.DescribeReplicationGroups("reset-rg", "", 0)
			require.ErrorIs(t, err, elasticache.ErrReplicationGroupNotFound)

			p, err := backend.DescribeEvents("", "", "", 0)
			require.NoError(t, err)
			assert.Empty(t, p.Data)

			_ = tt.name
		})
	}
}
