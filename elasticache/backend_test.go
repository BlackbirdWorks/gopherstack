package elasticache_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/elasticache"
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
		withDNS    bool
		wantPrefix string
		wantSuffix string
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
