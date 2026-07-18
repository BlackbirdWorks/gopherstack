package opensearch_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReset(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("d1", "")
	b.AddDomainInternal("d2", "")

	require.Equal(t, 2, opensearch.DomainCount(b))

	b.Reset()

	assert.Equal(t, 0, opensearch.DomainCount(b))
	assert.Equal(t, 0, opensearch.ARNIndexSize(b))
}

func TestMultipleResetCycles(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)

	for range 3 {
		b.AddDomainInternal("d1", "OpenSearch_2.11")
		b.Reset()
		assert.Equal(t, 0, opensearch.DomainCount(b))
	}
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	h := opensearch.NewHandler(b)
	b.AddDomainInternal("d1", "")

	require.Equal(t, 1, opensearch.DomainCount(b))
	h.Reset()
	assert.Equal(t, 0, opensearch.DomainCount(b))
}

func TestRegionAccountID(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)

	assert.Equal(t, testRegion, b.Region())
	assert.Equal(t, testAccountID, b.AccountID())
}

func TestExportCounts(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("d1", "")
	b.AddDomainInternal("d2", "")

	opensearch.SeedInboundConnection(b, "conn-1")

	_, err := b.AddDataSource("d1", "ds-1", "desc", "S3GLUE")
	require.NoError(t, err)

	_, err = b.AddDirectQueryDataSource("dq-1", "desc", "CloudWatchLogs", nil)
	require.NoError(t, err)

	_, err = b.CreateApplication("app-1", nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, opensearch.DomainCount(b))
	assert.Equal(t, 1, opensearch.ConnectionCount(b))
	assert.Equal(t, 1, opensearch.DataSourceCount(b))
	assert.Equal(t, 1, opensearch.DirectQueryDataSourceCount(b))
	assert.Equal(t, 1, opensearch.ApplicationCount(b))
	assert.Equal(t, 2, opensearch.ARNIndexSize(b))
}

// mockDNSRegistrar is a test double for opensearch.DNSRegistrar.
type mockDNSRegistrar struct {
	registered map[string]bool
}

func (m *mockDNSRegistrar) Register(hostname string) {
	m.registered[hostname] = true
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	delete(m.registered, hostname)
}

func TestOpenSearchBackend_DNSRegistrar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		domainName     string
		wantRegistered bool
		deleteAfter    bool
	}{
		{
			name:           "registers_on_create",
			domainName:     "my-domain",
			wantRegistered: true,
		},
		{
			name:           "deregisters_on_delete",
			domainName:     "del-domain",
			deleteAfter:    true,
			wantRegistered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			registrar := &mockDNSRegistrar{registered: make(map[string]bool)}
			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.SetDNSRegistrar(registrar)

			domain, err := b.CreateDomain(opensearch.CreateDomainInput{Name: tt.domainName})
			require.NoError(t, err)

			if tt.deleteAfter {
				_, err = b.DeleteDomain(tt.domainName)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantRegistered, registrar.registered[domain.Endpoint])
		})
	}
}
