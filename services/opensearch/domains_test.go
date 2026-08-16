package opensearch_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

func TestAddDomainInternal(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("seed-domain", "OpenSearch_2.3")

	d, err := b.DescribeDomain("seed-domain")
	require.NoError(t, err)
	assert.Equal(t, "OpenSearch_2.3", d.EngineVersion)
	assert.NotEmpty(t, d.ARN)
	assert.NotEmpty(t, d.Endpoint)
}

func TestAddDomainInternal_DefaultVersion(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("seed-domain", "")

	d, err := b.DescribeDomain("seed-domain")
	require.NoError(t, err)
	assert.Equal(t, "OpenSearch_2.11", d.EngineVersion)
}

func TestDeleteDomain_Cascade(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("my-domain", "")

	_, err := b.AddDataSource("my-domain", "ds1", "desc", json.RawMessage(`{"S3GlueDataCatalog":{}}`))
	require.NoError(t, err)

	b.AddPackageInternal("pkg-001", "test-pkg", "TXT-DICTIONARY")

	_, err = b.AssociatePackage("pkg-001", "my-domain")
	require.NoError(t, err)

	_, err = b.AuthorizeVpcEndpointAccess("my-domain", "111122223333", "")
	require.NoError(t, err)

	assert.Equal(t, 1, opensearch.DataSourceCount(b))

	_, err = b.DeleteDomain("my-domain")
	require.NoError(t, err)

	// Domain data sources cleaned up.
	assert.Equal(t, 0, opensearch.DataSourceCount(b))
	// ARN index cleaned up.
	assert.Equal(t, 0, opensearch.ARNIndexSize(b))
}

func TestListDomainNames_Sorted(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("zebra", "")
	b.AddDomainInternal("apple", "")
	b.AddDomainInternal("mango", "")

	names := b.ListDomainNames()
	require.Len(t, names, 3)
	assert.Equal(t, []string{"apple", "mango", "zebra"}, names)
}
