package elasticsearch_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// TestRefinement1_ErrValidationSentinel verifies that ErrValidation is exported and wraps correctly.
func TestRefinement1_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain(context.Background(), elasticsearch.CreateDomainInput{})
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_ErrNilAppContext verifies that Provider.Init returns ErrNilAppContext for nil ctx.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &elasticsearch.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrNilAppContext)
}

// TestRefinement1_BuildOpsFieldExists verifies the ops dispatch table is populated (fixed routes work).
func TestRefinement1_BuildOpsFieldExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// If ops table is built, the fixed route GET /2015-01-01/tags returns 404 for unknown ARN.
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/tags?arn=nonexistent", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestRefinement1_SortedListDomainNames verifies that ListDomainNames returns domains in sorted order.
func TestRefinement1_SortedListDomainNames(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	for _, name := range []string{"zoo-domain", "apple-dom", "mid-domain"} {
		_, err := b.CreateDomain(
			context.Background(), elasticsearch.CreateDomainInput{Name: name},
		)
		require.NoError(t, err)
	}

	names := b.ListDomainNames(context.Background())
	require.Len(t, names, 3)
	assert.Equal(t, "apple-dom", names[0])
	assert.Equal(t, "mid-domain", names[1])
	assert.Equal(t, "zoo-domain", names[2])
}

// TestRefinement1_SortedListTagsResponse verifies that the HTTP ListTags response is sorted by key.
func TestRefinement1_SortedListTagsResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	arn := createDomainAndGetARN(t, h, "sorted-tags-d")

	doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN": arn,
		"TagList": []map[string]string{
			{"Key": "zzz", "Value": "last"},
			{"Key": "aaa", "Value": "first"},
			{"Key": "mmm", "Value": "middle"},
		},
	}).Body.Close()

	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/tags?arn="+arn, nil)
	defer resp.Body.Close()

	var out struct {
		TagList []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"TagList"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.TagList, 3)
	assert.Equal(t, "aaa", out.TagList[0].Key)
	assert.Equal(t, "mmm", out.TagList[1].Key)
	assert.Equal(t, "zzz", out.TagList[2].Key)
}

// TestRefinement1_DomainNameValidationEmpty verifies empty domain name returns ValidationException.
func TestRefinement1_DomainNameValidationEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestRefinement1_DomainNameValidationTooShort verifies domain name < 3 chars is rejected.
func TestRefinement1_DomainNameValidationTooShort(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(context.Background(), elasticsearch.CreateDomainInput{Name: "ab"})
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_DomainNameValidationTooLong verifies domain name > 28 chars is rejected.
func TestRefinement1_DomainNameValidationTooLong(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(
		context.Background(),
		elasticsearch.CreateDomainInput{Name: "abcdefghijklmnopqrstuvwxyzabc"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_DomainNameValidationInvalidChars verifies underscore/uppercase is rejected.
func TestRefinement1_DomainNameValidationInvalidChars(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "my_domain"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_DomainNameMustStartWithLetter verifies domain name must start with a letter.
func TestRefinement1_DomainNameMustStartWithLetter(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "1bad-domain"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_VpcEndpointStatusActive verifies VPC endpoints are created with ACTIVE status.
func TestRefinement1_VpcEndpointStatusActive(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	ep, err := b.CreateVpcEndpoint(
		context.Background(), "arn:aws:es:us-east-1:123456789012:domain/test", map[string]string{"VpcId": "vpc-1"},
	)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", ep.Status)
}

// TestRefinement1_VpcOptionsDeepCopy verifies CreateVpcEndpoint deep-copies VpcOptions.
func TestRefinement1_VpcOptionsDeepCopy(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	opts := map[string]string{"VpcId": "vpc-1"}

	ep, err := b.CreateVpcEndpoint(context.Background(), "arn:aws:es:us-east-1:123456789012:domain/test", opts)
	require.NoError(t, err)

	// Mutating the original opts must not affect the returned endpoint.
	opts["VpcId"] = "vpc-mutated"
	assert.Equal(t, "vpc-1", ep.VpcOptions["VpcId"])
}

// TestRefinement1_AddDomainInternal verifies the seed helper adds a domain directly.
func TestRefinement1_AddDomainInternal(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal(context.Background(), elasticsearch.Domain{
		Name:                 "seed-domain",
		ARN:                  "arn:aws:es:us-east-1:123456789012:domain/seed-domain",
		ElasticsearchVersion: "7.10",
		Status:               "Active",
	})

	assert.Equal(t, 1, b.DomainCount())

	d, err := b.DescribeDomain(context.Background(), "seed-domain")
	require.NoError(t, err)
	assert.Equal(t, "seed-domain", d.Name)
	assert.Equal(t, "7.10", d.ElasticsearchVersion)
}

// TestRefinement1_ExportCountHelpers verifies DomainCount, PackageCount, InboundConnectionCount,
// OutboundConnectionCount, and VpcEndpointCount helpers work correctly.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	assert.Equal(t, 0, b.DomainCount())
	assert.Equal(t, 0, b.PackageCount())
	assert.Equal(t, 0, b.InboundConnectionCount())
	assert.Equal(t, 0, b.OutboundConnectionCount())
	assert.Equal(t, 0, b.VpcEndpointCount())

	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "cnt-domain"},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, b.DomainCount())

	_, err = b.CreatePackage(context.Background(), "my-pkg", "TXT-DICTIONARY", "desc")
	require.NoError(t, err)
	assert.Equal(t, 1, b.PackageCount())

	_, err = b.CreateVpcEndpoint(context.Background(), "arn:aws:es:us-east-1:123456789012:domain/cnt-domain", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, b.VpcEndpointCount())

	conn := elasticsearch.InboundConnection{
		ConnectionID:     "conn-001",
		ConnectionStatus: "PENDING_ACCEPTANCE",
	}
	b.AddInboundConnectionInternal(context.Background(), conn)
	assert.Equal(t, 1, b.InboundConnectionCount())

	_, err = b.CreateOutboundCrossClusterSearchConnection(
		context.Background(),
		elasticsearch.CrossClusterDomainInfo{DomainName: "local"},
		elasticsearch.CrossClusterDomainInfo{DomainName: "remote"},
		"my-alias",
	)
	require.NoError(t, err)
	assert.Equal(t, 1, b.OutboundConnectionCount())
}

// TestRefinement1_ResetClearsAllMaps verifies that Reset clears all internal maps.
func TestRefinement1_ResetClearsAllMaps(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "reset-dom"},
	)
	require.NoError(t, err)

	_, err = b.CreatePackage(context.Background(), "pkg1", "TXT-DICTIONARY", "")
	require.NoError(t, err)

	_, err = b.CreateVpcEndpoint(context.Background(), "arn:aws:es:us-east-1:123456789012:domain/reset-dom", nil)
	require.NoError(t, err)

	b.AddInboundConnectionInternal(context.Background(), elasticsearch.InboundConnection{ConnectionID: "c1"})

	h := elasticsearch.NewHandler(b)
	h.Reset()

	assert.Equal(t, 0, b.DomainCount())
	assert.Equal(t, 0, b.PackageCount())
	assert.Equal(t, 0, b.VpcEndpointCount())
	assert.Equal(t, 0, b.InboundConnectionCount())
}

// TestRefinement1_PersistenceCoversAllMaps verifies that packages and connections survive snapshot/restore.
func TestRefinement1_PersistenceCoversAllMaps(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreatePackage(context.Background(), "dict-pkg", "TXT-DICTIONARY", "my dictionary")
	require.NoError(t, err)

	_, err = b.CreateVpcEndpoint(
		context.Background(), "arn:aws:es:us-east-1:123456789012:domain/my-dom", map[string]string{"VpcId": "vpc-1"},
	)
	require.NoError(t, err)

	b.AddInboundConnectionInternal(context.Background(), elasticsearch.InboundConnection{
		ConnectionID: "conn-snap", ConnectionStatus: "PENDING_ACCEPTANCE",
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.PackageCount())
	assert.Equal(t, 1, b2.VpcEndpointCount())
	assert.Equal(t, 1, b2.InboundConnectionCount())
}

// TestRefinement1_GetSupportedOperationsIncludesListTagsRemoveTags verifies both ops are listed.
func TestRefinement1_GetSupportedOperationsIncludesListTagsRemoveTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "ListTags")
	assert.Contains(t, ops, "RemoveTags")
	assert.Len(t, ops, 51)
}

// TestRefinement1_HandlerResetDelegatesToBackend verifies handler.Reset() clears backend state.
func TestRefinement1_HandlerResetDelegatesToBackend(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "del-domain"},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, b.DomainCount())

	h := elasticsearch.NewHandler(b)
	h.Reset()

	assert.Equal(t, 0, b.DomainCount())
}

// TestRefinement1_NonNilEmptyDomainList verifies ListDomainNames returns [] not null in JSON.
func TestRefinement1_NonNilEmptyDomainList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	domainNames, ok := out["DomainNames"]
	require.True(t, ok)
	require.NotNil(t, domainNames)

	list, ok := domainNames.([]any)
	require.True(t, ok)
	assert.Empty(t, list)
}

// TestRefinement1_ErrValidation400Mapping verifies ErrValidation maps to HTTP 400.
func TestRefinement1_ErrValidation400Mapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// An invalid domain name (starts with digit) should return 400.
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": "1invalid",
	})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestRefinement1_PackageValidation verifies empty package name returns ErrValidation.
func TestRefinement1_PackageValidation(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreatePackage(context.Background(), "", "TXT-DICTIONARY", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_OutboundConnectionValidation verifies empty alias returns ErrValidation.
func TestRefinement1_OutboundConnectionValidation(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateOutboundCrossClusterSearchConnection(
		context.Background(),
		elasticsearch.CrossClusterDomainInfo{},
		elasticsearch.CrossClusterDomainInfo{},
		"",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_VpcEndpointValidation verifies empty DomainARN returns ErrValidation.
func TestRefinement1_VpcEndpointValidation(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateVpcEndpoint(context.Background(), "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_AuthorizeVpcEndpointAccessValidation verifies empty account returns ErrValidation.
func TestRefinement1_AuthorizeVpcEndpointAccessValidation(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "vpc-auth-dom"},
	)
	require.NoError(t, err)

	err = b.AuthorizeVpcEndpointAccess(context.Background(), "vpc-auth-dom", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrValidation)
}

// TestRefinement1_DescribeDomainDeepCopy verifies DescribeDomain returns an independent copy.
func TestRefinement1_DescribeDomainDeepCopy(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateDomain(
		context.Background(), elasticsearch.CreateDomainInput{Name: "copy-domain", ElasticsearchVersion: "7.10"},
	)
	require.NoError(t, err)

	d1, err := b.DescribeDomain(context.Background(), "copy-domain")
	require.NoError(t, err)

	d2, err := b.DescribeDomain(context.Background(), "copy-domain")
	require.NoError(t, err)

	// Both copies are independent; modifying one doesn't affect the other or the stored domain.
	d1.ElasticsearchVersion = "8.0"
	assert.Equal(t, "7.10", d2.ElasticsearchVersion)
}

// TestRefinement1_SnapshotWarnOnEmptyState verifies Snapshot returns valid JSON even with no data.
func TestRefinement1_SnapshotWarnOnEmptyState(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	var v map[string]any
	require.NoError(t, json.Unmarshal(snap, &v))
}

// TestRefinement1_PersistenceNextIDPreserved verifies nextID is preserved across snapshot/restore.
func TestRefinement1_PersistenceNextIDPreserved(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateVpcEndpoint(context.Background(), "arn:aws:es:us-east-1:123456789012:domain/test", nil)
	require.NoError(t, err)
	_, err = b.CreateVpcEndpoint(context.Background(), "arn:aws:es:us-east-1:123456789012:domain/test", nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// After restore, a new endpoint should get id 3, not 1.
	ep, err := b2.CreateVpcEndpoint(context.Background(), "arn:aws:es:us-east-1:123456789012:domain/test", nil)
	require.NoError(t, err)
	assert.Equal(t, "vpc-endpoint-0000000003", ep.ID)
}

// TestRefinement1_ListTagsNotFoundForUnknownARN verifies ListTags returns 404 for unknown ARN.
func TestRefinement1_ListTagsNotFoundForUnknownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/tags?arn=arn:aws:es:us-east-1:123456789012:domain/none", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
