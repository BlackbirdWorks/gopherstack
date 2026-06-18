package elasticsearch_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestElasticsearchStatefulOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T)
		name string
	}{
		{name: "packages", run: testStatefulPackages},
		{name: "vpc endpoints and access", run: testStatefulVpcEndpoints},
		{name: "upgrades and software", run: testStatefulUpgrade},
		{name: "reserved instances", run: testStatefulReservedInstances},
		{name: "instance metadata", run: testMetadataOperations},
		{name: "cross cluster connections", run: testStatefulConnections},
		{name: "snapshot extended state", run: testStatefulSnapshot},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.run(t)
		})
	}
}

func testStatefulPackages(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	domain := createTestDomainName(t, h, "pkg-state-domain")
	packageID := createTestPackage(t, h, "state-package")

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/packages/associate/"+packageID+"/"+domain, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/packages/update", map[string]any{
		"PackageID": packageID, "PackageDescription": "changed",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "changed", esCoverageReadBody(t, resp)["PackageDetails"].(map[string]any)["PackageDescription"])

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/packages/describe", nil)
	require.Len(t, esCoverageReadBody(t, resp)["PackageDetailsList"], 1)

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/packages/"+packageID+"/domains", nil)
	require.Len(t, esCoverageReadBody(t, resp)["DomainPackageDetailsList"], 1)

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/domain/"+domain+"/packages", nil)
	require.Len(t, esCoverageReadBody(t, resp)["DomainPackageDetailsList"], 1)

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/packages/dissociate/"+packageID+"/"+domain, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodDelete, "/2015-01-01/packages/"+packageID, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/packages/describe", nil)
	assert.Empty(t, esCoverageReadBody(t, resp)["PackageDetailsList"])
}

func testStatefulVpcEndpoints(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	domain := "vpc-state-domain"
	domainARN := createDomainAndGetARN(t, h, domain)

	resp := doRequest(t, h, http.MethodPost,
		"/2015-01-01/es/domain/"+domain+"/authorizeVpcEndpointAccess", map[string]any{"Account": "222222222222"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+domain+"/listVpcEndpointAccess", nil)
	require.Len(t, esCoverageReadBody(t, resp)["AuthorizedPrincipalList"], 1)

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/es/vpcEndpoints", map[string]any{
		"DomainArn": domainARN, "VpcOptions": map[string]string{"SubnetId": "subnet-a"},
	})
	endpointID := esCoverageReadBody(t, resp)["VpcEndpoint"].(map[string]any)["VpcEndpointId"].(string)

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/es/vpcEndpoints/update", map[string]any{
		"VpcEndpointId": endpointID, "VpcOptions": map[string]string{"SubnetId": "subnet-b"},
	})
	assert.Equal(
		t,
		"subnet-b",
		esCoverageReadBody(t, resp)["VpcEndpoint"].(map[string]any)["VpcOptions"].(map[string]any)["SubnetId"],
	)

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+domain+"/vpcEndpoints", nil)
	require.Len(t, esCoverageReadBody(t, resp)["VpcEndpointSummaryList"], 1)

	resp = doRequest(t, h, http.MethodPost,
		"/2015-01-01/es/domain/"+domain+"/revokeVpcEndpointAccess", map[string]any{"Account": "222222222222"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+domain+"/listVpcEndpointAccess", nil)
	assert.Empty(t, esCoverageReadBody(t, resp)["AuthorizedPrincipalList"])

	resp = doRequest(t, h, http.MethodDelete, "/2015-01-01/es/vpcEndpoints/"+endpointID, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func testStatefulUpgrade(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	createTestDomainName(t, h, "upgrade-state-domain")

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/upgradeDomain", map[string]any{
		"DomainName": "upgrade-state-domain", "TargetVersion": "7.11",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/upgrade-state-domain", nil)
	assert.Equal(t, "7.11", esCoverageReadBody(t, resp)["DomainStatus"].(map[string]any)["ElasticsearchVersion"])

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/es/serviceSoftwareUpdate", map[string]any{
		"DomainName": "missing-domain",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	resp.Body.Close()
}

func testStatefulReservedInstances(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/reservedInstanceOfferings", nil)
	require.Len(t, esCoverageReadBody(t, resp)["ReservedElasticsearchInstanceOfferings"], 1)

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/es/purchaseReservedInstanceOffering", map[string]any{
		"ReservedElasticsearchInstanceOfferingId": "offer-t3-small-1y",
		"ReservationName":                         "reserved-state",
	})
	require.NotEmpty(t, esCoverageReadBody(t, resp)["ReservedElasticsearchInstanceId"])

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/reservedInstances", nil)
	require.Len(t, esCoverageReadBody(t, resp)["ReservedElasticsearchInstances"], 1)
}

func testMetadataOperations(t *testing.T) {
	t.Helper()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/compatibleVersions", nil)
	assert.NotEmpty(t, esCoverageReadBody(t, resp)["CompatibleElasticsearchVersions"])

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/instanceTypes/7.10", nil)
	assert.NotEmpty(t, esCoverageReadBody(t, resp)["ElasticsearchInstanceTypes"])

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/instanceTypeLimits/t3.small.elasticsearch/7.10", nil)
	assert.NotEmpty(t, esCoverageReadBody(t, resp)["LimitsByRole"])
}

func testStatefulConnections(t *testing.T) {
	t.Helper()

	backend := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	backend.AddInboundConnectionInternal(context.Background(), elasticsearch.InboundConnection{
		ConnectionID: "connection-state", ConnectionStatus: "PENDING_ACCEPTANCE",
	})
	h := elasticsearch.NewHandler(backend)

	resp := doRequest(t, h, http.MethodPut,
		"/2015-01-01/es/ccs/inboundConnection/connection-state/reject", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := esCoverageReadBody(t, resp)
	connection := body["CrossClusterSearchConnection"].(map[string]any)
	assert.Equal(t, "REJECTED", connection["ConnectionStatus"].(map[string]any)["StatusCode"])

	resp = doRequest(t, h, http.MethodDelete, "/2015-01-01/es/ccs/inboundConnection/connection-state", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/es/ccs/inboundConnection/search", nil)
	assert.Empty(t, esCoverageReadBody(t, resp)["CrossClusterSearchConnections"])
}

func testStatefulSnapshot(t *testing.T) {
	t.Helper()

	backend := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := backend.CreateDomain(
		context.Background(), "saved-domain", "", elasticsearch.ClusterConfig{}, elasticsearch.EBSOptions{},
	)
	require.NoError(t, err)
	require.NoError(t, backend.AuthorizeVpcEndpointAccess(context.Background(), "saved-domain", "222222222222"))
	_, err = backend.PurchaseReservedElasticsearchInstanceOffering(
		context.Background(), "offer-t3-small-1y", "saved-reservation", 1,
	)
	require.NoError(t, err)

	restored := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(backend.Snapshot()))

	accounts, err := restored.ListVpcEndpointAccess(context.Background(), "saved-domain")
	require.NoError(t, err)
	assert.Equal(t, []string{"222222222222"}, accounts)
	assert.Len(t, restored.DescribeReservedElasticsearchInstances(context.Background()), 1)
}
