package elasticsearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func esCoverageReadBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))

	return m
}

func createTestDomainName(t *testing.T, h *elasticsearch.Handler, name string) string {
	t.Helper()
	// createDomainAndGetARN creates the domain; we just need the name for package ops
	createDomainAndGetARN(t, h, name)

	return name
}

func createTestPackage(t *testing.T, h *elasticsearch.Handler, name string) string {
	t.Helper()

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/packages", map[string]any{
		"PackageName":        name,
		"PackageType":        "TXT-DICTIONARY",
		"PackageSource":      map[string]any{"S3BucketName": "my-bucket", "S3Key": "dict.txt"},
		"PackageDescription": "test",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := esCoverageReadBody(t, resp)
	id, _ := body["PackageDetails"].(map[string]any)["PackageID"].(string)
	require.NotEmpty(t, id)

	return id
}

func TestElasticsearchCoverage_Package_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	pkgID := createTestPackage(t, h, "test-pkg")
	assert.NotEmpty(t, pkgID)

	// DescribePackages (POST)
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/packages/describe", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeletePackage
	resp = doRequest(t, h, http.MethodDelete, "/2015-01-01/packages/"+pkgID, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestElasticsearchCoverage_Package_AssociateDissociate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domain := createTestDomainName(t, h, "assoc-dom")
	pkgID := createTestPackage(t, h, "assoc-pkg")

	// AssociatePackage
	resp := doRequest(t, h, http.MethodPost,
		"/2015-01-01/packages/associate/"+pkgID+"/"+domain, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListPackagesForDomain
	resp = doRequest(t, h, http.MethodGet,
		"/2015-01-01/domain/"+domain+"/packages", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListDomainsForPackage
	resp = doRequest(t, h, http.MethodGet,
		"/2015-01-01/packages/"+pkgID+"/domains", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GetPackageVersionHistory
	resp = doRequest(t, h, http.MethodGet,
		"/2015-01-01/packages/"+pkgID+"/history", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// UpdatePackage
	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/packages/update", map[string]any{
		"PackageID":          pkgID,
		"PackageDescription": "updated",
		"PackageSource":      map[string]any{"S3BucketName": "bucket2", "S3Key": "dict2.txt"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DissociatePackage
	resp = doRequest(t, h, http.MethodPost,
		"/2015-01-01/packages/dissociate/"+pkgID+"/"+domain, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestElasticsearchCoverage_VpcEndpoint_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainARN := createDomainAndGetARN(t, h, "vpcdomain")

	// CreateVpcEndpoint
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/vpcEndpoints", map[string]any{
		"DomainArn":  domainARN,
		"VpcOptions": map[string]string{"SubnetId": "subnet-abc"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := esCoverageReadBody(t, resp)
	epID, _ := body["VpcEndpoint"].(map[string]any)["VpcEndpointId"].(string)
	require.NotEmpty(t, epID)

	// DescribeVpcEndpoints
	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/es/vpcEndpoints/describe", map[string]any{
		"VpcEndpointIds": []string{epID},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteVpcEndpoint
	resp = doRequest(t, h, http.MethodDelete, "/2015-01-01/es/vpcEndpoints/"+epID, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
