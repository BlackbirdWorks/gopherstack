package codeartifact_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real
// CodeArtifact operation, extracted from codeartifact@v1.41.4
// serializers.go: each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. None of this service's
// operations carry a URI-label path parameter -- every resource ID (domain,
// repository, package, ...) travels as a query parameter or JSON body
// field, so every path below is a fixed literal.
//
// DeleteRepositoryPermissionsPolicy is a real AWS API quirk: it alone uses
// the plural "/v1/repository/permissions/policies", while
// Get/PutRepositoryPermissionsPolicy use the singular
// "/v1/repository/permissions/policy" -- verified directly in
// serializers.go, not an extraction artifact.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateExternalConnection", "POST", "/v1/repository/external-connection"},
		{"CopyPackageVersions", "POST", "/v1/package/versions/copy"},
		{"CreateDomain", "POST", "/v1/domain"},
		{"CreatePackageGroup", "POST", "/v1/package-group"},
		{"CreateRepository", "POST", "/v1/repository"},
		{"DeleteDomain", "DELETE", "/v1/domain"},
		{"DeleteDomainPermissionsPolicy", "DELETE", "/v1/domain/permissions/policy"},
		{"DeletePackage", "DELETE", "/v1/package"},
		{"DeletePackageGroup", "DELETE", "/v1/package-group"},
		{"DeletePackageVersions", "POST", "/v1/package/versions/delete"},
		{"DeleteRepository", "DELETE", "/v1/repository"},
		{"DeleteRepositoryPermissionsPolicy", "DELETE", "/v1/repository/permissions/policies"},
		{"DescribeDomain", "GET", "/v1/domain"},
		{"DescribePackage", "GET", "/v1/package"},
		{"DescribePackageGroup", "GET", "/v1/package-group"},
		{"DescribePackageVersion", "GET", "/v1/package/version"},
		{"DescribeRepository", "GET", "/v1/repository"},
		{"DisassociateExternalConnection", "DELETE", "/v1/repository/external-connection"},
		{"DisposePackageVersions", "POST", "/v1/package/versions/dispose"},
		{"GetAssociatedPackageGroup", "GET", "/v1/get-associated-package-group"},
		{"GetAuthorizationToken", "POST", "/v1/authorization-token"},
		{"GetDomainPermissionsPolicy", "GET", "/v1/domain/permissions/policy"},
		{"GetPackageVersionAsset", "GET", "/v1/package/version/asset"},
		{"GetPackageVersionReadme", "GET", "/v1/package/version/readme"},
		{"GetRepositoryEndpoint", "GET", "/v1/repository/endpoint"},
		{"GetRepositoryPermissionsPolicy", "GET", "/v1/repository/permissions/policy"},
		{"ListAllowedRepositoriesForGroup", "GET", "/v1/package-group-allowed-repositories"},
		{"ListAssociatedPackages", "GET", "/v1/list-associated-packages"},
		{"ListDomains", "POST", "/v1/domains"},
		{"ListPackageGroups", "POST", "/v1/package-groups"},
		{"ListPackageVersionAssets", "POST", "/v1/package/version/assets"},
		{"ListPackageVersionDependencies", "POST", "/v1/package/version/dependencies"},
		{"ListPackageVersions", "POST", "/v1/package/versions"},
		{"ListPackages", "POST", "/v1/packages"},
		{"ListRepositories", "POST", "/v1/repositories"},
		{"ListRepositoriesInDomain", "POST", "/v1/domain/repositories"},
		{"ListSubPackageGroups", "POST", "/v1/package-groups/sub-groups"},
		{"ListTagsForResource", "POST", "/v1/tags"},
		{"PublishPackageVersion", "POST", "/v1/package/version/publish"},
		{"PutDomainPermissionsPolicy", "PUT", "/v1/domain/permissions/policy"},
		{"PutPackageOriginConfiguration", "POST", "/v1/package"},
		{"PutRepositoryPermissionsPolicy", "PUT", "/v1/repository/permissions/policy"},
		{"TagResource", "POST", "/v1/tag"},
		{"UntagResource", "POST", "/v1/untag"},
		{"UpdatePackageGroup", "PUT", "/v1/package-group"},
		{"UpdatePackageGroupOriginConfiguration", "PUT", "/v1/package-group-origin-configuration"},
		{"UpdatePackageVersionsStatus", "POST", "/v1/package/versions/update_status"},
		{"UpdateRepository", "PUT", "/v1/repository"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real CodeArtifact op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 3: re-extracted all 48 codeartifact ops from the pinned SDK and confirmed
// the existing route table already correct, including the real AWS
// DeleteRepositoryPermissionsPolicy singular/plural quirk (already
// deliberately handled with a doc comment before this pass) and every
// same-path/different-method collision (/v1/domain, /v1/repository,
// /v1/package all serve three methods each).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)
		})
	}
}
