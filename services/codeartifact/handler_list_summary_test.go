package codeartifact_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_ListPackages_SummaryShape locks that ListPackages emits the
// real types.PackageSummary shape instead of reusing DescribePackage's
// converter unscoped (gopherstack-tuh5). This assertion reads the raw JSON
// response body rather than going through an AWS SDK client, since the SDK
// deserializer silently drops keys it does not recognise and cannot observe
// an over-wide response.
func TestHandler_ListPackages_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lps-domain")
	setupRepo(t, h, "lps-domain", "lps-repo")

	rec := doRawRequest(
		t, h,
		"/v1/package/version/publish?domain=lps-domain&repository=lps-repo&format=npm&package=react"+
			"&version=18.0.0&asset=react.tgz",
		[]byte("content"),
	)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/v1/packages?domain=lps-domain&repository=lps-repo", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pkgs, ok := resp["packages"].([]any)
	require.True(t, ok)
	require.Len(t, pkgs, 1)
	item, ok := pkgs[0].(map[string]any)
	require.True(t, ok)

	for _, k := range []string{"format", "package"} {
		assert.Contains(t, item, k, "expected real PackageSummary member %q", k)
	}
	assert.Equal(t, "react", item["package"], "package identifier must be emitted under key \"package\", not \"name\"")
	for _, k := range []string{"name", "domainName", "domainOwner", "repository"} {
		assert.NotContains(t, item, k, "leaked Get-only member %q", k)
	}
}

// TestCodeArtifactSDK_ListPackages_IdentifierSurvivesRealClient drives
// ListPackages through a real aws-sdk-go-v2 client and proves the package
// identifier reaches types.PackageSummary.Package (gopherstack-tuh5). A
// raw-body assertion is weak here: the real deserializer silently drops any
// unrecognised key including a wrongly-keyed "name", so only a typed SDK
// caller demonstrates the identifier is actually lost when the wire uses
// the wrong key.
func TestCodeArtifactSDK_ListPackages_IdentifierSurvivesRealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lps-sdk-domain")
	setupRepo(t, h, "lps-sdk-domain", "lps-sdk-repo")

	rec := doRawRequest(
		t, h,
		"/v1/package/version/publish?domain=lps-sdk-domain&repository=lps-sdk-repo&format=npm&package=lodash"+
			"&version=4.0.0&asset=lodash.tgz",
		[]byte("content"),
	)
	require.Equal(t, http.StatusOK, rec.Code)

	client := newTestCodeArtifactClient(t, h)

	out, err := client.ListPackages(t.Context(), &casdk.ListPackagesInput{
		Domain:     aws.String("lps-sdk-domain"),
		Repository: aws.String("lps-sdk-repo"),
	})
	require.NoError(t, err)
	require.Len(t, out.Packages, 1)
	assert.Equal(t, "lodash", aws.ToString(out.Packages[0].Package),
		"real SDK client must see the package identifier; it is lost if the wire emits \"name\" instead of \"package\"")
}

// TestHandler_ListPackageVersions_SummaryShape locks that ListPackageVersions
// emits the real types.PackageVersionSummary shape instead of reusing
// DescribePackageVersion's converter unscoped (gopherstack-tuh5). Raw-body
// assertion, for the same reason as TestHandler_ListPackages_SummaryShape.
func TestHandler_ListPackageVersions_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lpvs-domain")
	setupRepo(t, h, "lpvs-domain", "lpvs-repo")

	rec := doRawRequest(
		t, h,
		"/v1/package/version/publish?domain=lpvs-domain&repository=lpvs-repo&format=npm&package=react"+
			"&version=18.0.0&asset=react.tgz",
		[]byte("content"),
	)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(
		t, h, http.MethodPost,
		"/v1/package/versions?domain=lpvs-domain&repository=lpvs-repo&format=npm&package=react", nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions, ok := resp["versions"].([]any)
	require.True(t, ok)
	require.Len(t, versions, 1)
	item, ok := versions[0].(map[string]any)
	require.True(t, ok)

	for _, k := range []string{"version", "status", "revision"} {
		assert.Contains(t, item, k, "expected real PackageVersionSummary member %q", k)
	}
	for _, k := range []string{"format", "packageName", "publishedTime", "namespace"} {
		assert.NotContains(t, item, k, "leaked Get-only member %q", k)
	}
}
