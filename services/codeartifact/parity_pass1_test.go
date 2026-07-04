package codeartifact_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListDomains_Pagination verifies maxResults/nextToken pagination on ListDomains.
func TestParity_ListDomains_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/domain?domain=dom-%02d", i), nil)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doRequest(t, h, http.MethodGet, "/v1/domains?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["domains"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodGet, "/v1/domains?maxResults=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["domains"].([]any)
	assert.Len(t, page2, 2)
}

// TestParity_ListRepositoriesInDomain_Pagination verifies pagination on ListRepositoriesInDomain.
func TestParity_ListRepositoriesInDomain_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pag-domain", nil)

	for i := range 5 {
		doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/repository?domain=pag-domain&repository=repo-%02d", i), nil)
	}

	rec1 := doRequest(t, h, http.MethodGet, "/v1/domain/repositories?domain=pag-domain&maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["repositories"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodGet,
		"/v1/domain/repositories?domain=pag-domain&maxResults=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["repositories"].([]any)
	assert.Len(t, page2, 2)
}

// TestParity_ListPackages_Pagination verifies pagination on ListPackages.
func TestParity_ListPackages_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pkgpag-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=pkgpag-domain&repository=pkgpag-repo", nil)

	for i := range 5 {
		path := fmt.Sprintf(
			"/v1/package/version?domain=pkgpag-domain&repository=pkgpag-repo&format=npm&package=pkg-%02d&version=1.0.0",
			i,
		)
		doRequest(t, h, http.MethodGet, path, nil)
	}

	rec1 := doRequest(t, h, http.MethodGet,
		"/v1/packages?domain=pkgpag-domain&repository=pkgpag-repo&maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["packages"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodGet,
		"/v1/packages?domain=pkgpag-domain&repository=pkgpag-repo&maxResults=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["packages"].([]any)
	assert.Len(t, page2, 2)
}

// TestParity_ListPackageVersions_Pagination verifies pagination on ListPackageVersions.
func TestParity_ListPackageVersions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pvpag-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=pvpag-domain&repository=pvpag-repo", nil)

	for i := range 5 {
		path := fmt.Sprintf(
			"/v1/package/version?domain=pvpag-domain&repository=pvpag-repo&format=npm&package=mypkg&version=1.%d.0",
			i,
		)
		doRequest(t, h, http.MethodGet, path, nil)
	}

	rec1 := doRequest(t, h, http.MethodGet,
		"/v1/package/versions?domain=pvpag-domain&repository=pvpag-repo&format=npm&package=mypkg&maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["versions"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodGet,
		"/v1/package/versions?domain=pvpag-domain&repository=pvpag-repo&format=npm&package=mypkg"+
			"&maxResults=2&nextToken="+nextToken,
		nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["versions"].([]any)
	assert.Len(t, page2, 2)
}

// TestParity_PackageVersion_HasPackageName verifies DescribePackageVersion returns packageName.
func TestParity_PackageVersion_HasPackageName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pkgName string
	}{
		{name: "npm_package", pkgName: "my-npm-pkg"},
		{name: "maven_package", pkgName: "my-maven-pkg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/v1/domain?domain=pn-domain", nil)
			doRequest(t, h, http.MethodPost, "/v1/repository?domain=pn-domain&repository=pn-repo", nil)

			path := fmt.Sprintf(
				"/v1/package/version?domain=pn-domain&repository=pn-repo&format=npm&package=%s&version=1.0.0",
				tt.pkgName,
			)
			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				PackageVersion struct {
					PackageName string `json:"packageName"`
					Version     string `json:"version"`
				} `json:"packageVersion"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.pkgName, out.PackageVersion.PackageName)
			assert.Equal(t, "1.0.0", out.PackageVersion.Version)
		})
	}
}

// TestParity_CreateRepository_UpstreamRepositories verifies upstream repos are persisted
// and returned in DescribeRepository.
func TestParity_CreateRepository_UpstreamRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=up-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=up-domain&repository=upstream-repo", nil)

	rec := doRequest(t, h, http.MethodPost, "/v1/repository?domain=up-domain&repository=my-repo",
		map[string]any{
			"upstreamRepositories": []map[string]any{
				{"repositoryName": "upstream-repo"},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	repo, _ := out["repository"].(map[string]any)
	upstreams, _ := repo["upstreamRepositories"].([]any)
	require.Len(t, upstreams, 1)
	entry, _ := upstreams[0].(map[string]any)
	assert.Equal(t, "upstream-repo", entry["repositoryName"])
}

// TestParity_UpdateRepository_UpstreamRepositories verifies UpdateRepository accepts
// and persists upstreamRepositories changes.
func TestParity_UpdateRepository_UpstreamRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=upd-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=upd-domain&repository=upstream-repo", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=upd-domain&repository=my-repo", nil)

	rec := doRequest(t, h, http.MethodPut, "/v1/repository?domain=upd-domain&repository=my-repo",
		map[string]any{
			"upstreamRepositories": []map[string]any{
				{"repositoryName": "upstream-repo"},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	repo, _ := out["repository"].(map[string]any)
	upstreams, _ := repo["upstreamRepositories"].([]any)
	require.Len(t, upstreams, 1)
	entry, _ := upstreams[0].(map[string]any)
	assert.Equal(t, "upstream-repo", entry["repositoryName"])
}

// TestParity_ListPackageGroups_Pagination verifies pagination on ListPackageGroups.
func TestParity_ListPackageGroups_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pgpag-domain", nil)

	for i := range 5 {
		doRequest(t, h, http.MethodPost, "/v1/package-group?domain=pgpag-domain",
			map[string]any{"pattern": fmt.Sprintf("/ns/pkg-%02d/*", i)},
		)
	}

	rec1 := doRequest(t, h, http.MethodGet,
		"/v1/package-groups?domain=pgpag-domain&maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["packageGroups"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodGet,
		"/v1/package-groups?domain=pgpag-domain&maxResults=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["packageGroups"].([]any)
	assert.Len(t, page2, 2)
}
