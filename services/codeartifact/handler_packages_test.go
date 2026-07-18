package codeartifact_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func TestHandler_DescribePackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=pkg-domain", nil)
				doRequest(t, h, http.MethodPost, "/v1/repository?domain=pkg-domain&repository=pkg-repo", nil)
			},
			path:       "/v1/package?domain=pkg-domain&repository=pkg-repo&format=npm&package=my-pkg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package?repository=pkg-repo&format=npm&package=my-pkg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package?domain=pkg-domain&format=npm&package=my-pkg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package?domain=pkg-domain&repository=pkg-repo&package=my-pkg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package?domain=pkg-domain&repository=pkg-repo&format=npm",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "repo_not_found",
			path:       "/v1/package?domain=pkg-domain&repository=nope&format=npm&package=my-pkg",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				pkg, _ := resp["package"].(map[string]any)
				assert.Equal(t, "npm", pkg["format"])
				assert.Equal(t, "my-pkg", pkg["name"])
			}
		})
	}
}

func TestHandler_DeletePackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=del-pkg-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=del-pkg-domain&repository=del-pkg-repo", nil)

	// Seed the package via DescribePackage (auto-creates stub).
	seedRec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/package?domain=del-pkg-domain&repository=del-pkg-repo&format=npm&package=lodash",
		nil,
	)
	assert.Equal(t, http.StatusOK, seedRec.Code)

	// Delete it.
	delRec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v1/package?domain=del-pkg-domain&repository=del-pkg-repo&format=npm&package=lodash",
		nil,
	)
	assert.Equal(t, http.StatusOK, delRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["deletedPackage"])

	// Confirm deletion: a second delete should return 404 since the package no longer exists.
	delRec2 := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v1/package?domain=del-pkg-domain&repository=del-pkg-repo&format=npm&package=lodash",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

func TestHandler_PackageMap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pkgmap-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=pkgmap-domain&repository=pkgmap-repo", nil)

	rec := doRequest(
		t, h, http.MethodGet,
		"/v1/package?domain=pkgmap-domain&repository=pkgmap-repo&format=npm&package=mypkg",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pkg, _ := resp["package"].(map[string]any)
	assert.Equal(t, "pkgmap-repo", pkg["repository"])
}

func TestHandler_MultiFormatPackages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "fmt-multi-domain")
	setupRepo(t, h, "fmt-multi-domain", "fmt-multi-repo")

	// Publish packages in different formats.
	for _, tc := range []struct {
		format, pkg, version string
	}{
		{"npm", "react", "18.0.0"},
		{"npm", "lodash", "4.17.21"},
		{"pypi", "boto3", "1.28.0"},
		{"maven", "spring-boot", "3.0.0"},
	} {
		doRawRequest(
			t, h,
			"/v1/package/versions/publish?domain=fmt-multi-domain&repository=fmt-multi-repo&format="+
				tc.format+"&package="+tc.pkg+"&version="+tc.version+"&asset="+tc.pkg+"-"+tc.version,
			[]byte("content"),
		)
	}

	// List all packages.
	allRec := doRequest(t, h, http.MethodPost, "/v1/packages?domain=fmt-multi-domain&repository=fmt-multi-repo", nil)
	require.Equal(t, http.StatusOK, allRec.Code)
	var allResp map[string]any
	require.NoError(t, json.Unmarshal(allRec.Body.Bytes(), &allResp))
	allPkgs, _ := allResp["packages"].([]any)
	assert.Len(t, allPkgs, 4)

	// Filter by npm format.
	npmRec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/packages?domain=fmt-multi-domain&repository=fmt-multi-repo&format=npm",
		nil,
	)
	require.Equal(t, http.StatusOK, npmRec.Code)
	var npmResp map[string]any
	require.NoError(t, json.Unmarshal(npmRec.Body.Bytes(), &npmResp))
	npmPkgs, _ := npmResp["packages"].([]any)
	assert.Len(t, npmPkgs, 2)

	// Filter by pypi format.
	pypiRec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/packages?domain=fmt-multi-domain&repository=fmt-multi-repo&format=pypi",
		nil,
	)
	require.Equal(t, http.StatusOK, pypiRec.Code)
	var pypiResp map[string]any
	require.NoError(t, json.Unmarshal(pypiRec.Body.Bytes(), &pypiResp))
	pypiPkgs, _ := pypiResp["packages"].([]any)
	assert.Len(t, pypiPkgs, 1)
}

func TestHandler_ListPackages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success_two_packages",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "lp-domain")
				setupRepo(t, h, "lp-domain", "lp-repo")
				doRawRequest(
					t,
					h,
					"/v1/package/versions/publish?domain=lp-domain&repository=lp-repo&format=npm&package=react"+
						"&version=18.0.0&asset=react.tgz",
					[]byte("content"),
				)
				doRawRequest(
					t,
					h,
					"/v1/package/versions/publish?domain=lp-domain&repository=lp-repo&format=npm&package=lodash"+
						"&version=4.0.0&asset=lodash.tgz",
					[]byte("content"),
				)
			},
			path:       "/v1/packages?domain=lp-domain&repository=lp-repo",
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "success_empty",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "lp2-domain")
				setupRepo(t, h, "lp2-domain", "lp2-repo")
			},
			path:       "/v1/packages?domain=lp2-domain&repository=lp2-repo",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "success_format_filter",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "lp3-domain")
				setupRepo(t, h, "lp3-domain", "lp3-repo")
				doRawRequest(
					t,
					h,
					"/v1/package/versions/publish?domain=lp3-domain&repository=lp3-repo&format=npm&package=react"+
						"&version=18.0.0&asset=react.tgz",
					[]byte("content"),
				)
				doRawRequest(
					t,
					h,
					"/v1/package/versions/publish?domain=lp3-domain&repository=lp3-repo&format=pypi&package=boto3"+
						"&version=1.0.0&asset=boto3.tar.gz",
					[]byte("content"),
				)
			},
			path:       "/v1/packages?domain=lp3-domain&repository=lp3-repo&format=npm",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "missing_domain",
			path:       "/v1/packages?repository=lp-repo",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/packages?domain=lp-domain",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "repo_not_found",
			path:       "/v1/packages?domain=lp-domain&repository=nope",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPost, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				pkgs, _ := resp["packages"].([]any)
				assert.Len(t, pkgs, tt.wantCount)
			}
		})
	}
}

func TestHandler_PutPackageOriginConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "ppoc-domain")
				setupRepo(t, h, "ppoc-domain", "ppoc-repo")
			},
			path:       "/v1/package?domain=ppoc-domain&repository=ppoc-repo&format=npm&package=lodash",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package?repository=ppoc-repo&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package?domain=ppoc-domain&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package?domain=ppoc-domain&repository=ppoc-repo&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package?domain=ppoc-domain&repository=ppoc-repo&format=npm",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "repo_not_found",
			path:       "/v1/package?domain=ppoc-domain&repository=nope&format=npm&package=lodash",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			// PutPackageOriginConfiguration has no path of its own in the real API: it
			// is POST on the shared "/v1/package" path (see parsePackageRoute).
			rec := doRequest(
				t, h, http.MethodPost, tt.path,
				map[string]any{"restrictions": map[string]any{"publish": "ALLOW", "upstream": "ALLOW"}},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				originConfig, _ := resp["originConfiguration"].(map[string]any)
				require.NotNil(t, originConfig)
				restrictions, _ := originConfig["restrictions"].(map[string]any)
				assert.Equal(t, "ALLOW", restrictions["publish"])
				assert.Equal(t, "ALLOW", restrictions["upstream"])
			}
		})
	}
}

func TestListPackages_Pagination(t *testing.T) {
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
		"/v1/packages?domain=pkgpag-domain&repository=pkgpag-repo&max-results=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["packages"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodGet,
		"/v1/packages?domain=pkgpag-domain&repository=pkgpag-repo&max-results=2&next-token="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["packages"].([]any)
	assert.Len(t, page2, 2)
}

// TestListPackageVersions_Pagination verifies pagination on ListPackageVersions.
