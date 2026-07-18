package codeartifact_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func TestHandler_PublishPackageVersion_AppearInList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pub-list-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=pub-list-domain&repository=pub-list-repo", nil)

	doRawRequest(
		t,
		h,
		"/v1/package/versions/publish?domain=pub-list-domain"+
			"&repository=pub-list-repo&format=npm&package=react&version=18.0.0&asset=react-18.0.0.tgz",
		[]byte("asset-content-18"),
	)
	doRawRequest(
		t,
		h,
		"/v1/package/versions/publish?domain=pub-list-domain"+
			"&repository=pub-list-repo&format=npm&package=react&version=19.0.0&asset=react-19.0.0.tgz",
		[]byte("asset-content-19"),
	)

	listRec := doRequest(
		t, h, http.MethodPost,
		"/v1/package/versions?domain=pub-list-domain&repository=pub-list-repo&format=npm&package=react",
		nil,
	)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	versions, _ := resp["versions"].([]any)
	assert.Len(t, versions, 2)
}

func TestHandler_GetPackageVersionAsset(t *testing.T) {
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
				setupDomain(t, h, "pva-domain")
				setupRepo(t, h, "pva-domain", "pva-repo")
				// GetPackageVersionAsset only serves assets actually uploaded via
				// PublishPackageVersion -- DescribePackageVersion's auto-create stub
				// creates a version record but no asset content.
				doRawRequest(
					t, h,
					"/v1/package/versions/publish"+
						"?domain=pva-domain&repository=pva-repo&format=npm&package=lodash&version=1.0.0"+
						"&asset=lodash-1.0.0.tgz",
					[]byte("tarball-content"),
				)
			},
			path: "/v1/package/version/asset" +
				"?domain=pva-domain&repository=pva-repo&format=npm&package=lodash&version=1.0.0&asset=lodash-1.0.0.tgz",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/version/asset?repository=pva-repo&format=npm&package=lodash&version=1.0.0&asset=x.tgz",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/version/asset?domain=pva-domain&format=npm&package=lodash&version=1.0.0&asset=x.tgz",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_format",
			path: "/v1/package/version/asset" +
				"?domain=pva-domain&repository=pva-repo&package=lodash&version=1.0.0&asset=x.tgz",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_package",
			path: "/v1/package/version/asset" +
				"?domain=pva-domain&repository=pva-repo&format=npm&version=1.0.0&asset=x.tgz",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_version",
			path: "/v1/package/version/asset" +
				"?domain=pva-domain&repository=pva-repo&format=npm&package=lodash&asset=x.tgz",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_asset",
			path: "/v1/package/version/asset" +
				"?domain=pva-domain&repository=pva-repo&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "version_not_found",
			path: "/v1/package/version/asset" +
				"?domain=pva-domain&repository=pva-repo&format=npm&package=lodash&version=9.9.9&asset=x.tgz",
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

			if tt.name == "success" {
				assert.Equal(t, "tarball-content", rec.Body.String())
			}
		})
	}
}

func TestHandler_GetPackageVersionReadme(t *testing.T) {
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
				setupDomain(t, h, "pvr-domain")
				setupRepo(t, h, "pvr-domain", "pvr-repo")
				doRequest(
					t, h, http.MethodGet,
					"/v1/package/version?domain=pvr-domain&repository=pvr-repo&format=npm&package=lodash&version=1.0.0",
					nil,
				)
			},
			path: "/v1/package/version/readme" +
				"?domain=pvr-domain&repository=pvr-repo&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/version/readme?repository=pvr-repo&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/version/readme?domain=pvr-domain&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/version/readme?domain=pvr-domain&repository=pvr-repo&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/version/readme?domain=pvr-domain&repository=pvr-repo&format=npm&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_version",
			path:       "/v1/package/version/readme?domain=pvr-domain&repository=pvr-repo&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "version_not_found",
			path: "/v1/package/version/readme" +
				"?domain=pvr-domain&repository=pvr-repo&format=npm&package=lodash&version=9.9.9",
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
				_, ok := resp["readme"]
				assert.True(t, ok)
			}
		})
	}
}

func TestHandler_ListPackageVersionAssets(t *testing.T) {
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
				setupDomain(t, h, "lpva-domain")
				setupRepo(t, h, "lpva-domain", "lpva-repo")
				doRequest(
					t,
					h,
					http.MethodGet,
					"/v1/package/version?domain=lpva-domain&repository=lpva-repo&format=npm&package=lodash&version=1.0.0",
					nil,
				)
			},
			path: "/v1/package/version/assets" +
				"?domain=lpva-domain&repository=lpva-repo&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/version/assets?repository=lpva-repo&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/version/assets?domain=lpva-domain&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/version/assets?domain=lpva-domain&repository=lpva-repo&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/version/assets?domain=lpva-domain&repository=lpva-repo&format=npm&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_version",
			path:       "/v1/package/version/assets?domain=lpva-domain&repository=lpva-repo&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "version_not_found",
			path: "/v1/package/version/assets" +
				"?domain=lpva-domain&repository=lpva-repo&format=npm&package=lodash&version=9.9.9",
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
				assets, _ := resp["assets"].([]any)
				assert.NotNil(t, assets)
			}
		})
	}
}

func TestHandler_ListPackageVersionDependencies(t *testing.T) {
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
				setupDomain(t, h, "lpvd-domain")
				setupRepo(t, h, "lpvd-domain", "lpvd-repo")
				doRequest(
					t,
					h,
					http.MethodGet,
					"/v1/package/version?domain=lpvd-domain&repository=lpvd-repo&format=npm&package=lodash&version=1.0.0",
					nil,
				)
			},
			path: "/v1/package/version/dependencies" +
				"?domain=lpvd-domain&repository=lpvd-repo&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/version/dependencies?repository=lpvd-repo&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/version/dependencies?domain=lpvd-domain&format=npm&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_format",
			path: "/v1/package/version/dependencies" +
				"?domain=lpvd-domain&repository=lpvd-repo&package=lodash&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/version/dependencies?domain=lpvd-domain&repository=lpvd-repo&format=npm&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_version",
			path:       "/v1/package/version/dependencies?domain=lpvd-domain&repository=lpvd-repo&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "version_not_found",
			path: "/v1/package/version/dependencies" +
				"?domain=lpvd-domain&repository=lpvd-repo&format=npm&package=lodash&version=9.9.9",
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
				deps, _ := resp["dependencies"].([]any)
				assert.NotNil(t, deps)
			}
		})
	}
}

func TestHandler_ListPackageVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success_two_versions",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "lpv-domain")
				setupRepo(t, h, "lpv-domain", "lpv-repo")
				doRequest(
					t,
					h,
					http.MethodGet,
					"/v1/package/version?domain=lpv-domain&repository=lpv-repo&format=npm&package=lodash&version=4.17.0",
					nil,
				)
				doRequest(
					t,
					h,
					http.MethodGet,
					"/v1/package/version?domain=lpv-domain&repository=lpv-repo&format=npm&package=lodash&version=4.17.21",
					nil,
				)
			},
			path:       "/v1/package/versions?domain=lpv-domain&repository=lpv-repo&format=npm&package=lodash",
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "success_empty",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "lpv2-domain")
				setupRepo(t, h, "lpv2-domain", "lpv2-repo")
			},
			path:       "/v1/package/versions?domain=lpv2-domain&repository=lpv2-repo&format=npm&package=lodash",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/versions?repository=lpv-repo&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/versions?domain=lpv-domain&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/versions?domain=lpv-domain&repository=lpv-repo&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/versions?domain=lpv-domain&repository=lpv-repo&format=npm",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "repo_not_found",
			path:       "/v1/package/versions?domain=lpv-domain&repository=nope&format=npm&package=lodash",
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
				versions, _ := resp["versions"].([]any)
				assert.Len(t, versions, tt.wantCount)
			}
		})
	}
}

func TestHandler_PublishPackageVersion(t *testing.T) {
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
				setupDomain(t, h, "ppv-domain")
				setupRepo(t, h, "ppv-domain", "ppv-repo")
			},
			path: "/v1/package/versions/publish" +
				"?domain=ppv-domain&repository=ppv-repo&format=generic&package=mylib&version=1.0.0&asset=mylib-1.0.0.tgz",
			wantStatus: http.StatusOK,
		},
		{
			name: "success_idempotent",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "ppv2-domain")
				setupRepo(t, h, "ppv2-domain", "ppv2-repo")
				doRawRequest(
					t,
					h,
					"/v1/package/versions/publish"+
						"?domain=ppv2-domain&repository=ppv2-repo&format=npm&package=mylib&version=2.0.0&asset=mylib-2.0.0.tgz",
					[]byte("asset-content"),
				)
			},
			path: "/v1/package/versions/publish" +
				"?domain=ppv2-domain&repository=ppv2-repo&format=npm&package=mylib&version=2.0.0&asset=mylib-2.0.0.tgz",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/versions/publish?repository=ppv-repo&format=generic&package=mylib&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/versions/publish?domain=ppv-domain&format=generic&package=mylib&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/versions/publish?domain=ppv-domain&repository=ppv-repo&package=mylib&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/versions/publish?domain=ppv-domain&repository=ppv-repo&format=generic&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_version",
			path:       "/v1/package/versions/publish?domain=ppv-domain&repository=ppv-repo&format=generic&package=mylib",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_asset",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "ppv3-domain")
				setupRepo(t, h, "ppv3-domain", "ppv3-repo")
			},
			path: "/v1/package/versions/publish" +
				"?domain=ppv3-domain&repository=ppv3-repo&format=generic&package=mylib&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRawRequest(t, h, tt.path, []byte("asset-content"))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["version"])
				assert.NotEmpty(t, resp["status"])
				assert.NotEmpty(t, resp["asset"])
			}
		})
	}
}
