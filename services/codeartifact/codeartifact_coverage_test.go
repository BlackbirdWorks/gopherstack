package codeartifact_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

// ---- Provider tests ----

func TestCAProvider_Name(t *testing.T) {
	t.Parallel()

	p := &codeartifact.Provider{}
	assert.Equal(t, "CodeArtifact", p.Name())
}

func TestCAProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codeartifact.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
}

func TestCAProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &codeartifact.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

// setupDomain creates a domain and returns its name.
func setupDomain(t *testing.T, h *codeartifact.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/v1/domain?domain="+name, nil)
	require.Equal(t, http.StatusOK, rec.Code)
}

// setupRepo creates a repository in a domain and returns its name.
func setupRepo(t *testing.T, h *codeartifact.Handler, domain, repo string) {
	t.Helper()

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/repository?domain="+domain+"&repository="+repo,
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)
}

// ---- DisassociateExternalConnection tests ----

func TestHandler_DisassociateExternalConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success_removes_connection",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "dis-domain")
				setupRepo(t, h, "dis-domain", "dis-repo")
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/repository/external-connection"+
						"?domain=dis-domain&repository=dis-repo&externalConnection=public:npmjs",
					nil,
				)
			},
			path: "/v1/repository/external-connection" +
				"?domain=dis-domain&repository=dis-repo&externalConnection=public:npmjs",
			wantStatus: http.StatusOK,
		},
		{
			name: "success_nonexistent_connection",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "dis2-domain")
				setupRepo(t, h, "dis2-domain", "dis2-repo")
			},
			path: "/v1/repository/external-connection" +
				"?domain=dis2-domain&repository=dis2-repo&externalConnection=public:npmjs",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/repository/external-connection?repository=dis-repo&externalConnection=public:npmjs",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/repository/external-connection?domain=dis-domain&externalConnection=public:npmjs",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_connection",
			path:       "/v1/repository/external-connection?domain=dis-domain&repository=dis-repo",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "repo_not_found",
			path: "/v1/repository/external-connection" +
				"?domain=dis-domain&repository=nope&externalConnection=public:npmjs",
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

			rec := doRequest(t, h, http.MethodDelete, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				repo, _ := resp["repository"].(map[string]any)
				assert.NotNil(t, repo)
			}
		})
	}
}

// ---- DisposePackageVersions tests ----

func TestHandler_DisposePackageVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success_existing_version",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "dp-domain")
				setupRepo(t, h, "dp-domain", "dp-repo")
				doRequest(
					t, h, http.MethodGet,
					"/v1/package/version"+
						"?domain=dp-domain&repository=dp-repo&format=npm&package=lodash&version=1.0.0",
					nil,
				)
			},
			path:       "/v1/package/versions/dispose?domain=dp-domain&repository=dp-repo&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "success_nonexistent_version",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "dp2-domain")
				setupRepo(t, h, "dp2-domain", "dp2-repo")
			},
			path:       "/v1/package/versions/dispose?domain=dp2-domain&repository=dp2-repo&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"9.9.9"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/versions/dispose?repository=dp-repo&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/versions/dispose?domain=dp-domain&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/versions/dispose?domain=dp-domain&repository=dp-repo&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/versions/dispose?domain=dp-domain&repository=dp-repo&format=npm",
			body:       map[string]any{"versions": []string{"1.0.0"}},
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

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["successfulVersions"])
				assert.NotNil(t, resp["failedVersions"])
			}
		})
	}
}

// ---- GetAssociatedPackageGroup tests (backend-level) ----

func TestBackend_GetAssociatedPackageGroup(t *testing.T) {
	t.Parallel()

	b := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateDomain(context.Background(), "apg-domain", "", nil)
	require.NoError(t, err)

	pg, err := b.GetAssociatedPackageGroup(context.Background(), "apg-domain", "npm", "", "lodash")
	require.NoError(t, err)
	assert.Nil(t, pg)

	_, err = b.GetAssociatedPackageGroup(context.Background(), "nonexistent", "npm", "", "lodash")
	require.Error(t, err)
}

// ---- GetPackageVersionAsset tests ----

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
				doRequest(
					t, h, http.MethodGet,
					"/v1/package/version"+
						"?domain=pva-domain&repository=pva-repo&format=npm&package=lodash&version=1.0.0",
					nil,
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
		})
	}
}

// ---- GetPackageVersionReadme tests ----

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

// ---- ListAllowedRepositoriesForGroup tests ----

func TestHandler_ListAllowedRepositoriesForGroup(t *testing.T) {
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
				setupDomain(t, h, "larg-domain")
			},
			path:       "/v1/package-group-allowed-repositories?domain=larg-domain&packageGroup=/npm/*",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group-allowed-repositories?packageGroup=/npm/*",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/package-group-allowed-repositories?domain=larg-domain",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/package-group-allowed-repositories?domain=nope&packageGroup=/npm/*",
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
				assert.NotNil(t, resp["allowedRepositories"])
			}
		})
	}
}

// ---- ListAssociatedPackages tests ----

func TestHandler_ListAssociatedPackages(t *testing.T) {
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
				setupDomain(t, h, "lap-domain")
				doRequest(
					t, h, http.MethodPost, "/v1/package-group?domain=lap-domain",
					map[string]any{"pattern": "/npm/*"},
				)
			},
			path:       "/v1/package-group-associated-packages?domain=lap-domain&packageGroup=/npm/*",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group-associated-packages?packageGroup=/npm/*",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/package-group-associated-packages?domain=lap-domain",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/package-group-associated-packages?domain=nope&packageGroup=/npm/*",
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
				pkgs, _ := resp["packages"].([]any)
				assert.NotNil(t, pkgs)
			}
		})
	}
}

// ---- ListPackageGroups tests ----

func TestHandler_ListPackageGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success_empty",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "lpg-domain")
			},
			path:       "/v1/package-groups?domain=lpg-domain",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "success_two_groups",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "lpg2-domain")
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package-group?domain=lpg2-domain",
					map[string]any{"pattern": "/npm/*"},
				)
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package-group?domain=lpg2-domain",
					map[string]any{"pattern": "/pypi/*"},
				)
			},
			path:       "/v1/package-groups?domain=lpg2-domain",
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "success_prefix_filter",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "lpg3-domain")
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package-group?domain=lpg3-domain",
					map[string]any{"pattern": "/npm/*"},
				)
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package-group?domain=lpg3-domain",
					map[string]any{"pattern": "/pypi/*"},
				)
			},
			path:       "/v1/package-groups?domain=lpg3-domain&prefix=/npm",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-groups",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/package-groups?domain=nope",
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
				groups, _ := resp["packageGroups"].([]any)
				assert.Len(t, groups, tt.wantCount)
			}
		})
	}
}

// ---- ListPackageVersionAssets tests ----

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

// ---- ListPackageVersionDependencies tests ----

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

// ---- ListPackageVersions tests ----

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

// ---- ListPackages tests ----

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
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package/versions/publish?domain=lp-domain&repository=lp-repo&format=npm&package=react&version=18.0.0",
					nil,
				)
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package/versions/publish?domain=lp-domain&repository=lp-repo&format=npm&package=lodash&version=4.0.0",
					nil,
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
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package/versions/publish?domain=lp3-domain&repository=lp3-repo&format=npm&package=react&version=18.0.0",
					nil,
				)
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package/versions/publish?domain=lp3-domain&repository=lp3-repo&format=pypi&package=boto3&version=1.0.0",
					nil,
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

// ---- ListSubPackageGroups tests ----

func TestBackend_ListSubPackageGroups(t *testing.T) {
	t.Parallel()

	b := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateDomain(context.Background(), "lspg-domain", "", nil)
	require.NoError(t, err)

	groups, err := b.ListSubPackageGroups(context.Background(), "lspg-domain", "/")
	require.NoError(t, err)
	assert.NotNil(t, groups)

	_, err = b.ListSubPackageGroups(context.Background(), "nonexistent", "/")
	require.Error(t, err)
}

// ---- PublishPackageVersion tests ----

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
				"?domain=ppv-domain&repository=ppv-repo&format=generic&package=mylib&version=1.0.0",
			wantStatus: http.StatusOK,
		},
		{
			name: "success_idempotent",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "ppv2-domain")
				setupRepo(t, h, "ppv2-domain", "ppv2-repo")
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package/versions/publish?domain=ppv2-domain&repository=ppv2-repo&format=npm&package=mylib&version=2.0.0",
					nil,
				)
			},
			path: "/v1/package/versions/publish" +
				"?domain=ppv2-domain&repository=ppv2-repo&format=npm&package=mylib&version=2.0.0",
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
				assert.NotEmpty(t, resp["version"])
				assert.NotEmpty(t, resp["status"])
			}
		})
	}
}

// ---- PutPackageOriginConfiguration tests ----

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
			path:       "/v1/package/origin-configuration?domain=ppoc-domain&repository=ppoc-repo&format=npm&package=lodash",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/origin-configuration?repository=ppoc-repo&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/origin-configuration?domain=ppoc-domain&format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/origin-configuration?domain=ppoc-domain&repository=ppoc-repo&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/origin-configuration?domain=ppoc-domain&repository=ppoc-repo&format=npm",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "repo_not_found",
			path:       "/v1/package/origin-configuration?domain=ppoc-domain&repository=nope&format=npm&package=lodash",
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

			rec := doRequest(
				t, h, http.MethodPut, tt.path,
				map[string]any{"restrictions": map[string]any{"publish": "ALLOW", "upstream": "ALLOW"}},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				pkg, _ := resp["package"].(map[string]any)
				assert.NotNil(t, pkg)
			}
		})
	}
}

// ---- UpdatePackageGroup tests ----

func TestHandler_UpdatePackageGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success_update_description",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upg-domain")
				doRequest(
					t, h, http.MethodPost, "/v1/package-group?domain=upg-domain",
					map[string]any{"pattern": "/npm/*"},
				)
			},
			path:       "/v1/package-group?domain=upg-domain&packageGroup=/npm/*",
			body:       map[string]any{"description": "updated description"},
			wantStatus: http.StatusOK,
		},
		{
			name: "success_update_contact_info",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upg2-domain")
				doRequest(
					t, h, http.MethodPost, "/v1/package-group?domain=upg2-domain",
					map[string]any{"pattern": "/pypi/*"},
				)
			},
			path:       "/v1/package-group?domain=upg2-domain&packageGroup=/pypi/*",
			body:       map[string]any{"contactInfo": "team@example.com"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group",
			body:       map[string]any{"packageGroup": "/test/"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "package_group_not_found",
			path:       "/v1/package-group?domain=upg-domain&packageGroup=/missing/*",
			body:       map[string]any{"description": "test"},
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

			rec := doRequest(t, h, http.MethodPut, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				pg, _ := resp["packageGroup"].(map[string]any)
				assert.NotNil(t, pg)
			}
		})
	}
}

// ---- UpdatePackageGroupOriginConfiguration tests ----

func TestHandler_UpdatePackageGroupOriginConfiguration(t *testing.T) {
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
				setupDomain(t, h, "upgoc-domain")
				doRequest(
					t, h, http.MethodPost, "/v1/package-group?domain=upgoc-domain",
					map[string]any{"pattern": "/npm/*"},
				)
			},
			path:       "/v1/package-group-origin-configuration?domain=upgoc-domain&packageGroup=/npm/*",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group-origin-configuration?packageGroup=/npm/*",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/package-group-origin-configuration?domain=upgoc-domain",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "package_group_not_found",
			path:       "/v1/package-group-origin-configuration?domain=upgoc-domain&packageGroup=/missing/*",
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

			rec := doRequest(
				t, h, http.MethodPut, tt.path,
				map[string]any{"restrictions": map[string]any{"publish": map[string]any{"restrictionMode": "ALLOW"}}},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				pg, _ := resp["packageGroup"].(map[string]any)
				assert.NotNil(t, pg)
			}
		})
	}
}

// ---- UpdatePackageVersionsStatus tests ----

func TestHandler_UpdatePackageVersionsStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success_archive",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upvs-domain")
				setupRepo(t, h, "upvs-domain", "upvs-repo")
				doRequest(
					t,
					h,
					http.MethodGet,
					"/v1/package/version?domain=upvs-domain&repository=upvs-repo&format=npm&package=lodash&version=1.0.0",
					nil,
				)
			},
			path: "/v1/package/versions/update_status" +
				"?domain=upvs-domain&repository=upvs-repo&format=npm&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "success_missing_version_returns_not_found_in_result",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upvs2-domain")
				setupRepo(t, h, "upvs2-domain", "upvs2-repo")
			},
			path: "/v1/package/versions/update_status" +
				"?domain=upvs2-domain&repository=upvs2-repo&format=npm&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"9.9.9"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/versions/update_status?repository=upvs-repo&format=npm&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/versions/update_status?domain=upvs-domain&format=npm&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/versions/update_status?domain=upvs-domain&repository=upvs-repo&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/versions/update_status?domain=upvs-domain&repository=upvs-repo&format=npm",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_target_status",
			path: "/v1/package/versions/update_status" +
				"?domain=upvs-domain&repository=upvs-repo&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
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

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["successfulVersions"])
				assert.NotNil(t, resp["failedVersions"])
			}
		})
	}
}

// ---- UpdateRepository tests ----

func TestHandler_UpdateRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success_update_description",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "ur-domain")
				setupRepo(t, h, "ur-domain", "ur-repo")
			},
			path:       "/v1/repository?domain=ur-domain&repository=ur-repo",
			body:       map[string]any{"description": "updated repo description"},
			wantStatus: http.StatusOK,
		},
		{
			name: "success_no_body",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "ur2-domain")
				setupRepo(t, h, "ur2-domain", "ur2-repo")
			},
			path:       "/v1/repository?domain=ur2-domain&repository=ur2-repo",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/repository?repository=ur-repo",
			body:       map[string]any{"description": "updated"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/repository?domain=ur-domain",
			body:       map[string]any{"description": "updated"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "repo_not_found",
			path:       "/v1/repository?domain=ur-domain&repository=nope",
			body:       map[string]any{"description": "test"},
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

			rec := doRequest(t, h, http.MethodPut, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				repo, _ := resp["repository"].(map[string]any)
				assert.NotNil(t, repo)
			}
		})
	}
}

// ---- Persistence coverage ----

func TestCABackend_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateDomain(context.Background(), "snap-domain", "", nil)
	require.NoError(t, err)

	_, err = b.CreateRepository(context.Background(), "snap-domain", "snap-repo", "", nil)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	err = b2.Restore(snap)
	require.NoError(t, err)

	doms := b2.ListDomains(context.Background())
	require.Len(t, doms, 1)
}
