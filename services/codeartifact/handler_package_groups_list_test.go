package codeartifact_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

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
			path:       "/v1/package-group-allowed-repositories?domain=larg-domain&package-group=/npm/*",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group-allowed-repositories?package-group=/npm/*",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/package-group-allowed-repositories?domain=larg-domain",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/package-group-allowed-repositories?domain=nope&package-group=/npm/*",
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
			path:       "/v1/list-associated-packages?domain=lap-domain&package-group=/npm/*",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/list-associated-packages?package-group=/npm/*",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/list-associated-packages?domain=lap-domain",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/list-associated-packages?domain=nope&package-group=/npm/*",
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
			path:       "/v1/package-group-origin-configuration?domain=upgoc-domain&package-group=/npm/*",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group-origin-configuration?package-group=/npm/*",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/package-group-origin-configuration?domain=upgoc-domain",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "package_group_not_found",
			path:       "/v1/package-group-origin-configuration?domain=upgoc-domain&package-group=/missing/*",
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

func TestListPackageGroups_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pgpag-domain", nil)

	for i := range 5 {
		doRequest(t, h, http.MethodPost, "/v1/package-group?domain=pgpag-domain",
			map[string]any{"pattern": fmt.Sprintf("/ns/pkg-%02d/*", i)},
		)
	}

	rec1 := doRequest(t, h, http.MethodGet,
		"/v1/package-groups?domain=pgpag-domain&max-results=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["packageGroups"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodGet,
		"/v1/package-groups?domain=pgpag-domain&max-results=2&next-token="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["packageGroups"].([]any)
	assert.Len(t, page2, 2)
}
