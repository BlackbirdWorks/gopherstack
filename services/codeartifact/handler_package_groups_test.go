package codeartifact_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func TestHandler_CreatePackageGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=pg-domain", nil)
			},
			path:       "/v1/package-group?domain=pg-domain",
			body:       map[string]any{"pattern": "/*"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group",
			body:       map[string]any{"pattern": "/*"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_pattern",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=pg-domain2", nil)
			},
			path:       "/v1/package-group?domain=pg-domain2",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/package-group?domain=nonexistent",
			body:       map[string]any{"pattern": "/*"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "duplicate",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=pg-dup", nil)
				doRequest(t, h, http.MethodPost, "/v1/package-group?domain=pg-dup", map[string]any{"pattern": "/npm/*"})
			},
			path:       "/v1/package-group?domain=pg-dup",
			body:       map[string]any{"pattern": "/npm/*"},
			wantStatus: http.StatusConflict,
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
				pg, _ := resp["packageGroup"].(map[string]any)
				assert.NotEmpty(t, pg["arn"])
				assert.Equal(t, "pg-domain", pg["domainName"])
				assert.Equal(t, "/*", pg["pattern"])
			}
		})
	}
}

func TestHandler_PackageGroupCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=crud-domain", nil)

	// Create
	createRec := doRequest(t, h, http.MethodPost, "/v1/package-group?domain=crud-domain", map[string]any{
		"pattern":     "/npm/mygroup/*",
		"description": "test group",
		"tags":        []map[string]any{{"key": "env", "value": "test"}},
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	pg, _ := createResp["packageGroup"].(map[string]any)
	assert.Equal(t, "/npm/mygroup/*", pg["pattern"])
	assert.Equal(t, "test group", pg["description"])

	// Describe
	descRec := doRequest(t, h, http.MethodGet, "/v1/package-group?domain=crud-domain&package-group=/npm/mygroup/*", nil)
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Delete
	delRec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v1/package-group?domain=crud-domain&package-group=/npm/mygroup/*",
		nil,
	)
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Verify gone
	descRec2 := doRequest(
		t, h, http.MethodGet, "/v1/package-group?domain=crud-domain&package-group=/npm/mygroup/*", nil,
	)
	assert.Equal(t, http.StatusNotFound, descRec2.Code)
}

func TestHandler_GetAssociatedPackageGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success_no_match",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=apg-domain", nil)
			},
			path:       "/v1/get-associated-package-group?domain=apg-domain&format=npm&package=mypkg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/get-associated-package-group?format=npm&package=mypkg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/get-associated-package-group?domain=apg-domain&package=mypkg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/get-associated-package-group?domain=apg-domain&format=npm",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/get-associated-package-group?domain=nope&format=npm&package=mypkg",
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

func TestHandler_ListSubPackageGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success_with_subgroups",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=lspg-domain", nil)
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package-group?domain=lspg-domain",
					map[string]any{"pattern": "/npm/*"},
				)
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package-group?domain=lspg-domain",
					map[string]any{"pattern": "/npm/react/*"},
				)
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package-group?domain=lspg-domain",
					map[string]any{"pattern": "/pypi/*"},
				)
			},
			path:       "/v1/package-groups/sub-groups?domain=lspg-domain&package-group=/npm/*",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "success_no_subgroups",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=lspg2-domain", nil)
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v1/package-group?domain=lspg2-domain",
					map[string]any{"pattern": "/npm/*"},
				)
			},
			path:       "/v1/package-groups/sub-groups?domain=lspg2-domain&package-group=/npm/*",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-groups/sub-groups?package-group=/npm/*",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/package-groups/sub-groups?domain=lspg-domain",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/package-groups/sub-groups?domain=nope&package-group=/npm/*",
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
				groups, _ := resp["packageGroups"].([]any)
				assert.Len(t, groups, tt.wantCount)
			}
		})
	}
}

func TestHandler_PackageGroupTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pgt-domain", nil)

	createRec := doRequest(t, h, http.MethodPost, "/v1/package-group?domain=pgt-domain", map[string]any{
		"pattern": "/npm/*",
		"tags":    []map[string]any{{"key": "env", "value": "prod"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	pg, _ := createResp["packageGroup"].(map[string]any)
	pgARN, _ := pg["arn"].(string)
	require.NotEmpty(t, pgARN)

	listRec := doRequest(t, h, http.MethodPost, "/v1/tags?resourceArn="+pgARN, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tagList, _ := listResp["tags"].([]any)
	assert.Len(t, tagList, 1)
}

func TestHandler_PackageGroupHierarchy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "hier-domain")

	// Create parent and child package groups.
	doRequest(t, h, http.MethodPost, "/v1/package-group?domain=hier-domain", map[string]any{"pattern": "/npm/*"})
	doRequest(t, h, http.MethodPost, "/v1/package-group?domain=hier-domain", map[string]any{"pattern": "/npm/react/*"})
	doRequest(t, h, http.MethodPost, "/v1/package-group?domain=hier-domain", map[string]any{"pattern": "/npm/lodash/*"})
	doRequest(t, h, http.MethodPost, "/v1/package-group?domain=hier-domain", map[string]any{"pattern": "/pypi/*"})

	// ListPackageGroups should return all 4.
	allRec := doRequest(t, h, http.MethodPost, "/v1/package-groups?domain=hier-domain", nil)
	require.Equal(t, http.StatusOK, allRec.Code)
	var allResp map[string]any
	require.NoError(t, json.Unmarshal(allRec.Body.Bytes(), &allResp))
	allGroups, _ := allResp["packageGroups"].([]any)
	assert.Len(t, allGroups, 4)

	// ListSubPackageGroups for /npm/* should return 2 sub-groups.
	subRec := doRequest(
		t, h, http.MethodGet, "/v1/package-groups/sub-groups?domain=hier-domain&package-group=/npm/*", nil,
	)
	require.Equal(t, http.StatusOK, subRec.Code)
	var subResp map[string]any
	require.NoError(t, json.Unmarshal(subRec.Body.Bytes(), &subResp))
	subGroups, _ := subResp["packageGroups"].([]any)
	assert.Len(t, subGroups, 2)

	// ListSubPackageGroups for /pypi/* should return 0 sub-groups.
	subRec2 := doRequest(
		t, h, http.MethodGet, "/v1/package-groups/sub-groups?domain=hier-domain&package-group=/pypi/*", nil,
	)
	require.Equal(t, http.StatusOK, subRec2.Code)
	var subResp2 map[string]any
	require.NoError(t, json.Unmarshal(subRec2.Body.Bytes(), &subResp2))
	subGroups2, _ := subResp2["packageGroups"].([]any)
	assert.Empty(t, subGroups2)
}

func TestHandler_AssociatedPackageGroup_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "missing_domain",
			path:       "/v1/get-associated-package-group?format=npm&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/get-associated-package-group?domain=d&package=lodash",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/get-associated-package-group?domain=d&format=npm",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/get-associated-package-group?domain=nope&format=npm&package=lodash",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

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
