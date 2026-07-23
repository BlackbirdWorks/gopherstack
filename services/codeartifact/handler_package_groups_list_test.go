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

	const restrictionSuffix = "&originRestrictionType=PUBLISH"

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
				doRequest(
					t, h, http.MethodPost, "/v1/package-group?domain=larg-domain",
					map[string]any{"pattern": "/npm/*"},
				)
			},
			path:       "/v1/package-group-allowed-repositories?domain=larg-domain&package-group=/npm/*" + restrictionSuffix,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group-allowed-repositories?package-group=/npm/*" + restrictionSuffix,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/package-group-allowed-repositories?domain=larg-domain" + restrictionSuffix,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_restriction_type",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "larg2-domain")
			},
			path:       "/v1/package-group-allowed-repositories?domain=larg2-domain&package-group=/npm/*",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_restriction_type",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "larg3-domain")
			},
			path: "/v1/package-group-allowed-repositories?domain=larg3-domain&package-group=/npm/*" +
				"&originRestrictionType=BOGUS",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			path:       "/v1/package-group-allowed-repositories?domain=nope&package-group=/npm/*" + restrictionSuffix,
			wantStatus: http.StatusNotFound,
		},
		{
			name: "package_group_not_found",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "larg4-domain")
			},
			path: "/v1/package-group-allowed-repositories?domain=larg4-domain&package-group=/npm/*" +
				restrictionSuffix,
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

// TestHandler_ListAssociatedPackages_MostSpecificMatch verifies packages are attributed to
// their most-specific matching group, not every group whose pattern happens to match.
func TestHandler_ListAssociatedPackages_MostSpecificMatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lapms-domain")
	setupRepo(t, h, "lapms-domain", "lapms-repo")

	doRequest(t, h, http.MethodPost, "/v1/package-group?domain=lapms-domain", map[string]any{"pattern": "/npm/*"})
	doRequest(
		t, h, http.MethodPost, "/v1/package-group?domain=lapms-domain", map[string]any{"pattern": "/npm/space/*"},
	)

	// react has no namespace, so it only matches the broader "/npm/*" group.
	doRawRequest(
		t, h,
		"/v1/package/versions/publish?domain=lapms-domain&repository=lapms-repo&format=npm&package=react"+
			"&version=18.0.0&asset=react.tgz",
		[]byte("content"),
	)
	// utils is namespaced under "space", so it matches the more specific "/npm/space/*" group.
	doRawRequest(
		t, h,
		"/v1/package/versions/publish?domain=lapms-domain&repository=lapms-repo&format=npm&namespace=space"+
			"&package=utils&version=1.0.0&asset=utils.tgz",
		[]byte("content"),
	)

	broadRec := doRequest(
		t, h, http.MethodGet, "/v1/list-associated-packages?domain=lapms-domain&package-group=/npm/*", nil,
	)
	require.Equal(t, http.StatusOK, broadRec.Code)

	var broadResp map[string]any
	require.NoError(t, json.Unmarshal(broadRec.Body.Bytes(), &broadResp))
	broadPkgs, _ := broadResp["packages"].([]any)
	require.Len(t, broadPkgs, 1)
	assert.Equal(t, "react", broadPkgs[0].(map[string]any)["package"])
	assert.Equal(t, "STRONG", broadPkgs[0].(map[string]any)["associationType"])

	specificRec := doRequest(
		t, h, http.MethodGet, "/v1/list-associated-packages?domain=lapms-domain&package-group=/npm/space/*", nil,
	)
	require.Equal(t, http.StatusOK, specificRec.Code)

	var specificResp map[string]any
	require.NoError(t, json.Unmarshal(specificRec.Body.Bytes(), &specificResp))
	specificPkgs, _ := specificResp["packages"].([]any)
	require.Len(t, specificPkgs, 1)
	assert.Equal(t, "utils", specificPkgs[0].(map[string]any)["package"])
	assert.Equal(t, "space", specificPkgs[0].(map[string]any)["namespace"])
}

// TestHandler_GetAssociatedPackageGroup_MostSpecificMatch verifies GetAssociatedPackageGroup
// picks the most specific of several matching groups, per AWS's pattern-specificity rule.
func TestHandler_GetAssociatedPackageGroup_MostSpecificMatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "gapgms-domain")

	doRequest(t, h, http.MethodPost, "/v1/package-group?domain=gapgms-domain", map[string]any{"pattern": "/*"})
	doRequest(t, h, http.MethodPost, "/v1/package-group?domain=gapgms-domain", map[string]any{"pattern": "/npm/*"})
	doRequest(
		t, h, http.MethodPost, "/v1/package-group?domain=gapgms-domain",
		map[string]any{"pattern": "/npm/space/react$"},
	)

	rec := doRequest(
		t, h, http.MethodGet,
		"/v1/get-associated-package-group?domain=gapgms-domain&format=npm&namespace=space&package=react", nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pg, _ := resp["packageGroup"].(map[string]any)
	require.NotNil(t, pg)
	assert.Equal(t, "/npm/space/react$", pg["pattern"])
	assert.Equal(t, "STRONG", resp["associationType"])

	// A package that only matches the two broader groups falls back to the most specific
	// OF THOSE, "/npm/*".
	rec2 := doRequest(
		t, h, http.MethodGet,
		"/v1/get-associated-package-group?domain=gapgms-domain&format=npm&package=lodash", nil,
	)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	pg2, _ := resp2["packageGroup"].(map[string]any)
	require.NotNil(t, pg2)
	assert.Equal(t, "/npm/*", pg2["pattern"])
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

	groups, err := b.ListSubPackageGroups(context.Background(), "lspg-domain", "/*")
	require.NoError(t, err)
	assert.NotNil(t, groups)

	_, err = b.ListSubPackageGroups(context.Background(), "nonexistent", "/*")
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
		body       any
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
			body:       map[string]any{"restrictions": map[string]any{"PUBLISH": "ALLOW"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package-group-origin-configuration?package-group=/npm/*",
			body:       map[string]any{"restrictions": map[string]any{"PUBLISH": "ALLOW"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package_group",
			path:       "/v1/package-group-origin-configuration?domain=upgoc-domain",
			body:       map[string]any{"restrictions": map[string]any{"PUBLISH": "ALLOW"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "package_group_not_found",
			path:       "/v1/package-group-origin-configuration?domain=upgoc-domain&package-group=/missing/*",
			body:       map[string]any{"restrictions": map[string]any{"PUBLISH": "ALLOW"}},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "invalid_restriction_type",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upgoc2-domain")
				doRequest(
					t, h, http.MethodPost, "/v1/package-group?domain=upgoc2-domain",
					map[string]any{"pattern": "/npm/*"},
				)
			},
			path:       "/v1/package-group-origin-configuration?domain=upgoc2-domain&package-group=/npm/*",
			body:       map[string]any{"restrictions": map[string]any{"BOGUS": "ALLOW"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_restriction_mode",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upgoc3-domain")
				doRequest(
					t, h, http.MethodPost, "/v1/package-group?domain=upgoc3-domain",
					map[string]any{"pattern": "/npm/*"},
				)
			},
			path:       "/v1/package-group-origin-configuration?domain=upgoc3-domain&package-group=/npm/*",
			body:       map[string]any{"restrictions": map[string]any{"PUBLISH": "BOGUS"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "add_allowed_repository_unknown_repo",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upgoc4-domain")
				doRequest(
					t, h, http.MethodPost, "/v1/package-group?domain=upgoc4-domain",
					map[string]any{"pattern": "/npm/*"},
				)
			},
			path: "/v1/package-group-origin-configuration?domain=upgoc4-domain&package-group=/npm/*",
			body: map[string]any{
				"addAllowedRepositories": []map[string]any{
					{"originRestrictionType": "PUBLISH", "repositoryName": "nope"},
				},
			},
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
				require.NotNil(t, pg)

				originConfig, _ := pg["originConfiguration"].(map[string]any)
				require.NotNil(t, originConfig)
				restrictions, _ := originConfig["restrictions"].(map[string]any)
				require.NotNil(t, restrictions)
				publish, _ := restrictions["PUBLISH"].(map[string]any)
				require.NotNil(t, publish)
				assert.Equal(t, "ALLOW", publish["mode"])
				assert.Equal(t, "ALLOW", publish["effectiveMode"])
			}
		})
	}
}

// TestHandler_UpdatePackageGroupOriginConfiguration_AllowedRepositories verifies
// addAllowedRepositories/removeAllowedRepositories mutate the group's allowed-repository
// list and are reflected in both the response's allowedRepositoryUpdates and a subsequent
// ListAllowedRepositoriesForGroup call.
func TestHandler_UpdatePackageGroupOriginConfiguration_AllowedRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "upgocr-domain")
	setupRepo(t, h, "upgocr-domain", "upgocr-repo")
	doRequest(
		t, h, http.MethodPost, "/v1/package-group?domain=upgocr-domain",
		map[string]any{"pattern": "/npm/*"},
	)

	addRec := doRequest(
		t, h, http.MethodPut,
		"/v1/package-group-origin-configuration?domain=upgocr-domain&package-group=/npm/*",
		map[string]any{
			"restrictions": map[string]any{"EXTERNAL_UPSTREAM": "ALLOW_SPECIFIC_REPOSITORIES"},
			"addAllowedRepositories": []map[string]any{
				{"originRestrictionType": "EXTERNAL_UPSTREAM", "repositoryName": "upgocr-repo"},
			},
		},
	)
	require.Equal(t, http.StatusOK, addRec.Code)

	var addResp map[string]any
	require.NoError(t, json.Unmarshal(addRec.Body.Bytes(), &addResp))
	updates, _ := addResp["allowedRepositoryUpdates"].(map[string]any)
	require.NotNil(t, updates)
	extUpstream, _ := updates["EXTERNAL_UPSTREAM"].(map[string]any)
	require.NotNil(t, extUpstream)
	added, _ := extUpstream["ADDED"].([]any)
	assert.Equal(t, []any{"upgocr-repo"}, added)

	listRec := doRequest(
		t, h, http.MethodGet,
		"/v1/package-group-allowed-repositories?domain=upgocr-domain&package-group=/npm/*"+
			"&originRestrictionType=EXTERNAL_UPSTREAM",
		nil,
	)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	allowed, _ := listResp["allowedRepositories"].([]any)
	assert.Equal(t, []any{"upgocr-repo"}, allowed)

	removeRec := doRequest(
		t, h, http.MethodPut,
		"/v1/package-group-origin-configuration?domain=upgocr-domain&package-group=/npm/*",
		map[string]any{
			"removeAllowedRepositories": []map[string]any{
				{"originRestrictionType": "EXTERNAL_UPSTREAM", "repositoryName": "upgocr-repo"},
			},
		},
	)
	require.Equal(t, http.StatusOK, removeRec.Code)

	listRec2 := doRequest(
		t, h, http.MethodGet,
		"/v1/package-group-allowed-repositories?domain=upgocr-domain&package-group=/npm/*"+
			"&originRestrictionType=EXTERNAL_UPSTREAM",
		nil,
	)
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp2))
	allowed2, _ := listResp2["allowedRepositories"].([]any)
	assert.Empty(t, allowed2)
}

// TestHandler_ListAllowedRepositoriesForGroup_MissingRestrictionType verifies the
// required originRestrictionType query parameter is enforced.
func TestHandler_ListAllowedRepositoriesForGroup_MissingRestrictionType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "larg2-domain")

	rec := doRequest(
		t, h, http.MethodGet,
		"/v1/package-group-allowed-repositories?domain=larg2-domain&package-group=/npm/*",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_UpdatePackageGroupOriginConfiguration_Inheritance verifies a group with no
// explicit restriction resolves its effectiveMode from the nearest ancestor group that has
// one set, and defaults to ALLOW when no ancestor has a non-INHERIT mode.
func TestHandler_UpdatePackageGroupOriginConfiguration_Inheritance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "inherit-domain")
	doRequest(t, h, http.MethodPost, "/v1/package-group?domain=inherit-domain", map[string]any{"pattern": "/npm/*"})
	doRequest(
		t, h, http.MethodPost, "/v1/package-group?domain=inherit-domain",
		map[string]any{"pattern": "/npm/space/*"},
	)

	// No restriction ever configured on either group: effective mode defaults to ALLOW.
	descRec := doRequest(
		t, h, http.MethodGet, "/v1/package-group?domain=inherit-domain&package-group=/npm/space/*", nil,
	)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	pg, _ := descResp["packageGroup"].(map[string]any)
	originConfig, _ := pg["originConfiguration"].(map[string]any)
	restrictions, _ := originConfig["restrictions"].(map[string]any)
	publish, _ := restrictions["PUBLISH"].(map[string]any)
	assert.Equal(t, "INHERIT", publish["mode"])
	assert.Equal(t, "ALLOW", publish["effectiveMode"])
	assert.Nil(t, publish["inheritedFrom"])

	// Block PUBLISH on the parent (/npm/*): the child (/npm/space/*), still left at its
	// default INHERIT, must now resolve its effective mode from that parent.
	blockRec := doRequest(
		t, h, http.MethodPut, "/v1/package-group-origin-configuration?domain=inherit-domain&package-group=/npm/*",
		map[string]any{"restrictions": map[string]any{"PUBLISH": "BLOCK"}},
	)
	require.Equal(t, http.StatusOK, blockRec.Code)

	descRec2 := doRequest(
		t, h, http.MethodGet, "/v1/package-group?domain=inherit-domain&package-group=/npm/space/*", nil,
	)
	require.Equal(t, http.StatusOK, descRec2.Code)

	var descResp2 map[string]any
	require.NoError(t, json.Unmarshal(descRec2.Body.Bytes(), &descResp2))
	pg2, _ := descResp2["packageGroup"].(map[string]any)
	originConfig2, _ := pg2["originConfiguration"].(map[string]any)
	restrictions2, _ := originConfig2["restrictions"].(map[string]any)
	publish2, _ := restrictions2["PUBLISH"].(map[string]any)
	assert.Equal(t, "INHERIT", publish2["mode"])
	assert.Equal(t, "BLOCK", publish2["effectiveMode"])
	inheritedFrom, _ := publish2["inheritedFrom"].(map[string]any)
	require.NotNil(t, inheritedFrom)
	assert.Equal(t, "/npm/*", inheritedFrom["pattern"])

	// The parent itself now reports the group hierarchy too.
	assert.Equal(t, "/npm/*", pg2["parent"].(map[string]any)["pattern"])
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
