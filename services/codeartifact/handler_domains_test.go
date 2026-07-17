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

func TestHandler_CreateAndDescribeDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainName string
		wantStatus int
	}{
		{
			name:       "success",
			domainName: "my-domain",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain_name",
			domainName: "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			path := "/v1/domain"
			if tt.domainName != "" {
				path += "?domain=" + tt.domainName
			}

			rec := doRequest(t, h, http.MethodPost, path, map[string]any{
				"tags": []map[string]any{{"key": "env", "value": "test"}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				domain, _ := resp["domain"].(map[string]any)
				assert.Equal(t, tt.domainName, domain["name"])
				assert.NotEmpty(t, domain["arn"])
				assert.Equal(t, "Active", domain["status"])

				// Describe the created domain.
				descRec := doRequest(t, h, http.MethodGet, "/v1/domain?domain="+tt.domainName, nil)
				assert.Equal(t, http.StatusOK, descRec.Code)
				var descResp map[string]any
				require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
				ddomain, _ := descResp["domain"].(map[string]any)
				assert.Equal(t, tt.domainName, ddomain["name"])
			}
		})
	}
}

func TestHandler_CreateDomain_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/v1/domain?domain=dup-domain", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/v1/domain?domain=dup-domain", nil)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestHandler_DescribeDomain_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/v1/domain?domain=missing", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteDomain(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a domain first.
	createRec := doRequest(t, h, http.MethodPost, "/v1/domain?domain=del-domain", nil)
	assert.Equal(t, http.StatusOK, createRec.Code)

	// Delete it.
	delRec := doRequest(t, h, http.MethodDelete, "/v1/domain?domain=del-domain", nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Verify it is gone.
	descRec := doRequest(t, h, http.MethodGet, "/v1/domain?domain=del-domain", nil)
	assert.Equal(t, http.StatusNotFound, descRec.Code)
}

func TestHandler_ListDomains(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/v1/domain?domain=list-domain-a", nil)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=list-domain-b", nil)

	rec := doRequest(t, h, http.MethodPost, "/v1/domains", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	domains, _ := resp["domains"].([]any)
	assert.Len(t, domains, 2)
}

func TestHandler_GetAuthorizationToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=auth-domain", nil)

	rec := doRequest(t, h, http.MethodPost, "/v1/authorization-token?domain=auth-domain", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["authorizationToken"])
	assert.NotEmpty(t, resp["expiration"])
}

func TestHandler_DomainPermissionsPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		wantCheck  func(t *testing.T, body []byte)
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name: "put_policy_returns_revision_and_arn",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=perm-domain", nil)
			},
			method:     http.MethodPut,
			path:       "/v1/domain/permissions/policy?domain=perm-domain",
			body:       map[string]any{"policyDocument": `{"Version":"2012-10-17","Statement":[]}`},
			wantStatus: http.StatusOK,
			wantCheck: func(t *testing.T, b []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(b, &resp))
				pol, _ := resp["policy"].(map[string]any)
				assert.NotEmpty(t, pol["revision"])
				assert.NotEmpty(t, pol["resourceArn"])
			},
		},
		{
			name: "get_policy_after_put",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=perm-domain2", nil)
				doRequest(t, h, http.MethodPut, "/v1/domain/permissions/policy?domain=perm-domain2", map[string]any{
					"policyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`,
				})
			},
			method:     http.MethodGet,
			path:       "/v1/domain/permissions/policy?domain=perm-domain2",
			wantStatus: http.StatusOK,
			wantCheck: func(t *testing.T, b []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(b, &resp))
				pol, _ := resp["policy"].(map[string]any)
				assert.Contains(t, pol["document"], "Allow")
			},
		},
		{
			name: "delete_policy",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=perm-domain3", nil)
				doRequest(t, h, http.MethodPut, "/v1/domain/permissions/policy?domain=perm-domain3", map[string]any{
					"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
				})
			},
			method:     http.MethodDelete,
			path:       "/v1/domain/permissions/policy?domain=perm-domain3",
			wantStatus: http.StatusOK,
		},
		{
			name: "get_after_delete_returns_404",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=perm-domain4", nil)
				doRequest(t, h, http.MethodPut, "/v1/domain/permissions/policy?domain=perm-domain4", map[string]any{
					"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
				})
				doRequest(t, h, http.MethodDelete, "/v1/domain/permissions/policy?domain=perm-domain4", nil)
			},
			method:     http.MethodGet,
			path:       "/v1/domain/permissions/policy?domain=perm-domain4",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get_on_nonexistent_domain_returns_404",
			method:     http.MethodGet,
			path:       "/v1/domain/permissions/policy?domain=no-such-domain",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "put_on_nonexistent_domain_returns_404",
			method:     http.MethodPut,
			path:       "/v1/domain/permissions/policy?domain=no-such-domain",
			body:       map[string]any{"policyDocument": `{}`},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_domain_param_returns_400",
			method:     http.MethodGet,
			path:       "/v1/domain/permissions/policy",
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

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCheck != nil {
				tt.wantCheck(t, rec.Body.Bytes())
			}
		})
	}
}

func TestHandler_DeleteDomainCascade(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=cascade-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=cascade-domain&repository=repo1", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=cascade-domain&repository=repo2", nil)
	doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=cascade-domain&repository=repo1&format=npm&package=pkg1&version=1.0.0",
		nil,
	)
	doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=cascade-domain&repository=repo2&format=npm&package=pkg2&version=2.0.0",
		nil,
	)

	delRec := doRequest(t, h, http.MethodDelete, "/v1/domain?domain=cascade-domain", nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	descRec1 := doRequest(t, h, http.MethodGet, "/v1/repository?domain=cascade-domain&repository=repo1", nil)
	assert.Equal(t, http.StatusNotFound, descRec1.Code)

	descRec2 := doRequest(t, h, http.MethodGet, "/v1/repository?domain=cascade-domain&repository=repo2", nil)
	assert.Equal(t, http.StatusNotFound, descRec2.Code)
}

func TestHandler_RepositoryCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=count-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=count-domain&repository=r1", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=count-domain&repository=r2", nil)

	rec := doRequest(t, h, http.MethodGet, "/v1/domain?domain=count-domain", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	domainMap, _ := resp["domain"].(map[string]any)
	count, _ := domainMap["repositoryCount"].(float64)
	assert.InEpsilon(t, float64(2), count, 0)
}

func TestHandler_DomainSummaryVsFullDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "summary-domain")
	setupRepo(t, h, "summary-domain", "r1")
	setupRepo(t, h, "summary-domain", "r2")

	// ListDomains returns summary (no repositoryCount).
	listRec := doRequest(t, h, http.MethodPost, "/v1/domains", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	domains, _ := listResp["domains"].([]any)
	require.Len(t, domains, 1)
	d, _ := domains[0].(map[string]any)
	assert.NotEmpty(t, d["arn"])
	assert.NotEmpty(t, d["name"])

	// DescribeDomain returns full info with repositoryCount.
	descRec := doRequest(t, h, http.MethodGet, "/v1/domain?domain=summary-domain", nil)
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	dd, _ := descResp["domain"].(map[string]any)
	count, _ := dd["repositoryCount"].(float64)
	assert.InEpsilon(t, float64(2), count, 0)
}

func TestListDomains_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/domain?domain=dom-%02d", i), nil)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Unlike every other List op, ListDomains sends maxResults/nextToken as JSON body
	// fields, not query params -- see listDomainsBody's doc comment in handler.go.
	rec1 := doRequest(t, h, http.MethodPost, "/v1/domains", map[string]any{"maxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["domains"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodPost, "/v1/domains", map[string]any{"maxResults": 2, "nextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["domains"].([]any)
	assert.Len(t, page2, 2)
}

// TestListRepositoriesInDomain_Pagination verifies pagination on ListRepositoriesInDomain.
