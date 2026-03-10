package codeartifact_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func newTestHandler(t *testing.T) *codeartifact.Handler {
	t.Helper()

	backend := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	return codeartifact.NewHandler(backend)
}

func doRequest(t *testing.T, h *codeartifact.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(data)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CodeArtifact", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateDomain")
	assert.Contains(t, ops, "CreateRepository")
	assert.Contains(t, ops, "TagResource")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "domain_path",
			path: "/v1/domain",
			want: true,
		},
		{
			name: "domains_path",
			path: "/v1/domains",
			want: true,
		},
		{
			name: "repository_path",
			path: "/v1/repository",
			want: true,
		},
		{
			name: "repositories_path",
			path: "/v1/repositories",
			want: true,
		},
		{
			name: "repository_endpoint",
			path: "/v1/repository/endpoint",
			want: true,
		},
		{
			name: "tags_path",
			path: "/v1/tags",
			want: true,
		},
		{
			name: "tag_path",
			path: "/v1/tag",
			want: true,
		},
		{
			name: "untag_path",
			path: "/v1/untag",
			want: true,
		},
		{
			name: "auth_token_path",
			path: "/v1/authorization-token",
			want: true,
		},
		{
			name: "domain_permissions",
			path: "/v1/domain/permissions/policy",
			want: true,
		},
		{
			name: "batch_path",
			path: "/v1/createcomputeenvironment",
			want: false,
		},
		{
			name: "appsync_path",
			path: "/v1/apis",
			want: false,
		},
		{
			name: "other_path",
			path: "/sns/subscribe",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

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

func TestHandler_CreateRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		repo       string
		wantStatus int
	}{
		{
			name:       "success",
			domain:     "test-domain",
			repo:       "my-repo",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			domain:     "",
			repo:       "my-repo",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			domain:     "test-domain",
			repo:       "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_not_found",
			domain:     "nonexistent-domain",
			repo:       "my-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create domain for success cases.
			if tt.domain == "test-domain" {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=test-domain", nil)
			}

			path := "/v1/repository"
			sep := "?"
			if tt.domain != "" {
				path += sep + "domain=" + tt.domain
				sep = "&"
			}
			if tt.repo != "" {
				path += sep + "repository=" + tt.repo
			}

			rec := doRequest(t, h, http.MethodPost, path, map[string]any{
				"description": "test repo",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				repo, _ := resp["repository"].(map[string]any)
				assert.Equal(t, tt.repo, repo["name"])
				assert.Equal(t, tt.domain, repo["domainName"])
				assert.NotEmpty(t, repo["arn"])
			}
		})
	}
}

func TestHandler_DescribeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=d1", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=d1&repository=r1", nil)

	rec := doRequest(t, h, http.MethodGet, "/v1/repository?domain=d1&repository=r1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	repo, _ := resp["repository"].(map[string]any)
	assert.Equal(t, "r1", repo["name"])
	assert.Equal(t, "d1", repo["domainName"])
}

func TestHandler_DeleteRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=d2", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=d2&repository=r2", nil)

	delRec := doRequest(t, h, http.MethodDelete, "/v1/repository?domain=d2&repository=r2", nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	descRec := doRequest(t, h, http.MethodGet, "/v1/repository?domain=d2&repository=r2", nil)
	assert.Equal(t, http.StatusNotFound, descRec.Code)
}

func TestHandler_ListRepositoriesInDomain(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=d3", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=d3&repository=r3a", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=d3&repository=r3b", nil)

	rec := doRequest(t, h, http.MethodPost, "/v1/domain/repositories?domain=d3", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	repos, _ := resp["repositories"].([]any)
	assert.Len(t, repos, 2)
}

func TestHandler_ListRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=da", nil)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=db", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=da&repository=ra", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=db&repository=rb", nil)

	rec := doRequest(t, h, http.MethodPost, "/v1/repositories", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	repos, _ := resp["repositories"].([]any)
	assert.Len(t, repos, 2)
}

func TestHandler_TagsForDomain(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create domain.
	createRec := doRequest(t, h, http.MethodPost, "/v1/domain?domain=tag-domain", map[string]any{
		"tags": []map[string]any{{"key": "env", "value": "dev"}},
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	domainMap, _ := createResp["domain"].(map[string]any)
	domainARN := domainMap["arn"].(string)

	// List tags.
	listRec := doRequest(t, h, http.MethodPost, "/v1/tags?resourceArn="+domainARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tagList, _ := listResp["tags"].([]any)
	assert.Len(t, tagList, 1)

	// Tag resource.
	tagRec := doRequest(t, h, http.MethodPost, "/v1/tag?resourceArn="+domainARN, map[string]any{
		"tags": []map[string]any{{"key": "team", "value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// List tags again - should have 2 now.
	listRec2 := doRequest(t, h, http.MethodPost, "/v1/tags?resourceArn="+domainARN, nil)
	assert.Equal(t, http.StatusOK, listRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp2))
	tagList2, _ := listResp2["tags"].([]any)
	assert.Len(t, tagList2, 2)

	// Untag resource.
	untagRec := doRequest(t, h, http.MethodPost, "/v1/untag?resourceArn="+domainARN, map[string]any{
		"tagKeys": []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)

	// List tags - should have 1 now.
	listRec3 := doRequest(t, h, http.MethodPost, "/v1/tags?resourceArn="+domainARN, nil)
	assert.Equal(t, http.StatusOK, listRec3.Code)

	var listResp3 map[string]any
	require.NoError(t, json.Unmarshal(listRec3.Body.Bytes(), &listResp3))
	tagList3, _ := listResp3["tags"].([]any)
	assert.Len(t, tagList3, 1)
}

func TestHandler_GetRepositoryEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=ep-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=ep-domain&repository=ep-repo", nil)

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/repository/endpoint?domain=ep-domain&repository=ep-repo&format=npm",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["repositoryEndpoint"])
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

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=perm-domain", nil)

	getRec := doRequest(t, h, http.MethodGet, "/v1/domain/permissions/policy?domain=perm-domain", nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	putRec := doRequest(t, h, http.MethodPut, "/v1/domain/permissions/policy?domain=perm-domain", map[string]any{
		"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	assert.Equal(t, http.StatusOK, putRec.Code)

	delRec := doRequest(t, h, http.MethodDelete, "/v1/domain/permissions/policy?domain=perm-domain", nil)
	assert.Equal(t, http.StatusOK, delRec.Code)
}

func TestHandler_Persistence(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=persist-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=persist-domain&repository=persist-repo", nil)

	// Snapshot and restore.
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(snap))

	descRec := doRequest(t, h2, http.MethodGet, "/v1/domain?domain=persist-domain", nil)
	assert.Equal(t, http.StatusOK, descRec.Code)

	repoRec := doRequest(t, h2, http.MethodGet, "/v1/repository?domain=persist-domain&repository=persist-repo", nil)
	assert.Equal(t, http.StatusOK, repoRec.Code)
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "create_domain",
			method: http.MethodPost,
			path:   "/v1/domain",
			wantOp: "CreateDomain",
		},
		{
			name:   "describe_domain",
			method: http.MethodGet,
			path:   "/v1/domain",
			wantOp: "DescribeDomain",
		},
		{
			name:   "delete_domain",
			method: http.MethodDelete,
			path:   "/v1/domain",
			wantOp: "DeleteDomain",
		},
		{
			name:   "list_domains",
			method: http.MethodPost,
			path:   "/v1/domains",
			wantOp: "ListDomains",
		},
		{
			name:   "create_repository",
			method: http.MethodPost,
			path:   "/v1/repository",
			wantOp: "CreateRepository",
		},
		{
			name:   "describe_repository",
			method: http.MethodGet,
			path:   "/v1/repository",
			wantOp: "DescribeRepository",
		},
		{
			name:   "delete_repository",
			method: http.MethodDelete,
			path:   "/v1/repository",
			wantOp: "DeleteRepository",
		},
		{
			name:   "list_tags",
			method: http.MethodPost,
			path:   "/v1/tags",
			wantOp: "ListTagsForResource",
		},
		{
			name:   "tag_resource",
			method: http.MethodPost,
			path:   "/v1/tag",
			wantOp: "TagResource",
		},
		{
			name:   "untag_resource",
			method: http.MethodPost,
			path:   "/v1/untag",
			wantOp: "UntagResource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(tt.method, tt.path, nil)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_BackendRegion(t *testing.T) {
	t.Parallel()

	backend := codeartifact.NewInMemoryBackend(config.DefaultAccountID, "eu-west-1")
	assert.Equal(t, "eu-west-1", backend.Region())
}

func TestHandler_ChaosAndPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "codeartifact", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{config.DefaultRegion}, h.ChaosRegions())
	assert.Equal(t, service.PriorityPathVersioned+1, h.MatchPriority())
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "domain_only",
			path: "/v1/domain?domain=my-domain",
			want: "my-domain",
		},
		{
			name: "domain_and_repo",
			path: "/v1/repository?domain=my-domain&repository=my-repo",
			want: "my-domain/my-repo",
		},
		{
			name: "resource_arn",
			path: "/v1/tags?resourceArn=arn:aws:codeartifact:us-east-1:123:domain/test",
			want: "arn:aws:codeartifact:us-east-1:123:domain/test",
		},
		{
			name: "no_params",
			path: "/v1/domains",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestHandler_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "describe_domain_not_found",
			method:     http.MethodGet,
			path:       "/v1/domain?domain=nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_domain_not_found",
			method:     http.MethodDelete,
			path:       "/v1/domain?domain=nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "describe_repository_not_found",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=d", nil)
			},
			method:     http.MethodGet,
			path:       "/v1/repository?domain=d&repository=nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete_repository_not_found",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=d", nil)
			},
			method:     http.MethodDelete,
			path:       "/v1/repository?domain=d&repository=nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_repos_missing_domain",
			method:     http.MethodPost,
			path:       "/v1/domain/repositories",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get_repo_endpoint_missing_domain",
			method:     http.MethodGet,
			path:       "/v1/repository/endpoint?repository=r",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get_repo_endpoint_missing_repo",
			method:     http.MethodGet,
			path:       "/v1/repository/endpoint?domain=d",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get_repo_endpoint_not_found",
			method:     http.MethodGet,
			path:       "/v1/repository/endpoint?domain=missing&repository=r",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get_auth_token_missing_domain",
			method:     http.MethodPost,
			path:       "/v1/authorization-token",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get_auth_token_domain_not_found",
			method:     http.MethodPost,
			path:       "/v1/authorization-token?domain=nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_tags_missing_arn",
			method:     http.MethodPost,
			path:       "/v1/tags",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list_tags_not_found",
			method:     http.MethodPost,
			path:       "/v1/tags?resourceArn=arn:aws:codeartifact:us-east-1:123:domain/nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "tag_resource_missing_arn",
			method:     http.MethodPost,
			path:       "/v1/tag",
			body:       map[string]any{"tags": []any{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag_resource_not_found",
			method:     http.MethodPost,
			path:       "/v1/tag?resourceArn=arn:aws:codeartifact:us-east-1:123:domain/nope",
			body:       map[string]any{"tags": []any{}},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "untag_resource_missing_arn",
			method:     http.MethodPost,
			path:       "/v1/untag",
			body:       map[string]any{"tagKeys": []string{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "untag_resource_not_found",
			method:     http.MethodPost,
			path:       "/v1/untag?resourceArn=arn:aws:codeartifact:us-east-1:123:domain/nope",
			body:       map[string]any{"tagKeys": []string{"k"}},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "domain_permissions_missing_domain",
			method:     http.MethodGet,
			path:       "/v1/domain/permissions/policy",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "domain_permissions_not_found",
			method:     http.MethodGet,
			path:       "/v1/domain/permissions/policy?domain=nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "put_domain_permissions_not_found",
			method:     http.MethodPut,
			path:       "/v1/domain/permissions/policy?domain=nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "put_domain_permissions_missing_domain",
			method:     http.MethodPut,
			path:       "/v1/domain/permissions/policy",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_domain_permissions_not_found",
			method:     http.MethodDelete,
			path:       "/v1/domain/permissions/policy?domain=nope",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_domain_permissions_missing_domain",
			method:     http.MethodDelete,
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
		})
	}
}
