package codeartifact_test

import (
	"bytes"
	"context"
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

// doRawRequest POSTs body as the raw HTTP payload (application/octet-stream), unlike
// doRequest which always JSON-marshals it. PublishPackageVersion's httpPayload is the
// raw asset content, not a JSON document -- see handler.go's Handler() doc comment. It
// is always POST because PublishPackageVersion, the only op with a binary payload, is
// always POST.

func doRawRequest(t *testing.T, h *codeartifact.Handler, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

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

func TestHandler_NewOperations_RouteMatching(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "package_group", path: "/v1/package-group", want: true},
		{name: "package", path: "/v1/package", want: true},
		{name: "package_version", path: "/v1/package/version", want: true},
		{name: "package_versions_copy", path: "/v1/package/versions/copy", want: true},
		{name: "package_versions_delete", path: "/v1/package/versions/delete", want: true},
		{name: "repo_external_connection", path: "/v1/repository/external-connection", want: true},
		{name: "repo_permissions_policy", path: "/v1/repository/permissions/policy", want: true},
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

func TestHandler_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expectedOps := []string{
		"AssociateExternalConnection",
		"CopyPackageVersions",
		"CreatePackageGroup",
		"DeletePackage",
		"DeletePackageGroup",
		"DeletePackageVersions",
		"DeleteRepositoryPermissionsPolicy",
		"DescribePackage",
		"DescribePackageGroup",
		"DescribePackageVersion",
	}

	for _, op := range expectedOps {
		assert.Contains(t, ops, op)
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	_, err := b.CreateDomain(context.Background(), "reset-domain", "", nil)
	require.NoError(t, err)

	_, err = b.CreateRepository(context.Background(), "reset-domain", "reset-repo", "", nil, nil)
	require.NoError(t, err)

	b.Reset()

	_, err = b.DescribeDomain(context.Background(), "reset-domain")
	require.Error(t, err)

	_, err = b.DescribeRepository(context.Background(), "reset-domain", "reset-repo")
	require.Error(t, err)
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=rst-domain", nil)

	descRec := doRequest(t, h, http.MethodGet, "/v1/domain?domain=rst-domain", nil)
	assert.Equal(t, http.StatusOK, descRec.Code)

	h.Reset()

	descRec2 := doRequest(t, h, http.MethodGet, "/v1/domain?domain=rst-domain", nil)
	assert.Equal(t, http.StatusNotFound, descRec2.Code)
}

func TestProvider_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codeartifact.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
}

func TestHandler_SortedOutput(t *testing.T) {
	t.Parallel()

	t.Run("domains_sorted", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/v1/domain?domain=sort-c", nil)
		doRequest(t, h, http.MethodPost, "/v1/domain?domain=sort-a", nil)
		doRequest(t, h, http.MethodPost, "/v1/domain?domain=sort-b", nil)

		rec := doRequest(t, h, http.MethodPost, "/v1/domains", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		domains, _ := resp["domains"].([]any)
		require.Len(t, domains, 3)

		names := make([]string, 3)
		for i, d := range domains {
			dm, _ := d.(map[string]any)
			names[i], _ = dm["name"].(string)
		}
		assert.Equal(t, []string{"sort-a", "sort-b", "sort-c"}, names)
	})

	t.Run("repos_in_domain_sorted", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/v1/domain?domain=sort-dom", nil)
		doRequest(t, h, http.MethodPost, "/v1/repository?domain=sort-dom&repository=repo-z", nil)
		doRequest(t, h, http.MethodPost, "/v1/repository?domain=sort-dom&repository=repo-a", nil)
		doRequest(t, h, http.MethodPost, "/v1/repository?domain=sort-dom&repository=repo-m", nil)

		rec := doRequest(t, h, http.MethodPost, "/v1/domain/repositories?domain=sort-dom", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		repos, _ := resp["repositories"].([]any)
		require.Len(t, repos, 3)

		names := make([]string, 3)
		for i, r := range repos {
			rm, _ := r.(map[string]any)
			names[i], _ = rm["name"].(string)
		}
		assert.Equal(t, []string{"repo-a", "repo-m", "repo-z"}, names)
	})
}

func TestHandler_ErrValidationMapsTo400(t *testing.T) {
	t.Parallel()

	t.Run("delete_domain_missing_name_returns_400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodDelete, "/v1/domain", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("associate_duplicate_connection_returns_409", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/v1/domain?domain=dup-conn-domain", nil)
		doRequest(t, h, http.MethodPost, "/v1/repository?domain=dup-conn-domain&repository=dup-conn-repo", nil)

		doRequest(
			t,
			h,
			http.MethodPost,
			"/v1/repository/external-connection"+
				"?domain=dup-conn-domain&repository=dup-conn-repo&external-connection=public:npmjs",
			nil,
		)
		rec := doRequest(
			t,
			h,
			http.MethodPost,
			"/v1/repository/external-connection"+
				"?domain=dup-conn-domain&repository=dup-conn-repo&external-connection=public:npmjs",
			nil,
		)
		assert.Equal(t, http.StatusConflict, rec.Code)
	})
}
