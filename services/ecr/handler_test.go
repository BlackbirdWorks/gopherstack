package ecr_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecr"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
	testEndpoint  = "localhost:8000"
)

func newTestHandler(t *testing.T) *ecr.Handler {
	t.Helper()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)

	return ecr.NewHandler(backend, nil)
}

func doECRRequest(t *testing.T, h *ecr.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestECR_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ECR", h.Name())
}

func TestECR_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateRepository")
	assert.Contains(t, ops, "DescribeRepositories")
	assert.Contains(t, ops, "DeleteRepository")
	assert.Contains(t, ops, "GetAuthorizationToken")
}

func TestECR_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestECR_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		path      string
		wantMatch bool
	}{
		{
			name:      "matching control plane target",
			target:    "AmazonEC2ContainerRegistry_V20150921.CreateRepository",
			path:      "/",
			wantMatch: true,
		},
		{
			name:      "non-matching target",
			target:    "OtherService.Action",
			path:      "/",
			wantMatch: false,
		},
		{
			name:      "v2 path without registry enabled",
			target:    "",
			path:      "/v2/",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestECR_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		target   string
		want     string
		path     string
		useLocal bool
	}{
		{
			name:   "create repository action",
			target: "AmazonEC2ContainerRegistry_V20150921.CreateRepository",
			want:   "CreateRepository",
		},
		{
			name:   "empty target",
			target: "",
			want:   "Unknown",
		},
		{
			name:   "other service target",
			target: "OtherService.Action",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			path := "/"
			if tt.path != "" {
				path = tt.path
			}

			req := httptest.NewRequest(http.MethodPost, path, nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestECR_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "with repositoryName",
			body: `{"repositoryName":"my-repo"}`,
			want: "my-repo",
		},
		{
			name: "with repositoryNames",
			body: `{"repositoryNames":["repo-a","repo-b"]}`,
			want: "repo-a",
		},
		{
			name: "empty body",
			body: `{}`,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(tt.body)))
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestECR_CreateRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
		wantARN  bool
		wantURI  bool
	}{
		{
			name:     "success",
			input:    map[string]any{"repositoryName": "my-repo"},
			wantCode: http.StatusOK,
			wantARN:  true,
			wantURI:  true,
		},
		{
			name:     "missing repository name",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECRRequest(t, h, "CreateRepository", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN || tt.wantURI {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				repo, ok := resp["repository"].(map[string]any)
				require.True(t, ok)

				if tt.wantARN {
					assert.NotEmpty(t, repo["repositoryArn"])
				}

				if tt.wantURI {
					assert.NotEmpty(t, repo["repositoryUri"])
				}
			}
		})
	}
}

func TestECR_CreateRepository_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "my-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "my-repo"})
	require.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "RepositoryAlreadyExistsException")
}

func TestECR_DescribeRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		repos    []string
		filter   []string
		wantCode int
		wantLen  int
	}{
		{
			name:     "list all",
			repos:    []string{"repo-a", "repo-b"},
			filter:   nil,
			wantCode: http.StatusOK,
			wantLen:  2,
		},
		{
			name:     "filter by name",
			repos:    []string{"repo-a", "repo-b"},
			filter:   []string{"repo-a"},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name:     "filter not found",
			repos:    []string{"repo-a"},
			filter:   []string{"repo-missing"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, repoName := range tt.repos {
				rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": repoName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			input := map[string]any{}
			if tt.filter != nil {
				input["repositoryNames"] = tt.filter
			}

			rec := doECRRequest(t, h, "DescribeRepositories", input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantLen > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				repos, ok := resp["repositories"].([]any)
				require.True(t, ok)
				assert.Len(t, repos, tt.wantLen)
			}
		})
	}
}

func TestECR_DeleteRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		create   string
		delete   string
		wantCode int
	}{
		{
			name:     "success",
			create:   "my-repo",
			delete:   "my-repo",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			create:   "",
			delete:   "nonexistent",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.create != "" {
				rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": tt.create})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doECRRequest(t, h, "DeleteRepository", map[string]any{"repositoryName": tt.delete})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				repo, ok := resp["repository"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.delete, repo["repositoryName"])
			}
		})
	}
}

func TestECR_GetAuthorizationToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	authData, ok := resp["authorizationData"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, authData)

	entry, ok := authData[0].(map[string]any)
	require.True(t, ok)

	tokenRaw, ok := entry["authorizationToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, tokenRaw)

	decoded, err := base64.StdEncoding.DecodeString(tokenRaw)
	require.NoError(t, err)
	assert.Equal(t, "AWS:dummy-password", string(decoded))

	assert.NotZero(t, entry["expiresAt"])
}

func TestECR_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECRRequest(t, h, "UnknownAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECR_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &ecr.Provider{}
	assert.Equal(t, "ECR", p.Name())

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestECR_Provider_Init_WithLocalRegistry(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)
	assert.True(t, h.RegistryEnabled())
}

func TestECR_RouteMatcher_V2Path_WithRegistryEnabled(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))
}

func TestECR_Handler_V2Path_ProxiesRegistry(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)
	// Distribution registry responds 200 for /v2/ when no auth is configured.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestECR_ExtractOperation_V2Path_WithRegistryEnabled(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "RegistryV2", h.ExtractOperation(c))
}

func TestECR_Backend_SetEndpoint(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, "")

	backend.SetEndpoint("localhost:9000")

	repo, err := backend.CreateRepository("my-repo")
	require.NoError(t, err)
	assert.Contains(t, repo.RepositoryURI, "localhost:9000")
}

func TestECR_Persistence(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "persist-me"})
	require.Equal(t, http.StatusOK, rec.Code)

	snapshot := h.Snapshot()
	require.NotEmpty(t, snapshot)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(snapshot))

	rec2 := doECRRequest(t, h2, "DescribeRepositories", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	repos, ok := resp["repositories"].([]any)
	require.True(t, ok)
	assert.Len(t, repos, 1)
}
