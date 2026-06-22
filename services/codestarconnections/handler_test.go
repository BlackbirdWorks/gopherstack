package codestarconnections_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

func newTestHandler(t *testing.T) *codestarconnections.Handler {
	t.Helper()

	return codestarconnections.NewHandler(codestarconnections.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *codestarconnections.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "CodeStar_connections_20191201."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CodeStarConnections", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "codestar-connections", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateConnection")
	assert.Contains(t, ops, "GetConnection")
	assert.Contains(t, ops, "ListConnections")
	assert.Contains(t, ops, "DeleteConnection")
	assert.Contains(t, ops, "CreateHost")
	assert.Contains(t, ops, "GetHost")
	assert.Contains(t, ops, "ListHosts")
	assert.Contains(t, ops, "DeleteHost")
	assert.Contains(t, ops, "UpdateHost")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matching target",
			target: "CodeStar_connections_20191201.CreateConnection",
			want:   true,
		},
		{
			name:   "non-matching target",
			target: "CodeBuild_20161006.CreateProject",
			want:   false,
		},
		{
			name:   "empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "valid target",
			target: "CodeStar_connections_20191201.CreateConnection",
			want:   "CreateConnection",
		},
		{
			name:   "different operation",
			target: "CodeStar_connections_20191201.ListHosts",
			want:   "ListHosts",
		},
		{
			name:   "no prefix",
			target: "SomethingElse",
			want:   "SomethingElse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Empty(t, h.ExtractResource(c))
}

func TestHandler_CreateConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantKey string
		wantErr bool
	}{
		{
			name:    "happy path",
			body:    map[string]any{"ConnectionName": "my-conn", "ProviderType": "GitHub"},
			wantErr: false,
			wantKey: "ConnectionArn",
		},
		{
			name:    "missing name",
			body:    map[string]any{"ProviderType": "GitHub"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateConnection", tt.body)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out[tt.wantKey])
		})
	}
}

func TestHandler_CreateConnection_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"ConnectionName": "dup-conn", "ProviderType": "GitHub"}
	rec1 := doRequest(t, h, "CreateConnection", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateConnection", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_GetConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				conn, err := h.Backend.CreateConnection(context.Background(), "test-conn", "GitHub", "", nil)
				if err != nil {
					return ""
				}

				return conn.ConnectionArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:connection/nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": arn})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			conn, ok := out["Connection"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "test-conn", conn["ConnectionName"])
			assert.Equal(t, "GitHub", conn["ProviderType"])
			assert.Equal(t, "AVAILABLE", conn["ConnectionStatus"])
			assert.Equal(t, "000000000000", conn["OwnerAccountId"])
		})
	}
}

func TestHandler_ListConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn       func(h *codestarconnections.Handler) string
		filter        map[string]any
		name          string
		wantProviders []string
		wantCount     int
	}{
		{
			name: "list all",
			setupFn: func(h *codestarconnections.Handler) string {
				_, _ = h.Backend.CreateConnection(context.Background(), "conn1", "GitHub", "", nil)
				_, _ = h.Backend.CreateConnection(context.Background(), "conn2", "Bitbucket", "", nil)

				return ""
			},
			filter:    map[string]any{},
			wantCount: 2,
		},
		{
			name: "filter by provider type",
			setupFn: func(h *codestarconnections.Handler) string {
				_, _ = h.Backend.CreateConnection(context.Background(), "conn1", "GitHub", "", nil)
				_, _ = h.Backend.CreateConnection(context.Background(), "conn2", "Bitbucket", "", nil)

				return ""
			},
			filter:    map[string]any{"ProviderTypeFilter": "GitHub"},
			wantCount: 1,
		},
		{
			name: "filter by host arn",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost(
					context.Background(),
					"my-host",
					"GitHubEnterpriseServer",
					"https://example.com",
					nil,
				)
				if err != nil {
					return ""
				}

				_, _ = h.Backend.CreateConnection(
					context.Background(),
					"conn-with-host",
					"GitHubEnterpriseServer",
					host.HostArn,
					nil,
				)
				_, _ = h.Backend.CreateConnection(context.Background(), "conn-without-host", "GitHub", "", nil)

				return host.HostArn
			},
			filter:    map[string]any{},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			hostArn := tt.setupFn(h)

			filter := tt.filter
			if tt.name == "filter by host arn" {
				filter = map[string]any{"HostArnFilter": hostArn}
			}

			rec := doRequest(t, h, "ListConnections", filter)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			conns, ok := out["Connections"].([]any)
			require.True(t, ok)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}

func TestHandler_DeleteConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				conn, err := h.Backend.CreateConnection(context.Background(), "del-conn", "GitHub", "", nil)
				if err != nil {
					return ""
				}

				return conn.ConnectionArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:connection/nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "DeleteConnection", map[string]any{"ConnectionArn": arn})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_CreateHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			body: map[string]any{
				"Name":             "my-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://example.com",
			},
			wantErr: false,
		},
		{
			name:    "missing name",
			body:    map[string]any{"ProviderType": "GitHubEnterpriseServer"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateHost", tt.body)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out["HostArn"])
		})
	}
}

func TestHandler_CreateHost_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"Name":             "dup-host",
		"ProviderType":     "GitHubEnterpriseServer",
		"ProviderEndpoint": "https://x.com",
	}
	rec1 := doRequest(t, h, "CreateHost", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateHost", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_GetHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost(
					context.Background(),
					"test-host",
					"GitHubEnterpriseServer",
					"https://example.com",
					nil,
				)
				if err != nil {
					return ""
				}

				return host.HostArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:host/nonexistent/abc12345"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "GetHost", map[string]any{"HostArn": arn})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, "test-host", out["Name"])
			assert.Equal(t, "GitHubEnterpriseServer", out["ProviderType"])
			assert.Equal(t, "https://example.com", out["ProviderEndpoint"])
			assert.Equal(t, "AVAILABLE", out["Status"])
		})
	}
}

func TestHandler_ListHosts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateHost(context.Background(), "host1", "GitHubEnterpriseServer", "https://a.com", nil)
	_, _ = h.Backend.CreateHost(context.Background(), "host2", "GitHubEnterpriseServer", "https://b.com", nil)

	rec := doRequest(t, h, "ListHosts", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	hosts, ok := out["Hosts"].([]any)
	require.True(t, ok)
	assert.Len(t, hosts, 2)
}

func TestHandler_DeleteHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost(
					context.Background(),
					"del-host",
					"GitHubEnterpriseServer",
					"https://x.com",
					nil,
				)
				if err != nil {
					return ""
				}

				return host.HostArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:host/nonexistent/abc12345"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "DeleteHost", map[string]any{"HostArn": arn})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_UpdateHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost(
					context.Background(),
					"upd-host",
					"GitHubEnterpriseServer",
					"https://old.com",
					nil,
				)
				if err != nil {
					return ""
				}

				return host.HostArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:host/nonexistent/abc12345"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "UpdateHost", map[string]any{"HostArn": arn, "ProviderEndpoint": "https://new.com"})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_TagResource_Connection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection(context.Background(), "tagged-conn", "GitHub", "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": conn.ConnectionArn})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	tags, ok := out["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "prod", tag["Value"])
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:codestar-connections:us-east-1:000000000000:connection/nonexistent",
		"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection(
		context.Background(),
		"untag-conn",
		"GitHub",
		"",
		map[string]string{"env": "prod", "team": "ops"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tags, err := h.Backend.ListTagsForResource(context.Background(), conn.ConnectionArn)
	require.NoError(t, err)
	assert.NotContains(t, tags, "env")
	assert.Contains(t, tags, "team")
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection(
		context.Background(),
		"list-tags-conn",
		"GitHub",
		"",
		map[string]string{"k1": "v1"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": conn.ConnectionArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	tags, ok := out["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "k1", tag["Key"])
	assert.Equal(t, "v1", tag["Value"])
}

func TestHandler_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "CodeStar_connections_20191201.CreateConnection")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UnknownOp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Equal(t, h.GetSupportedOperations(), ops)
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, "us-east-1", regions[0])
}

func TestHandler_CreateRepositoryLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			body: map[string]any{
				"ConnectionArn":  "arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
				"OwnerId":        "my-owner",
				"RepositoryName": "my-repo",
			},
			wantErr: false,
		},
		{
			name:    "missing connection arn",
			body:    map[string]any{"OwnerId": "owner", "RepositoryName": "repo"},
			wantErr: true,
		},
		{
			name: "missing owner id",
			body: map[string]any{
				"ConnectionArn":  "arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
				"RepositoryName": "repo",
			},
			wantErr: true,
		},
		{
			name: "missing repository name",
			body: map[string]any{
				"ConnectionArn": "arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
				"OwnerId":       "owner",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateRepositoryLink", tt.body)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			info, ok := out["RepositoryLinkInfo"].(map[string]any)
			require.True(t, ok)
			assert.NotEmpty(t, info["RepositoryLinkId"])
			assert.NotEmpty(t, info["RepositoryLinkArn"])
			assert.Equal(t, "my-owner", info["OwnerId"])
			assert.Equal(t, "my-repo", info["RepositoryName"])
		})
	}
}

func TestHandler_GetRepositoryLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				link, err := h.Backend.CreateRepositoryLink(
					context.Background(),
					"arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
					"my-owner", "my-repo", "",
				)
				if err != nil {
					return ""
				}

				return link.RepositoryLinkID
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "nonexistent-id"
			},
			wantErr: true,
		},
		{
			name: "missing id",
			setupFn: func(_ *codestarconnections.Handler) string {
				return ""
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setupFn(h)

			rec := doRequest(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": id})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			info, ok := out["RepositoryLinkInfo"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "my-owner", info["OwnerId"])
			assert.Equal(t, "my-repo", info["RepositoryName"])
		})
	}
}

func TestHandler_DeleteRepositoryLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				link, err := h.Backend.CreateRepositoryLink(
					context.Background(),
					"arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
					"owner", "repo", "",
				)
				if err != nil {
					return ""
				}

				return link.RepositoryLinkID
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "nonexistent-id"
			},
			wantErr: true,
		},
		{
			name: "missing id",
			setupFn: func(_ *codestarconnections.Handler) string {
				return ""
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setupFn(h)

			rec := doRequest(t, h, "DeleteRepositoryLink", map[string]any{"RepositoryLinkId": id})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_ListRepositoryLinks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateRepositoryLink(
		context.Background(),
		"arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
		"owner1", "repo1", "",
	)
	_, _ = h.Backend.CreateRepositoryLink(
		context.Background(),
		"arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
		"owner2", "repo2", "",
	)

	rec := doRequest(t, h, "ListRepositoryLinks", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	links, ok := out["RepositoryLinks"].([]any)
	require.True(t, ok)
	assert.Len(t, links, 2)
}

func TestHandler_CreateSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			body: map[string]any{
				"Branch":           "main",
				"ConfigFile":       "config.yaml",
				"RepositoryLinkId": "link-id",
				"ResourceName":     "my-stack",
				"RoleArn":          "arn:aws:iam::000000000000:role/my-role",
				"SyncType":         "CFN_STACK_SYNC",
			},
			wantErr: false,
		},
		{
			name: "missing branch",
			body: map[string]any{
				"ConfigFile":       "f",
				"RepositoryLinkId": "id",
				"ResourceName":     "n",
				"RoleArn":          "r",
				"SyncType":         "CFN_STACK_SYNC",
			},
			wantErr: true,
		},
		{
			name: "invalid sync type",
			body: map[string]any{
				"Branch":           "main",
				"ConfigFile":       "f",
				"RepositoryLinkId": "id",
				"ResourceName":     "n",
				"RoleArn":          "r",
				"SyncType":         "INVALID",
			},
			wantErr: true,
		},
		{
			name: "missing sync type",
			body: map[string]any{
				"Branch":           "main",
				"ConfigFile":       "f",
				"RepositoryLinkId": "id",
				"ResourceName":     "n",
				"RoleArn":          "r",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateSyncConfiguration", tt.body)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			cfg, ok := out["SyncConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "main", cfg["Branch"])
			assert.Equal(t, "my-stack", cfg["ResourceName"])
			assert.Equal(t, "CFN_STACK_SYNC", cfg["SyncType"])
		})
	}
}

func TestHandler_GetSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler)
		input   map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) {
				_, _ = h.Backend.CreateSyncConfiguration(
					context.Background(),
					"main", "config.yaml", "link-id", "my-stack",
					"arn:aws:iam::000000000000:role/role", "CFN_STACK_SYNC",
				)
			},
			input:   map[string]any{"ResourceName": "my-stack", "SyncType": "CFN_STACK_SYNC"},
			wantErr: false,
		},
		{
			name:    "not found",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"ResourceName": "nonexistent", "SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
		{
			name:    "missing resource name",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "GetSyncConfiguration", tt.input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			cfg, ok := out["SyncConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "my-stack", cfg["ResourceName"])
			assert.Equal(t, "CFN_STACK_SYNC", cfg["SyncType"])
		})
	}
}

func TestHandler_DeleteSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler)
		input   map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) {
				_, _ = h.Backend.CreateSyncConfiguration(
					context.Background(),
					"main", "config.yaml", "link-id", "del-stack",
					"arn:aws:iam::000000000000:role/role", "CFN_STACK_SYNC",
				)
			},
			input:   map[string]any{"ResourceName": "del-stack", "SyncType": "CFN_STACK_SYNC"},
			wantErr: false,
		},
		{
			name:    "not found",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"ResourceName": "nonexistent", "SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "DeleteSyncConfiguration", tt.input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_GetRepositorySyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				link, err := h.Backend.CreateRepositoryLink(
					context.Background(),
					"arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
					"owner", "repo", "",
				)
				if err != nil {
					return ""
				}

				return link.RepositoryLinkID
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "nonexistent-id"
			},
			wantErr: true,
		},
		{
			name: "missing link id",
			setupFn: func(_ *codestarconnections.Handler) string {
				return ""
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setupFn(h)

			rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
				"RepositoryLinkId": id,
				"Branch":           "main",
				"SyncType":         "CFN_STACK_SYNC",
			})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			latest, ok := out["LatestSync"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", latest["Status"])
			assert.NotEmpty(t, latest["StartedAt"])
		})
	}
}

func TestHandler_GetResourceSyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler)
		input   map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) {
				_, _ = h.Backend.CreateSyncConfiguration(
					context.Background(),
					"main", "config.yaml", "link-id", "my-resource",
					"arn:aws:iam::000000000000:role/role", "CFN_STACK_SYNC",
				)
			},
			input:   map[string]any{"ResourceName": "my-resource", "SyncType": "CFN_STACK_SYNC"},
			wantErr: false,
		},
		{
			name:    "not found",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"ResourceName": "nonexistent", "SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
		{
			name:    "missing resource name",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "GetResourceSyncStatus", tt.input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			latest, ok := out["LatestSync"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", latest["Status"])
			assert.NotEmpty(t, latest["StartedAt"])
		})
	}
}

func TestHandler_GetSyncBlockerSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler)
		input   map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) {
				_, _ = h.Backend.CreateSyncConfiguration(
					context.Background(),
					"main", "config.yaml", "link-id", "blocker-resource",
					"arn:aws:iam::000000000000:role/role", "CFN_STACK_SYNC",
				)
			},
			input:   map[string]any{"ResourceName": "blocker-resource", "SyncType": "CFN_STACK_SYNC"},
			wantErr: false,
		},
		{
			name:    "not found",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"ResourceName": "nonexistent", "SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
		{
			name:    "missing resource name",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"SyncType": "CFN_STACK_SYNC"},
			wantErr: true,
		},
		{
			name:    "missing sync type",
			setupFn: func(_ *codestarconnections.Handler) {},
			input:   map[string]any{"ResourceName": "some-resource"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "GetSyncBlockerSummary", tt.input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summary, ok := out["SyncBlockerSummary"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "blocker-resource", summary["ResourceName"])
			blockers, ok := summary["LatestBlockers"].([]any)
			require.True(t, ok)
			assert.Empty(t, blockers)
		})
	}
}

func TestHandler_RepositoryLink_SyncConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	conn, err := h.Backend.CreateConnection(context.Background(), "my-conn", "GitHub", "", nil)
	require.NoError(t, err)

	// Create a repository link.
	recLink := doRequest(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":  conn.ConnectionArn,
		"OwnerId":        "my-owner",
		"RepositoryName": "my-repo",
	})
	require.Equal(t, http.StatusOK, recLink.Code)

	var linkOut map[string]any
	require.NoError(t, json.Unmarshal(recLink.Body.Bytes(), &linkOut))
	linkInfo := linkOut["RepositoryLinkInfo"].(map[string]any)
	linkID := linkInfo["RepositoryLinkId"].(string)
	require.NotEmpty(t, linkID)

	// Create a sync configuration using the link.
	recSync := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "config.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "my-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/sync-role",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, recSync.Code)

	var syncOut map[string]any
	require.NoError(t, json.Unmarshal(recSync.Body.Bytes(), &syncOut))
	syncCfg := syncOut["SyncConfiguration"].(map[string]any)
	assert.Equal(t, "my-repo", syncCfg["RepositoryName"])
	assert.Equal(t, "GitHub", syncCfg["ProviderType"])

	// Get repository sync status.
	recStatus := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
		"RepositoryLinkId": linkID,
		"Branch":           "main",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, recStatus.Code)

	// Get resource sync status.
	recResStatus := doRequest(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, recResStatus.Code)

	// List repository links.
	recList := doRequest(t, h, "ListRepositoryLinks", map[string]any{})
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	links, ok := listOut["RepositoryLinks"].([]any)
	require.True(t, ok)
	assert.Len(t, links, 1)

	// Delete sync configuration.
	recDelSync := doRequest(t, h, "DeleteSyncConfiguration", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, recDelSync.Code)

	// Delete repository link.
	recDelLink := doRequest(t, h, "DeleteRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
	assert.Equal(t, http.StatusOK, recDelLink.Code)
}

func TestHandler_NewOps_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"CreateRepositoryLink",
		"GetRepositoryLink",
		"DeleteRepositoryLink",
		"ListRepositoryLinks",
		"CreateSyncConfiguration",
		"GetSyncConfiguration",
		"DeleteSyncConfiguration",
		"GetRepositorySyncStatus",
		"GetResourceSyncStatus",
		"GetSyncBlockerSummary",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op)
	}
}

// ======================== Refinement 1 Tests ========================

// TestRefinement1_Reset verifies Backend.Reset() and Handler.Reset() clear all state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Seed some state.
	_, err := h.Backend.CreateConnection(context.Background(), "c1", "GitHub", "", nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateHost(context.Background(), "h1", "GitHub", "https://example.com", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, h.Backend.ConnectionCount())
	assert.Equal(t, 1, h.Backend.HostCount())

	// Reset via handler.
	h.Reset()

	assert.Equal(t, 0, h.Backend.ConnectionCount())
	assert.Equal(t, 0, h.Backend.HostCount())
	assert.Equal(t, 0, h.Backend.RepositoryLinkCount())
	assert.Equal(t, 0, h.Backend.SyncConfigurationCount())
}

// TestRefinement1_ProviderInit_NilCtx verifies ErrNilAppContext is returned for nil ctx.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codestarconnections.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codestarconnections.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsPreBuilt verifies the dispatch table is built once in NewHandler.
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	// Call Handler() multiple times and confirm responses are consistent (ops not rebuilt per call).
	h := newTestHandler(t)

	_, err := h.Backend.CreateConnection(context.Background(), "conn-one", "GitHub", "", nil)
	require.NoError(t, err)

	// Two separate calls; both must route correctly.
	r1 := doRequest(t, h, "ListConnections", map[string]any{})
	r2 := doRequest(t, h, "ListConnections", map[string]any{})
	require.Equal(t, http.StatusOK, r1.Code)
	require.Equal(t, http.StatusOK, r2.Code)
	assert.Equal(t, r1.Body.String(), r2.Body.String())
}

// TestRefinement1_ConnectionNameUniqueness verifies duplicate connection names are rejected.
func TestRefinement1_ConnectionNameUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec1 := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "dup",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "dup",
		"ProviderType":   "GitHub",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestRefinement1_DeleteConnectionCleansIndex verifies connectionsByName index is updated on delete.
func TestRefinement1_DeleteConnectionCleansIndex(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "myconn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	arn := resp["ConnectionArn"].(string)

	// Delete the connection.
	rec2 := doRequest(t, h, "DeleteConnection", map[string]any{"ConnectionArn": arn})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Re-create with the same name must succeed (index was cleaned).
	rec3 := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "myconn",
		"ProviderType":   "GitHub",
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestRefinement1_HostNameUniqueness verifies duplicate host names are rejected.
func TestRefinement1_HostNameUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec1 := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "host-a",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "host-a",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://other.com",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestRefinement1_DeleteHostCleansIndex verifies hostsByName index is updated on delete.
func TestRefinement1_DeleteHostCleansIndex(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "myhost",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	hostArn := resp["HostArn"].(string)

	rec2 := doRequest(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Re-create with same name must succeed.
	rec3 := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "myhost",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestRefinement1_ProviderTypeValidation verifies invalid ProviderType is rejected.
func TestRefinement1_ProviderTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		op      string
		wantErr bool
	}{
		{
			name:    "CreateConnection invalid provider type",
			op:      "CreateConnection",
			body:    map[string]any{"ConnectionName": "c1", "ProviderType": "BadProvider"},
			wantErr: true,
		},
		{
			name:    "CreateConnection valid provider type",
			op:      "CreateConnection",
			body:    map[string]any{"ConnectionName": "c2", "ProviderType": "GitHub"},
			wantErr: false,
		},
		{
			name: "CreateHost invalid provider type",
			op:   "CreateHost",
			body: map[string]any{
				"Name": "h1", "ProviderType": "NotAProvider",
				"ProviderEndpoint": "https://example.com",
			},
			wantErr: true,
		},
		{
			name: "CreateHost valid provider type",
			op:   "CreateHost",
			body: map[string]any{
				"Name": "h2", "ProviderType": "Bitbucket",
				"ProviderEndpoint": "https://example.com",
			},
			wantErr: false,
		},
		{
			name:    "CreateConnection empty provider type is allowed",
			op:      "CreateConnection",
			body:    map[string]any{"ConnectionName": "c3", "ProviderType": ""},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

// TestRefinement1_SyncTypeValidation verifies invalid SyncType is rejected.
func TestRefinement1_SyncTypeValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "config.yaml",
		"RepositoryLinkId": "link-id",
		"ResourceName":     "stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "INVALID_SYNC_TYPE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_SortedListConnections verifies connections are sorted by name.
func TestRefinement1_SortedListConnections(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra", "alpha", "middle"} {
		rec := doRequest(t, h, "CreateConnection", map[string]any{
			"ConnectionName": name,
			"ProviderType":   "GitHub",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListConnections", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	conns := out["Connections"].([]any)
	require.Len(t, conns, 3)

	names := make([]string, len(conns))
	for i, c := range conns {
		names[i] = c.(map[string]any)["ConnectionName"].(string)
	}

	assert.Equal(t, []string{"alpha", "middle", "zebra"}, names)
}

// TestRefinement1_SortedListHosts verifies hosts are sorted by name.
func TestRefinement1_SortedListHosts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zoo-host", "apple-host", "mango-host"} {
		rec := doRequest(t, h, "CreateHost", map[string]any{
			"Name":             name,
			"ProviderType":     "GitHub",
			"ProviderEndpoint": "https://example.com",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListHosts", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	hosts := out["Hosts"].([]any)
	require.Len(t, hosts, 3)

	names := make([]string, len(hosts))
	for i, host := range hosts {
		names[i] = host.(map[string]any)["Name"].(string)
	}

	assert.Equal(t, []string{"apple-host", "mango-host", "zoo-host"}, names)
}

// TestRefinement1_SortedTags verifies ListTagsForResource returns tags sorted by key.
func TestRefinement1_SortedTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "tagged-conn",
		"ProviderType":   "GitHub",
		"Tags": []map[string]any{
			{"Key": "z-tag", "Value": "z"},
			{"Key": "a-tag", "Value": "a"},
			{"Key": "m-tag", "Value": "m"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	arn := resp["ConnectionArn"].(string)

	recTags := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, recTags.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recTags.Body.Bytes(), &out))
	tags := out["Tags"].([]any)

	keys := make([]string, len(tags))
	for i, tag := range tags {
		keys[i] = tag.(map[string]any)["Key"].(string)
	}

	assert.Equal(t, []string{"a-tag", "m-tag", "z-tag"}, keys)
}

// TestRefinement1_GetConnectionIncludesTags verifies Tags are included in GetConnection response.
func TestRefinement1_GetConnectionIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "conn-with-tags",
		"ProviderType":   "GitHub",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn := parseResp(t, rec)["ConnectionArn"].(string)

	recGet := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": arn})
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	conn := out["Connection"].(map[string]any)
	tags, ok := conn["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"].(string))
}

// TestRefinement1_GetHostIncludesTags verifies Tags are included in GetHost response.
func TestRefinement1_GetHostIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "tagged-host",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
		"Tags": []map[string]any{
			{"Key": "team", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	hostArn := parseResp(t, rec)["HostArn"].(string)

	recGet := doRequest(t, h, "GetHost", map[string]any{"HostArn": hostArn})
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	tags, ok := out["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)
	assert.Equal(t, "platform", tags[0].(map[string]any)["Value"].(string))
}

// TestRefinement1_TagsDeepCopy verifies modifying returned connection doesn't affect stored data.
func TestRefinement1_TagsDeepCopy(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")

	conn, err := b.CreateConnection(context.Background(), "dc-conn", "GitHub", "", map[string]string{"k": "v1"})
	require.NoError(t, err)

	// Modify the returned copy's tags.
	conn.Tags["k"] = "mutated"

	// Original stored conn must be unaffected.
	got, err := b.GetConnection(context.Background(), conn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "v1", got.Tags["k"])
}

// TestRefinement1_SeedHelpers verifies AddConnectionInternal/AddHostInternal/AddRepositoryLinkInternal.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")

	b.AddConnectionInternal(&codestarconnections.Connection{
		ConnectionArn:    "arn:aws:codestar-connections:us-east-1:000000000000:connection/seed1",
		ConnectionName:   "seeded-conn",
		ConnectionStatus: "AVAILABLE",
		ProviderType:     "GitHub",
		OwnerAccountID:   "000000000000",
		Tags:             map[string]string{},
	})
	b.AddHostInternal(&codestarconnections.Host{
		HostArn: "arn:aws:codestar-connections:us-east-1:000000000000:host/seeded-host/abc",
		Name:    "seeded-host",
		Status:  "AVAILABLE",
		Tags:    map[string]string{},
	})
	b.AddRepositoryLinkInternal(context.Background(), &codestarconnections.RepositoryLink{
		RepositoryLinkID:  "seed-link-id",
		RepositoryLinkArn: "arn:aws:codestar-connections:us-east-1:000000000000:repository-link/seed-link-id",
		ConnectionArn:     "arn:aws:codestar-connections:us-east-1:000000000000:connection/seed1",
		OwnerID:           "seed-owner",
		RepositoryName:    "seed-repo",
	})

	assert.Equal(t, 1, b.ConnectionCount())
	assert.Equal(t, 1, b.HostCount())
	assert.Equal(t, 1, b.RepositoryLinkCount())
}

// TestRefinement1_ExportCountHelpers verifies the export_test.go count helpers.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, b.ConnectionCount())
	assert.Equal(t, 0, b.HostCount())
	assert.Equal(t, 0, b.RepositoryLinkCount())
	assert.Equal(t, 0, b.SyncConfigurationCount())

	_, err := b.CreateConnection(context.Background(), "c1", "GitHub", "", nil)
	require.NoError(t, err)

	_, err = b.CreateRepositoryLink(context.Background(), "conn-arn", "owner", "repo", "")
	require.NoError(t, err)

	_, err = b.CreateSyncConfiguration(
		context.Background(),
		"main",
		"f",
		"link-id",
		"res",
		"role-arn",
		"CFN_STACK_SYNC",
	)
	require.NoError(t, err)

	assert.Equal(t, 1, b.ConnectionCount())
	assert.Equal(t, 1, b.RepositoryLinkCount())
	assert.Equal(t, 1, b.SyncConfigurationCount())
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves all state.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("111111111111", "eu-west-1")

	_, err := b.CreateConnection(context.Background(), "persist-conn", "GitHub", "", map[string]string{"env": "test"})
	require.NoError(t, err)
	_, err = b.CreateHost(context.Background(), "persist-host", "GitHub", "https://example.com", nil)
	require.NoError(t, err)
	link, err := b.CreateRepositoryLink(context.Background(), "conn-arn", "owner", "persist-repo", "")
	require.NoError(t, err)
	_, err = b.CreateSyncConfiguration(
		context.Background(),
		"main",
		"f",
		link.RepositoryLinkID,
		"res",
		"arn:r",
		"CFN_STACK_SYNC",
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.ConnectionCount())
	assert.Equal(t, 1, b2.HostCount())
	assert.Equal(t, 1, b2.RepositoryLinkCount())
	assert.Equal(t, 1, b2.SyncConfigurationCount())
	assert.Equal(t, "111111111111", b2.AccountID())
	assert.Equal(t, "eu-west-1", b2.Region())

	// Tag data must survive round trip.
	conns := b2.ListConnections(context.Background(), "", "")
	require.Len(t, conns, 1)
	assert.Equal(t, "test", conns[0].Tags["env"])
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation errors map to 400 in the handler.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Invalid sync type triggers ErrValidation in backend.
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "f",
		"RepositoryLinkId": "id",
		"ResourceName":     "res",
		"RoleArn":          "arn",
		"SyncType":         "UNKNOWN",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement1_ErrAlreadyExistsMapping verifies ErrAlreadyExists maps to 400.
func TestRefinement1_ErrAlreadyExistsMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateConnection", map[string]any{"ConnectionName": "dup", "ProviderType": "GitHub"})

	rec := doRequest(t, h, "CreateConnection", map[string]any{"ConnectionName": "dup", "ProviderType": "GitHub"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidInputException", resp["__type"])
}

// TestRefinement1_TagResourceOnHost verifies TagResource works on hosts.
func TestRefinement1_TagResourceOnHost(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "tag-host",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	hostArn := parseResp(t, rec)["HostArn"].(string)

	recTag := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": hostArn,
		"Tags":        []map[string]any{{"Key": "purpose", "Value": "ci"}},
	})
	require.Equal(t, http.StatusOK, recTag.Code)

	recList := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	tags := out["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "ci", tags[0].(map[string]any)["Value"].(string))
}

// TestRefinement1_UntagResource verifies UntagResource removes specified keys.
func TestRefinement1_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "untag-conn",
		"ProviderType":   "GitHub",
		"Tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn := parseResp(t, rec)["ConnectionArn"].(string)

	recUntag := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []string{"remove"},
	})
	require.Equal(t, http.StatusOK, recUntag.Code)

	recList := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	tags := out["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "keep", tags[0].(map[string]any)["Key"].(string))
}

// TestRefinement1_ListRepositoryLinks_Sorted verifies repository links are returned sorted by ID.
func TestRefinement1_ListRepositoryLinks_Sorted(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed links with known IDs so we can verify order.
	b.AddRepositoryLinkInternal(context.Background(), &codestarconnections.RepositoryLink{
		RepositoryLinkID: "b-link",
		ConnectionArn:    "arn1",
		OwnerID:          "owner",
		RepositoryName:   "repo-b",
	})
	b.AddRepositoryLinkInternal(context.Background(), &codestarconnections.RepositoryLink{
		RepositoryLinkID: "a-link",
		ConnectionArn:    "arn1",
		OwnerID:          "owner",
		RepositoryName:   "repo-a",
	})

	links := b.ListRepositoryLinks(context.Background())
	require.Len(t, links, 2)
	assert.Equal(t, "a-link", links[0].RepositoryLinkID)
	assert.Equal(t, "b-link", links[1].RepositoryLinkID)
}

// TestRefinement1_ConnectionTags_NonNil verifies CreateConnection always sets a non-nil Tags map.
func TestRefinement1_ConnectionTags_NonNil(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	conn, err := b.CreateConnection(context.Background(), "no-tag-conn", "GitHub", "", nil)
	require.NoError(t, err)
	require.NotNil(t, conn.Tags, "Tags must never be nil")
}

// TestRefinement1_HostTags_NonNil verifies CreateHost always sets a non-nil Tags map.
func TestRefinement1_HostTags_NonNil(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	host, err := b.CreateHost(context.Background(), "no-tag-host", "GitHub", "https://example.com", nil)
	require.NoError(t, err)
	require.NotNil(t, host.Tags, "Tags must never be nil")
}
