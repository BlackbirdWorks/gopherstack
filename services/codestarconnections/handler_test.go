package codestarconnections_test

import (
	"bytes"
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
				conn, err := h.Backend.CreateConnection("test-conn", "GitHub", "", nil)
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
				_, _ = h.Backend.CreateConnection("conn1", "GitHub", "", nil)
				_, _ = h.Backend.CreateConnection("conn2", "Bitbucket", "", nil)

				return ""
			},
			filter:    map[string]any{},
			wantCount: 2,
		},
		{
			name: "filter by provider type",
			setupFn: func(h *codestarconnections.Handler) string {
				_, _ = h.Backend.CreateConnection("conn1", "GitHub", "", nil)
				_, _ = h.Backend.CreateConnection("conn2", "Bitbucket", "", nil)

				return ""
			},
			filter:    map[string]any{"ProviderTypeFilter": "GitHub"},
			wantCount: 1,
		},
		{
			name: "filter by host arn",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost("my-host", "GitHubEnterpriseServer", "https://example.com", nil)
				if err != nil {
					return ""
				}

				_, _ = h.Backend.CreateConnection("conn-with-host", "GitHubEnterpriseServer", host.HostArn, nil)
				_, _ = h.Backend.CreateConnection("conn-without-host", "GitHub", "", nil)

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
				conn, err := h.Backend.CreateConnection("del-conn", "GitHub", "", nil)
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
				host, err := h.Backend.CreateHost("test-host", "GitHubEnterpriseServer", "https://example.com", nil)
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
	_, _ = h.Backend.CreateHost("host1", "GitHubEnterpriseServer", "https://a.com", nil)
	_, _ = h.Backend.CreateHost("host2", "GitHubEnterpriseServer", "https://b.com", nil)

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
				host, err := h.Backend.CreateHost("del-host", "GitHubEnterpriseServer", "https://x.com", nil)
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
				host, err := h.Backend.CreateHost("upd-host", "GitHubEnterpriseServer", "https://old.com", nil)
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
	conn, err := h.Backend.CreateConnection("tagged-conn", "GitHub", "", nil)
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
	conn, err := h.Backend.CreateConnection("untag-conn", "GitHub", "", map[string]string{"env": "prod", "team": "ops"})
	require.NoError(t, err)

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tags, err := h.Backend.ListTagsForResource(conn.ConnectionArn)
	require.NoError(t, err)
	assert.NotContains(t, tags, "env")
	assert.Contains(t, tags, "team")
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection("list-tags-conn", "GitHub", "", map[string]string{"k1": "v1"})
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
