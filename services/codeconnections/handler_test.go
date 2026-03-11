package codeconnections_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

func newTestHandler() *codeconnections.Handler {
	backend := codeconnections.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return codeconnections.NewHandler(backend)
}

func doREST(
	t *testing.T,
	h *codeconnections.Handler,
	method, path string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
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

// createConn is a test helper that creates a connection and returns its ARN.
func createConn(t *testing.T, h *codeconnections.Handler, name, providerType string) string {
	t.Helper()

	rec := doREST(t, h, http.MethodPost, "/connections", map[string]any{
		"ConnectionName": name,
		"ProviderType":   providerType,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)

	arn, ok := resp["ConnectionArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

// TestHandlerStringMetadata covers string-valued metadata methods.
func TestHandlerStringMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "Name", got: h.Name(), want: "CodeConnections"},
		{name: "ChaosServiceName", got: h.ChaosServiceName(), want: "codeconnections"},
		{name: "Region", got: h.Backend.Region(), want: config.DefaultRegion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

// TestHandlerSliceMetadata covers slice-returning metadata methods.
func TestHandlerSliceMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name     string
		contains string
		got      []string
	}{
		{
			name:     "GetSupportedOperations_CreateConnection",
			got:      h.GetSupportedOperations(),
			contains: "CreateConnection",
		},
		{
			name:     "GetSupportedOperations_DeleteConnection",
			got:      h.GetSupportedOperations(),
			contains: "DeleteConnection",
		},
		{name: "GetSupportedOperations_TagResource", got: h.GetSupportedOperations(), contains: "TagResource"},
		{name: "GetSupportedOperations_UntagResource", got: h.GetSupportedOperations(), contains: "UntagResource"},
		{
			name:     "GetSupportedOperations_ListTagsForResource",
			got:      h.GetSupportedOperations(),
			contains: "ListTagsForResource",
		},
		{name: "ChaosOperations", got: h.ChaosOperations(), contains: "CreateConnection"},
		{name: "ChaosRegions", got: h.ChaosRegions(), contains: config.DefaultRegion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, tt.got, tt.contains)
		})
	}
}

// TestMatchPriority verifies MatchPriority returns a positive value.
func TestMatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want bool
	}{
		{name: "positive", want: true},
	}

	h := newTestHandler()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, h.MatchPriority() > 0)
		})
	}
}

// TestRouteMatcher verifies that the RouteMatcher correctly identifies CodeConnections requests.
func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "connections_collection", path: "/connections", want: true},
		{
			name: "connections_item",
			path: "/connections/arn:aws:codeconnections:us-east-1:123:connection/abc",
			want: true,
		},
		{
			name: "tags_codeconnections_arn",
			path: "/tags/arn:aws:codeconnections:us-east-1:123:connection/abc",
			want: true,
		},
		{
			name: "tags_codestar_connections_arn",
			path: "/tags/arn:aws:codestar-connections:us-east-1:123:connection/abc",
			want: true,
		},
		{name: "unrelated_path", path: "/applications", want: false},
		{name: "backup_path", path: "/backup-vaults", want: false},
		{name: "tags_other_arn", path: "/tags/arn:aws:backup:us-east-1:123:vault:v", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestExtractOperationAndResource verifies ExtractOperation and ExtractResource for various REST paths.
func TestExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name    string
		method  string
		path    string
		wantOp  string
		wantRes string
	}{
		{
			name:   "create_connection",
			method: http.MethodPost,
			path:   "/connections",
			wantOp: "CreateConnection",
		},
		{
			name:   "list_connections",
			method: http.MethodGet,
			path:   "/connections",
			wantOp: "ListConnections",
		},
		{
			name:    "get_connection",
			method:  http.MethodGet,
			path:    "/connections/arn:aws:codeconnections:us-east-1:123:connection/abc",
			wantOp:  "GetConnection",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:    "delete_connection",
			method:  http.MethodDelete,
			path:    "/connections/arn:aws:codeconnections:us-east-1:123:connection/abc",
			wantOp:  "DeleteConnection",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:    "tag_resource_codeconnections",
			method:  http.MethodPost,
			path:    "/tags/arn:aws:codeconnections:us-east-1:123:connection/abc",
			wantOp:  "TagResource",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:    "tag_resource_codestar",
			method:  http.MethodPost,
			path:    "/tags/arn:aws:codestar-connections:us-east-1:123:connection/abc",
			wantOp:  "TagResource",
			wantRes: "arn:aws:codestar-connections:us-east-1:123:connection/abc",
		},
		{
			name:    "untag_resource",
			method:  http.MethodDelete,
			path:    "/tags/arn:aws:codeconnections:us-east-1:123:connection/abc",
			wantOp:  "UntagResource",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:    "list_tags_for_resource",
			method:  http.MethodGet,
			path:    "/tags/arn:aws:codeconnections:us-east-1:123:connection/abc",
			wantOp:  "ListTagsForResource",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:   "unknown_operation",
			method: http.MethodPatch,
			path:   "/connections",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

// TestCreateConnection exercises the CreateConnection handler.
func TestCreateConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantArn    bool
	}{
		{
			name: "success",
			body: map[string]any{
				"ConnectionName": "my-conn",
				"ProviderType":   "GitHub",
				"Tags":           []map[string]string{{"Key": "Env", "Value": "test"}},
			},
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"ProviderType": "GitHub"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_body",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doREST(t, h, http.MethodPost, "/connections", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantArn {
				resp := parseResp(t, rec)
				assert.NotEmpty(t, resp["ConnectionArn"])
			}
		})
	}
}

// TestGetConnection exercises the GetConnection handler.
func TestGetConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, h *codeconnections.Handler) string
		name        string
		wantName    string
		wantType    string
		wantStatus2 string
		wantStatus  int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "my-conn", "GitHub")
			},
			wantStatus:  http.StatusOK,
			wantName:    "my-conn",
			wantType:    "GitHub",
			wantStatus2: "AVAILABLE",
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:connection/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)
			rec := doREST(t, h, http.MethodGet, "/connections/"+connArn, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				conn, ok := resp["Connection"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, conn["ConnectionName"])
				assert.Equal(t, tt.wantType, conn["ProviderType"])
				assert.Equal(t, tt.wantStatus2, conn["ConnectionStatus"])
				assert.Equal(t, "123456789012", conn["OwnerAccountId"])
				assert.NotEmpty(t, conn["ConnectionArn"])
			}
		})
	}
}

// TestListConnections exercises the ListConnections handler.
func TestListConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codeconnections.Handler)
		name       string
		path       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty_list",
			setup:      func(_ *testing.T, _ *codeconnections.Handler) {},
			path:       "/connections",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "multiple_connections",
			setup: func(t *testing.T, h *codeconnections.Handler) {
				t.Helper()
				createConn(t, h, "conn1", "GitHub")
				createConn(t, h, "conn2", "GitLab")
			},
			path:       "/connections",
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "filtered_by_provider_type",
			setup: func(t *testing.T, h *codeconnections.Handler) {
				t.Helper()
				createConn(t, h, "conn1", "GitHub")
				createConn(t, h, "conn2", "GitLab")
			},
			path:       "/connections?providerType=GitHub",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.setup(t, h)

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResp(t, rec)
			conns, ok := resp["Connections"].([]any)
			require.True(t, ok)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}

// TestDeleteConnection exercises the DeleteConnection handler.
func TestDeleteConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codeconnections.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "my-conn", "GitHub")
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:connection/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)
			rec := doREST(t, h, http.MethodDelete, "/connections/"+connArn, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				getRec := doREST(t, h, http.MethodGet, "/connections/"+connArn, nil)
				assert.Equal(t, http.StatusBadRequest, getRec.Code)
			}
		})
	}
}

// TestTagResource exercises the TagResource handler.
func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codeconnections.Handler) string
		name       string
		inputTags  []map[string]string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "conn", "GitHub")
			},
			inputTags:  []map[string]string{{"Key": "Team", "Value": "platform"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:connection/missing"
			},
			inputTags:  []map[string]string{{"Key": "k", "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "bad_body",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "conn", "GitHub")
			},
			inputTags:  nil, // signals bad body test
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)

			var rec *httptest.ResponseRecorder
			if tt.inputTags == nil {
				req := httptest.NewRequest(http.MethodPost, "/tags/"+connArn, bytes.NewBufferString("notjson"))
				req.Header.Set("Content-Type", "application/json")
				rec2 := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rec2)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = rec2
			} else {
				rec = doREST(t, h, http.MethodPost, "/tags/"+connArn, map[string]any{
					"Tags": tt.inputTags,
				})
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestUntagResource exercises the UntagResource handler.
func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *codeconnections.Handler) string
		name          string
		tagsBefore    []map[string]string
		keysToRemove  []string
		wantStatus    int
		wantTagsAfter int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "conn", "GitHub")
			},
			tagsBefore:    []map[string]string{{"Key": "Team", "Value": "p"}, {"Key": "Env", "Value": "prod"}},
			keysToRemove:  []string{"Team"},
			wantStatus:    http.StatusOK,
			wantTagsAfter: 1,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:connection/missing"
			},
			keysToRemove: []string{"k"},
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)

			if len(tt.tagsBefore) > 0 {
				tagRec := doREST(t, h, http.MethodPost, "/tags/"+connArn, map[string]any{
					"Tags": tt.tagsBefore,
				})
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			query := ""
			for _, k := range tt.keysToRemove {
				if query == "" {
					query = "?tagKeys=" + k
				} else {
					query += "&tagKeys=" + k
				}
			}

			req := httptest.NewRequest(http.MethodDelete, "/tags/"+connArn+query, nil)
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				listRec := doREST(t, h, http.MethodGet, "/tags/"+connArn, nil)
				resp := parseResp(t, listRec)
				tags, ok := resp["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, tt.wantTagsAfter)
			}
		})
	}
}

// TestListTagsForResource exercises the ListTagsForResource handler.
func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codeconnections.Handler) string
		name       string
		arnPrefix  string
		tagsToAdd  []map[string]string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success_with_tags",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "conn", "GitHub")
			},
			tagsToAdd:  []map[string]string{{"Key": "A", "Value": "1"}, {"Key": "B", "Value": "2"}},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:connection/missing"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "codestar_connections_prefix_not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:123:connection/missing"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)

			if len(tt.tagsToAdd) > 0 {
				tagRec := doREST(t, h, http.MethodPost, "/tags/"+connArn, map[string]any{
					"Tags": tt.tagsToAdd,
				})
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			rec := doREST(t, h, http.MethodGet, "/tags/"+connArn, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				tags, ok := resp["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, tt.wantCount)
			}
		})
	}
}

// TestCreateConnectionBadBody verifies bad JSON body handling.
func TestCreateConnectionBadBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rawBody    string
		wantStatus int
	}{
		{name: "not_json", rawBody: "notjson", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			req := httptest.NewRequest(http.MethodPost, "/connections", bytes.NewBufferString(tt.rawBody))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestUnknownOperation verifies that unknown operations return 404.
func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{name: "patch_connections", method: http.MethodPatch, path: "/connections", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestBackendListConnections exercises ListConnections filtering directly.
func TestBackendListConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *codeconnections.InMemoryBackend)
		name         string
		filter       string
		wantProvider string
		wantCount    int
	}{
		{
			name: "no_filter_returns_all",
			setup: func(t *testing.T, b *codeconnections.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateConnection("c1", "GitHub", nil)
				require.NoError(t, err)
				_, err = b.CreateConnection("c2", "GitLab", nil)
				require.NoError(t, err)
			},
			filter:    "",
			wantCount: 2,
		},
		{
			name: "filter_by_provider",
			setup: func(t *testing.T, b *codeconnections.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateConnection("c1", "GitHub", nil)
				require.NoError(t, err)
				_, err = b.CreateConnection("c2", "GitLab", nil)
				require.NoError(t, err)
			},
			filter:       "GitHub",
			wantCount:    1,
			wantProvider: "GitHub",
		},
		{
			name:      "empty_backend",
			setup:     func(_ *testing.T, _ *codeconnections.InMemoryBackend) {},
			filter:    "",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(t, b)
			conns := b.ListConnections(tt.filter)
			assert.Len(t, conns, tt.wantCount)

			if tt.wantProvider != "" {
				for _, c := range conns {
					assert.Equal(t, tt.wantProvider, c.ProviderType)
				}
			}
		})
	}
}

// TestBackendNotFoundErrors exercises not-found error paths in backend methods.
func TestBackendNotFoundErrors(t *testing.T) {
	t.Parallel()

	const missingArn = "arn:aws:codeconnections:us-east-1:123:connection/missing"

	tests := []struct {
		call    func(b *codeconnections.InMemoryBackend) error
		name    string
		wantErr bool
	}{
		{
			name:    "GetConnection_not_found",
			wantErr: true,
			call: func(b *codeconnections.InMemoryBackend) error {
				_, err := b.GetConnection(missingArn)

				return err
			},
		},
		{
			name:    "DeleteConnection_not_found",
			wantErr: true,
			call:    func(b *codeconnections.InMemoryBackend) error { return b.DeleteConnection(missingArn) },
		},
		{
			name:    "TagResource_not_found",
			wantErr: true,
			call: func(b *codeconnections.InMemoryBackend) error {
				return b.TagResource(missingArn, map[string]string{"k": "v"})
			},
		},
		{
			name:    "UntagResource_not_found",
			wantErr: true,
			call:    func(b *codeconnections.InMemoryBackend) error { return b.UntagResource(missingArn, []string{"k"}) },
		},
		{
			name:    "ListTagsForResource_not_found",
			wantErr: true,
			call: func(b *codeconnections.InMemoryBackend) error {
				_, err := b.ListTagsForResource(missingArn)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			err := tt.call(b)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackendCreateAndGet exercises happy-path create and get.
func TestBackendCreateAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		inputTags    map[string]string
		name         string
		connName     string
		providerType string
		wantStatus   string
	}{
		{
			name:         "github_connection",
			connName:     "my-conn",
			providerType: "GitHub",
			inputTags:    map[string]string{"Env": "prod"},
			wantStatus:   "AVAILABLE",
		},
		{
			name:         "gitlab_connection_no_tags",
			connName:     "gl-conn",
			providerType: "GitLab",
			wantStatus:   "AVAILABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			conn, err := b.CreateConnection(tt.connName, tt.providerType, tt.inputTags)
			require.NoError(t, err)
			assert.NotEmpty(t, conn.ConnectionArn)
			assert.Equal(t, tt.connName, conn.ConnectionName)
			assert.Equal(t, tt.providerType, conn.ProviderType)
			assert.Equal(t, tt.wantStatus, conn.Status)
			assert.Equal(t, "123456789012", conn.OwnerAccountID)
			assert.Contains(t, conn.ConnectionArn, "arn:aws:codeconnections:us-east-1:123456789012:connection/")

			got, err := b.GetConnection(conn.ConnectionArn)
			require.NoError(t, err)
			assert.Equal(t, conn.ConnectionArn, got.ConnectionArn)
		})
	}
}
