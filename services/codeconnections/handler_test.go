package codeconnections_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

const ccTargetPrefix = "CodeConnections_20231201."

func newTestHandler() *codeconnections.Handler {
	backend := codeconnections.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return codeconnections.NewHandler(backend)
}

// doJSON sends a POST / request with the X-Amz-Target header set to action and
// the given body marshalled as JSON. This simulates the JSON 1.0 protocol used
// by the AWS CodeConnections SDK.
func doJSON(
	t *testing.T,
	h *codeconnections.Handler,
	action string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", ccTargetPrefix+action)
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

	rec := doJSON(t, h, "CreateConnection", map[string]any{
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
		{
			name:     "GetSupportedOperations_TagResource",
			got:      h.GetSupportedOperations(),
			contains: "TagResource",
		},
		{
			name:     "GetSupportedOperations_UntagResource",
			got:      h.GetSupportedOperations(),
			contains: "UntagResource",
		},
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

// TestRouteMatcher verifies that the RouteMatcher correctly identifies CodeConnections requests
// via the X-Amz-Target header.
func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "create_connection_target",
			target: "CodeConnections_20231201.CreateConnection",
			want:   true,
		},
		{
			name:   "list_connections_target",
			target: "CodeConnections_20231201.ListConnections",
			want:   true,
		},
		{
			name:   "tag_resource_target",
			target: "CodeConnections_20231201.TagResource",
			want:   true,
		},
		{
			name:   "other_service_target",
			target: "AWSCognitoIdentityProviderService.CreateUserPool",
			want:   false,
		},
		{
			name:   "empty_target",
			target: "",
			want:   false,
		},
		{
			name:   "partial_prefix",
			target: "CodeConnections.",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestExtractOperationAndResource verifies ExtractOperation and ExtractResource
// for various X-Amz-Target values and JSON bodies.
func TestExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name    string
		target  string
		body    map[string]any
		wantOp  string
		wantRes string
	}{
		{
			name:   "create_connection",
			target: ccTargetPrefix + "CreateConnection",
			wantOp: "CreateConnection",
		},
		{
			name:   "list_connections",
			target: ccTargetPrefix + "ListConnections",
			wantOp: "ListConnections",
		},
		{
			name:   "get_connection",
			target: ccTargetPrefix + "GetConnection",
			body: map[string]any{
				"ConnectionArn": "arn:aws:codeconnections:us-east-1:123:connection/abc",
			},
			wantOp:  "GetConnection",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:   "delete_connection",
			target: ccTargetPrefix + "DeleteConnection",
			body: map[string]any{
				"ConnectionArn": "arn:aws:codeconnections:us-east-1:123:connection/abc",
			},
			wantOp:  "DeleteConnection",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:   "tag_resource",
			target: ccTargetPrefix + "TagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:codeconnections:us-east-1:123:connection/abc",
			},
			wantOp:  "TagResource",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:   "untag_resource",
			target: ccTargetPrefix + "UntagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:codeconnections:us-east-1:123:connection/abc",
			},
			wantOp:  "UntagResource",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
		{
			name:   "list_tags_for_resource",
			target: ccTargetPrefix + "ListTagsForResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:codeconnections:us-east-1:123:connection/abc",
			},
			wantOp:  "ListTagsForResource",
			wantRes: "arn:aws:codeconnections:us-east-1:123:connection/abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var bodyBytes []byte

			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("X-Amz-Target", tt.target)
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
			rec := doJSON(t, h, "CreateConnection", tt.body)
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
			rec := doJSON(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
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
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty_list",
			setup:      func(_ *testing.T, _ *codeconnections.Handler) {},
			body:       map[string]any{},
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
			body:       map[string]any{},
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
			body:       map[string]any{"ProviderTypeFilter": "GitHub"},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.setup(t, h)

			rec := doJSON(t, h, "ListConnections", tt.body)
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
			rec := doJSON(t, h, "DeleteConnection", map[string]any{"ConnectionArn": connArn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				getRec := doJSON(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)

			rec := doJSON(t, h, "TagResource", map[string]any{
				"ResourceArn": connArn,
				"Tags":        tt.inputTags,
			})

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
			tagsBefore: []map[string]string{
				{"Key": "Team", "Value": "p"},
				{"Key": "Env", "Value": "prod"},
			},
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
				tagRec := doJSON(t, h, "TagResource", map[string]any{
					"ResourceArn": connArn,
					"Tags":        tt.tagsBefore,
				})
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			rec := doJSON(t, h, "UntagResource", map[string]any{
				"ResourceArn": connArn,
				"TagKeys":     tt.keysToRemove,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				listRec := doJSON(
					t,
					h,
					"ListTagsForResource",
					map[string]any{"ResourceArn": connArn},
				)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)

			if len(tt.tagsToAdd) > 0 {
				tagRec := doJSON(t, h, "TagResource", map[string]any{
					"ResourceArn": connArn,
					"Tags":        tt.tagsToAdd,
				})
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			rec := doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
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

// TestMissingTarget verifies that requests with no X-Amz-Target return 400.
func TestMissingTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "no_target_header", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			req := httptest.NewRequest(
				http.MethodPost,
				"/",
				bytes.NewBufferString(`{"ConnectionName":"test-conn"}`),
			)
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestUnknownOperation verifies that unknown operations return 400.
func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		wantStatus int
	}{
		{
			name:       "unknown_action",
			target:     ccTargetPrefix + "DescribeNonExistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
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
				_, err := b.CreateConnection(context.Background(), "c1", "GitHub", "", nil)
				require.NoError(t, err)
				_, err = b.CreateConnection(context.Background(), "c2", "GitLab", "", nil)
				require.NoError(t, err)
			},
			filter:    "",
			wantCount: 2,
		},
		{
			name: "filter_by_provider",
			setup: func(t *testing.T, b *codeconnections.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateConnection(context.Background(), "c1", "GitHub", "", nil)
				require.NoError(t, err)
				_, err = b.CreateConnection(context.Background(), "c2", "GitLab", "", nil)
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
			conns := b.ListConnections(context.Background(), tt.filter, "")
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
				_, err := b.GetConnection(context.Background(), missingArn)

				return err
			},
		},
		{
			name:    "DeleteConnection_not_found",
			wantErr: true,
			call: func(b *codeconnections.InMemoryBackend) error {
				return b.DeleteConnection(context.Background(), missingArn)
			},
		},
		{
			name:    "TagResource_not_found",
			wantErr: true,
			call: func(b *codeconnections.InMemoryBackend) error {
				return b.TagResource(context.Background(), missingArn, map[string]string{"k": "v"})
			},
		},
		{
			name:    "UntagResource_not_found",
			wantErr: true,
			call: func(b *codeconnections.InMemoryBackend) error {
				return b.UntagResource(context.Background(), missingArn, []string{"k"})
			},
		},
		{
			name:    "ListTagsForResource_not_found",
			wantErr: true,
			call: func(b *codeconnections.InMemoryBackend) error {
				_, err := b.ListTagsForResource(context.Background(), missingArn)

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
			conn, err := b.CreateConnection(
				context.Background(),
				tt.connName,
				tt.providerType,
				"",
				tt.inputTags,
			)
			require.NoError(t, err)
			assert.NotEmpty(t, conn.ConnectionArn)
			assert.Equal(t, tt.connName, conn.ConnectionName)
			assert.Equal(t, tt.providerType, conn.ProviderType)
			assert.Equal(t, tt.wantStatus, conn.Status)
			assert.Equal(t, "123456789012", conn.OwnerAccountID)
			assert.Contains(
				t,
				conn.ConnectionArn,
				"arn:aws:codeconnections:us-east-1:123456789012:connection/",
			)

			got, err := b.GetConnection(context.Background(), conn.ConnectionArn)
			require.NoError(t, err)
			assert.Equal(t, conn.ConnectionArn, got.ConnectionArn)
		})
	}
}

// TestListConnectionsPagination verifies NextToken/MaxResults pagination for ListConnections.
func TestListConnectionsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		count      int
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:       "first_page_limited",
			count:      3,
			maxResults: 2,
			wantCount:  2,
			wantToken:  true,
		},
		{
			name:       "all_results_no_token",
			count:      2,
			maxResults: 10,
			wantCount:  2,
			wantToken:  false,
		},
		{
			name:       "zero_max_uses_default",
			count:      2,
			maxResults: 0,
			wantCount:  2,
			wantToken:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tt.count {
				createConn(t, h, "conn-"+strconv.Itoa(i), "GitHub")
			}

			body := map[string]any{}
			if tt.maxResults > 0 {
				body["MaxResults"] = tt.maxResults
			}

			rec := doJSON(t, h, "ListConnections", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conns, ok := resp["Connections"].([]any)
			require.True(t, ok)
			assert.Len(t, conns, tt.wantCount)

			_, hasToken := resp["NextToken"]
			assert.Equal(t, tt.wantToken, hasToken)
		})
	}
}

// TestListConnectionsContinuation verifies two-page traversal using NextToken.
func TestListConnectionsContinuation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for i := range 3 {
		createConn(t, h, "conn-"+strconv.Itoa(i), "GitHub")
	}

	// First page: 2 of 3.
	rec1 := doJSON(t, h, "ListConnections", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResp(t, rec1)
	page1, ok := resp1["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	nextToken, hasToken := resp1["NextToken"].(string)
	require.True(t, hasToken, "expected NextToken in first page response")
	require.NotEmpty(t, nextToken)

	// Second page: remaining 1.
	rec2 := doJSON(t, h, "ListConnections", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	page2, ok := resp2["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)

	_, stillHasToken := resp2["NextToken"]
	assert.False(t, stillHasToken, "last page should have no NextToken")

	// Collectively all connection names present.
	names := make([]string, 0, 3)
	for _, item := range append(page1, page2...) {
		conn := item.(map[string]any)
		names = append(names, conn["ConnectionName"].(string))
	}
	assert.ElementsMatch(t, []string{"conn-0", "conn-1", "conn-2"}, names)
}

// createHost is a test helper that creates a host and returns its ARN.
func createHost(
	t *testing.T,
	h *codeconnections.Handler,
	name, providerType, endpoint string,
) string {
	t.Helper()

	rec := doJSON(t, h, "CreateHost", map[string]any{
		"Name":             name,
		"ProviderType":     providerType,
		"ProviderEndpoint": endpoint,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)

	hostArn, ok := resp["HostArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, hostArn)

	return hostArn
}

// createRepositoryLink is a test helper that creates a repository link and returns its ID.
func createRepositoryLink(
	t *testing.T,
	h *codeconnections.Handler,
	connectionArn, ownerID, repoName string,
) string {
	t.Helper()

	rec := doJSON(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":  connectionArn,
		"OwnerId":        ownerID,
		"RepositoryName": repoName,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)

	info, ok := resp["RepositoryLinkInfo"].(map[string]any)
	require.True(t, ok)

	linkID, ok := info["RepositoryLinkId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, linkID)

	return linkID
}

// TestCreateHost exercises the CreateHost handler.
func TestCreateHost(t *testing.T) {
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
				"Name":             "my-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://ghe.example.com",
			},
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
		{
			name: "missing_name",
			body: map[string]any{
				"ProviderEndpoint": "https://ghe.example.com",
				"ProviderType":     "GitHubEnterpriseServer",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_endpoint",
			body:       map[string]any{"Name": "my-host", "ProviderType": "GitHubEnterpriseServer"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doJSON(t, h, "CreateHost", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantArn {
				resp := parseResp(t, rec)
				assert.NotEmpty(t, resp["HostArn"])
			}
		})
	}
}

// TestGetHost exercises the GetHost handler.
func TestGetHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupHostArn   func(t *testing.T, h *codeconnections.Handler) string
		name           string
		wantName       string
		wantEndpoint   string
		wantHostStatus string
		wantStatus     int
	}{
		{
			name: "success",
			setupHostArn: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createHost(
					t,
					h,
					"my-host",
					"GitHubEnterpriseServer",
					"https://ghe.example.com",
				)
			},
			wantStatus:     http.StatusOK,
			wantName:       "my-host",
			wantEndpoint:   "https://ghe.example.com",
			wantHostStatus: "AVAILABLE",
		},
		{
			name: "not_found",
			setupHostArn: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:host/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_arn",
			setupHostArn: func(_ *testing.T, _ *codeconnections.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn := tt.setupHostArn(t, h)
			rec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				assert.Equal(t, tt.wantName, resp["Name"])
				assert.Equal(t, tt.wantEndpoint, resp["ProviderEndpoint"])
				assert.Equal(t, tt.wantHostStatus, resp["Status"])
			}
		})
	}
}

// TestDeleteHost exercises the DeleteHost handler.
func TestDeleteHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupHostArn func(t *testing.T, h *codeconnections.Handler) string
		name         string
		wantStatus   int
	}{
		{
			name: "success",
			setupHostArn: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createHost(
					t,
					h,
					"my-host",
					"GitHubEnterpriseServer",
					"https://ghe.example.com",
				)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setupHostArn: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:host/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn := tt.setupHostArn(t, h)
			rec := doJSON(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				getRec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
				assert.Equal(t, http.StatusBadRequest, getRec.Code)
			}
		})
	}
}

// TestCreateRepositoryLink exercises the CreateRepositoryLink handler.
func TestCreateRepositoryLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantLink   bool
	}{
		{
			name: "success",
			body: map[string]any{
				"ConnectionArn":  "arn:aws:codeconnections:us-east-1:123:connection/abc",
				"OwnerId":        "my-org",
				"RepositoryName": "my-repo",
			},
			wantStatus: http.StatusOK,
			wantLink:   true,
		},
		{
			name:       "missing_connection_arn",
			body:       map[string]any{"OwnerId": "my-org", "RepositoryName": "my-repo"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_owner_id",
			body: map[string]any{
				"ConnectionArn":  "arn:aws:codeconnections:us-east-1:123:connection/abc",
				"RepositoryName": "my-repo",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_repository_name",
			body: map[string]any{
				"ConnectionArn": "arn:aws:codeconnections:us-east-1:123:connection/abc",
				"OwnerId":       "my-org",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doJSON(t, h, "CreateRepositoryLink", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantLink {
				resp := parseResp(t, rec)
				info, ok := resp["RepositoryLinkInfo"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, info["RepositoryLinkId"])
				assert.NotEmpty(t, info["RepositoryLinkArn"])
				assert.Equal(t, "my-org", info["OwnerId"])
				assert.Equal(t, "my-repo", info["RepositoryName"])
			}
		})
	}
}

// TestGetRepositoryLink exercises the GetRepositoryLink handler.
func TestGetRepositoryLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupLinkID  func(t *testing.T, h *codeconnections.Handler) string
		name         string
		wantOwner    string
		wantRepoName string
		wantStatus   int
	}{
		{
			name: "success",
			setupLinkID: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()
				connArn := createConn(t, h, "my-conn", "GitHub")

				return createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			},
			wantStatus:   http.StatusOK,
			wantOwner:    "my-org",
			wantRepoName: "my-repo",
		},
		{
			name: "not_found",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "nonexistent-id"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_id",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := tt.setupLinkID(t, h)
			rec := doJSON(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				info, ok := resp["RepositoryLinkInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantOwner, info["OwnerId"])
				assert.Equal(t, tt.wantRepoName, info["RepositoryName"])
			}
		})
	}
}

// TestDeleteRepositoryLink exercises the DeleteRepositoryLink handler.
func TestDeleteRepositoryLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupLinkID func(t *testing.T, h *codeconnections.Handler) string
		name        string
		wantStatus  int
	}{
		{
			name: "success",
			setupLinkID: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()
				connArn := createConn(t, h, "my-conn", "GitHub")

				return createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "nonexistent-id"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := tt.setupLinkID(t, h)
			rec := doJSON(t, h, "DeleteRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				getRec := doJSON(
					t,
					h,
					"GetRepositoryLink",
					map[string]any{"RepositoryLinkId": linkID},
				)
				assert.Equal(t, http.StatusBadRequest, getRec.Code)
			}
		})
	}
}

// TestCreateSyncConfiguration exercises the CreateSyncConfiguration handler.
func TestCreateSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupLinkID func(t *testing.T, h *codeconnections.Handler) string
		body        func(linkID string) map[string]any
		name        string
		wantStatus  int
		wantSync    bool
	}{
		{
			name: "success",
			setupLinkID: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()
				connArn := createConn(t, h, "my-conn", "GitHub")

				return createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			},
			body: func(linkID string) map[string]any {
				return map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				}
			},
			wantStatus: http.StatusOK,
			wantSync:   true,
		},
		{
			name:        "missing_branch",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string { return "some-id" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": "some-id",
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:        "missing_config_file",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string { return "some-id" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"Branch":           "main",
					"RepositoryLinkId": "some-id",
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:        "missing_role_arn",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string { return "some-id" },
			body: func(_ string) map[string]any {
				return map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": "some-id",
					"ResourceName":     "my-stack",
					"SyncType":         "CFN_STACK_SYNC",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := tt.setupLinkID(t, h)
			rec := doJSON(t, h, "CreateSyncConfiguration", tt.body(linkID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSync {
				resp := parseResp(t, rec)
				cfg, ok := resp["SyncConfiguration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "main", cfg["Branch"])
				assert.Equal(t, "config.yaml", cfg["ConfigFile"])
				assert.Equal(t, "my-stack", cfg["ResourceName"])
				assert.Equal(t, "CFN_STACK_SYNC", cfg["SyncType"])
			}
		})
	}
}

// TestDeleteSyncConfiguration exercises the DeleteSyncConfiguration handler.
func TestDeleteSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "my-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doJSON(t, h, "DeleteSyncConfiguration", map[string]any{
				"ResourceName": "my-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestGetRepositorySyncStatus exercises the GetRepositorySyncStatus handler.
func TestGetRepositorySyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupLinkID func(t *testing.T, h *codeconnections.Handler) string
		name        string
		wantStatus  int
		wantSync    bool
	}{
		{
			name: "success",
			setupLinkID: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()
				connArn := createConn(t, h, "my-conn", "GitHub")

				return createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			},
			wantStatus: http.StatusOK,
			wantSync:   true,
		},
		{
			name: "not_found",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "nonexistent-id"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_repository_link_id",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := tt.setupLinkID(t, h)
			rec := doJSON(t, h, "GetRepositorySyncStatus", map[string]any{
				"RepositoryLinkId": linkID,
				"Branch":           "main",
				"SyncType":         "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSync {
				resp := parseResp(t, rec)
				latest, ok := resp["LatestSync"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "SUCCEEDED", latest["Status"])
				assert.NotEmpty(t, latest["StartedAt"])
			}
		})
	}
}

// TestGetResourceSyncStatus exercises the GetResourceSyncStatus handler.
func TestGetResourceSyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantSync   bool
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
			wantSync:   true,
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "my-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "my-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doJSON(t, h, "GetResourceSyncStatus", map[string]any{
				"ResourceName": "my-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSync {
				resp := parseResp(t, rec)
				latest, ok := resp["LatestSync"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "SUCCEEDED", latest["Status"])
				assert.NotEmpty(t, latest["StartedAt"])
			}
		})
	}
}

// TestRepositoryLinkProviderTypeDerivedFromConnection verifies that provider type
// is inherited from the associated connection when creating a repository link.
func TestRepositoryLinkProviderTypeDerivedFromConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	connArn := createConn(t, h, "my-conn", "GitHub")
	linkID := createRepositoryLink(t, h, connArn, "acme-corp", "acme-service")

	rec := doJSON(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	info, ok := resp["RepositoryLinkInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "GitHub", info["ProviderType"])
	assert.Equal(t, "acme-corp", info["OwnerId"])
}

// TestSyncConfigurationRoundTrip verifies create/delete round-trip for SyncConfiguration.
func TestSyncConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	connArn := createConn(t, h, "my-conn", "GitHub")
	linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

	createRec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "config.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "my-stack",
		"RoleArn":          "arn:aws:iam::123456789012:role/sync-role",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	resp := parseResp(t, createRec)
	cfg, ok := resp["SyncConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-org", cfg["OwnerId"])
	assert.Equal(t, "my-repo", cfg["RepositoryName"])
	assert.Equal(t, "GitHub", cfg["ProviderType"])

	// Verify resource sync status works.
	syncRec := doJSON(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, syncRec.Code)

	// Delete and verify gone.
	delRec := doJSON(t, h, "DeleteSyncConfiguration", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Resource sync status should now return not found.
	afterDelRec := doJSON(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "my-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusBadRequest, afterDelRec.Code)
}

// --- Refinement 1 tests ---

// TestRefinement1_Reset exercises Reset() on the backend and handler.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, h *codeconnections.Handler)
		name  string
	}{
		{
			name: "reset_clears_connections",
			setup: func(t *testing.T, h *codeconnections.Handler) {
				t.Helper()
				createConn(t, h, "conn-to-clear", "GitHub")
				h.Reset()
				rec := doJSON(t, h, "ListConnections", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				conns, ok := resp["Connections"].([]any)
				require.True(t, ok)
				assert.Empty(t, conns)
			},
		},
		{
			name: "reset_clears_hosts",
			setup: func(t *testing.T, h *codeconnections.Handler) {
				t.Helper()
				hostArn := createHost(
					t,
					h,
					"host-to-clear",
					"GitHubEnterpriseServer",
					"https://ghe.example.com",
				)
				h.Reset()
				rec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "reset_clears_repository_links",
			setup: func(t *testing.T, h *codeconnections.Handler) {
				t.Helper()
				connArn := createConn(t, h, "conn-reset", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				h.Reset()
				rec := doJSON(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "reset_clears_sync_configurations",
			setup: func(t *testing.T, h *codeconnections.Handler) {
				t.Helper()
				connArn := createConn(t, h, "conn-sync-reset", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "reset-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/role",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				h.Reset()
				rec = doJSON(t, h, "GetResourceSyncStatus", map[string]any{
					"ResourceName": "reset-stack",
					"SyncType":     "CFN_STACK_SYNC",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)
		})
	}
}

// TestRefinement1_ProviderInit_NilCtx verifies that Provider.Init handles a nil AppContext.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_ctx_succeeds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &codeconnections.Provider{}
			reg, err := p.Init(nil)
			require.NoError(t, err)
			assert.NotNil(t, reg)
		})
	}
}

// TestRefinement1_ConnectionNameUniqueness verifies duplicate connection names are rejected.
func TestRefinement1_ConnectionNameUniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		connName      string
		wantStatus    int
		wantDuplicate bool
	}{
		{
			name:       "unique_name_succeeds",
			connName:   "only-created-once",
			wantStatus: http.StatusOK,
		},
		{
			name:          "duplicate_name_rejected",
			connName:      "duplicate-target",
			wantDuplicate: true,
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			// Pre-seed the duplicate case.
			if tt.wantDuplicate {
				createConn(t, h, tt.connName, "GitHub")
			}

			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": tt.connName,
				"ProviderType":   "GitHub",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_ProviderTypeValidation verifies invalid provider types are rejected.
func TestRefinement1_ProviderTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		providerType string
		wantStatus   int
	}{
		{name: "valid_github", providerType: "GitHub", wantStatus: http.StatusOK},
		{name: "valid_gitlab", providerType: "GitLab", wantStatus: http.StatusOK},
		{name: "valid_bitbucket", providerType: "Bitbucket", wantStatus: http.StatusOK},
		{name: "valid_ghe", providerType: "GitHubEnterpriseServer", wantStatus: http.StatusOK},
		{
			name:         "invalid_provider",
			providerType: "InvalidProvider",
			wantStatus:   http.StatusBadRequest,
		},
		{name: "empty_provider_rejected", providerType: "", wantStatus: http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "conn-" + strconv.Itoa(i),
				"ProviderType":   tt.providerType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_SyncTypeValidation verifies invalid sync types are rejected.
func TestRefinement1_SyncTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		syncType   string
		name       string
		wantStatus int
	}{
		{name: "valid_cfn_stack_sync", syncType: "CFN_STACK_SYNC", wantStatus: http.StatusOK},
		{name: "invalid_sync_type", syncType: "INVALID_SYNC", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			connArn := createConn(t, h, "conn-synctype", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":           "main",
				"ConfigFile":       "config.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "my-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/role",
				"SyncType":         tt.syncType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_TagsOnHosts verifies that tags can be managed on hosts.
func TestRefinement1_TagsOnHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag_untag_list_on_host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			hostArn := createHost(
				t,
				h,
				"tagged-host",
				"GitLabSelfManaged",
				"https://gitlab.example.com",
			)

			tagRec := doJSON(t, h, "TagResource", map[string]any{
				"ResourceArn": hostArn,
				"Tags": []map[string]string{
					{"Key": "Env", "Value": "prod"},
					{"Key": "Team", "Value": "infra"},
				},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := parseResp(t, listRec)
			tagArr, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tagArr, 2)

			untagRec := doJSON(t, h, "UntagResource", map[string]any{
				"ResourceArn": hostArn,
				"TagKeys":     []string{"Env"},
			})
			require.Equal(t, http.StatusOK, untagRec.Code)

			listRec2 := doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
			require.Equal(t, http.StatusOK, listRec2.Code)
			resp2 := parseResp(t, listRec2)
			tagArr2, ok := resp2["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tagArr2, 1)
		})
	}
}

// TestRefinement1_SortedTagsOutput verifies that ListTagsForResource returns sorted tags.
func TestRefinement1_SortedTagsOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inputTags []map[string]string
		wantKeys  []string
	}{
		{
			name: "sorted_alpha",
			inputTags: []map[string]string{
				{"Key": "Zebra", "Value": "z"},
				{"Key": "Apple", "Value": "a"},
				{"Key": "Mango", "Value": "m"},
			},
			wantKeys: []string{"Apple", "Mango", "Zebra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			connArn := createConn(t, h, "sorted-tag-conn", "GitHub")

			rec := doJSON(t, h, "TagResource", map[string]any{
				"ResourceArn": connArn,
				"Tags":        tt.inputTags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			listRec := doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := parseResp(t, listRec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)

			gotKeys := make([]string, 0, len(tags))
			for _, tagItem := range tags {
				tmap, isMap := tagItem.(map[string]any)
				require.True(t, isMap)
				gotKeys = append(gotKeys, tmap["Key"].(string))
			}

			assert.Equal(t, tt.wantKeys, gotKeys)
		})
	}
}

// TestRefinement1_HostArnFilter verifies HostArnFilter in ListConnections.
func TestRefinement1_HostArnFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantCount   int
		applyFilter bool
	}{
		{name: "no_filter_returns_all", wantCount: 2, applyFilter: false},
		{name: "host_arn_filter_returns_one", wantCount: 1, applyFilter: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")

			// Seed two connections: one with HostArn, one without.
			conn1 := &codeconnections.Connection{
				ConnectionName: "with-host",
				ConnectionArn:  "arn:aws:codeconnections:us-east-1:123456789012:connection/aaa",
				ProviderType:   "GitHubEnterpriseServer",
				HostArn:        "arn:aws:codeconnections:us-east-1:123456789012:host/hst-1",
				Status:         "AVAILABLE",
				OwnerAccountID: "123456789012",
				Tags:           map[string]string{},
			}
			conn2 := &codeconnections.Connection{
				ConnectionName: "no-host",
				ConnectionArn:  "arn:aws:codeconnections:us-east-1:123456789012:connection/bbb",
				ProviderType:   "GitHub",
				Status:         "AVAILABLE",
				OwnerAccountID: "123456789012",
				Tags:           map[string]string{},
			}
			b.AddConnectionInternal(context.Background(), conn1)
			b.AddConnectionInternal(context.Background(), conn2)

			filter := ""
			if tt.applyFilter {
				filter = "arn:aws:codeconnections:us-east-1:123456789012:host/hst-1"
			}

			conns := b.ListConnections(context.Background(), "", filter)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}

// TestRefinement1_GetConnectionIncludesTags verifies Tags are included in GetConnection response.
func TestRefinement1_GetConnectionIncludesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantTags int
	}{
		{name: "tags_in_get_response", wantTags: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "tagged-conn",
				"ProviderType":   "GitHub",
				"Tags": []map[string]string{
					{"Key": "Env", "Value": "prod"},
					{"Key": "Owner", "Value": "team"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			connArn := parseResp(t, rec)["ConnectionArn"].(string)

			getRec := doJSON(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
			require.Equal(t, http.StatusOK, getRec.Code)
			resp := parseResp(t, getRec)
			conn, ok := resp["Connection"].(map[string]any)
			require.True(t, ok)
			tags, ok := conn["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

// TestRefinement1_CreateHostWithTags verifies that tags can be passed when creating a host.
func TestRefinement1_CreateHostWithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []map[string]string
		wantTags int
	}{
		{
			name: "host_with_tags",
			tags: []map[string]string{
				{"Key": "Owner", "Value": "ops"},
				{"Key": "Tier", "Value": "infra"},
			},
			wantTags: 2,
		},
		{
			name:     "host_without_tags",
			tags:     nil,
			wantTags: 0,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			body := map[string]any{
				"Name":             "tagged-host-" + strconv.Itoa(i),
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://ghe.example.com",
			}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}

			rec := doJSON(t, h, "CreateHost", body)
			require.Equal(t, http.StatusOK, rec.Code)
			hostArn := parseResp(t, rec)["HostArn"].(string)

			getRec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
			require.Equal(t, http.StatusOK, getRec.Code)
			resp := parseResp(t, getRec)
			tags, _ := resp["Tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

// TestRefinement1_ErrValidation verifies ErrValidation is a distinct wrapped error.
func TestRefinement1_ErrValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "is_distinct_from_not_found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.NotEqual(t, codeconnections.ErrNotFound, codeconnections.ErrValidation)
			assert.Error(t, codeconnections.ErrValidation)
		})
	}
}

// TestRefinement1_SnapshotRestore verifies Snapshot/Restore round-trip preserves state.
func TestRefinement1_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot_restore_round_trip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			createConn(t, h, "conn-snap", "GitHub")
			createHost(t, h, "host-snap", "GitHubEnterpriseServer", "https://ghe.example.com")
			connArn := createConn(t, h, "conn-snap2", "GitLab")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			snap := h.Backend.Snapshot()
			require.NotNil(t, snap)

			newBackend := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, newBackend.Restore(snap))

			conns := newBackend.ListConnections(context.Background(), "", "")
			assert.Len(t, conns, 2)

			_, err := newBackend.GetRepositoryLink(context.Background(), linkID)
			require.NoError(t, err)
		})
	}
}

// TestRefinement1_ErrAlreadyExistsMapping verifies that ErrAlreadyExists maps to
// an HTTP error response.
func TestRefinement1_ErrAlreadyExistsMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "duplicate_connection_is_400", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec1 := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "dup-conn",
				"ProviderType":   "GitHub",
			})
			require.Equal(t, http.StatusOK, rec1.Code)

			rec2 := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "dup-conn",
				"ProviderType":   "GitHub",
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
			resp := parseResp(t, rec2)
			assert.Equal(t, "ResourceAlreadyExistsException", resp["__type"])
		})
	}
}

// TestRefinement1_DeleteConnectionCleansIndex verifies the name index is cleared on delete.
func TestRefinement1_DeleteConnectionCleansIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "can_recreate_after_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			arn1 := createConn(t, h, "reuse-conn", "GitHub")

			delRec := doJSON(t, h, "DeleteConnection", map[string]any{"ConnectionArn": arn1})
			require.Equal(t, http.StatusOK, delRec.Code)

			// After deletion, same name should be allowed.
			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "reuse-conn",
				"ProviderType":   "GitLab",
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// TestRefinement1_SeedHelpers verifies AddConnectionInternal and AddHostInternal.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "add_connection_internal"},
		{name: "add_host_internal"},
		{name: "add_repository_link_internal"},
	}

	b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")

	conn := &codeconnections.Connection{
		ConnectionName: "seeded-conn",
		ConnectionArn:  "arn:aws:codeconnections:us-east-1:123456789012:connection/seed-1",
		ProviderType:   "GitHub",
		Status:         "AVAILABLE",
		OwnerAccountID: "123456789012",
		Tags:           map[string]string{"seeded": "true"},
	}

	host := &codeconnections.Host{
		Name:             "seeded-host",
		HostArn:          "arn:aws:codeconnections:us-east-1:123456789012:host/seed-h1",
		ProviderType:     "GitHubEnterpriseServer",
		ProviderEndpoint: "https://ghe.example.com",
		Status:           "AVAILABLE",
		Tags:             map[string]string{},
	}

	link := &codeconnections.RepositoryLink{
		RepositoryLinkID:  "seed-link-1",
		RepositoryLinkArn: "arn:aws:codeconnections:us-east-1:123456789012:repository-link/seed-link-1",
		ConnectionArn:     conn.ConnectionArn,
		OwnerID:           "my-org",
		RepositoryName:    "my-repo",
		ProviderType:      "GitHub",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			switch tt.name {
			case "add_connection_internal":
				b.AddConnectionInternal(context.Background(), conn)
				got, err := b.GetConnection(context.Background(), conn.ConnectionArn)
				require.NoError(t, err)
				assert.Equal(t, "seeded-conn", got.ConnectionName)
			case "add_host_internal":
				b.AddHostInternal(context.Background(), host)
				got, err := b.GetHost(context.Background(), host.HostArn)
				require.NoError(t, err)
				assert.Equal(t, "seeded-host", got.Name)
			case "add_repository_link_internal":
				b.AddRepositoryLinkInternal(context.Background(), link)
				got, err := b.GetRepositoryLink(context.Background(), link.RepositoryLinkID)
				require.NoError(t, err)
				assert.Equal(t, "my-org", got.OwnerID)
			}
		})
	}
}
