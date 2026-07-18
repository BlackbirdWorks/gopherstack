package managedblockchain_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_HandlerMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "metadata methods"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			assert.Equal(t, "ManagedBlockchain", h.Name())
			assert.NotEmpty(t, h.GetSupportedOperations())
			assert.Equal(t, "managedblockchain", h.ChaosServiceName())
			assert.NotEmpty(t, h.ChaosOperations())
			assert.NotEmpty(t, h.ChaosRegions())
			assert.Positive(t, h.MatchPriority())
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		serviceName string
		wantMatch   bool
	}{
		{
			name:        "matches networks path with correct service",
			path:        "/networks",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "matches networks sub-path",
			path:        "/networks/abc/members",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "does not match networks with wrong service",
			path:        "/networks",
			serviceName: "iotwireless",
			wantMatch:   false,
		},
		{
			name:        "does not match unknown path",
			path:        "/unknown",
			serviceName: "managedblockchain",
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)

			if tt.serviceName != "" {
				req.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.serviceName+"/aws4_request")
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_RouteMatcherNewPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		serviceName string
		wantMatch   bool
	}{
		{
			name:        "matches accessors path",
			path:        "/accessors",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "matches accessors sub-path",
			path:        "/accessors/some-id",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "matches invitations path",
			path:        "/invitations",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "matches invitations sub-path",
			path:        "/invitations/inv-id",
			serviceName: "managedblockchain",
			wantMatch:   true,
		},
		{
			name:        "does not match accessors with wrong service",
			path:        "/accessors",
			serviceName: "kms",
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)

			if tt.serviceName != "" {
				req.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.serviceName+"/aws4_request")
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		method        string
		path          string
		wantOperation string
		wantResource  string
	}{
		{
			name:          "create network",
			method:        http.MethodPost,
			path:          "/networks",
			wantOperation: "CreateNetwork",
			wantResource:  "",
		},
		{
			name:          "get network",
			method:        http.MethodGet,
			path:          "/networks/net123",
			wantOperation: "GetNetwork",
			wantResource:  "net123",
		},
		{
			name:          "get member",
			method:        http.MethodGet,
			path:          "/networks/net123/members/mem456",
			wantOperation: "GetMember",
			wantResource:  "net123/mem456",
		},
		{
			// Real aws-sdk-go-v2 wire shape: node paths nest directly under the
			// network, NOT under the member (see serializers.go's opPath for
			// CreateNode/GetNode/ListNodes/DeleteNode/UpdateNode, which all
			// resolve to "/networks/{NetworkId}/nodes[/{NodeId}]" -- MemberId
			// travels as a query parameter or body field, never in the URI).
			name:          "create node",
			method:        http.MethodPost,
			path:          "/networks/net123/nodes",
			wantOperation: "CreateNode",
			wantResource:  "net123",
		},
		{
			name:          "list nodes",
			method:        http.MethodGet,
			path:          "/networks/net123/nodes",
			wantOperation: "ListNodes",
			wantResource:  "net123",
		},
		{
			name:          "get node",
			method:        http.MethodGet,
			path:          "/networks/net123/nodes/node456",
			wantOperation: "GetNode",
			wantResource:  "net123/node456",
		},
		{
			name:          "delete node",
			method:        http.MethodDelete,
			path:          "/networks/net123/nodes/node456",
			wantOperation: "DeleteNode",
			wantResource:  "net123/node456",
		},
		{
			name:          "update node",
			method:        http.MethodPatch,
			path:          "/networks/net123/nodes/node456",
			wantOperation: "UpdateNode",
			wantResource:  "net123/node456",
		},
		{
			// The old (never-real) member-nested node shape must NOT resolve
			// to any operation -- it never matches a real SDK request.
			name:          "member-nested node shape is not a valid route",
			method:        http.MethodPost,
			path:          "/networks/net123/members/mem456/nodes",
			wantOperation: "",
			wantResource:  "",
		},
		{
			name:          "unknown path",
			method:        http.MethodGet,
			path:          "/unknown",
			wantOperation: "",
			wantResource:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOperation, h.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "unknown path",
			method:     http.MethodGet,
			path:       "/unknown-resource",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UnknownNestedPath verifies unrecognized nested paths return 404,
// distinct from TestHandler_UnknownPath's single-segment case above.
func TestHandler_UnknownNestedPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_ErrorResponseHasCodeField verifies that error responses include a Code
// field in addition to Message. Real AWS Managed Blockchain returns both fields.
func TestHandler_ErrorResponseHasCodeField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		method   string
		path     string
		wantCode string
		wantHTTP int
	}{
		{
			name:     "get missing network returns ResourceNotFoundException code",
			method:   http.MethodGet,
			path:     "/networks/no-such-network-id",
			wantHTTP: http.StatusNotFound,
			wantCode: "ResourceNotFoundException",
		},
		{
			name:   "create member with bad body returns InvalidRequestException code",
			method: http.MethodPost,
			path:   "/networks/no-net/members",
			body:   map[string]any{"MemberConfiguration": map[string]any{}},
			// MemberConfiguration.Name is empty → invalid
			wantHTTP: http.StatusBadRequest,
			wantCode: "InvalidRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantHTTP, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantCode, resp["Code"],
				"error response must include Code field; body: %s", rec.Body.String())
			assert.NotEmpty(t, resp["message"],
				"error response must include message field")
		})
	}
}

// TestHandler_ErrorResponseMessageNotEmpty verifies all error paths return non-empty
// messages alongside error codes.
func TestHandler_ErrorResponseMessageNotEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	errorCases := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "get missing network",
			method: http.MethodGet,
			path:   "/networks/no-such",
		},
		{
			name:   "get member on missing network",
			method: http.MethodGet,
			path:   "/networks/no-net/members/no-mem",
		},
		{
			name:   "get missing proposal",
			method: http.MethodGet,
			path:   "/networks/no-net/proposals/no-prop",
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.NotEqual(t, http.StatusOK, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["message"], "error response must include non-empty message")
			assert.NotEmpty(t, resp["Code"], "error response must include non-empty Code")
		})
	}
}
