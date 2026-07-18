// Package codeconnections_test holds the black-box test suite for the
// CodeConnections service. This file carries the shared test infrastructure
// (handler/backend constructors, request helpers) plus generic dispatch and
// protocol-level tests that are not specific to any single resource family.
// Family-specific tests live in connections_test.go, hosts_test.go,
// repository_links_test.go, sync_configurations_test.go,
// repository_sync_test.go, and tags_test.go.
package codeconnections_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

const ccTargetPrefix = "CodeConnections_20231201."

// ccFixedAccountID/ccFixedRegion/ccFixedArnPrefix are used by tests that
// assert on exact ARN string shape (rather than merely non-emptiness), where
// a fixed, well-known account/region makes the expected ARN prefix
// deterministic to spell out.
const (
	ccFixedAccountID = "000000000000"
	ccFixedRegion    = "us-east-1"
	ccFixedArnPrefix = "arn:aws:codeconnections:us-east-1:000000000000:"
)

func newTestHandler() *codeconnections.Handler {
	backend := codeconnections.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return codeconnections.NewHandler(backend)
}

func newTestBackend() *codeconnections.InMemoryBackend {
	return codeconnections.NewInMemoryBackend("123456789012", config.DefaultRegion)
}

// newHandlerFixedAccount creates a handler backed by a fixed account/region
// (see ccFixedAccountID/ccFixedRegion), for tests that assert on exact ARN shape.
func newHandlerFixedAccount(t *testing.T) *codeconnections.Handler {
	t.Helper()

	return codeconnections.NewHandler(codeconnections.NewInMemoryBackend(ccFixedAccountID, ccFixedRegion))
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

// TestReset exercises Reset() on the backend and handler.
func TestReset(t *testing.T) {
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

// TestProviderInitNilCtx verifies that Provider.Init tolerates a nil AppContext.
func TestProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_ctx_no_panic"},
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

// TestErrValidation verifies ErrValidation is a distinct wrapped error.
func TestErrValidation(t *testing.T) {
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

// TestSeedHelpers verifies AddConnectionInternal, AddHostInternal, and
// AddRepositoryLinkInternal.
func TestSeedHelpers(t *testing.T) {
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

// TestContentTypeSuccess verifies the response Content-Type header on success paths.
func TestContentTypeSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "CreateConnection",
			action: "CreateConnection",
			body:   map[string]any{"ConnectionName": "ct-conn", "ProviderType": "GitHub"},
		},
		{
			name:   "ListConnections",
			action: "ListConnections",
			body:   map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerFixedAccount(t)
			rec := doJSON(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/x-amz-json-1.0")
		})
	}
}

// TestContentTypeError verifies the response Content-Type header on error paths.
func TestContentTypeError(t *testing.T) {
	t.Parallel()

	h := newHandlerFixedAccount(t)
	rec := doJSON(
		t,
		h,
		"GetConnection",
		map[string]any{
			"ConnectionArn": "arn:aws:codeconnections:us-east-1:000000000000:connection/nonexistent",
		},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/x-amz-json-1.0")
}

// TestErrorEnvelope verifies the __type/message error envelope shape.
func TestErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := newHandlerFixedAccount(t)
	rec := doJSON(
		t,
		h,
		"GetConnection",
		map[string]any{
			"ConnectionArn": "arn:aws:codeconnections:us-east-1:000000000000:connection/nonexistent",
		},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	m := parseResp(t, rec)
	assert.Equal(t, "ResourceNotFoundException", m["__type"])
	assert.NotEmpty(t, m["message"])
}

// TestNonPostReturns405 verifies non-POST requests are rejected.
func TestNonPostReturns405(t *testing.T) {
	t.Parallel()

	h := newHandlerFixedAccount(t)

	req := httptest.NewRequest(http.MethodPut, "/", nil)
	req.Header.Set("X-Amz-Target", "CodeConnections_20231201.ListConnections")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestMissingTargetReturns400 verifies a POST with no X-Amz-Target header returns 400.
func TestMissingTargetReturns400(t *testing.T) {
	t.Parallel()

	h := newHandlerFixedAccount(t)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
