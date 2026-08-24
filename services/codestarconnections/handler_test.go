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

// createCSCConn is a test helper that creates a connection and returns its ARN.
func createCSCConn(t *testing.T, h *codestarconnections.Handler, name, providerType string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateConnection", map[string]any{
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

// createCSCHost is a test helper that creates a host and returns its ARN.
func createCSCHost(t *testing.T, h *codestarconnections.Handler, name, providerType, endpoint string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             name,
		"ProviderType":     providerType,
		"ProviderEndpoint": endpoint,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	arn, ok := resp["HostArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

// createCSCRepositoryLink is a test helper that creates a repository link and returns its ID.
func createCSCRepositoryLink(t *testing.T, h *codestarconnections.Handler, connArn, repoName string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":  connArn,
		"OwnerId":        "my-org",
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

func TestHandler_GetSupportedOperations_IncludesRepositoryAndSyncOps(t *testing.T) {
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

// TestHandler_Reset verifies Backend.Reset() and Handler.Reset() clear all state.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Seed some state.
	_, err := h.Backend.CreateConnection(context.Background(), "c1", "GitHub", "", nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateHost(context.Background(), "h1", "GitHub", "https://example.com", nil, nil)
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

// TestProviderInit_NilCtx verifies ErrNilAppContext is returned for nil ctx.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codestarconnections.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codestarconnections.ErrNilAppContext)
}

// TestHandlerOpsPreBuilt verifies the dispatch table is built once in NewHandler.
func TestHandlerOpsPreBuilt(t *testing.T) {
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

// TestProviderTypeValidation verifies invalid ProviderType is rejected.
func TestProviderTypeValidation(t *testing.T) {
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

// TestInMemoryBackend_SeedHelpers verifies AddConnectionInternal/AddHostInternal/AddRepositoryLinkInternal.
func TestInMemoryBackend_SeedHelpers(t *testing.T) {
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

// TestInMemoryBackend_ExportCountHelpers verifies the export_test.go count helpers.
func TestInMemoryBackend_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, b.ConnectionCount())
	assert.Equal(t, 0, b.HostCount())
	assert.Equal(t, 0, b.RepositoryLinkCount())
	assert.Equal(t, 0, b.SyncConfigurationCount())

	_, err := b.CreateConnection(context.Background(), "c1", "GitHub", "", nil)
	require.NoError(t, err)

	_, err = b.CreateRepositoryLink(context.Background(), "conn-arn", "owner", "repo", "", nil)
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

// TestErrValidationMapping verifies ErrValidation errors map to 400 in the
// handler with the real InvalidInputException type (the real
// aws-sdk-go-v2/service/codestarconnections error catalog has no
// ValidationException type at all -- see errors.go's ErrValidation doc
// comment).
func TestErrValidationMapping(t *testing.T) {
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
	assert.Equal(t, "InvalidInputException", resp["__type"])
}

// TestErrResourceAlreadyExistsMapping verifies ErrResourceAlreadyExists maps
// to ResourceAlreadyExistsException, the code CreateRepositoryLink's own
// error switch actually types for a duplicate identity (unlike
// CreateConnection/CreateHost -- see TestConnectionNameNotUnique/
// TestHostNameNotUnique).
func TestErrResourceAlreadyExistsMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	connRec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "link-conn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, connRec.Code)
	connArn := parseResp(t, connRec)["ConnectionArn"].(string)

	linkBody := map[string]any{
		"ConnectionArn":    connArn,
		"OwnerId":          "owner",
		"RepositoryName":   "repo",
		"EncryptionKeyArn": "arn:aws:kms:us-east-1:000000000000:key/key1",
	}
	rec := doRequest(t, h, "CreateRepositoryLink", linkBody)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateRepositoryLink", linkBody)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	resp := parseResp(t, rec)
	assert.Equal(t, "ResourceAlreadyExistsException", resp["__type"])
}

func TestErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      string
		body        map[string]any
		wantErrType string
	}{
		{
			name:   "not found returns ResourceNotFoundException",
			action: "GetConnection",
			body: map[string]any{
				"ConnectionArn": "arn:aws:codestar-connections:us-east-1:000000000000:connection/nonexistent",
			},
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "validation error returns InvalidInputException",
			action:      "CreateConnection",
			body:        map[string]any{"ConnectionName": "valid-err-conn", "ProviderType": "INVALID"},
			wantErrType: "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateConnection", tt.body)
			if tt.action != "CreateConnection" {
				rec = doRequest(t, h, tt.action, tt.body)
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			resp := parseResp(t, rec)
			assert.Equal(t, tt.wantErrType, resp["__type"], "for test %q", tt.name)
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCSCConn(t, h, "reset-conn", "GitHub")
	createCSCHost(t, h, "reset-host", "GitHubEnterpriseServer", "https://x.com")

	assert.Equal(t, 1, h.Backend.ConnectionCount())
	assert.Equal(t, 1, h.Backend.HostCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.ConnectionCount())
	assert.Equal(t, 0, h.Backend.HostCount())
}

// TestExtractResource verifies ExtractResource correctly pulls ARNs from request body.
func TestExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantRes string
	}{
		{
			name:    "connection_arn",
			body:    map[string]any{"ConnectionArn": "arn:aws:codestar-connections:us-east-1:123:connection/abc"},
			wantRes: "arn:aws:codestar-connections:us-east-1:123:connection/abc",
		},
		{
			name:    "resource_arn",
			body:    map[string]any{"ResourceArn": "arn:aws:codestar-connections:us-east-1:123:connection/xyz"},
			wantRes: "arn:aws:codestar-connections:us-east-1:123:connection/xyz",
		},
		{
			name:    "host_arn",
			body:    map[string]any{"HostArn": "arn:aws:codestar-connections:us-east-1:123:host/myhost"},
			wantRes: "arn:aws:codestar-connections:us-east-1:123:host/myhost",
		},
		{
			name:    "repository_link_id",
			body:    map[string]any{"RepositoryLinkId": "repo-link-id-abc"},
			wantRes: "repo-link-id-abc",
		},
		{
			name:    "resource_name",
			body:    map[string]any{"ResourceName": "my-stack"},
			wantRes: "my-stack",
		},
		{
			name:    "empty_body",
			body:    map[string]any{},
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractResource(c)
			assert.Equal(t, tt.wantRes, got)
		})
	}
}

// TestProvider_NilCtx verifies codestarconnections Provider.Init requires non-nil ctx.
func TestProvider_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "nil_ctx_returns_error", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &codestarconnections.Provider{}
			reg, err := p.Init(nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, reg)
				assert.ErrorIs(t, err, codestarconnections.ErrNilAppContext)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, reg)
			}
		})
	}
}
