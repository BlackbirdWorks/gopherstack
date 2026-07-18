package serverlessrepo_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

const testAccountID = "000000000000"

func newTestHandler(t *testing.T) *serverlessrepo.Handler {
	t.Helper()

	return serverlessrepo.NewHandler(serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doServerlessRepoRequest(
	t *testing.T,
	h *serverlessrepo.Handler,
	method string,
	path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/serverlessrepo/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doServerlessRepoRequestEncoded sends a request using a pre-encoded URL path. Unlike
// doServerlessRepoRequest, the path is not re-encoded: callers may pass paths that contain
// percent-encoded characters (e.g. %2F, %3A) that must be preserved as-is so that the handler
// correctly routes ARN-form application IDs.
func doServerlessRepoRequestEncoded(
	t *testing.T,
	h *serverlessrepo.Handler,
	method string,
	rawPath string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, rawPath, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/serverlessrepo/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// arnPathFor returns the percent-encoded URL path for a SAR request whose applicationId is
// the ARN form: arn:aws:serverlessrepo:us-east-1:testAccountID:applications/{name}.
// The ARN is percent-encoded with path-safe encoding so that %2F is preserved as an opaque
// token and does not break path routing.
func arnPathFor(name string) string {
	arnStr := "arn:aws:serverlessrepo:us-east-1:" + testAccountID + ":applications/" + name

	return "/applications/" + url.PathEscape(arnStr)
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ServerlessRepo", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateApplication")
	assert.Contains(t, ops, "GetApplication")
	assert.Contains(t, ops, "ListApplications")
	assert.Contains(t, ops, "UpdateApplication")
	assert.Contains(t, ops, "DeleteApplication")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 87, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "serverlessrepo", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, "us-east-1", regions[0])
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		service string
		path    string
		want    bool
	}{
		{
			name:    "matches /applications with serverlessrepo service",
			service: "serverlessrepo",
			path:    "/applications",
			want:    true,
		},
		{
			name:    "matches /applications/{id} with serverlessrepo service",
			service: "serverlessrepo",
			path:    "/applications/my-app",
			want:    true,
		},
		{
			name:    "does not match wrong service name",
			service: "sagemaker",
			path:    "/applications",
			want:    false,
		},
		{
			name:    "does not match wrong path",
			service: "serverlessrepo",
			path:    "/models",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.service+"/aws4_request",
			)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &serverlessrepo.Provider{}
	assert.Equal(t, "ServerlessRepo", p.Name())

	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "ServerlessRepo", reg.Name())
}

func TestProvider_Init_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &serverlessrepo.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, serverlessrepo.ErrNilAppContext)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Use a path that doesn't match any operation (PUT is not supported)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPut, "/applications", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/serverlessrepo/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// errBoom is a plain, unwrapped error that doesn't match any of the awserr sentinels
// (NotFound/Conflict/InvalidParameter), simulating an unexpected internal failure.
var errBoom = errors.New("boom")

// errBackend wraps an InMemoryBackend and forces GetApplication to return errBoom.
type errBackend struct {
	*serverlessrepo.InMemoryBackend
}

func (b *errBackend) GetApplication(_ string) (*serverlessrepo.Application, error) {
	return nil, errBoom
}

func TestHandler_UnexpectedError_ReturnsInternalServerErrorException(t *testing.T) {
	t.Parallel()

	h := serverlessrepo.NewHandler(&errBackend{
		InMemoryBackend: serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1"),
	})

	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/my-app", nil)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// The aws-sdk-go-v2 restjson1 error deserializer only recognizes the exact string
	// "InternalServerErrorException" (case-insensitively) in the __type field to build a
	// typed types.InternalServerErrorException; any other spelling (e.g.
	// "InternalServerException") falls through to a generic smithy.GenericAPIError.
	assert.Equal(t, "InternalServerErrorException", resp["__type"])
}

func TestHandler_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 14, serverlessrepo.HandlerOpsLen(h))
}

func TestErrorShape_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/missing-app", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "NotFoundException", resp["__type"])
	assert.NotEmpty(t, resp["message"])
}

func TestErrorShape_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{"name": "dup", "description": "d", "author": "a"}

	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doServerlessRepoRequest(t, h, http.MethodPost, "/applications", body)
	require.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ConflictException", resp["__type"])
}

func TestErrorShape_BadRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", map[string]any{
		"name":   "x",
		"author": "a",
		// description missing
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "BadRequestException", resp["__type"])
}
