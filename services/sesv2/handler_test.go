package sesv2_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// newHandler creates a new SES v2 handler with a fresh backend.
func newHandler() *sesv2.Handler {
	return sesv2.NewHandler(sesv2.NewInMemoryBackend())
}

// doRequest performs a request against the handler and returns the recorder.
func doRequest(
	t *testing.T,
	h *sesv2.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestSESv2Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	tests := []struct {
		name      string
		method    string
		path      string
		wantMatch bool
	}{
		{
			name:      "matches v2 email path",
			method:    http.MethodGet,
			path:      "/v2/email/identities",
			wantMatch: true,
		},
		{
			name:      "matches v2 config sets path",
			method:    http.MethodGet,
			path:      "/v2/email/configuration-sets",
			wantMatch: true,
		},
		{
			name:      "rejects dashboard path",
			method:    http.MethodGet,
			path:      "/dashboard/ses",
			wantMatch: false,
		},
		{
			name:      "rejects non-v2 path",
			method:    http.MethodGet,
			path:      "/",
			wantMatch: false,
		},
		{
			name:      "rejects v1 SES path",
			method:    http.MethodPost,
			path:      "/",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestSESv2Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "GET identities list",
			method: http.MethodGet,
			path:   "/v2/email/identities",
			wantOp: "ListEmailIdentities",
		},
		{
			name:   "POST identities create",
			method: http.MethodPost,
			path:   "/v2/email/identities",
			wantOp: "CreateEmailIdentity",
		},
		{
			name:   "GET identity by name",
			method: http.MethodGet,
			path:   "/v2/email/identities/test@example.com",
			wantOp: "GetEmailIdentity",
		},
		{
			name:   "DELETE identity",
			method: http.MethodDelete,
			path:   "/v2/email/identities/test@example.com",
			wantOp: "DeleteEmailIdentity",
		},
		{
			name:   "POST outbound-emails",
			method: http.MethodPost,
			path:   "/v2/email/outbound-emails",
			wantOp: "SendEmail",
		},
		{
			name:   "POST configuration-sets",
			method: http.MethodPost,
			path:   "/v2/email/configuration-sets",
			wantOp: "CreateConfigurationSet",
		},
		{
			name:   "GET configuration-sets list",
			method: http.MethodGet,
			path:   "/v2/email/configuration-sets",
			wantOp: "ListConfigurationSets",
		},
		{
			name:   "GET configuration set by name",
			method: http.MethodGet,
			path:   "/v2/email/configuration-sets/my-config",
			wantOp: "GetConfigurationSet",
		},
		{
			name:   "DELETE configuration set",
			method: http.MethodDelete,
			path:   "/v2/email/configuration-sets/my-config",
			wantOp: "DeleteConfigurationSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestSESv2Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, "/v2/email/identities/test@example.com", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "test@example.com", h.ExtractResource(c))
}

func TestSESv2Handler_HandlerName(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "SESv2", h.Name())
}

func TestSESv2Handler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreateEmailIdentity")
	assert.Contains(t, ops, "GetEmailIdentity")
	assert.Contains(t, ops, "ListEmailIdentities")
	assert.Contains(t, ops, "DeleteEmailIdentity")
	assert.Contains(t, ops, "SendEmail")
	assert.Contains(t, ops, "CreateConfigurationSet")
	assert.Contains(t, ops, "GetConfigurationSet")
	assert.Contains(t, ops, "ListConfigurationSets")
	assert.Contains(t, ops, "DeleteConfigurationSet")
}

func TestSESv2Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, 85, h.MatchPriority())
}

func TestSESv2Handler_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &sesv2.Provider{}
	assert.Equal(t, "SESv2", p.Name())
}

func TestSESv2Handler_ProviderInitWithAppCtx(t *testing.T) {
	t.Parallel()

	p := &sesv2.Provider{}

	appCtx := &service.AppContext{
		Logger: slog.Default(),
	}

	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "SESv2", svc.Name())
}

func TestSESv2Handler_UnknownRoute(t *testing.T) {
	t.Parallel()

	h := newHandler()

	req := httptest.NewRequest(http.MethodGet, "/v2/email/unknown-resource", nil)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSESv2Handler_ChaosProvider(t *testing.T) {
	t.Parallel()

	h := newHandler()

	assert.Equal(t, "sesv2", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

// TestSESv2Handler_Persistence is a cross-cutting infra test: it exercises the generic
// Handler-level Snapshot/Restore contract across two unrelated families (identities and
// configuration sets) at once, so it doesn't have one obvious single-family home and
// stays here alongside the other routing/infra tests.
func TestSESv2Handler_Persistence(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create some state.
	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/identities",
		map[string]any{"EmailIdentity": "persist@example.com"},
	)
	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/configuration-sets",
		map[string]any{"ConfigurationSetName": "persist-config"},
	)

	// Snapshot.
	snap := h.Snapshot(t.Context())
	assert.NotEmpty(t, snap)

	// Restore to a fresh backend.
	h2 := newHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify state was restored.
	rec := doRequest(t, h2, http.MethodGet, "/v2/email/identities/persist@example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h2, http.MethodGet, "/v2/email/configuration-sets/persist-config", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestSESv2Handler_InvalidJSON, TestSESv2Handler_ErrorResponseIncludesType, and
// TestSESv2Handler_URLEncodedIdentity exercise generic HTTP-dispatch error-handling
// behavior (malformed body, error response shape, percent-encoded path segments) that
// applies to every route; they use the identities endpoint only incidentally as a
// convenient handler to drive the request through, so they stay here as core infra
// tests rather than moving to email_identities_test.go.
func TestSESv2Handler_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newHandler()

	req := httptest.NewRequest(
		http.MethodPost,
		"/v2/email/identities",
		strings.NewReader("not-json"),
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSESv2Handler_ErrorResponseIncludesType(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Trigger a NotFoundException by requesting an identity that doesn't exist.
	rec := doRequest(t, h, http.MethodGet, "/v2/email/identities/missing@example.com", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(
		t,
		"NotFoundException",
		out["__type"],
		"error response must include __type for SDK/Terraform error classification",
	)
	assert.NotEmpty(t, out["message"])
}

func TestSESv2Handler_URLEncodedIdentity(t *testing.T) {
	t.Parallel()

	identity := "encoded@example.com"
	encodedIdentity := "encoded%40example.com"

	h := newHandler()

	// Create the identity with its plain name.
	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/identities",
		map[string]any{"EmailIdentity": identity},
	)

	// GET using percent-encoded path (AWS SDK / Terraform style).
	rec := doRequest(t, h, http.MethodGet, "/v2/email/identities/"+encodedIdentity, nil)

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, identity, out["EmailIdentity"])

	// DELETE using percent-encoded path.
	rec2 := doRequest(t, h, http.MethodDelete, "/v2/email/identities/"+encodedIdentity, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Confirm it is gone.
	rec3 := doRequest(t, h, http.MethodGet, "/v2/email/identities/"+encodedIdentity, nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}
