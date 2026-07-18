package account_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/awstest"
	"github.com/blackbirdworks/gopherstack/services/account"
)

func newTestHandler(t *testing.T) *account.Handler {
	t.Helper()

	return account.NewHandler(account.NewInMemoryBackend("000000000000", "us-east-1"))
}

// doRequest issues a POST request against path with the given JSON body.
// Account Management is a rest-json1 service: every operation is a POST to a
// fixed operation-named path, with every parameter (including AccountId,
// RegionName, AlternateContactType, ...) carried in the JSON request body.
func doRequest(t *testing.T, h *account.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	c, rec := awstest.NewJSONContext(t, http.MethodPost, path, body)
	c.Request().Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=key/20230101/us-east-1/account/aws4_request",
	)
	require.NoError(t, h.Handler()(c))

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	assert.Equal(t, "Account", h.Name())
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	h.Reset()
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"GetContactInformation", "PutContactInformation",
		"GetAlternateContact", "PutAlternateContact", "DeleteAlternateContact",
		"ListRegions", "GetRegionOptStatus", "EnableRegion", "DisableRegion",
		"GetPrimaryEmail", "StartPrimaryEmailUpdate", "AcceptPrimaryEmailUpdate",
		"GetAccountInformation", "PutAccountName",
	} {
		assert.Contains(t, ops, op)
	}

	// DescribeAccount/CloseAccount are not real Account Management
	// operations (CloseAccount belongs to AWS Organizations) and must not
	// be advertised.
	assert.NotContains(t, ops, "DescribeAccount")
	assert.NotContains(t, ops, "CloseAccount")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		method    string
		authSvc   string
		wantMatch bool
	}{
		{
			name:      "get contact info matches",
			path:      "/getContactInformation",
			method:    http.MethodPost,
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "put contact info matches",
			path:      "/putContactInformation",
			method:    http.MethodPost,
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "list regions matches",
			path:      "/listRegions",
			method:    http.MethodPost,
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "get account information matches",
			path:      "/getAccountInformation",
			method:    http.MethodPost,
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "wrong service",
			path:      "/getContactInformation",
			method:    http.MethodPost,
			authSvc:   "s3",
			wantMatch: false,
		},
		{name: "wrong path", path: "/other", method: http.MethodPost, authSvc: "account", wantMatch: false},
		{
			name: "GET is rejected -- every account op is POST-only", path: "/getContactInformation",
			method: http.MethodGet, authSvc: "account", wantMatch: false,
		},
		{
			name: "legacy REST-ish path no longer matches", path: "/account/contact",
			method: http.MethodGet, authSvc: "account", wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)

			if tt.authSvc != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=key/20230101/us-east-1/"+tt.authSvc+"/aws4_request",
				)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		wantOp string
	}{
		{name: "GetContactInformation", path: "/getContactInformation", wantOp: "GetContactInformation"},
		{name: "PutContactInformation", path: "/putContactInformation", wantOp: "PutContactInformation"},
		{name: "GetAlternateContact", path: "/getAlternateContact", wantOp: "GetAlternateContact"},
		{name: "PutAlternateContact", path: "/putAlternateContact", wantOp: "PutAlternateContact"},
		{name: "DeleteAlternateContact", path: "/deleteAlternateContact", wantOp: "DeleteAlternateContact"},
		{name: "ListRegions", path: "/listRegions", wantOp: "ListRegions"},
		{name: "GetRegionOptStatus", path: "/getRegionOptStatus", wantOp: "GetRegionOptStatus"},
		{name: "EnableRegion", path: "/enableRegion", wantOp: "EnableRegion"},
		{name: "DisableRegion", path: "/disableRegion", wantOp: "DisableRegion"},
		{name: "GetPrimaryEmail", path: "/getPrimaryEmail", wantOp: "GetPrimaryEmail"},
		{name: "StartPrimaryEmailUpdate", path: "/startPrimaryEmailUpdate", wantOp: "StartPrimaryEmailUpdate"},
		{name: "AcceptPrimaryEmailUpdate", path: "/acceptPrimaryEmailUpdate", wantOp: "AcceptPrimaryEmailUpdate"},
		{name: "GetAccountInformation", path: "/getAccountInformation", wantOp: "GetAccountInformation"},
		{name: "PutAccountName", path: "/putAccountName", wantOp: "PutAccountName"},
		{name: "Unknown", path: "/unknown-path", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/getAlternateContact", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	assert.Equal(t, "getAlternateContact", h.ExtractResource(c))
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_WrongMethod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	c, rec := awstest.NewJSONContext(t, http.MethodGet, "/getContactInformation", nil)
	c.Request().Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=key/20230101/us-east-1/account/aws4_request",
	)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestHandler_ErrorEnvelope verifies a modeled error carries the exception
// type in both the X-Amzn-Errortype header and the body's __type field --
// aws-sdk-go-v2's rest-json1 deserializer resolves the exception from these,
// not from the message text.
func TestHandler_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/getAlternateContact", map[string]any{"AlternateContactType": "BILLING"})
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "ResourceNotFoundException", rec.Header().Get("X-Amzn-Errortype"))

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}
