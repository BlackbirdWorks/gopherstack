package ssooidc_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oidc "github.com/blackbirdworks/gopherstack/services/ssooidc"
)

const opNoMatch = "Unknown"

func authFor(svc string) string {
	return "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/" + svc + "/aws4_request, SignedHeaders=host, Signature=00"
}

func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, method, uri, auth, wantOp string
		wantMatch                       bool
	}{
		{name: "register_unsigned", method: "POST", uri: "/client/register", wantMatch: true, wantOp: "RegisterClient"},
		{
			name:      "device_unsigned",
			method:    "POST",
			uri:       "/device_authorization",
			wantMatch: true,
			wantOp:    "StartDeviceAuthorization",
		},
		{name: "token_unsigned", method: "POST", uri: "/token", wantMatch: true, wantOp: "CreateToken"},
		{
			name:      "token_signed_oauth",
			method:    "POST",
			uri:       "/token",
			auth:      "sso-oauth",
			wantMatch: true,
			wantOp:    "CreateToken",
		},
		{
			name:      "token_iam",
			method:    "POST",
			uri:       "/token?aws_iam=t",
			auth:      "sso-oauth",
			wantMatch: true,
			wantOp:    "CreateTokenWithIAM",
		},
		{name: "token_s3_signed", method: "POST", uri: "/token", auth: "s3", wantOp: "CreateToken"},
		{name: "token_s3_subresource", method: "POST", uri: "/token?delete", wantOp: "CreateToken"},
		{name: "token_get", method: "GET", uri: "/token", wantOp: opNoMatch},
		{
			name:   "register_exec_api",
			method: "POST",
			uri:    "/client/register",
			auth:   "execute-api",
			wantOp: "RegisterClient",
		},
		{name: "other_path", method: "POST", uri: "/other", wantOp: opNoMatch},
	}

	h := oidc.NewHandler(oidc.NewInMemoryBackend())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.uri, http.NoBody)
			if tt.auth != "" {
				req.Header.Set("Authorization", authFor(tt.auth))
			}

			c := echo.New().NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestUnmatchedRequest(t *testing.T) {
	t.Parallel()

	h := oidc.NewHandler(oidc.NewInMemoryBackend())
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/", http.NoBody), rec)

	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Errortype"))
}

func TestErrorBodyShape(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, f.srv.URL+"/token", http.NoBody)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "InvalidRequestException", resp.Header.Get("X-Amzn-Errortype"))
}
