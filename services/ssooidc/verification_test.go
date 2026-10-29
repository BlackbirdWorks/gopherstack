package ssooidc_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oidc "github.com/blackbirdworks/gopherstack/services/ssooidc"
)

func (f fixture) page(t *testing.T, method, rawURL string, form url.Values) (int, string) {
	t.Helper()

	var body io.Reader = http.NoBody
	if form != nil {
		body = strings.NewReader(form.Encode())
	}

	req, err := http.NewRequestWithContext(t.Context(), method, rawURL, body)
	require.NoError(t, err)

	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(b)
}

func TestDeviceVerificationEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query      func(code string) string
		method     string
		form       func(code string) url.Values
		name       string
		wantBody   string
		wantUser   string
		wantCode   string
		wantStatus int
	}{
		{
			name: "prompt_page", method: http.MethodGet, wantStatus: http.StatusOK, wantBody: "Approve",
			query: func(c string) string { return "?user_code=" + c }, wantCode: "AuthorizationPendingException",
		},
		{
			name: "code_entry_page", method: http.MethodGet, wantStatus: http.StatusOK, wantBody: "Enter your code",
			query: func(string) string { return "" }, wantCode: "AuthorizationPendingException",
		},
		{
			name: "get_approve_default_user", method: http.MethodGet, wantStatus: http.StatusOK, wantBody: "alice",
			query: func(c string) string { return "?action=approve&user_code=" + c }, wantUser: "alice",
		},
		{
			name: "get_approve_named_user_without_hyphen", method: http.MethodGet, wantStatus: http.StatusOK,
			query: func(c string) string {
				return "?action=approve&user=bob&user_code=" + strings.ReplaceAll(strings.ToLower(c), "-", "")
			},
			wantUser: "bob",
		},
		{
			name: "post_approve", method: http.MethodPost, wantStatus: http.StatusOK, wantUser: "bob",
			form: func(c string) url.Values {
				return url.Values{"user_code": {c}, "user": {"bob"}, "action": {"approve"}}
			},
		},
		{
			name: "deny", method: http.MethodGet, wantStatus: http.StatusOK, wantBody: "denied",
			query: func(c string) string { return "?action=deny&user_code=" + c }, wantCode: "AccessDeniedException",
		},
		{
			name: "unknown_user", method: http.MethodGet, wantStatus: http.StatusNotFound,
			query:    func(c string) string { return "?action=approve&user=mallory&user_code=" + c },
			wantCode: "AuthorizationPendingException",
		},
		{
			name: "unknown_code", method: http.MethodGet, wantStatus: http.StatusNotFound,
			query:    func(string) string { return "?action=approve&user_code=ZZZZ-ZZZZ" },
			wantCode: "AuthorizationPendingException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id, secret := f.register(t, grantDevice)
			dev := f.startDevice(t, id, secret)
			code := aws.ToString(dev.UserCode)

			target := f.srv.URL + oidc.DevicePath
			var form url.Values

			if tt.query != nil {
				target += tt.query(code)
			}

			if tt.form != nil {
				form = tt.form(code)
			}

			status, body := f.page(t, tt.method, target, form)
			assert.Equal(t, tt.wantStatus, status)
			assert.Contains(t, body, tt.wantBody)

			out, err := f.deviceToken(t, id, secret, aws.ToString(dev.DeviceCode))
			if tt.wantUser == "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			sess, serr := f.handler.Backend.SessionForAccessToken(aws.ToString(out.AccessToken))
			require.NoError(t, serr)
			assert.Equal(t, tt.wantUser, sess.Subject.UserName)
		})
	}
}

func TestDeviceVerificationWithoutResolver(t *testing.T) {
	t.Parallel()

	h := oidc.NewHandler(oidc.NewInMemoryBackend())
	reg, regErr := h.Backend.RegisterClient(&oidc.RegisterClientInput{ClientName: "c", ClientType: "public"})
	require.Nil(t, regErr)

	dev, devErr := h.Backend.StartDeviceAuthorization(reg.ClientID, reg.ClientSecret, startURL)
	require.Nil(t, devErr)

	req := httptest.NewRequest(http.MethodGet, oidc.DevicePath+"?action=approve&user_code="+dev.UserCode, http.NoBody)
	rec := httptest.NewRecorder()

	require.NoError(t, h.ServeDeviceVerification(echo.New().NewContext(req, rec)))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.True(t, h.Backend.DeviceUserCodeValid(dev.UserCode), "a failed approval leaves the code pending")
}
