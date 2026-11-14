package gcpidentitytoolkit_test

import (
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) getPage(t *testing.T, path string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, e.url+path, nil)
	require.NoError(t, err)

	resp, err := e.hc.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body)
}

func linkPath(t *testing.T, link string) string {
	t.Helper()

	u, err := url.Parse(link)
	require.NoError(t, err)

	return u.RequestURI()
}

func TestEmulatorActionLinks(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "act@example.com")

	r := e.client(t, "sendOobCode", map[string]any{"requestType": "VERIFY_EMAIL", "idToken": up.str("idToken")})
	require.Equal(t, http.StatusOK, r.status)

	r = e.client(t, "sendOobCode", map[string]any{"requestType": "PASSWORD_RESET", "email": "act@example.com"})
	require.Equal(t, http.StatusOK, r.status)

	links := map[string]string{}
	for _, c := range e.oobCodes(t) {
		links[c["requestType"].(string)] = linkPath(t, c["oobLink"].(string))
	}

	code, body := e.getPage(t, links["VERIFY_EMAIL"])
	assert.Equal(t, http.StatusOK, code, body)

	lk := e.client(t, "lookup", map[string]any{"idToken": up.str("idToken")})
	assert.Equal(t, true, lk.body["users"].([]any)[0].(map[string]any)["emailVerified"])

	code, body = e.getPage(t, links["VERIFY_EMAIL"])
	assert.Equal(t, http.StatusBadRequest, code, body)

	code, body = e.getPage(t, links["PASSWORD_RESET"])
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `name="newPassword"`)

	code, body = e.getPage(t, links["PASSWORD_RESET"]+"&newPassword=brandnew1")
	assert.Equal(t, http.StatusOK, code, body)

	in := e.client(t, "signInWithPassword", map[string]any{"email": "act@example.com", "password": "brandnew1"})
	assert.Equal(t, http.StatusOK, in.status)

	code, _ = e.getPage(t, "/emulator/action?mode=resetPassword&oobCode=nope")
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestEmulatorActionContinueLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cont     string
		wantLink bool
	}{
		{name: "authorized host", cont: "http://localhost:3000/done", wantLink: true},
		{name: "unauthorized host", cont: "https://evil.test/phish"},
		{name: "javascript scheme", cont: "javascript:alert(1)"},
		{name: "none", cont: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			up := e.signUp(t, "c@example.com")

			r := e.client(t, "sendOobCode", map[string]any{"requestType": "VERIFY_EMAIL", "idToken": up.str("idToken")})
			require.Equal(t, http.StatusOK, r.status)

			link := linkPath(t, e.oobCodes(t)[0]["oobLink"].(string))
			if tt.cont != "" {
				link += "&continueUrl=" + url.QueryEscape(tt.cont)
			}

			code, body := e.getPage(t, link)
			require.Equal(t, http.StatusOK, code, body)

			if tt.wantLink {
				assert.Contains(t, body, `href="`+tt.cont+`"`)

				return
			}

			assert.NotContains(t, body, "<a href")
			assert.NotContains(t, body, "evil.test")
		})
	}
}
