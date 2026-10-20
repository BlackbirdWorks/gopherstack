package cognitoidp_test

import (
	"net/http"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func idpInitRelay(clientID, redirect string) string {
	return url.Values{
		"client_id": {clientID}, "redirect_uri": {redirect}, "response_type": {"code"}, "state": {"app-state"},
	}.Encode()
}

func TestSAMLIdPInitiated(t *testing.T) {
	t.Parallel()

	wrapped := func(s string) string {
		a := regexp.MustCompile(`(?s)<saml:Assertion .*</saml:Assertion>`).FindString(s)
		evil := `<saml:Assertion xmlns:saml="` + samlNSAssert + `" ID="_evil"><saml:Issuer>` + samlEntityID +
			`</saml:Issuer><saml:Subject><saml:NameID>mallory@corp.example</saml:NameID></saml:Subject></saml:Assertion>`

		return regexp.MustCompile(`(?s)<saml:Assertion .*</saml:Assertion>`).
			ReplaceAllLiteralString(s, `<samlp:Extensions>`+a+`</samlp:Extensions>`+evil)
	}

	tests := []struct {
		relay       func(clientID string) string
		extra       map[string]string
		name        string
		wantErrDesc string
		opts        samlOpts
		wantStatus  int
		wantCode    bool
		replayTwice bool
	}{
		{
			name: "enabled", extra: map[string]string{"IDPInit": "true"}, wantCode: true,
			opts: samlOpts{unsolicit: true},
		},
		{
			name: "enabled signed response", extra: map[string]string{"IDPInit": "true"}, wantCode: true,
			opts: samlOpts{unsolicit: true, sign: "response"},
		},
		{
			name: "not enabled", wantErrDesc: "IdP-initiated sign-in is not enabled for this client",
			opts: samlOpts{unsolicit: true},
		},
		{
			name: "unsigned", extra: map[string]string{"IDPInit": "true"},
			wantErrDesc: "SAML response validation failed", opts: samlOpts{unsolicit: true, sign: "none"},
		},
		{
			name: "wrong audience", extra: map[string]string{"IDPInit": "true"},
			wantErrDesc: "SAML response validation failed",
			opts:        samlOpts{unsolicit: true, audience: "urn:amazon:cognito:sp:other"},
		},
		{
			name: "expired", extra: map[string]string{"IDPInit": "true"},
			wantErrDesc: "SAML response validation failed", opts: samlOpts{unsolicit: true, expires: -time.Hour},
		},
		{
			name: "solicited response replayed as unsolicited", extra: map[string]string{"IDPInit": "true"},
			wantErrDesc: "SAML response validation failed", opts: samlOpts{inResponse: "_other"},
		},
		{
			name: "signature wrapping", extra: map[string]string{"IDPInit": "true"},
			wantErrDesc: "SAML response validation failed",
			opts:        samlOpts{unsolicit: true, mutate: wrapped},
		},
		{
			name: "replayed assertion", extra: map[string]string{"IDPInit": "true"}, wantCode: true,
			replayTwice: true, opts: samlOpts{unsolicit: true},
		},
		{
			name: "unregistered redirect", extra: map[string]string{"IDPInit": "true"},
			relay: func(id string) string { return idpInitRelay(id, "https://evil.example/cb") },
			opts:  samlOpts{unsolicit: true}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown client", extra: map[string]string{"IDPInit": "true"},
			relay: func(string) string { return idpInitRelay("nope", oauthRedirect) },
			opts:  samlOpts{unsolicit: true}, wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMLEnvWith(t, false, nil, tt.extra)
			relay := idpInitRelay(env.clientID, oauthRedirect)

			if tt.relay != nil {
				relay = tt.relay(env.clientID)
			}

			opts := tt.opts
			doc := env.idp.response("", env.acs(), env.poolID, &opts)
			resp := env.post(t, relay, doc)

			if tt.wantStatus != 0 {
				assert.Equal(t, tt.wantStatus, resp.StatusCode)

				return
			}

			q := locationQuery(t, resp)
			assert.Equal(t, "app-state", q.Get("state"))

			if tt.wantCode {
				require.NotEmpty(t, q.Get("code"), q.Encode())
				assert.Equal(t, oauthRedirect, resp.Header.Get("Location")[:len(oauthRedirect)])
			} else {
				assert.Empty(t, q.Get("code"))
				assert.Equal(t, tt.wantErrDesc, q.Get("error_description"))
			}

			if tt.replayTwice {
				again := locationQuery(t, env.post(t, relay, doc))
				assert.Empty(t, again.Get("code"))
				assert.Equal(t, "SAML response validation failed", again.Get("error_description"))
			}
		})
	}
}
