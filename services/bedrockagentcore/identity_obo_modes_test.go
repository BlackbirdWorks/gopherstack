package bedrockagentcore_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

var errGateRefused = errors.New("destination is outside loopback")

type recordingIDP struct {
	*httptest.Server
	forms []url.Values
	mu    sync.Mutex
}

func newRecordingIDP(t *testing.T) *recordingIDP {
	t.Helper()

	idp := &recordingIDP{}
	idp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()

		idp.mu.Lock()
		idp.forms = append(idp.forms, r.PostForm)
		idp.mu.Unlock()

		tok := "obo-token"
		if r.PostForm.Get("grant_type") == "client_credentials" {
			tok = "actor-token"
		}

		_ = json.MarshalEncode(jsontext.NewEncoder(w, jsonx.V1()), map[string]string{"access_token": tok})
	}))
	t.Cleanup(idp.Close)

	return idp
}

func (i *recordingIDP) requests() []url.Values {
	i.mu.Lock()
	defer i.mu.Unlock()

	return append([]url.Values(nil), i.forms...)
}

type refusingDoer struct{}

func (refusingDoer) Do(*http.Request) (*http.Response, error) { return nil, errGateRefused }

func oboUserJWT() string {
	payload, _ := json.Marshal(map[string]string{"sub": "user-9", "marker": "SECRET-SUBJECT"}, jsonx.V1())

	return "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func oboFixture(t *testing.T, tokenURL, vendor string, obo map[string]any) (*fixture, string) {
	t.Helper()

	ctl := identityControl(tokenURL)
	p := ctl.providers["oauth2|oa"]
	p.Vendor = vendor
	p.Config["onBehalfOfTokenExchangeConfig"] = obo
	ctl.providers["oauth2|oa"] = p

	f := newFixture(t, ctl)
	jwtTok := oboUserJWT()

	out, err := f.client.GetWorkloadAccessTokenForJWT(t.Context(), &sdk.GetWorkloadAccessTokenForJWTInput{
		WorkloadName: new(testWorkld), UserToken: &jwtTok,
	})
	require.NoError(t, err)

	return f, *out.WorkloadAccessToken
}

func TestIdentity_OnBehalfOfGrantModes(t *testing.T) {
	t.Parallel()

	jwtTok := oboUserJWT()

	tests := []struct {
		obo      map[string]any
		custom   map[string]string
		name     string
		vendor   string
		wantCode string
		want     []map[string]string
	}{
		{
			name: "token exchange without actor",
			obo: map[string]any{
				"grantType":                    "TOKEN_EXCHANGE",
				"tokenExchangeGrantTypeConfig": map[string]any{"actorTokenContent": "NONE"},
			},
			want: []map[string]string{{
				"grant_type": "urn:ietf:params:oauth:grant-type:token-exchange", "subject_token": jwtTok,
				"subject_token_type": "urn:ietf:params:oauth:token-type:jwt", "actor_token": "",
			}},
		},
		{
			name: "token exchange with m2m actor",
			obo: map[string]any{
				"grantType": "TOKEN_EXCHANGE",
				"tokenExchangeGrantTypeConfig": map[string]any{
					"actorTokenContent": "M2M", "actorTokenScopes": []any{"s1", "s2"},
				},
			},
			want: []map[string]string{
				{"grant_type": "client_credentials", "scope": "s1 s2"},
				{
					"grant_type": "urn:ietf:params:oauth:grant-type:token-exchange", "subject_token": jwtTok,
					"actor_token": "actor-token", "actor_token_type": "urn:ietf:params:oauth:token-type:access_token",
					"scope": "read",
				},
			},
		},
		{
			name: "jwt authorization grant",
			obo:  map[string]any{"grantType": "JWT_AUTHORIZATION_GRANT"},
			want: []map[string]string{{
				"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": jwtTok,
				"subject_token": "", "requested_token_use": "",
			}},
		},
		{
			name: "microsoft adds requested_token_use", vendor: "MicrosoftOauth2",
			obo: map[string]any{"grantType": "JWT_AUTHORIZATION_GRANT"},
			want: []map[string]string{{
				"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "requested_token_use": "on_behalf_of",
			}},
		},
		{
			name: "custom parameter overrides the subject token",
			obo:  map[string]any{"grantType": "TOKEN_EXCHANGE"},
			custom: map[string]string{
				"subject_token": "other", "grant_type": "ignored",
			},
			want: []map[string]string{{
				"grant_type": "urn:ietf:params:oauth:grant-type:token-exchange", "subject_token": "other",
			}},
		},
		{
			name: "iam id token actor is refused", wantCode: "ValidationException",
			obo: map[string]any{
				"grantType":                    "TOKEN_EXCHANGE",
				"tokenExchangeGrantTypeConfig": map[string]any{"actorTokenContent": "AWS_IAM_ID_TOKEN_JWT"},
			},
		},
		{
			name: "unknown grant is refused", wantCode: "ValidationException",
			obo: map[string]any{"grantType": "SOMETHING"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newRecordingIDP(t)
			f, wat := oboFixture(t, idp.URL, tt.vendor, tt.obo)

			got, err := f.client.GetResourceOauth2Token(t.Context(), &sdk.GetResourceOauth2TokenInput{
				ResourceCredentialProviderName: new("oa"), WorkloadIdentityToken: &wat,
				Oauth2Flow: types.Oauth2FlowTypeOnBehalfOfTokenExchange, Scopes: []string{"read"},
				CustomParameters: tt.custom,
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))
				assert.Empty(t, idp.requests(), "nothing is sent to the token endpoint")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "obo-token", *got.AccessToken)

			reqs := idp.requests()
			require.Len(t, reqs, len(tt.want))

			for i, want := range tt.want {
				for k, v := range want {
					assert.Equal(t, v, reqs[i].Get(k), "request %d parameter %s", i, k)
				}
			}
		})
	}
}

func TestIdentity_TokenEndpointUsesTheGatedClient(t *testing.T) {
	t.Parallel()

	idp := newRecordingIDP(t)
	ctl := identityControl(idp.URL)
	ctl.outbound = refusingDoer{}
	f := newFixture(t, ctl)
	wat := workloadToken(t, f)

	_, err := f.client.GetResourceOauth2Token(t.Context(), &sdk.GetResourceOauth2TokenInput{
		ResourceCredentialProviderName: new("oa"), WorkloadIdentityToken: &wat,
		Oauth2Flow: types.Oauth2FlowTypeM2m, Scopes: []string{"read"},
	})
	require.Error(t, err)
	assert.Equal(t, "ValidationException", apiCode(err))
	assert.Empty(t, idp.requests(), "the control plane's outbound gate decides, not the engine client")
}

func TestIdentity_UserJWTNeverPersisted(t *testing.T) {
	t.Parallel()

	idp := newRecordingIDP(t)
	src, wat := oboFixture(t, idp.URL, "", map[string]any{"grantType": "TOKEN_EXCHANGE"})

	snap := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)
	assert.False(t, bytes.Contains(snap, []byte(oboUserJWT())), "the user JWT is not in the snapshot")
	assert.False(t, bytes.Contains(snap, []byte("SECRET-SUBJECT")), "no part of the user JWT is in the snapshot")
	assert.Contains(t, string(snap), wat, "the workload token itself is persisted")

	dst := newFixture(t, identityControl(idp.URL))
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	_, err := dst.client.GetResourceOauth2Token(t.Context(), &sdk.GetResourceOauth2TokenInput{
		ResourceCredentialProviderName: new("oa"), WorkloadIdentityToken: &wat,
		Oauth2Flow: types.Oauth2FlowTypeOnBehalfOfTokenExchange, Scopes: []string{"read"},
	})
	assert.Equal(t, "ValidationException", apiCode(err), "a restored token has no subject token to exchange")
	assert.Empty(t, idp.requests())
}

var _ bedrockagentcore.HTTPDoer = refusingDoer{}
