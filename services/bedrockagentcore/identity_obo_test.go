package bedrockagentcore_test

import (
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func exchangeServer(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		if r.FormValue("grant_type") != "urn:ietf:params:oauth:grant-type:token-exchange" ||
			r.FormValue("subject_token_type") != "urn:ietf:params:oauth:token-type:jwt" ||
			user != "cid" || pass != "shh" {
			http.Error(w, "denied", http.StatusUnauthorized)

			return
		}

		_ = json.MarshalEncode(
			jsontext.NewEncoder(w, jsonx.V1()),
			map[string]string{"access_token": "obo(" + r.FormValue("subject_token") + ")" + r.FormValue("scope")},
		)
	}))
	t.Cleanup(srv.Close)

	return srv
}

func TestIdentity_OnBehalfOfTokenExchange(t *testing.T) {
	t.Parallel()

	payload, _ := json.Marshal(map[string]string{"sub": "user-9"}, jsonx.V1())
	userJWT := "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"

	tests := []struct {
		name      string
		wantToken string
		wantCode  string
		fromJWT   bool
	}{
		{name: "from user jwt", fromJWT: true, wantToken: "obo(" + userJWT + ")read"},
		{name: "from user id", wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := exchangeServer(t)
			f := newFixture(t, identityControl(srv.URL))

			wat := workloadToken(t, f)
			if tt.fromJWT {
				out, err := f.client.GetWorkloadAccessTokenForJWT(t.Context(), &sdk.GetWorkloadAccessTokenForJWTInput{
					WorkloadName: new(testWorkld), UserToken: &userJWT,
				})
				require.NoError(t, err)

				wat = *out.WorkloadAccessToken
			}

			got, err := f.client.GetResourceOauth2Token(t.Context(), &sdk.GetResourceOauth2TokenInput{
				ResourceCredentialProviderName: new("oa"), WorkloadIdentityToken: &wat,
				Oauth2Flow: types.Oauth2FlowTypeOnBehalfOfTokenExchange, Scopes: []string{"read"},
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantToken, *got.AccessToken)
		})
	}
}
