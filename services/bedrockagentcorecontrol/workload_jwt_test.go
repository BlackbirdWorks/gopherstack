package bedrockagentcorecontrol_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyWorkloadJWTEnforcesAuthorizer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		claims  jwt.MapClaims
		name    string
		wantErr bool
	}{
		{name: "allowed", claims: jwt.MapClaims{"sub": "alice"}},
		{name: "wrong audience", claims: jwt.MapClaims{"sub": "alice", "aud": "other"}, wantErr: true},
		{name: "wrong issuer", claims: jwt.MapClaims{"sub": "alice", "iss": "https://evil.example.com"}, wantErr: true},
		{name: "no subject", claims: jwt.MapClaims{}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			m.backend.SetReferences(fakeInvoker{repos: map[string]bool{"agent": true}, calls: m.calls})

			out, err := m.client.CreateAgentRuntime(t.Context(), &sdk.CreateAgentRuntimeInput{
				AgentRuntimeName:     aws.String("secured"),
				AgentRuntimeArtifact: containerArtifact(testImage),
				RoleArn:              aws.String(testRole),
				NetworkConfiguration: &types.NetworkConfiguration{NetworkMode: types.NetworkModePublic},
				AuthorizerConfiguration: &types.AuthorizerConfigurationMemberCustomJWTAuthorizer{
					Value: types.CustomJWTAuthorizerConfiguration{
						DiscoveryUrl:    aws.String(testIssuer + "/.well-known/openid-configuration"),
						AllowedAudience: []string{"aud"},
					},
				},
			})
			require.NoError(t, err)

			arn := aws.ToString(out.WorkloadIdentityDetails.WorkloadIdentityArn)
			workload := arn[strings.LastIndex(arn, "/")+1:]

			sub, err := m.backend.DataPlane().VerifyWorkloadJWT(t.Context(), dpScope(), workload, m.token(t, tt.claims))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "alice", sub)
		})
	}
}

func TestVerifyUserJWTNegativeCache(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)

	f := newFixture(t)
	idp := newJWKSIDP(t)
	token := idp.sign(t, idp.key, jwt.MapClaims{"iss": bad.URL, "sub": "a", "exp": time.Now().Add(time.Hour).Unix()})

	for range 3 {
		_, err := f.backend.DataPlane().VerifyUserJWT(t.Context(), token)
		require.Error(t, err)
	}

	assert.Equal(t, int32(1), hits.Load(), "a failing issuer is not refetched per request")

	f.clock.advance()

	_, err := f.backend.DataPlane().VerifyUserJWT(t.Context(), token)
	require.Error(t, err)
	assert.Equal(t, int32(2), hits.Load(), "the negative entry expires on the backend clock")
}
