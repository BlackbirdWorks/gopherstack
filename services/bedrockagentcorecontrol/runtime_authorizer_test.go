package bedrockagentcorecontrol_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeJWTAuthorizerIsEnforced(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	m.backend.SetReferences(fakeInvoker{
		repos: map[string]bool{"agent": true},
		calls: m.calls,
	})

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
	m.clock.advance()

	info, err := m.backend.DataPlane().Runtime(dpScope(), aws.ToString(out.AgentRuntimeId), "DEFAULT")
	require.NoError(t, err)
	require.NotNil(t, info.Authorize)

	tests := []struct {
		name   string
		header string
		status int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"valid token", "Bearer " + m.token(t, nil), 0},
		{"wrong audience", "Bearer " + m.token(t, jwt.MapClaims{"aud": "other"}), http.StatusUnauthorized},
		{"sigv4 only", "AWS4-HMAC-SHA256 Credential=x", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
			if tt.header != "" {
				r.Header.Set("Authorization", tt.header)
			}

			f := info.Authorize(t.Context(), r)
			if tt.status == 0 {
				assert.Nil(t, f)

				return
			}

			require.NotNil(t, f)
			assert.Equal(t, tt.status, f.Status)
		})
	}
}

func TestRuntimeWithoutAuthorizerHasNoCheck(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	rt := createRuntime(t, f.client, "open_agent")
	f.clock.advance()

	info, err := f.backend.DataPlane().Runtime(dpScope(), aws.ToString(rt.AgentRuntimeId), "DEFAULT")
	require.NoError(t, err)
	assert.Nil(t, info.Authorize)
}
