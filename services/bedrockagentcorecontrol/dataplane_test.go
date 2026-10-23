package bedrockagentcorecontrol_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

type memorySecrets struct {
	fakeRefs
	values map[string]string
}

func (m *memorySecrets) PutSecret(account, region, name, value string) (string, error) {
	arn := "arn:aws:secretsmanager:" + region + ":" + account + ":secret:" + name + "-AbCdEf"
	m.values[arn] = value

	return arn, nil
}

func (m *memorySecrets) GetSecret(arn string) (string, bool) {
	v, ok := m.values[arn]

	return v, ok
}

func (m *memorySecrets) DeleteSecret(arn string) { delete(m.values, arn) }

func dpScope() dataplane.Scope { return dataplane.Scope{Account: testAccount, Region: "us-east-1"} }

func TestDataPlaneSeam_Resources(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	seam := f.backend.DataPlane()

	rt := createRuntime(t, f.client, "seam_agent")
	info, err := seam.Runtime(dpScope(), aws.ToString(rt.AgentRuntimeId), "DEFAULT")
	require.NoError(t, err)
	assert.Equal(t, testImage, info.ImageURI)

	_, err = seam.Runtime(dpScope(), aws.ToString(rt.AgentRuntimeId), "nope")
	require.ErrorIs(t, err, dataplane.ErrNotFound)

	_, err = seam.Runtime(dpScope(), "ghost", "DEFAULT")
	require.ErrorIs(t, err, dataplane.ErrNotFound)

	mem, err := f.client.CreateMemory(ctx, &sdk.CreateMemoryInput{
		Name: aws.String("seam_mem"), EventExpiryDuration: aws.Int32(30),
		MemoryStrategies: []types.MemoryStrategyInput{&types.MemoryStrategyInputMemberSemanticMemoryStrategy{
			Value: types.SemanticMemoryStrategyInput{Name: aws.String("facts")},
		}},
	})
	require.NoError(t, err)

	m, ok := seam.Memory(dpScope(), aws.ToString(mem.Memory.Id))
	require.True(t, ok)
	assert.Equal(t, 30, m.ExpiryDays)
	require.Len(t, m.Strategies, 1)
	assert.Equal(t, "SEMANTIC", m.Strategies[0].Type)

	_, ok = seam.Tool(dpScope(), dataplane.ToolCodeInterpreter, "aws.codeinterpreter.v1")
	assert.True(t, ok, "the system code interpreter is visible")

	_, ok = seam.Tool(dpScope(), dataplane.ToolBrowser, "ghost")
	assert.False(t, ok)

	_, err = f.client.CreateWorkloadIdentity(ctx, &sdk.CreateWorkloadIdentityInput{Name: aws.String("wl")})
	require.NoError(t, err)
	assert.True(t, seam.WorkloadIdentity(dpScope(), "wl"))
	assert.False(t, seam.WorkloadIdentity(dpScope(), "ghost"))
}

func TestCredentialProviders_StoreRealSecrets(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	store := &memorySecrets{values: map[string]string{}}
	f.backend.SetReferences(store)

	ctx := t.Context()
	seam := f.backend.DataPlane()

	key, err := f.client.CreateApiKeyCredentialProvider(ctx, &sdk.CreateApiKeyCredentialProviderInput{
		Name: aws.String("vendor"), ApiKey: aws.String("sk-1"),
	})
	require.NoError(t, err)

	arn := aws.ToString(key.ApiKeySecretArn.SecretArn)
	assert.Contains(t, arn, ":secret:bedrock-agentcore-identity-default/apikey/vendor/apiKey")

	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(store.values[arn]), &payload))
	assert.Equal(t, "sk-1", payload["apiKey"], "the key is stored, not discarded")

	info, ok := seam.Provider(dpScope(), dataplane.ProviderAPIKey, "vendor")
	require.True(t, ok)

	val, ok := seam.SecretValue(info.SecretARN, info.SecretJSONID)
	require.True(t, ok)
	assert.Equal(t, "sk-1", val)

	_, err = f.client.UpdateApiKeyCredentialProvider(ctx, &sdk.UpdateApiKeyCredentialProviderInput{
		Name: aws.String("vendor"), ApiKey: aws.String("sk-2"),
	})
	require.NoError(t, err)

	val, _ = seam.SecretValue(info.SecretARN, info.SecretJSONID)
	assert.Equal(t, "sk-2", val, "an update replaces the stored key")

	oauth, err := f.client.CreateOauth2CredentialProvider(ctx, &sdk.CreateOauth2CredentialProviderInput{
		Name:                     aws.String("idp"),
		CredentialProviderVendor: types.CredentialProviderVendorTypeCustomOauth2,
		Oauth2ProviderConfigInput: &types.Oauth2ProviderConfigInputMemberCustomOauth2ProviderConfig{
			Value: types.CustomOauth2ProviderConfigInput{
				ClientId: aws.String("cid"), ClientSecret: aws.String("csecret"),
				OauthDiscovery: &types.Oauth2DiscoveryMemberDiscoveryUrl{
					Value: "https://idp.example/.well-known/openid-configuration",
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(oauth.ClientSecretArn.SecretArn), "oauth2/idp/clientSecret")

	oinfo, ok := seam.Provider(dpScope(), dataplane.ProviderOAuth2, "idp")
	require.True(t, ok)
	assert.Equal(t, "cid", oinfo.Config["clientId"])

	secret, ok := seam.SecretValue(oinfo.SecretARN, oinfo.SecretJSONID)
	require.True(t, ok)
	assert.Equal(t, "csecret", secret)

	_, err = f.client.DeleteApiKeyCredentialProvider(ctx, &sdk.DeleteApiKeyCredentialProviderInput{
		Name: aws.String("vendor"),
	})
	require.NoError(t, err)

	_, found := store.values[arn]
	assert.False(t, found, "deleting the provider removes its secret")
}
