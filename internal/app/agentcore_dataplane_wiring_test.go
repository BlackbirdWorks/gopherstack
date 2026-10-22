package app

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	datatypes "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	agenttypes "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentCoreDataPlaneWiring(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()
	control := bedrockagentcorecontrol.NewFromConfig(fx.cfg)
	data := bedrockagentcore.NewFromConfig(fx.cfg)

	_, err := control.CreateWorkloadIdentity(ctx, &bedrockagentcorecontrol.CreateWorkloadIdentityInput{
		Name: aws.String("agent-identity"),
	})
	require.NoError(t, err)

	provider, err := control.CreateApiKeyCredentialProvider(ctx,
		&bedrockagentcorecontrol.CreateApiKeyCredentialProviderInput{
			Name: aws.String("vendor"), ApiKey: aws.String("sk-live-123"),
		})
	require.NoError(t, err)

	sm := secretsmanager.NewFromConfig(fx.cfg)
	secretARN := aws.ToString(provider.ApiKeySecretArn.SecretArn)

	stored, err := sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(secretARN)})
	require.NoError(t, err, "the managed secret exists in Secrets Manager")
	assert.JSONEq(t, `{"apiKey":"sk-live-123"}`, aws.ToString(stored.SecretString))

	token, err := data.GetWorkloadAccessToken(ctx, &bedrockagentcore.GetWorkloadAccessTokenInput{
		WorkloadName: aws.String("agent-identity"),
	})
	require.NoError(t, err)

	key, err := data.GetResourceApiKey(ctx, &bedrockagentcore.GetResourceApiKeyInput{
		ResourceCredentialProviderName: aws.String("vendor"), WorkloadIdentityToken: token.WorkloadAccessToken,
	})
	require.NoError(t, err)
	assert.Equal(t, "sk-live-123", aws.ToString(key.ApiKey))

	_, err = control.UpdateApiKeyCredentialProvider(ctx, &bedrockagentcorecontrol.UpdateApiKeyCredentialProviderInput{
		Name: aws.String("vendor"), ApiKey: aws.String("sk-live-456"),
	})
	require.NoError(t, err)

	key, err = data.GetResourceApiKey(ctx, &bedrockagentcore.GetResourceApiKeyInput{
		ResourceCredentialProviderName: aws.String("vendor"), WorkloadIdentityToken: token.WorkloadAccessToken,
	})
	require.NoError(t, err)
	assert.Equal(t, "sk-live-456", aws.ToString(key.ApiKey), "updating the provider rotates the stored secret")

	_, err = control.DeleteApiKeyCredentialProvider(ctx, &bedrockagentcorecontrol.DeleteApiKeyCredentialProviderInput{
		Name: aws.String("vendor"),
	})
	require.NoError(t, err)

	_, err = sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(secretARN)})
	assert.Error(t, err, "deleting the provider removes its managed secret")
}

func TestAgentCoreMemoryDataPlaneWiring(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()

	mem, err := bedrockagentcorecontrol.NewFromConfig(fx.cfg).
		CreateMemory(ctx, &bedrockagentcorecontrol.CreateMemoryInput{
			Name: aws.String("notes"), EventExpiryDuration: aws.Int32(30),
			MemoryStrategies: []agenttypes.MemoryStrategyInput{
				&agenttypes.MemoryStrategyInputMemberSemanticMemoryStrategy{
					Value: agenttypes.SemanticMemoryStrategyInput{Name: aws.String("facts")},
				},
			},
		})
	require.NoError(t, err)

	data := bedrockagentcore.NewFromConfig(fx.cfg)
	now := time.Now()

	_, err = data.CreateEvent(ctx, &bedrockagentcore.CreateEventInput{
		MemoryId: mem.Memory.Id, ActorId: aws.String("a"), SessionId: aws.String("s"), EventTimestamp: &now,
		Payload: []datatypes.PayloadType{&datatypes.PayloadTypeMemberConversational{Value: datatypes.Conversational{
			Role: datatypes.RoleUser, Content: &datatypes.ContentMemberText{Value: "hi"},
		}}},
	})
	require.NoError(t, err)

	events, err := data.ListEvents(ctx, &bedrockagentcore.ListEventsInput{
		MemoryId: mem.Memory.Id, ActorId: aws.String("a"), SessionId: aws.String("s"),
	})
	require.NoError(t, err)
	assert.Len(t, events.Events, 1)

	_, err = data.CreateEvent(ctx, &bedrockagentcore.CreateEventInput{
		MemoryId: aws.String("ghost"), ActorId: aws.String("a"), SessionId: aws.String("s"), EventTimestamp: &now,
		Payload: []datatypes.PayloadType{&datatypes.PayloadTypeMemberConversational{Value: datatypes.Conversational{
			Role: datatypes.RoleUser, Content: &datatypes.ContentMemberText{Value: "hi"},
		}}},
	})
	assert.Equal(t, "ResourceNotFoundException", agentCoreErrCode(err))
}
