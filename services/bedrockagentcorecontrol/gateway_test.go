package bedrockagentcorecontrol_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const lambdaARN = "arn:aws:lambda:us-east-1:" + testAccount + ":function:tools"

func jwtAuthorizer() types.AuthorizerConfiguration {
	return &types.AuthorizerConfigurationMemberCustomJWTAuthorizer{
		Value: types.CustomJWTAuthorizerConfiguration{
			DiscoveryUrl:    aws.String("https://idp.example.com/.well-known/openid-configuration"),
			AllowedAudience: []string{"aud"},
		},
	}
}

func createGateway(t *testing.T, c *sdk.Client, name string) *sdk.CreateGatewayOutput {
	t.Helper()

	out, err := c.CreateGateway(t.Context(), &sdk.CreateGatewayInput{
		Name: aws.String(name), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeCustomJwt,
		AuthorizerConfiguration: jwtAuthorizer(),
	})
	require.NoError(t, err)

	return out
}

func lambdaTarget(arn string) types.TargetConfiguration {
	return &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberLambda{
		Value: types.McpLambdaTargetConfiguration{
			LambdaArn: aws.String(arn),
			ToolSchema: &types.ToolSchemaMemberInlinePayload{Value: []types.ToolDefinition{{
				Name: aws.String("echo"), Description: aws.String("echo"),
				InputSchema: &types.SchemaDefinition{Type: types.SchemaTypeObject},
			}}},
		},
	}}
}

func TestGatewayAndTargets(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetReferences(fakeRefs{functions: map[string]bool{lambdaARN: true}})
	f.backend.SetLifecycleDelay(time.Minute)

	ctx := t.Context()
	gw := createGateway(t, f.client, "My-Gateway")
	assert.Equal(t, types.GatewayStatusCreating, gw.Status)
	assert.Contains(t, aws.ToString(gw.GatewayId), "my-gateway-")
	assert.Contains(t, aws.ToString(gw.GatewayUrl), aws.ToString(gw.GatewayId))

	_, err := f.client.CreateGatewayTarget(ctx, &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: gw.GatewayId, Name: aws.String("t1"), TargetConfiguration: lambdaTarget(lambdaARN),
	})
	require.NoError(t, err, "the parent only needs to exist, not be READY")

	f.clock.advance()

	tgt, err := f.client.CreateGatewayTarget(ctx, &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: gw.GatewayId, Name: aws.String("t2"), TargetConfiguration: lambdaTarget(lambdaARN),
		CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
			{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.TargetStatusCreating, tgt.Status)
	assert.Equal(t, types.TargetProtocolTypeMcp, tgt.ProtocolType)
	assert.Equal(t, gw.GatewayArn, tgt.GatewayArn)

	_, err = f.client.CreateGatewayTarget(ctx, &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: gw.GatewayId, Name: aws.String("t3"),
		TargetConfiguration: lambdaTarget("arn:aws:lambda:us-east-1:" + testAccount + ":function:missing"),
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.CreateGatewayTarget(ctx, &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: gw.GatewayId, Name: aws.String("t2"), TargetConfiguration: lambdaTarget(lambdaARN),
	})
	assert.Equal(t, "ConflictException", apiCode(err))

	f.clock.advance()

	list, err := f.client.ListGatewayTargets(ctx, &sdk.ListGatewayTargetsInput{GatewayIdentifier: gw.GatewayId})
	require.NoError(t, err)
	assert.Len(t, list.Items, 2)

	sync, err := f.client.SynchronizeGatewayTargets(ctx, &sdk.SynchronizeGatewayTargetsInput{
		GatewayIdentifier: gw.GatewayId, TargetIdList: []string{aws.ToString(tgt.TargetId)},
	})
	require.NoError(t, err)
	require.Len(t, sync.Targets, 1)
	assert.Equal(t, types.TargetStatusSynchronizing, sync.Targets[0].Status)

	f.clock.advance()

	got, err := f.client.GetGatewayTarget(
		ctx,
		&sdk.GetGatewayTargetInput{GatewayIdentifier: gw.GatewayId, TargetId: tgt.TargetId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.TargetStatusReady, got.Status)
	require.NotNil(t, got.LastSynchronizedAt)

	_, err = f.client.DeleteGateway(ctx, &sdk.DeleteGatewayInput{GatewayIdentifier: gw.GatewayId})
	assert.Equal(t, "ConflictException", apiCode(err), "targets block gateway deletion")

	for _, tg := range list.Items {
		_, err = f.client.DeleteGatewayTarget(
			ctx,
			&sdk.DeleteGatewayTargetInput{GatewayIdentifier: gw.GatewayId, TargetId: tg.TargetId},
		)
		require.NoError(t, err)
	}

	f.clock.advance()

	_, err = f.client.DeleteGateway(ctx, &sdk.DeleteGatewayInput{GatewayIdentifier: gw.GatewayId})
	require.NoError(t, err)
}

func TestGatewayValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	tests := []struct {
		name     string
		in       *sdk.CreateGatewayInput
		wantCode string
	}{
		{"jwt without config", &sdk.CreateGatewayInput{
			Name: aws.String("gw1"), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeCustomJwt,
		}, "ValidationException"},
		{"iam ok", &sdk.CreateGatewayInput{
			Name: aws.String("gw2"), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeAwsIam,
		}, ""},
		{"bad name", &sdk.CreateGatewayInput{
			Name: aws.String("-bad--name-"), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeAwsIam,
		}, "ValidationException"},
		{"unknown policy engine", &sdk.CreateGatewayInput{
			Name: aws.String("gw3"), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeAwsIam,
			PolicyEngineConfiguration: &types.GatewayPolicyEngineConfiguration{
				Arn:  aws.String("arn:aws:bedrock-agentcore:us-east-1:" + testAccount + ":policy-engine/x"),
				Mode: types.GatewayPolicyEngineModeEnforce,
			},
		}, "ResourceNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.CreateGateway(t.Context(), tt.in)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestGatewayRulesAndRateLimits(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	gw := createGateway(t, f.client, "rules-gw")

	rule, err := f.client.CreateGatewayRule(ctx, &sdk.CreateGatewayRuleInput{
		GatewayIdentifier: gw.GatewayId, Priority: aws.Int32(10),
		Actions: routeAction("t1"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(10), aws.ToInt32(rule.Priority))
	assert.Nil(t, rule.System, "only service-managed rules carry a system block")

	_, err = f.client.CreateGatewayRule(ctx, &sdk.CreateGatewayRuleInput{
		GatewayIdentifier: gw.GatewayId, Priority: aws.Int32(10),
		Actions: routeAction("t1"),
	})
	assert.Equal(t, "ConflictException", apiCode(err))

	_, err = f.client.UpdateGatewayRule(ctx, &sdk.UpdateGatewayRuleInput{
		GatewayIdentifier: gw.GatewayId, RuleId: rule.RuleId, Description: aws.String("new"),
	})
	require.NoError(t, err)

	got, err := f.client.GetGatewayRule(
		ctx,
		&sdk.GetGatewayRuleInput{GatewayIdentifier: gw.GatewayId, RuleId: rule.RuleId},
	)
	require.NoError(t, err)
	assert.Equal(t, "new", aws.ToString(got.Description))
	assert.Equal(t, types.GatewayRuleStatusActive, got.Status)

	rules, err := f.client.ListGatewayRules(ctx, &sdk.ListGatewayRulesInput{GatewayIdentifier: gw.GatewayId})
	require.NoError(t, err)
	assert.Len(t, rules.GatewayRules, 1)

	entries := []types.LimitEntry{
		{
			Dimensions: map[string]string{"tenant": "*"},
			Requests:   []types.RateConfig{{Rate: aws.Float64(100), Period: types.PeriodMinute}},
		},
	}

	rl, err := f.client.CreateGatewayRateLimit(ctx, &sdk.CreateGatewayRateLimitInput{
		GatewayIdentifier: gw.GatewayId, DimensionKeys: []string{"tenant"}, Entries: entries,
	})
	require.NoError(t, err)
	assert.Equal(t, types.GatewayRateLimitStatusCreating, rl.Status)

	batch, err := f.client.BatchPutGatewayRateLimits(ctx, &sdk.BatchPutGatewayRateLimitsInput{
		GatewayIdentifier: gw.GatewayId,
		RateLimits: []types.BatchPutLimitEntry{
			{RateLimitId: aws.String("a1"), DimensionKeys: []string{"tenant"}, Entries: entries},
			{DimensionKeys: []string{"user"}, Entries: entries},
		},
	})
	require.NoError(t, err)
	assert.Len(t, batch.RateLimits, 2)

	list, err := f.client.ListGatewayRateLimits(ctx, &sdk.ListGatewayRateLimitsInput{GatewayIdentifier: gw.GatewayId})
	require.NoError(t, err)
	assert.Len(t, list.RateLimits, 2, "the batch replaced the earlier rate limit")

	_, err = f.client.GetGatewayRateLimit(
		ctx,
		&sdk.GetGatewayRateLimitInput{GatewayIdentifier: gw.GatewayId, RateLimitId: rl.RateLimitId},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteGatewayRateLimit(
		ctx,
		&sdk.DeleteGatewayRateLimitInput{GatewayIdentifier: gw.GatewayId, RateLimitId: aws.String("a1")},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteGatewayRule(
		ctx,
		&sdk.DeleteGatewayRuleInput{GatewayIdentifier: gw.GatewayId, RuleId: rule.RuleId},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteGateway(ctx, &sdk.DeleteGatewayInput{GatewayIdentifier: gw.GatewayId})
	require.NoError(t, err, "rules and rate limits are removed with their gateway")
	assert.Equal(t, 0, f.backend.ResourceCount())
}

func routeAction(target string) []types.Action {
	return []types.Action{&types.ActionMemberRouteToTarget{
		Value: &types.RouteToTargetActionMemberStaticRoute{Value: types.StaticRoute{TargetName: aws.String(target)}},
	}}
}
