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

func TestPaymentsFlow(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetLifecycleDelay(time.Minute)
	ctx := t.Context()

	prov, err := f.client.CreatePaymentCredentialProvider(ctx, &sdk.CreatePaymentCredentialProviderInput{
		Name: aws.String("coinbase"), CredentialProviderVendor: types.PaymentCredentialProviderVendorTypeCoinbaseCDP,
		ProviderConfigurationInput: &types.PaymentProviderConfigurationInputMemberCoinbaseCdpConfiguration{
			Value: types.CoinbaseCdpConfigurationInput{
				ApiKeyId: aws.String("kid"), ApiKeySecret: aws.String("s1"), WalletSecret: aws.String("s2"),
			},
		},
	})
	require.NoError(t, err)

	gotProv, err := f.client.GetPaymentCredentialProvider(
		ctx,
		&sdk.GetPaymentCredentialProviderInput{Name: aws.String("coinbase")},
	)
	require.NoError(t, err)

	cfg, ok := gotProv.ProviderConfigurationOutput.(*types.
		PaymentProviderConfigurationOutputMemberCoinbaseCdpConfiguration)
	require.True(t, ok)
	assert.Equal(t, "kid", aws.ToString(cfg.Value.ApiKeyId))
	assert.Contains(t, aws.ToString(cfg.Value.ApiKeySecretArn.SecretArn), ":secret:", "secrets are never echoed back")

	mgr, err := f.client.CreatePaymentManager(ctx, &sdk.CreatePaymentManagerInput{
		Name: aws.String("wallet"), AuthorizerType: types.PaymentsAuthorizerTypeAwsIam, RoleArn: aws.String(testRole),
	})
	require.NoError(t, err)
	assert.Equal(t, types.PaymentManagerStatusCreating, mgr.Status)
	require.NotNil(t, mgr.WorkloadIdentityDetails)

	f.clock.advance()

	_, err = f.client.CreatePaymentConnector(ctx, &sdk.CreatePaymentConnectorInput{
		PaymentManagerId: mgr.PaymentManagerId, Name: aws.String("cb"), Type: types.PaymentConnectorTypeCoinbaseCdp,
		CredentialProviderConfigurations: []types.CredentialsProviderConfiguration{
			&types.CredentialsProviderConfigurationMemberCoinbaseCDP{
				Value: types.PaymentCredentialProviderConfiguration{
					CredentialProviderArn: aws.String(
						"arn:aws:bedrock-agentcore:us-east-1:" + testAccount + ":token-vault/default/paymentcredentialprovider/ghost",
					),
				},
			},
		},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	conn, err := f.client.CreatePaymentConnector(ctx, &sdk.CreatePaymentConnectorInput{
		PaymentManagerId: mgr.PaymentManagerId, Name: aws.String("cb"), Type: types.PaymentConnectorTypeCoinbaseCdp,
		CredentialProviderConfigurations: []types.CredentialsProviderConfiguration{
			&types.CredentialsProviderConfigurationMemberCoinbaseCDP{
				Value: types.PaymentCredentialProviderConfiguration{
					CredentialProviderArn: prov.CredentialProviderArn,
				},
			},
		},
	})
	require.NoError(t, err)

	f.clock.advance()

	rot, err := f.client.RotatePaymentConnectorCredentials(ctx, &sdk.RotatePaymentConnectorCredentialsInput{
		PaymentManagerId: mgr.PaymentManagerId, PaymentConnectorId: conn.PaymentConnectorId,
		CredentialsToRotate: &types.CredentialRotationConfigMemberCoinbaseCDP{Value: types.CoinbaseCdpRotationTargets{
			Secrets: []types.CoinbaseCdpSecret{types.CoinbaseCdpSecretApiKey},
		}},
	})
	require.NoError(t, err)

	assert.Equal(t, types.PaymentConnectorStatusUpdating, rot.Status)

	f.clock.advance()

	got, err := f.client.GetPaymentConnector(
		ctx,
		&sdk.GetPaymentConnectorInput{
			PaymentManagerId:   mgr.PaymentManagerId,
			PaymentConnectorId: conn.PaymentConnectorId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.PaymentConnectorStatusReady, got.Status)
	assert.NotNil(t, got.CredentialsUpdatedAt)

	list, err := f.client.ListPaymentConnectors(
		ctx,
		&sdk.ListPaymentConnectorsInput{PaymentManagerId: mgr.PaymentManagerId},
	)
	require.NoError(t, err)
	assert.Len(t, list.PaymentConnectors, 1)

	_, err = f.client.DeletePaymentManager(ctx, &sdk.DeletePaymentManagerInput{PaymentManagerId: mgr.PaymentManagerId})
	assert.Equal(t, "ConflictException", apiCode(err))

	_, err = f.client.DeletePaymentConnector(
		ctx,
		&sdk.DeletePaymentConnectorInput{
			PaymentManagerId:   mgr.PaymentManagerId,
			PaymentConnectorId: conn.PaymentConnectorId,
		},
	)
	require.NoError(t, err)

	f.clock.advance()

	_, err = f.client.DeletePaymentManager(ctx, &sdk.DeletePaymentManagerInput{PaymentManagerId: mgr.PaymentManagerId})
	require.NoError(t, err)
}

func TestEvaluatorsAndOnlineConfigs(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	ev, err := f.client.CreateEvaluator(ctx, &sdk.CreateEvaluatorInput{
		EvaluatorName: aws.String("quality"), Level: types.EvaluatorLevelTrace,
		EvaluatorConfig: &types.EvaluatorConfigMemberLlmAsAJudge{Value: types.LlmAsAJudgeEvaluatorConfig{
			Instructions: aws.String("score it"),
			ModelConfig: &types.EvaluatorModelConfigMemberBedrockEvaluatorModelConfig{
				Value: types.BedrockEvaluatorModelConfig{ModelId: aws.String("m")},
			},
			RatingScale: &types.RatingScaleMemberNumerical{Value: []types.NumericalScaleDefinition{
				{Label: aws.String("bad"), Definition: aws.String("bad"), Value: aws.Float64(0)},
				{Label: aws.String("good"), Definition: aws.String("good"), Value: aws.Float64(1)},
			}},
		}},
	})
	require.NoError(t, err)

	got, err := f.client.GetEvaluator(ctx, &sdk.GetEvaluatorInput{EvaluatorId: ev.EvaluatorId})
	require.NoError(t, err)
	assert.Equal(t, types.EvaluatorStatusActive, got.Status)
	assert.Equal(t, types.EvaluatorLevelTrace, got.Level)

	_, err = f.client.UpdateEvaluator(
		ctx,
		&sdk.UpdateEvaluatorInput{EvaluatorId: ev.EvaluatorId, Description: aws.String("d")},
	)
	require.NoError(t, err)

	_, err = f.client.CreateOnlineEvaluationConfig(ctx, &sdk.CreateOnlineEvaluationConfigInput{
		OnlineEvaluationConfigName: aws.String(
			"live",
		),
		EnableOnCreate:             aws.Bool(true),
		EvaluationExecutionRoleArn: aws.String(testRole),
		DataSourceConfig: &types.DataSourceConfigMemberCloudWatchLogs{
			Value: types.CloudWatchLogsInputConfig{LogGroupNames: []string{"/agent"}, ServiceNames: []string{"svc"}},
		},
		Rule:       &types.Rule{SamplingConfig: &types.SamplingConfig{SamplingPercentage: aws.Float64(10)}},
		Evaluators: []types.EvaluatorReference{&types.EvaluatorReferenceMemberEvaluatorId{Value: "ghost"}},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	cfg, err := f.client.CreateOnlineEvaluationConfig(ctx, &sdk.CreateOnlineEvaluationConfigInput{
		OnlineEvaluationConfigName: aws.String(
			"live",
		),
		EnableOnCreate:             aws.Bool(true),
		EvaluationExecutionRoleArn: aws.String(testRole),
		DataSourceConfig: &types.DataSourceConfigMemberCloudWatchLogs{
			Value: types.CloudWatchLogsInputConfig{LogGroupNames: []string{"/agent"}, ServiceNames: []string{"svc"}},
		},
		Rule: &types.Rule{SamplingConfig: &types.SamplingConfig{SamplingPercentage: aws.Float64(10)}},
		Evaluators: []types.EvaluatorReference{
			&types.EvaluatorReferenceMemberEvaluatorId{Value: aws.ToString(ev.EvaluatorId)},
			&types.EvaluatorReferenceMemberEvaluatorId{Value: "Builtin.Helpfulness"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.OnlineEvaluationExecutionStatusEnabled, cfg.ExecutionStatus)

	_, err = f.client.DeleteEvaluator(ctx, &sdk.DeleteEvaluatorInput{EvaluatorId: ev.EvaluatorId})
	assert.Equal(t, "ConflictException", apiCode(err), "an online config still uses the evaluator")

	_, err = f.client.UpdateOnlineEvaluationConfig(ctx, &sdk.UpdateOnlineEvaluationConfigInput{
		OnlineEvaluationConfigId: cfg.OnlineEvaluationConfigId,
		ExecutionStatus:          types.OnlineEvaluationExecutionStatusDisabled,
	})
	require.NoError(t, err)

	read, err := f.client.GetOnlineEvaluationConfig(
		ctx,
		&sdk.GetOnlineEvaluationConfigInput{OnlineEvaluationConfigId: cfg.OnlineEvaluationConfigId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.OnlineEvaluationExecutionStatusDisabled, read.ExecutionStatus)

	_, err = f.client.DeleteOnlineEvaluationConfig(
		ctx,
		&sdk.DeleteOnlineEvaluationConfigInput{OnlineEvaluationConfigId: cfg.OnlineEvaluationConfigId},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteEvaluator(ctx, &sdk.DeleteEvaluatorInput{EvaluatorId: ev.EvaluatorId})
	require.NoError(t, err)
}

func TestConsentPortal(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	gw := createGateway(t, f.client, "portal-gw")

	oauth, err := f.client.CreateOauth2CredentialProvider(ctx, &sdk.CreateOauth2CredentialProviderInput{
		Name: aws.String("idp"), CredentialProviderVendor: types.CredentialProviderVendorTypeGithubOauth2,
		Oauth2ProviderConfigInput: &types.Oauth2ProviderConfigInputMemberGithubOauth2ProviderConfig{
			Value: types.GithubOauth2ProviderConfigInput{ClientId: aws.String("c"), ClientSecret: aws.String("s")},
		},
	})
	require.NoError(t, err)

	in := &sdk.CreateConsentPortalInput{
		Name:             aws.String("consent"),
		ExecutionRoleArn: aws.String(testRole),
		IdpConfig: &types.ConsentPortalIdpConfig{
			CredentialProviderArn: oauth.CredentialProviderArn,
			Scopes:                []string{"openid"},
		},
		Sources: []types.ConsentPortalSource{
			{Type: types.ConsentPortalSourceTypeAgentcoreGateway, Identifier: gw.GatewayId},
		},
	}

	created, err := f.client.CreateConsentPortal(ctx, in)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.PortalUrl))

	byName, err := f.client.GetConsentPortal(
		ctx,
		&sdk.GetConsentPortalInput{ConsentPortalIdentifier: aws.String("consent")},
	)
	require.NoError(t, err)
	assert.Equal(t, created.ConsentPortalId, byName.ConsentPortalId, "a portal resolves by name as well as id")

	bad := *in
	bad.Name = aws.String("ghosted")
	bad.Sources = []types.ConsentPortalSource{
		{Type: types.ConsentPortalSourceTypeAgentcoreGateway, Identifier: aws.String("ghost")},
	}
	_, err = f.client.CreateConsentPortal(ctx, &bad)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.UpdateConsentPortal(
		ctx,
		&sdk.UpdateConsentPortalInput{ConsentPortalIdentifier: created.ConsentPortalId, Description: aws.String("new")},
	)
	require.NoError(t, err)

	list, err := f.client.ListConsentPortals(ctx, &sdk.ListConsentPortalsInput{})
	require.NoError(t, err)
	assert.Len(t, list.ConsentPortals, 1)

	_, err = f.client.DeleteConsentPortal(
		ctx,
		&sdk.DeleteConsentPortalInput{ConsentPortalIdentifier: created.ConsentPortalId},
	)
	require.NoError(t, err)
}
