package bedrockagentcorecontrol_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayUpdateAndLists(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetReferences(fakeRefs{functions: map[string]bool{lambdaARN: true}})
	ctx := t.Context()
	gw := createGateway(t, f.client, "sweep-gw")

	got, err := f.client.GetGateway(ctx, &sdk.GetGatewayInput{GatewayIdentifier: gw.GatewayId})
	require.NoError(t, err)
	assert.Equal(t, types.GatewayStatusReady, got.Status)
	assert.Equal(t, gw.GatewayArn, got.GatewayArn)

	upd, err := f.client.UpdateGateway(ctx, &sdk.UpdateGatewayInput{
		GatewayIdentifier: gw.GatewayId, Name: aws.String("renamed-gw"), RoleArn: aws.String(testRole),
		AuthorizerType: types.AuthorizerTypeAwsIam, Description: aws.String("d"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.GatewayStatusUpdating, upd.Status)
	assert.Equal(t, "renamed-gw", aws.ToString(upd.Name))

	other := createGateway(t, f.client, "other-gw")
	_, err = f.client.UpdateGateway(ctx, &sdk.UpdateGatewayInput{
		GatewayIdentifier: other.GatewayId,
		Name:              aws.String("renamed-gw"),
		RoleArn:           aws.String(testRole),
		AuthorizerType:    types.AuthorizerTypeAwsIam,
	})
	assert.Equal(t, "ConflictException", apiCode(err), "a rename cannot take another gateway's name")

	list, err := f.client.ListGateways(ctx, &sdk.ListGatewaysInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.NotNil(t, list.NextToken)

	tgt, err := f.client.CreateGatewayTarget(ctx, &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: gw.GatewayId, Name: aws.String("tool"), TargetConfiguration: lambdaTarget(lambdaARN),
	})
	require.NoError(t, err)

	_, err = f.client.UpdateGatewayTarget(ctx, &sdk.UpdateGatewayTargetInput{
		GatewayIdentifier:   gw.GatewayId,
		TargetId:            tgt.TargetId,
		TargetConfiguration: lambdaTarget(lambdaARN),
		Description:         aws.String("new"),
	})
	require.NoError(t, err)

	rl, err := f.client.CreateGatewayRateLimit(ctx, &sdk.CreateGatewayRateLimitInput{
		GatewayIdentifier: gw.GatewayId,
		DimensionKeys:     []string{"tenant"},
		Entries: []types.LimitEntry{
			{
				Dimensions: map[string]string{"tenant": "*"},
				Requests:   []types.RateConfig{{Rate: aws.Float64(5), Period: types.PeriodMinute}},
			},
		},
	})
	require.NoError(t, err)

	updRL, err := f.client.UpdateGatewayRateLimit(ctx, &sdk.UpdateGatewayRateLimitInput{
		GatewayIdentifier: gw.GatewayId,
		RateLimitId:       rl.RateLimitId,
		Description:       aws.String("tight"),
		Entries: []types.LimitEntry{
			{
				Dimensions: map[string]string{"tenant": "*"},
				Requests:   []types.RateConfig{{Rate: aws.Float64(1), Period: types.PeriodMinute}},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "tight", aws.ToString(updRL.Description))
}

func TestPolicySweep(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	eng, err := f.client.CreatePolicyEngine(ctx, &sdk.CreatePolicyEngineInput{Name: aws.String("sweep_engine")})
	require.NoError(t, err)

	got, err := f.client.GetPolicyEngine(ctx, &sdk.GetPolicyEngineInput{PolicyEngineId: eng.PolicyEngineId})
	require.NoError(t, err)
	assert.Equal(t, types.PolicyEngineStatusActive, got.Status)

	sum, err := f.client.GetPolicyEngineSummary(
		ctx,
		&sdk.GetPolicyEngineSummaryInput{PolicyEngineId: eng.PolicyEngineId},
	)
	require.NoError(t, err)
	assert.Equal(t, eng.PolicyEngineArn, sum.PolicyEngineArn)

	_, err = f.client.UpdatePolicyEngine(
		ctx,
		&sdk.UpdatePolicyEngineInput{
			PolicyEngineId: eng.PolicyEngineId,
			Description:    &types.UpdatedDescription{OptionalValue: aws.String("d")},
		},
	)
	require.NoError(t, err)

	engines, err := f.client.ListPolicyEngines(ctx, &sdk.ListPolicyEnginesInput{})
	require.NoError(t, err)
	assert.Len(t, engines.PolicyEngines, 1)

	engineSummaries, err := f.client.ListPolicyEngineSummaries(ctx, &sdk.ListPolicyEngineSummariesInput{})
	require.NoError(t, err)
	assert.Len(t, engineSummaries.PolicyEngines, 1)

	cedar := &types.PolicyDefinitionMemberCedar{
		Value: types.CedarPolicy{Statement: aws.String("permit(principal, action, resource);")},
	}

	pol, err := f.client.CreatePolicy(
		ctx,
		&sdk.CreatePolicyInput{PolicyEngineId: eng.PolicyEngineId, Name: aws.String("p1"), Definition: cedar},
	)
	require.NoError(t, err)

	gotPol, err := f.client.GetPolicy(
		ctx,
		&sdk.GetPolicyInput{PolicyEngineId: eng.PolicyEngineId, PolicyId: pol.PolicyId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.PolicyStatusActive, gotPol.Status)

	polSum, err := f.client.GetPolicySummary(
		ctx,
		&sdk.GetPolicySummaryInput{PolicyEngineId: eng.PolicyEngineId, PolicyId: pol.PolicyId},
	)
	require.NoError(t, err)
	assert.Equal(t, pol.PolicyArn, polSum.PolicyArn)

	_, err = f.client.UpdatePolicy(ctx, &sdk.UpdatePolicyInput{
		PolicyEngineId: eng.PolicyEngineId, PolicyId: pol.PolicyId, Definition: cedar,
	})
	require.NoError(t, err)

	pols, err := f.client.ListPolicies(ctx, &sdk.ListPoliciesInput{PolicyEngineId: eng.PolicyEngineId})
	require.NoError(t, err)
	assert.Len(t, pols.Policies, 1)

	gen, err := f.client.StartPolicyGeneration(ctx, &sdk.StartPolicyGenerationInput{
		PolicyEngineId: eng.PolicyEngineId, Name: aws.String("gen1"),
		Resource: &types.ResourceMemberArn{Value: "arn:aws:bedrock-agentcore:us-east-1:" + testAccount + ":gateway/g"},
		Content:  &types.ContentMemberRawText{Value: "x"},
	})
	require.NoError(t, err)

	gotGen, err := f.client.GetPolicyGeneration(
		ctx,
		&sdk.GetPolicyGenerationInput{PolicyEngineId: eng.PolicyEngineId, PolicyGenerationId: gen.PolicyGenerationId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.PolicyGenerationStatusGenerateFailed, gotGen.Status)

	gens, err := f.client.ListPolicyGenerations(
		ctx,
		&sdk.ListPolicyGenerationsInput{PolicyEngineId: eng.PolicyEngineId},
	)
	require.NoError(t, err)
	assert.Len(t, gens.PolicyGenerations, 1)

	genSums, err := f.client.ListPolicyGenerationSummaries(
		ctx,
		&sdk.ListPolicyGenerationSummariesInput{PolicyEngineId: eng.PolicyEngineId},
	)
	require.NoError(t, err)
	assert.Len(t, genSums.PolicyGenerations, 1)

	_, err = f.client.DeletePolicy(
		ctx,
		&sdk.DeletePolicyInput{PolicyEngineId: eng.PolicyEngineId, PolicyId: pol.PolicyId},
	)
	require.NoError(t, err)

	_, err = f.client.DeletePolicyEngine(ctx, &sdk.DeletePolicyEngineInput{PolicyEngineId: eng.PolicyEngineId})
	require.NoError(t, err)
}

func TestRegistryAndRecordUpdate(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	reg, err := f.client.CreateRegistry(ctx, &sdk.CreateRegistryInput{
		Name: aws.String("auto"), ApprovalConfiguration: &types.ApprovalConfiguration{AutoApproval: true},
	})
	require.NoError(t, err)

	id := regIDOf(aws.ToString(reg.RegistryArn))

	got, err := f.client.GetRegistry(ctx, &sdk.GetRegistryInput{RegistryId: aws.String(id)})
	require.NoError(t, err)
	assert.Equal(t, types.RegistryStatusReady, got.Status)

	_, err = f.client.UpdateRegistry(
		ctx,
		&sdk.UpdateRegistryInput{
			RegistryId:  aws.String(id),
			Description: &types.UpdatedDescription{OptionalValue: aws.String("d")},
		},
	)
	require.NoError(t, err)

	set, err := f.client.GetRegistry(ctx, &sdk.GetRegistryInput{RegistryId: aws.String(id)})
	require.NoError(t, err)
	assert.Equal(t, "d", aws.ToString(set.Description), "the optionalValue wrapper is unwrapped")

	_, err = f.client.UpdateRegistry(
		ctx,
		&sdk.UpdateRegistryInput{RegistryId: aws.String(id), Description: &types.UpdatedDescription{}},
	)
	require.NoError(t, err)

	cleared, err := f.client.GetRegistry(ctx, &sdk.GetRegistryInput{RegistryId: aws.String(id)})
	require.NoError(t, err)
	assert.Nil(t, cleared.Description, "an empty wrapper clears the member")

	list, err := f.client.ListRegistries(ctx, &sdk.ListRegistriesInput{Status: types.RegistryStatusReady})
	require.NoError(t, err)
	assert.Len(t, list.Registries, 1)

	rec, err := f.client.CreateRegistryRecord(ctx, &sdk.CreateRegistryRecordInput{
		RegistryId: aws.String(id), Name: aws.String("tool"), DescriptorType: types.DescriptorTypeCustom,
		Descriptors: &types.Descriptors{Custom: &types.CustomDescriptor{InlineContent: aws.String(`{}`)}},
	})
	require.NoError(t, err)

	recID := regIDOf(aws.ToString(rec.RecordArn))

	_, err = f.client.UpdateRegistryRecord(ctx, &sdk.UpdateRegistryRecordInput{
		RegistryId: aws.String(
			id,
		),
		RecordId:    aws.String(recID),
		Description: &types.UpdatedDescription{OptionalValue: aws.String("d")},
	})
	require.NoError(t, err)

	read, err := f.client.GetRegistryRecord(
		ctx,
		&sdk.GetRegistryRecordInput{RegistryId: aws.String(id), RecordId: aws.String(recID)},
	)
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(read.RecordVersion), "editing a record bumps its version")

	sub, err := f.client.SubmitRegistryRecordForApproval(
		ctx,
		&sdk.SubmitRegistryRecordForApprovalInput{RegistryId: aws.String(id), RecordId: aws.String(recID)},
	)
	require.NoError(t, err)
	assert.Equal(t, types.RegistryRecordStatusApproved, sub.Status, "the registry auto-approves")
}

func TestHarnessEndpointsAndLists(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	h, err := f.client.CreateHarness(ctx, &sdk.CreateHarnessInput{
		HarnessName:      aws.String("lst"),
		ExecutionRoleArn: aws.String(testRole),
		Model: &types.HarnessModelConfigurationMemberBedrockModelConfig{
			Value: types.HarnessBedrockModelConfig{ModelId: aws.String("m")},
		},
	})
	require.NoError(t, err)

	ep, err := f.client.CreateHarnessEndpoint(
		ctx,
		&sdk.CreateHarnessEndpointInput{HarnessId: h.Harness.HarnessId, EndpointName: aws.String("live")},
	)
	require.NoError(t, err)
	assert.Equal(t, types.HarnessEndpointStatusCreating, ep.Endpoint.Status)

	got, err := f.client.GetHarnessEndpoint(
		ctx,
		&sdk.GetHarnessEndpointInput{HarnessId: h.Harness.HarnessId, EndpointName: aws.String("live")},
	)
	require.NoError(t, err)
	assert.Equal(t, types.HarnessEndpointStatusReady, got.Endpoint.Status)

	list, err := f.client.ListHarnesses(ctx, &sdk.ListHarnessesInput{})
	require.NoError(t, err)
	assert.Len(t, list.Harnesses, 1)

	_, err = f.client.DeleteHarnessEndpoint(
		ctx,
		&sdk.DeleteHarnessEndpointInput{HarnessId: h.Harness.HarnessId, EndpointName: aws.String("live")},
	)
	require.NoError(t, err)
}

func TestSandboxAndBundleLists(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	br, err := f.client.CreateBrowser(ctx, &sdk.CreateBrowserInput{
		Name: aws.String(
			"b1",
		),
		NetworkConfiguration: &types.BrowserNetworkConfiguration{NetworkMode: types.BrowserNetworkModePublic},
	})
	require.NoError(t, err)

	gotB, err := f.client.GetBrowser(ctx, &sdk.GetBrowserInput{BrowserId: br.BrowserId})
	require.NoError(t, err)
	assert.Equal(t, types.BrowserStatusReady, gotB.Status)

	ci, err := f.client.CreateCodeInterpreter(ctx, &sdk.CreateCodeInterpreterInput{
		Name: aws.String(
			"c1",
		),
		NetworkConfiguration: &types.CodeInterpreterNetworkConfiguration{
			NetworkMode: types.CodeInterpreterNetworkModePublic,
		},
	})
	require.NoError(t, err)

	cis, err := f.client.ListCodeInterpreters(ctx, &sdk.ListCodeInterpretersInput{Type: types.ResourceTypeCustom})
	require.NoError(t, err)
	assert.Len(t, cis.CodeInterpreterSummaries, 1)

	_, err = f.client.DeleteCodeInterpreter(
		ctx,
		&sdk.DeleteCodeInterpreterInput{CodeInterpreterId: ci.CodeInterpreterId},
	)
	require.NoError(t, err)

	prof, err := f.client.CreateBrowserProfile(ctx, &sdk.CreateBrowserProfileInput{Name: aws.String("prof")})
	require.NoError(t, err)

	gotP, err := f.client.GetBrowserProfile(ctx, &sdk.GetBrowserProfileInput{ProfileId: prof.ProfileId})
	require.NoError(t, err)
	assert.Equal(t, types.BrowserProfileStatusReady, gotP.Status)

	bundle, err := f.client.CreateConfigurationBundle(ctx, &sdk.CreateConfigurationBundleInput{
		BundleName: aws.String(
			"b",
		),
		Components: map[string]types.ComponentConfiguration{
			"c": {Configuration: document.NewLazyDocument(map[string]any{"k": 1})},
		},
	})
	require.NoError(t, err)

	bundles, err := f.client.ListConfigurationBundles(ctx, &sdk.ListConfigurationBundlesInput{})
	require.NoError(t, err)
	assert.Len(t, bundles.Bundles, 1)

	_, err = f.client.DeleteConfigurationBundle(ctx, &sdk.DeleteConfigurationBundleInput{BundleId: bundle.BundleId})
	require.NoError(t, err)

	ds, err := f.client.CreateDataset(ctx, &sdk.CreateDatasetInput{
		DatasetName: aws.String(
			"d",
		),
		SchemaType: types.DatasetSchemaTypeThirdPartyEvaluationV1,
		Source:     inlineSource(map[string]any{"i": 1}),
	})
	require.NoError(t, err)

	_, err = f.client.UpdateDataset(ctx, &sdk.UpdateDatasetInput{DatasetId: ds.DatasetId, Description: aws.String("d")})
	require.NoError(t, err)

	dsl, err := f.client.ListDatasets(ctx, &sdk.ListDatasetsInput{})
	require.NoError(t, err)
	assert.Len(t, dsl.Datasets, 1)

	evs, err := f.client.ListEvaluators(ctx, &sdk.ListEvaluatorsInput{})
	require.NoError(t, err)
	assert.Empty(t, evs.Evaluators)

	oec, err := f.client.ListOnlineEvaluationConfigs(ctx, &sdk.ListOnlineEvaluationConfigsInput{})
	require.NoError(t, err)
	assert.Empty(t, oec.OnlineEvaluationConfigs)

	wis, err := f.client.ListWorkloadIdentities(ctx, &sdk.ListWorkloadIdentitiesInput{})
	require.NoError(t, err)
	assert.Empty(t, wis.WorkloadIdentities)

	cps, err := f.client.ListCapacityProviders(ctx, &sdk.ListCapacityProvidersInput{})
	require.NoError(t, err)
	assert.Empty(t, cps.CapacityProviders)

	pms, err := f.client.ListPaymentManagers(ctx, &sdk.ListPaymentManagersInput{})
	require.NoError(t, err)
	assert.Empty(t, pms.PaymentManagers)

	aks, err := f.client.ListApiKeyCredentialProviders(ctx, &sdk.ListApiKeyCredentialProvidersInput{})
	require.NoError(t, err)
	assert.Empty(t, aks.CredentialProviders)

	pcp, err := f.client.ListPaymentCredentialProviders(ctx, &sdk.ListPaymentCredentialProvidersInput{})
	require.NoError(t, err)
	assert.Empty(t, pcp.CredentialProviders)
}

func TestProviderUpdatesAndPaymentUpdates(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	_, err := f.client.CreateApiKeyCredentialProvider(
		ctx,
		&sdk.CreateApiKeyCredentialProviderInput{Name: aws.String("k1"), ApiKey: aws.String("one")},
	)
	require.NoError(t, err)

	upd, err := f.client.UpdateApiKeyCredentialProvider(
		ctx,
		&sdk.UpdateApiKeyCredentialProviderInput{Name: aws.String("k1"), ApiKey: aws.String("two")},
	)
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(upd.ApiKeySecretArn.SecretArn), ":secret:")

	oauthIn := func(secret string) types.Oauth2ProviderConfigInput {
		return &types.Oauth2ProviderConfigInputMemberGithubOauth2ProviderConfig{
			Value: types.GithubOauth2ProviderConfigInput{ClientId: aws.String("c"), ClientSecret: aws.String(secret)},
		}
	}

	_, err = f.client.CreateOauth2CredentialProvider(ctx, &sdk.CreateOauth2CredentialProviderInput{
		Name: aws.String(
			"o1",
		),
		CredentialProviderVendor:  types.CredentialProviderVendorTypeGithubOauth2,
		Oauth2ProviderConfigInput: oauthIn("a"),
	})
	require.NoError(t, err)

	_, err = f.client.UpdateOauth2CredentialProvider(ctx, &sdk.UpdateOauth2CredentialProviderInput{
		Name: aws.String(
			"o1",
		),
		CredentialProviderVendor:  types.CredentialProviderVendorTypeGithubOauth2,
		Oauth2ProviderConfigInput: oauthIn("b"),
	})
	require.NoError(t, err)

	_, err = f.client.UpdateOauth2CredentialProvider(ctx, &sdk.UpdateOauth2CredentialProviderInput{
		Name: aws.String(
			"o1",
		),
		CredentialProviderVendor:  types.CredentialProviderVendorTypeGoogleOauth2,
		Oauth2ProviderConfigInput: oauthIn("b"),
	})
	assert.Equal(t, "ValidationException", apiCode(err), "the vendor cannot change")

	_, err = f.client.DeleteOauth2CredentialProvider(
		ctx,
		&sdk.DeleteOauth2CredentialProviderInput{Name: aws.String("o1")},
	)
	require.NoError(t, err)

	_, err = f.client.CreateWorkloadIdentity(ctx, &sdk.CreateWorkloadIdentityInput{Name: aws.String("w1")})
	require.NoError(t, err)

	_, err = f.client.DeleteWorkloadIdentity(ctx, &sdk.DeleteWorkloadIdentityInput{Name: aws.String("w1")})
	require.NoError(t, err)

	cb := &types.PaymentProviderConfigurationInputMemberCoinbaseCdpConfiguration{
		Value: types.CoinbaseCdpConfigurationInput{
			ApiKeyId: aws.String("k"), ApiKeySecret: aws.String("s"), WalletSecret: aws.String("w"),
		},
	}

	prov, err := f.client.CreatePaymentCredentialProvider(ctx, &sdk.CreatePaymentCredentialProviderInput{
		Name: aws.String(
			"pp",
		),
		CredentialProviderVendor:   types.PaymentCredentialProviderVendorTypeCoinbaseCDP,
		ProviderConfigurationInput: cb,
	})
	require.NoError(t, err)

	_, err = f.client.UpdatePaymentCredentialProvider(ctx, &sdk.UpdatePaymentCredentialProviderInput{
		Name: aws.String(
			"pp",
		),
		CredentialProviderVendor:   types.PaymentCredentialProviderVendorTypeCoinbaseCDP,
		ProviderConfigurationInput: cb,
	})
	require.NoError(t, err)

	mgr, err := f.client.CreatePaymentManager(ctx, &sdk.CreatePaymentManagerInput{
		Name: aws.String("pm"), AuthorizerType: types.PaymentsAuthorizerTypeAwsIam, RoleArn: aws.String(testRole),
	})
	require.NoError(t, err)

	_, err = f.client.UpdatePaymentManager(
		ctx,
		&sdk.UpdatePaymentManagerInput{PaymentManagerId: mgr.PaymentManagerId, Description: aws.String("d")},
	)
	require.NoError(t, err)

	gotMgr, err := f.client.GetPaymentManager(ctx, &sdk.GetPaymentManagerInput{PaymentManagerId: mgr.PaymentManagerId})
	require.NoError(t, err)
	assert.Equal(t, types.PaymentManagerStatusReady, gotMgr.Status)

	conn, err := f.client.CreatePaymentConnector(ctx, &sdk.CreatePaymentConnectorInput{
		PaymentManagerId: mgr.PaymentManagerId, Name: aws.String("c"), Type: types.PaymentConnectorTypeCoinbaseCdp,
		CredentialProviderConfigurations: []types.CredentialsProviderConfiguration{
			&types.CredentialsProviderConfigurationMemberCoinbaseCDP{
				Value: types.PaymentCredentialProviderConfiguration{CredentialProviderArn: prov.CredentialProviderArn},
			},
		},
	})
	require.NoError(t, err)

	_, err = f.client.UpdatePaymentConnector(ctx, &sdk.UpdatePaymentConnectorInput{
		PaymentManagerId:   mgr.PaymentManagerId,
		PaymentConnectorId: conn.PaymentConnectorId,
		Description:        aws.String("d"),
	})
	require.NoError(t, err)

	_, err = f.client.DeletePaymentCredentialProvider(
		ctx,
		&sdk.DeletePaymentCredentialProviderInput{Name: aws.String("pp")},
	)
	require.NoError(t, err)
}

func TestCapacityProviderUpdate(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	cp, err := f.client.CreateCapacityProvider(ctx, &sdk.CreateCapacityProviderInput{
		Name: aws.String("cap"),
		ComputeConfiguration: &types.ComputeConfigurationMemberEc2Configuration{Value: types.Ec2Configuration{
			VpcConfiguration: &types.VpcConfiguration{Subnets: []string{"subnet-1"}, SecurityGroups: []string{"sg-1"}},
			LaunchTemplateSource: &types.LaunchTemplateSourceMemberLaunchParameters{Value: types.LaunchParameters{
				InstanceRequirements: &types.InstanceRequirements{
					AllowedInstanceTypes: []string{"m7g.large"},
				},
				OperatingSystem: types.OperatingSystemLinuxArm64,
			}},
		}},
		PermissionsConfiguration: &types.PermissionsConfiguration{
			CapacityProviderOperatorRoleArn: aws.String(testRole),
		},
	})
	require.NoError(t, err)

	got, err := f.client.GetCapacityProvider(
		ctx,
		&sdk.GetCapacityProviderInput{CapacityProviderId: cp.CapacityProviderId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.CapacityProviderStatusReady, got.Status)

	_, err = f.client.UpdateCapacityProvider(
		ctx,
		&sdk.UpdateCapacityProviderInput{
			CapacityProviderId: cp.CapacityProviderId,
			Description:        &types.UpdatedDescription{OptionalValue: aws.String("d")},
		},
	)
	require.NoError(t, err)

	list, err := f.client.ListCapacityProviders(ctx, &sdk.ListCapacityProvidersInput{})
	require.NoError(t, err)
	assert.Len(t, list.CapacityProviders, 1)
}
