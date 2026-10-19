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

func TestConfigurationBundleVersions(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	comps := map[string]types.ComponentConfiguration{
		"agent": {Configuration: document.NewLazyDocument(map[string]any{"temp": 0.1})},
	}

	created, err := f.client.CreateConfigurationBundle(ctx, &sdk.CreateConfigurationBundleInput{
		BundleName: aws.String("prod-config"), Components: comps, BranchName: aws.String("main"),
	})
	require.NoError(t, err)

	v1 := created.VersionId

	upd, err := f.client.UpdateConfigurationBundle(ctx, &sdk.UpdateConfigurationBundleInput{
		BundleId: created.BundleId, ParentVersionIds: []string{aws.ToString(v1)}, CommitMessage: aws.String("tune"),
		Components: comps, BranchName: aws.String("tuning"),
	})
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(v1), aws.ToString(upd.VersionId))

	_, err = f.client.UpdateConfigurationBundle(ctx, &sdk.UpdateConfigurationBundleInput{
		BundleId: created.BundleId, ParentVersionIds: []string{"missing"}, CommitMessage: aws.String("x"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	all, err := f.client.ListConfigurationBundleVersions(
		ctx,
		&sdk.ListConfigurationBundleVersionsInput{BundleId: created.BundleId},
	)
	require.NoError(t, err)
	assert.Len(t, all.Versions, 2)

	branch, err := f.client.ListConfigurationBundleVersions(ctx, &sdk.ListConfigurationBundleVersionsInput{
		BundleId: created.BundleId, Filter: &types.VersionFilter{BranchName: aws.String("tuning")},
	})
	require.NoError(t, err)
	require.Len(t, branch.Versions, 1)
	assert.Equal(t, upd.VersionId, branch.Versions[0].VersionId)

	one, err := f.client.GetConfigurationBundleVersion(
		ctx,
		&sdk.GetConfigurationBundleVersionInput{BundleId: created.BundleId, VersionId: v1},
	)
	require.NoError(t, err)
	assert.Equal(t, "prod-config", aws.ToString(one.BundleName))
	assert.Equal(t, "main", aws.ToString(one.LineageMetadata.BranchName))

	latest, err := f.client.GetConfigurationBundle(
		ctx,
		&sdk.GetConfigurationBundleInput{BundleId: created.BundleId, BranchName: aws.String("main")},
	)
	require.NoError(t, err)
	assert.Equal(t, v1, latest.VersionId)

	_, err = f.client.GetConfigurationBundle(
		ctx,
		&sdk.GetConfigurationBundleInput{BundleId: created.BundleId, BranchName: aws.String("ghost")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestHarnessVersionsAndEndpoints(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	created, err := f.client.CreateHarness(ctx, &sdk.CreateHarnessInput{
		HarnessName:      aws.String("helper"),
		ExecutionRoleArn: aws.String(testRole),
		Model: &types.HarnessModelConfigurationMemberBedrockModelConfig{
			Value: types.HarnessBedrockModelConfig{ModelId: aws.String("m")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "1", aws.ToString(created.Harness.HarnessVersion))
	assert.Equal(t, types.HarnessStatusCreating, created.Harness.Status)

	id := created.Harness.HarnessId

	upd, err := f.client.UpdateHarness(
		ctx,
		&sdk.UpdateHarnessInput{HarnessId: id, SystemPrompt: []types.HarnessSystemContentBlock{
			&types.HarnessSystemContentBlockMemberText{Value: "be brief"},
		}},
	)
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(upd.Harness.HarnessVersion))

	old, err := f.client.GetHarness(ctx, &sdk.GetHarnessInput{HarnessId: id, HarnessVersion: aws.String("1")})
	require.NoError(t, err)
	assert.Empty(t, old.Harness.SystemPrompt)

	versions, err := f.client.ListHarnessVersions(ctx, &sdk.ListHarnessVersionsInput{HarnessId: id})
	require.NoError(t, err)
	assert.Len(t, versions.HarnessVersions, 2)

	ep, err := f.client.CreateHarnessEndpoint(ctx, &sdk.CreateHarnessEndpointInput{
		HarnessId: id, EndpointName: aws.String("live"), TargetVersion: aws.String("1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "1", aws.ToString(ep.Endpoint.TargetVersion))

	_, err = f.client.CreateHarnessEndpoint(ctx, &sdk.CreateHarnessEndpointInput{
		HarnessId: id, EndpointName: aws.String("bad"), TargetVersion: aws.String("7"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.UpdateHarnessEndpoint(ctx, &sdk.UpdateHarnessEndpointInput{
		HarnessId: id, EndpointName: aws.String("live"), TargetVersion: aws.String("2"),
	})
	require.NoError(t, err)

	eps, err := f.client.ListHarnessEndpoints(ctx, &sdk.ListHarnessEndpointsInput{HarnessId: id})
	require.NoError(t, err)
	assert.Len(t, eps.Endpoints, 1)

	_, err = f.client.DeleteHarness(ctx, &sdk.DeleteHarnessInput{HarnessId: id})
	require.NoError(t, err)
	assert.Equal(t, 0, f.backend.ResourceCount(), "endpoints go with the harness")
}

func TestSandboxResources(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	system, err := f.client.ListBrowsers(ctx, &sdk.ListBrowsersInput{Type: types.ResourceTypeSystem})
	require.NoError(t, err)
	require.Len(t, system.BrowserSummaries, 1)
	assert.Equal(t, "aws.browser.v1", aws.ToString(system.BrowserSummaries[0].BrowserId))

	_, err = f.client.DeleteBrowser(ctx, &sdk.DeleteBrowserInput{BrowserId: aws.String("aws.browser.v1")})
	assert.Equal(t, "AccessDeniedException", apiCode(err))

	br, err := f.client.CreateBrowser(ctx, &sdk.CreateBrowserInput{
		Name: aws.String(
			"shop",
		),
		NetworkConfiguration: &types.BrowserNetworkConfiguration{NetworkMode: types.BrowserNetworkModePublic},
	})
	require.NoError(t, err)
	assert.Equal(t, types.BrowserStatusCreating, br.Status)

	_, err = f.client.CreateBrowser(ctx, &sdk.CreateBrowserInput{
		Name: aws.String(
			"vpcb",
		),
		NetworkConfiguration: &types.BrowserNetworkConfiguration{NetworkMode: types.BrowserNetworkModeVpc},
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	custom, err := f.client.ListBrowsers(ctx, &sdk.ListBrowsersInput{Type: types.ResourceTypeCustom})
	require.NoError(t, err)
	require.Len(t, custom.BrowserSummaries, 1)

	ci, err := f.client.CreateCodeInterpreter(ctx, &sdk.CreateCodeInterpreterInput{
		Name: aws.String(
			"py",
		),
		NetworkConfiguration: &types.CodeInterpreterNetworkConfiguration{
			NetworkMode: types.CodeInterpreterNetworkModeSandbox,
		},
	})
	require.NoError(t, err)

	got, err := f.client.GetCodeInterpreter(ctx, &sdk.GetCodeInterpreterInput{CodeInterpreterId: ci.CodeInterpreterId})
	require.NoError(t, err)
	assert.Equal(t, types.CodeInterpreterStatusReady, got.Status)

	prof, err := f.client.CreateBrowserProfile(ctx, &sdk.CreateBrowserProfileInput{Name: aws.String("profile")})
	require.NoError(t, err)

	profs, err := f.client.ListBrowserProfiles(ctx, &sdk.ListBrowserProfilesInput{Name: aws.String("PRO")})
	require.NoError(t, err)
	assert.Len(t, profs.ProfileSummaries, 1)

	_, err = f.client.DeleteBrowserProfile(ctx, &sdk.DeleteBrowserProfileInput{ProfileId: prof.ProfileId})
	require.NoError(t, err)
}

func TestIdentityResources(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetReferences(fakeRefs{secrets: map[string]bool{}})
	ctx := t.Context()

	wi, err := f.client.CreateWorkloadIdentity(ctx, &sdk.CreateWorkloadIdentityInput{
		Name: aws.String("wl"), AllowedResourceOauth2ReturnUrls: []string{"https://app/cb"},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(wi.WorkloadIdentityArn), "workload-identity/wl")

	_, err = f.client.CreateWorkloadIdentity(ctx, &sdk.CreateWorkloadIdentityInput{Name: aws.String("wl")})
	assert.Equal(t, "ConflictException", apiCode(err))

	_, err = f.client.UpdateWorkloadIdentity(
		ctx,
		&sdk.UpdateWorkloadIdentityInput{
			Name:                            aws.String("wl"),
			AllowedResourceOauth2ReturnUrls: []string{"https://b"},
		},
	)
	require.NoError(t, err)

	got, err := f.client.GetWorkloadIdentity(ctx, &sdk.GetWorkloadIdentityInput{Name: aws.String("wl")})
	require.NoError(t, err)
	assert.Equal(t, []string{"https://b"}, got.AllowedResourceOauth2ReturnUrls)
	assert.NotNil(t, got.CreatedTime)

	key, err := f.client.CreateApiKeyCredentialProvider(ctx, &sdk.CreateApiKeyCredentialProviderInput{
		Name: aws.String("keys"), ApiKey: aws.String("sekret"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.SecretSourceTypeManaged, key.ApiKeySecretSource)
	assert.Contains(t, aws.ToString(key.ApiKeySecretArn.SecretArn), ":secret:")

	_, err = f.client.CreateApiKeyCredentialProvider(
		ctx,
		&sdk.CreateApiKeyCredentialProviderInput{Name: aws.String("nokey")},
	)
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.CreateApiKeyCredentialProvider(ctx, &sdk.CreateApiKeyCredentialProviderInput{
		Name: aws.String("ext"), ApiKeySecretConfig: &types.SecretReference{
			SecretId: aws.String(
				"arn:aws:secretsmanager:us-east-1:" + testAccount + ":secret:mine-AbCdEf",
			),
			JsonKey: aws.String("k"),
		},
	})
	require.Error(t, err)
	assert.Equal(t, "ValidationException", apiCode(err), "the secret does not exist: %v", err)

	oauth, err := f.client.CreateOauth2CredentialProvider(ctx, &sdk.CreateOauth2CredentialProviderInput{
		Name: aws.String("gh"), CredentialProviderVendor: types.CredentialProviderVendorTypeGithubOauth2,
		Oauth2ProviderConfigInput: &types.Oauth2ProviderConfigInputMemberGithubOauth2ProviderConfig{
			Value: types.GithubOauth2ProviderConfigInput{ClientId: aws.String("cid"), ClientSecret: aws.String("csec")},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(oauth.CallbackUrl))
	assert.Contains(t, aws.ToString(oauth.ClientSecretArn.SecretArn), ":secret:")

	gotO, err := f.client.GetOauth2CredentialProvider(
		ctx,
		&sdk.GetOauth2CredentialProviderInput{Name: aws.String("gh")},
	)
	require.NoError(t, err)

	cfg, ok := gotO.Oauth2ProviderConfigOutput.(*types.Oauth2ProviderConfigOutputMemberGithubOauth2ProviderConfig)
	require.True(t, ok)
	assert.Equal(t, "cid", aws.ToString(cfg.Value.ClientId))

	lst, err := f.client.ListOauth2CredentialProviders(ctx, &sdk.ListOauth2CredentialProvidersInput{})
	require.NoError(t, err)
	assert.Len(t, lst.CredentialProviders, 1)

	for _, name := range []string{"keys"} {
		_, err = f.client.DeleteApiKeyCredentialProvider(
			ctx,
			&sdk.DeleteApiKeyCredentialProviderInput{Name: aws.String(name)},
		)
		require.NoError(t, err)
	}

	_, err = f.client.GetApiKeyCredentialProvider(ctx, &sdk.GetApiKeyCredentialProviderInput{Name: aws.String("keys")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestTokenVault(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	got, err := f.client.GetTokenVault(ctx, &sdk.GetTokenVaultInput{})
	require.NoError(t, err)
	assert.Equal(t, types.KeyTypeServiceManagedKey, got.KmsConfiguration.KeyType)

	_, err = f.client.SetTokenVaultCMK(ctx, &sdk.SetTokenVaultCMKInput{
		KmsConfiguration: &types.KmsConfiguration{
			KeyType:   types.KeyTypeCustomerManagedKey,
			KmsKeyArn: aws.String("nope"),
		},
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	key := "arn:aws:kms:us-east-1:" + testAccount + ":key/abc"

	_, err = f.client.SetTokenVaultCMK(ctx, &sdk.SetTokenVaultCMKInput{
		KmsConfiguration: &types.KmsConfiguration{KeyType: types.KeyTypeCustomerManagedKey, KmsKeyArn: aws.String(key)},
	})
	require.NoError(t, err)

	got, err = f.client.GetTokenVault(ctx, &sdk.GetTokenVaultInput{})
	require.NoError(t, err)
	assert.Equal(t, key, aws.ToString(got.KmsConfiguration.KmsKeyArn))
}

func TestPolicyEngineAndGeneration(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	eng, err := f.client.CreatePolicyEngine(ctx, &sdk.CreatePolicyEngineInput{Name: aws.String("guard")})
	require.NoError(t, err)

	cedar := &types.PolicyDefinitionMemberCedar{
		Value: types.CedarPolicy{Statement: aws.String("permit(principal, action, resource);")},
	}

	pol, err := f.client.CreatePolicy(
		ctx,
		&sdk.CreatePolicyInput{PolicyEngineId: eng.PolicyEngineId, Name: aws.String("allow_all"), Definition: cedar},
	)
	require.NoError(t, err)
	assert.Equal(t, types.PolicyStatusCreating, pol.Status)

	_, err = f.client.CreatePolicy(ctx, &sdk.CreatePolicyInput{
		PolicyEngineId: eng.PolicyEngineId, Name: aws.String("empty"),
		Definition: &types.PolicyDefinitionMemberCedar{Value: types.CedarPolicy{Statement: aws.String(" ")}},
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	sum, err := f.client.ListPolicySummaries(ctx, &sdk.ListPolicySummariesInput{PolicyEngineId: eng.PolicyEngineId})
	require.NoError(t, err)
	assert.Len(t, sum.Policies, 1)

	scoped, err := f.client.ListPolicies(
		ctx,
		&sdk.ListPoliciesInput{PolicyEngineId: eng.PolicyEngineId, TargetResourceScope: aws.String("nomatch")},
	)
	require.NoError(t, err)
	assert.Empty(t, scoped.Policies)

	_, err = f.client.DeletePolicyEngine(ctx, &sdk.DeletePolicyEngineInput{PolicyEngineId: eng.PolicyEngineId})
	assert.Equal(t, "ConflictException", apiCode(err))

	gen, err := f.client.StartPolicyGeneration(ctx, &sdk.StartPolicyGenerationInput{
		PolicyEngineId: eng.PolicyEngineId, Name: aws.String("nl"),
		Resource: &types.ResourceMemberArn{Value: "arn:aws:bedrock-agentcore:us-east-1:" + testAccount + ":gateway/g"},
		Content:  &types.ContentMemberRawText{Value: "allow everyone"},
	})
	require.NoError(t, err)
	assert.Equal(t, types.PolicyGenerationStatusGenerating, gen.Status)

	got, err := f.client.GetPolicyGenerationSummary(ctx, &sdk.GetPolicyGenerationSummaryInput{
		PolicyEngineId: eng.PolicyEngineId, PolicyGenerationId: gen.PolicyGenerationId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.PolicyGenerationStatusGenerateFailed, got.Status)

	assets, err := f.client.ListPolicyGenerationAssets(ctx, &sdk.ListPolicyGenerationAssetsInput{
		PolicyEngineId: eng.PolicyEngineId, PolicyGenerationId: gen.PolicyGenerationId,
	})
	require.NoError(t, err)
	assert.Empty(t, assets.PolicyGenerationAssets)
}

func TestRegistryApprovalFlow(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	reg, err := f.client.CreateRegistry(ctx, &sdk.CreateRegistryInput{Name: aws.String("catalog")})
	require.NoError(t, err)

	regID := aws.ToString(reg.RegistryArn)[len(aws.ToString(reg.RegistryArn))-len(regIDOf(aws.ToString(reg.RegistryArn))):]

	rec, err := f.client.CreateRegistryRecord(ctx, &sdk.CreateRegistryRecordInput{
		RegistryId: aws.String(regID), Name: aws.String("tool"), DescriptorType: types.DescriptorTypeCustom,
		Descriptors: &types.Descriptors{Custom: &types.CustomDescriptor{InlineContent: aws.String(`{"a":1}`)}},
	})
	require.NoError(t, err)
	assert.Equal(t, types.RegistryRecordStatusCreating, rec.Status)

	recID := regIDOf(aws.ToString(rec.RecordArn))

	got, err := f.client.GetRegistryRecord(
		ctx,
		&sdk.GetRegistryRecordInput{RegistryId: aws.String(regID), RecordId: aws.String(recID)},
	)
	require.NoError(t, err)
	assert.Equal(t, types.RegistryRecordStatusDraft, got.Status)

	_, err = f.client.UpdateRegistryRecordStatus(ctx, &sdk.UpdateRegistryRecordStatusInput{
		RegistryId: aws.String(
			regID,
		),
		RecordId:     aws.String(recID),
		Status:       types.RegistryRecordStatusApproved,
		StatusReason: aws.String("x"),
	})
	assert.Equal(t, "ConflictException", apiCode(err), "a draft must be submitted first")

	sub, err := f.client.SubmitRegistryRecordForApproval(
		ctx,
		&sdk.SubmitRegistryRecordForApprovalInput{RegistryId: aws.String(regID), RecordId: aws.String(recID)},
	)
	require.NoError(t, err)
	assert.Equal(t, types.RegistryRecordStatusPendingApproval, sub.Status)

	_, err = f.client.DeleteRegistry(ctx, &sdk.DeleteRegistryInput{RegistryId: aws.String(regID)})
	assert.Equal(t, "ConflictException", apiCode(err))

	ok, err := f.client.UpdateRegistryRecordStatus(ctx, &sdk.UpdateRegistryRecordStatusInput{
		RegistryId: aws.String(
			regID,
		),
		RecordId:     aws.String(recID),
		Status:       types.RegistryRecordStatusApproved,
		StatusReason: aws.String("ship it"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.RegistryRecordStatusApproved, ok.Status)

	approved, err := f.client.ListRegistryRecords(
		ctx,
		&sdk.ListRegistryRecordsInput{RegistryId: aws.String(regID), Status: types.RegistryRecordStatusApproved},
	)
	require.NoError(t, err)
	assert.Len(t, approved.RegistryRecords, 1)

	drafts, err := f.client.ListRegistryRecords(
		ctx,
		&sdk.ListRegistryRecordsInput{RegistryId: aws.String(regID), Status: types.RegistryRecordStatusDraft},
	)
	require.NoError(t, err)
	assert.Empty(t, drafts.RegistryRecords)

	_, err = f.client.DeleteRegistryRecord(
		ctx,
		&sdk.DeleteRegistryRecordInput{RegistryId: aws.String(regID), RecordId: aws.String(recID)},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteRegistry(ctx, &sdk.DeleteRegistryInput{RegistryId: aws.String(regID)})
	require.NoError(t, err)
}

func regIDOf(arn string) string {
	for i := len(arn) - 1; i >= 0; i-- {
		if arn[i] == '/' {
			return arn[i+1:]
		}
	}

	return arn
}

func TestCapacityProviderUse(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetReferences(fakeRefs{repos: map[string]bool{"agent": true}})
	ctx := t.Context()

	cp, err := f.client.CreateCapacityProvider(ctx, &sdk.CreateCapacityProviderInput{
		Name: aws.String("pool"),
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

	_, err = f.client.CreateAgentRuntime(ctx, &sdk.CreateAgentRuntimeInput{
		AgentRuntimeName: aws.String(
			"pooled",
		),
		AgentRuntimeArtifact: containerArtifact(testImage),
		RoleArn:              aws.String(testRole),
		CapacityProviderConfiguration: &types.CapacityProviderConfiguration{
			CapacityProviderArn: cp.CapacityProviderArn,
		},
	})
	require.NoError(t, err)

	by, err := f.client.ListAgentRuntimeVersionsByCapacityProvider(
		ctx,
		&sdk.ListAgentRuntimeVersionsByCapacityProviderInput{CapacityProviderId: cp.CapacityProviderId},
	)
	require.NoError(t, err)
	assert.Len(t, by.AgentRuntimes, 1)

	_, err = f.client.DeleteCapacityProvider(
		ctx,
		&sdk.DeleteCapacityProviderInput{CapacityProviderId: cp.CapacityProviderId},
	)
	assert.Equal(t, "ConflictException", apiCode(err))
}
