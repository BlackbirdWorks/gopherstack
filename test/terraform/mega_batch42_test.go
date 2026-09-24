package terraform_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasvc42 "github.com/aws/aws-sdk-go-v2/service/acmpca"
	appconfigsvc42 "github.com/aws/aws-sdk-go-v2/service/appconfig"
	bedrocksvc42 "github.com/aws/aws-sdk-go-v2/service/bedrock"
	bedrockagentsvc42 "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	verifiedpermissionssvc42 "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	vptypes42 "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch42 provisions a Bedrock Agents supervisor/collaborator
// pair with an alias, collaborator association, Lambda action group, and a
// knowledge base with an S3 data source and association; Bedrock guardrail
// version, inference profile, model invocation logging, a custom model
// customization job, and provisioned model throughput; a Verified Permissions
// policy store with a schema, policy template, static and template-linked
// policies, and a Cognito-backed identity source; an ACM PCA root CA with its
// self-signed certificate, permission, and resource policy; and an AppConfig
// application/environment/hosted configuration/deployment plus an extension
// and its association -- via Terraform, verifying each through its own SDK
// client.
func TestTerraform_MegaBatch42(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-42",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()

				functionZip := filepath.Join(dir, "mega-batch-42-function.zip")
				writeZipFixture(t, functionZip, "index.js",
					"exports.handler = async () => ({statusCode: 200});\n")

				return map[string]any{
					"FunctionZip": functionZip,
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch42BedrockAgent(ctx, t)
				verifyMegaBatch42Bedrock(ctx, t)
				verifyMegaBatch42VerifiedPermissions(ctx, t)
				verifyMegaBatch42ACMPCA(ctx, t)
				verifyMegaBatch42AppConfig(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyMegaBatch42BedrockAgent(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := bedrockagentsvc42.NewFromConfig(cfg, func(o *bedrockagentsvc42.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	agentsOut, err := client.ListAgents(ctx, &bedrockagentsvc42.ListAgentsInput{})
	require.NoError(t, err, "ListAgents should succeed")

	var supervisorID, collaboratorID string

	for _, a := range agentsOut.AgentSummaries {
		switch aws.ToString(a.AgentName) {
		case "mb42-supervisor-agent":
			supervisorID = aws.ToString(a.AgentId)
		case "mb42-collaborator-agent":
			collaboratorID = aws.ToString(a.AgentId)
		}
	}

	require.NotEmpty(t, supervisorID, "supervisor agent should be listed")
	require.NotEmpty(t, collaboratorID, "collaborator agent should be listed")

	getSupervisor, err := client.GetAgent(ctx, &bedrockagentsvc42.GetAgentInput{AgentId: aws.String(supervisorID)})
	require.NoError(t, err, "GetAgent should succeed for supervisor")
	assert.Equal(t, "SUPERVISOR", string(getSupervisor.Agent.AgentCollaboration),
		"supervisor should have SUPERVISOR collaboration")

	aliasesOut, err := client.ListAgentAliases(ctx, &bedrockagentsvc42.ListAgentAliasesInput{
		AgentId: aws.String(collaboratorID),
	})
	require.NoError(t, err, "ListAgentAliases should succeed")
	require.NotEmpty(t, aliasesOut.AgentAliasSummaries)

	var aliasID string

	for _, al := range aliasesOut.AgentAliasSummaries {
		if aws.ToString(al.AgentAliasName) == "mb42-collaborator-alias" {
			aliasID = aws.ToString(al.AgentAliasId)
		}
	}

	require.NotEmpty(t, aliasID, "collaborator alias should be listed")

	getAlias, err := client.GetAgentAlias(ctx, &bedrockagentsvc42.GetAgentAliasInput{
		AgentId:      aws.String(collaboratorID),
		AgentAliasId: aws.String(aliasID),
	})
	require.NoError(t, err, "GetAgentAlias should succeed")
	assert.Equal(t, "mb42-collaborator-alias", aws.ToString(getAlias.AgentAlias.AgentAliasName))

	collabsOut, err := client.ListAgentCollaborators(ctx, &bedrockagentsvc42.ListAgentCollaboratorsInput{
		AgentId:      aws.String(supervisorID),
		AgentVersion: aws.String("DRAFT"),
	})
	require.NoError(t, err, "ListAgentCollaborators should succeed")
	require.NotEmpty(t, collabsOut.AgentCollaboratorSummaries)

	var collaboratorAssocID string

	for _, c := range collabsOut.AgentCollaboratorSummaries {
		if aws.ToString(c.CollaboratorName) == "mb42-collaborator" {
			collaboratorAssocID = aws.ToString(c.CollaboratorId)
		}
	}

	require.NotEmpty(t, collaboratorAssocID, "collaborator association should be listed")

	getCollab, err := client.GetAgentCollaborator(ctx, &bedrockagentsvc42.GetAgentCollaboratorInput{
		AgentId:        aws.String(supervisorID),
		AgentVersion:   aws.String("DRAFT"),
		CollaboratorId: aws.String(collaboratorAssocID),
	})
	require.NoError(t, err, "GetAgentCollaborator should succeed")
	assert.Equal(t, aws.ToString(getAlias.AgentAlias.AgentAliasArn),
		aws.ToString(getCollab.AgentCollaborator.AgentDescriptor.AliasArn))

	actionGroupsOut, err := client.ListAgentActionGroups(ctx, &bedrockagentsvc42.ListAgentActionGroupsInput{
		AgentId:      aws.String(supervisorID),
		AgentVersion: aws.String("DRAFT"),
	})
	require.NoError(t, err, "ListAgentActionGroups should succeed")

	var actionGroupID string

	for _, ag := range actionGroupsOut.ActionGroupSummaries {
		if aws.ToString(ag.ActionGroupName) == "mb42-action-group" {
			actionGroupID = aws.ToString(ag.ActionGroupId)
		}
	}

	require.NotEmpty(t, actionGroupID, "action group should be listed")

	getActionGroup, err := client.GetAgentActionGroup(ctx, &bedrockagentsvc42.GetAgentActionGroupInput{
		AgentId:       aws.String(supervisorID),
		AgentVersion:  aws.String("DRAFT"),
		ActionGroupId: aws.String(actionGroupID),
	})
	require.NoError(t, err, "GetAgentActionGroup should succeed")
	require.NotNil(t, getActionGroup.AgentActionGroup.ActionGroupExecutor)

	kbsOut, err := client.ListKnowledgeBases(ctx, &bedrockagentsvc42.ListKnowledgeBasesInput{})
	require.NoError(t, err, "ListKnowledgeBases should succeed")

	var kbID string

	for _, kb := range kbsOut.KnowledgeBaseSummaries {
		if aws.ToString(kb.Name) == "mb42-knowledge-base" {
			kbID = aws.ToString(kb.KnowledgeBaseId)
		}
	}

	require.NotEmpty(t, kbID, "knowledge base should be listed")

	getKBAssoc, err := client.GetAgentKnowledgeBase(ctx, &bedrockagentsvc42.GetAgentKnowledgeBaseInput{
		AgentId:         aws.String(supervisorID),
		AgentVersion:    aws.String("DRAFT"),
		KnowledgeBaseId: aws.String(kbID),
	})
	require.NoError(t, err, "GetAgentKnowledgeBase should succeed")
	assert.Equal(t, "ENABLED", string(getKBAssoc.AgentKnowledgeBase.KnowledgeBaseState))

	dsOut, err := client.ListDataSources(ctx, &bedrockagentsvc42.ListDataSourcesInput{
		KnowledgeBaseId: aws.String(kbID),
	})
	require.NoError(t, err, "ListDataSources should succeed")

	var dataSourceID string

	for _, ds := range dsOut.DataSourceSummaries {
		if aws.ToString(ds.Name) == "mb42-data-source" {
			dataSourceID = aws.ToString(ds.DataSourceId)
		}
	}

	require.NotEmpty(t, dataSourceID, "data source should be listed")

	getDS, err := client.GetDataSource(ctx, &bedrockagentsvc42.GetDataSourceInput{
		KnowledgeBaseId: aws.String(kbID),
		DataSourceId:    aws.String(dataSourceID),
	})
	require.NoError(t, err, "GetDataSource should succeed")
	assert.Equal(t, "mb42-data-source", aws.ToString(getDS.DataSource.Name))
}

func verifyMegaBatch42Bedrock(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := bedrocksvc42.NewFromConfig(cfg, func(o *bedrocksvc42.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	guardrailsOut, err := client.ListGuardrails(ctx, &bedrocksvc42.ListGuardrailsInput{})
	require.NoError(t, err, "ListGuardrails should succeed")

	var guardrailID string

	for _, g := range guardrailsOut.Guardrails {
		if aws.ToString(g.Name) == "mega-batch-42-guardrail" {
			guardrailID = aws.ToString(g.Id)
		}
	}

	require.NotEmpty(t, guardrailID, "guardrail should be listed")

	getGuardrailVersion, err := client.GetGuardrail(ctx, &bedrocksvc42.GetGuardrailInput{
		GuardrailIdentifier: aws.String(guardrailID),
		GuardrailVersion:    aws.String("1"),
	})
	require.NoError(t, err, "GetGuardrail should succeed for version 1")
	assert.Equal(t, "1", aws.ToString(getGuardrailVersion.Version))

	profOut, err := client.GetInferenceProfile(ctx, &bedrocksvc42.GetInferenceProfileInput{
		InferenceProfileIdentifier: aws.String("mega-batch-42-inference-profile"),
	})
	require.NoError(t, err, "GetInferenceProfile should succeed")
	assert.Equal(t, "mega-batch-42-inference-profile", aws.ToString(profOut.InferenceProfileName))

	loggingOut, err := client.GetModelInvocationLoggingConfiguration(
		ctx, &bedrocksvc42.GetModelInvocationLoggingConfigurationInput{},
	)
	require.NoError(t, err, "GetModelInvocationLoggingConfiguration should succeed")
	require.NotNil(t, loggingOut.LoggingConfig)
	require.NotNil(t, loggingOut.LoggingConfig.S3Config)
	assert.Equal(t, "mega-batch-42-logging-bucket", aws.ToString(loggingOut.LoggingConfig.S3Config.BucketName))
	require.NotNil(t, loggingOut.LoggingConfig.CloudWatchConfig)
	assert.Contains(t, aws.ToString(loggingOut.LoggingConfig.CloudWatchConfig.LogGroupName), "mega-batch-42")

	// The customization job completes asynchronously (bedrock's janitor advances
	// InProgress jobs to Completed on a short fixed delay); poll until the
	// resulting custom model is materialized.
	var customModelOut *bedrocksvc42.GetCustomModelOutput

	require.Eventually(t, func() bool {
		var getErr error

		customModelOut, getErr = client.GetCustomModel(ctx, &bedrocksvc42.GetCustomModelInput{
			ModelIdentifier: aws.String("mega-batch-42-custom-model"),
		})

		return getErr == nil
	}, 20*time.Second, 500*time.Millisecond,
		"GetCustomModel should eventually succeed once the customization job completes")
	assert.Equal(t, "mega-batch-42-custom-model", aws.ToString(customModelOut.ModelName))

	provisionedOut, err := client.GetProvisionedModelThroughput(ctx, &bedrocksvc42.GetProvisionedModelThroughputInput{
		ProvisionedModelId: aws.String("mega-batch-42-provisioned-throughput"),
	})
	require.NoError(t, err, "GetProvisionedModelThroughput should succeed")
	assert.EqualValues(t, 1, aws.ToInt32(provisionedOut.DesiredModelUnits))
}

func verifyMegaBatch42VerifiedPermissions(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := verifiedpermissionssvc42.NewFromConfig(cfg, func(o *verifiedpermissionssvc42.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	storesOut, err := client.ListPolicyStores(ctx, &verifiedpermissionssvc42.ListPolicyStoresInput{})
	require.NoError(t, err, "ListPolicyStores should succeed")

	var storeID string

	for _, s := range storesOut.PolicyStores {
		if aws.ToString(s.Description) == "mega-batch-42 policy store" {
			storeID = aws.ToString(s.PolicyStoreId)
		}
	}

	require.NotEmpty(t, storeID, "policy store should be listed")

	getStore, err := client.GetPolicyStore(ctx, &verifiedpermissionssvc42.GetPolicyStoreInput{
		PolicyStoreId: aws.String(storeID),
	})
	require.NoError(t, err, "GetPolicyStore should succeed")
	assert.Equal(t, "OFF", string(getStore.ValidationSettings.Mode))

	schemaOut, err := client.GetSchema(ctx, &verifiedpermissionssvc42.GetSchemaInput{
		PolicyStoreId: aws.String(storeID),
	})
	require.NoError(t, err, "GetSchema should succeed")
	assert.Contains(t, aws.ToString(schemaOut.Schema), "MB42")

	templatesOut, err := client.ListPolicyTemplates(ctx, &verifiedpermissionssvc42.ListPolicyTemplatesInput{
		PolicyStoreId: aws.String(storeID),
	})
	require.NoError(t, err, "ListPolicyTemplates should succeed")
	require.NotEmpty(t, templatesOut.PolicyTemplates)

	templateID := aws.ToString(templatesOut.PolicyTemplates[0].PolicyTemplateId)

	getTemplate, err := client.GetPolicyTemplate(ctx, &verifiedpermissionssvc42.GetPolicyTemplateInput{
		PolicyStoreId:    aws.String(storeID),
		PolicyTemplateId: aws.String(templateID),
	})
	require.NoError(t, err, "GetPolicyTemplate should succeed")
	assert.Contains(t, aws.ToString(getTemplate.Statement), "permit")

	policiesOut, err := client.ListPolicies(ctx, &verifiedpermissionssvc42.ListPoliciesInput{
		PolicyStoreId: aws.String(storeID),
	})
	require.NoError(t, err, "ListPolicies should succeed")
	require.Len(t, policiesOut.Policies, 2, "expected one static and one template-linked policy")

	var staticCount, templateLinkedCount int

	for _, p := range policiesOut.Policies {
		switch p.Definition.(type) {
		case *vptypes42.PolicyDefinitionItemMemberStatic:
			staticCount++
		case *vptypes42.PolicyDefinitionItemMemberTemplateLinked:
			templateLinkedCount++
		}
	}

	assert.Equal(t, 1, staticCount, "expected exactly one static policy")
	assert.Equal(t, 1, templateLinkedCount, "expected exactly one template-linked policy")

	identitySourcesOut, err := client.ListIdentitySources(ctx, &verifiedpermissionssvc42.ListIdentitySourcesInput{
		PolicyStoreId: aws.String(storeID),
	})
	require.NoError(t, err, "ListIdentitySources should succeed")
	require.NotEmpty(t, identitySourcesOut.IdentitySources)

	idSourceID := aws.ToString(identitySourcesOut.IdentitySources[0].IdentitySourceId)

	getIDSource, err := client.GetIdentitySource(ctx, &verifiedpermissionssvc42.GetIdentitySourceInput{
		PolicyStoreId:    aws.String(storeID),
		IdentitySourceId: aws.String(idSourceID),
	})
	require.NoError(t, err, "GetIdentitySource should succeed")
	require.NotNil(t, getIDSource.Configuration)
}

func verifyMegaBatch42ACMPCA(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := acmpcasvc42.NewFromConfig(cfg, func(o *acmpcasvc42.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	listOut, err := client.ListCertificateAuthorities(ctx, &acmpcasvc42.ListCertificateAuthoritiesInput{})
	require.NoError(t, err, "ListCertificateAuthorities should succeed")

	var caArn string

	for _, ca := range listOut.CertificateAuthorities {
		if ca.CertificateAuthorityConfiguration != nil && ca.CertificateAuthorityConfiguration.Subject != nil &&
			aws.ToString(ca.CertificateAuthorityConfiguration.Subject.CommonName) == "mega-batch-42.example.com" {
			caArn = aws.ToString(ca.Arn)
		}
	}

	require.NotEmpty(t, caArn, "mega-batch-42 CA should be listed")

	describeOut, err := client.DescribeCertificateAuthority(ctx, &acmpcasvc42.DescribeCertificateAuthorityInput{
		CertificateAuthorityArn: aws.String(caArn),
	})
	require.NoError(t, err, "DescribeCertificateAuthority should succeed")
	assert.Equal(t, "ACTIVE", string(describeOut.CertificateAuthority.Status),
		"CA should be active once its certificate is installed")

	getCACert, err := client.GetCertificateAuthorityCertificate(
		ctx,
		&acmpcasvc42.GetCertificateAuthorityCertificateInput{
			CertificateAuthorityArn: aws.String(caArn),
		},
	)
	require.NoError(t, err, "GetCertificateAuthorityCertificate should succeed")
	assert.NotEmpty(t, aws.ToString(getCACert.Certificate))

	permsOut, err := client.ListPermissions(ctx, &acmpcasvc42.ListPermissionsInput{
		CertificateAuthorityArn: aws.String(caArn),
	})
	require.NoError(t, err, "ListPermissions should succeed")
	require.NotEmpty(t, permsOut.Permissions)
	assert.Equal(t, "acm.amazonaws.com", aws.ToString(permsOut.Permissions[0].Principal))

	policyOut, err := client.GetPolicy(ctx, &acmpcasvc42.GetPolicyInput{
		ResourceArn: aws.String(caArn),
	})
	require.NoError(t, err, "GetPolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "MB42AcmPcaPolicy")
}

func verifyMegaBatch42AppConfig(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := appconfigsvc42.NewFromConfig(cfg, func(o *appconfigsvc42.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	appsOut, err := client.ListApplications(ctx, &appconfigsvc42.ListApplicationsInput{})
	require.NoError(t, err, "ListApplications should succeed")

	var appID string

	for _, a := range appsOut.Items {
		if aws.ToString(a.Name) == "mega-batch-42-app" {
			appID = aws.ToString(a.Id)
		}
	}

	require.NotEmpty(t, appID, "application should be listed")

	envsOut, err := client.ListEnvironments(ctx, &appconfigsvc42.ListEnvironmentsInput{
		ApplicationId: aws.String(appID),
	})
	require.NoError(t, err, "ListEnvironments should succeed")

	var envID string

	for _, e := range envsOut.Items {
		if aws.ToString(e.Name) == "mega-batch-42-env" {
			envID = aws.ToString(e.Id)
		}
	}

	require.NotEmpty(t, envID, "environment should be listed")

	profilesOut, err := client.ListConfigurationProfiles(ctx, &appconfigsvc42.ListConfigurationProfilesInput{
		ApplicationId: aws.String(appID),
	})
	require.NoError(t, err, "ListConfigurationProfiles should succeed")

	var profileID string

	for _, p := range profilesOut.Items {
		if aws.ToString(p.Name) == "mega-batch-42-profile" {
			profileID = aws.ToString(p.Id)
		}
	}

	require.NotEmpty(t, profileID, "configuration profile should be listed")

	versionsOut, err := client.ListHostedConfigurationVersions(
		ctx,
		&appconfigsvc42.ListHostedConfigurationVersionsInput{
			ApplicationId:          aws.String(appID),
			ConfigurationProfileId: aws.String(profileID),
		},
	)
	require.NoError(t, err, "ListHostedConfigurationVersions should succeed")
	require.NotEmpty(t, versionsOut.Items)

	versionNumber := versionsOut.Items[0].VersionNumber

	getVersion, err := client.GetHostedConfigurationVersion(ctx, &appconfigsvc42.GetHostedConfigurationVersionInput{
		ApplicationId:          aws.String(appID),
		ConfigurationProfileId: aws.String(profileID),
		VersionNumber:          aws.Int32(versionNumber),
	})
	require.NoError(t, err, "GetHostedConfigurationVersion should succeed")
	assert.Contains(t, string(getVersion.Content), "isThingEnabled")

	strategiesOut, err := client.ListDeploymentStrategies(ctx, &appconfigsvc42.ListDeploymentStrategiesInput{})
	require.NoError(t, err, "ListDeploymentStrategies should succeed")

	var strategyID string

	for _, s := range strategiesOut.Items {
		if aws.ToString(s.Name) == "mega-batch-42-deployment-strategy" {
			strategyID = aws.ToString(s.Id)
		}
	}

	require.NotEmpty(t, strategyID, "deployment strategy should be listed")

	deploymentsOut, err := client.ListDeployments(ctx, &appconfigsvc42.ListDeploymentsInput{
		ApplicationId: aws.String(appID),
		EnvironmentId: aws.String(envID),
	})
	require.NoError(t, err, "ListDeployments should succeed")
	require.NotEmpty(t, deploymentsOut.Items)

	getDeployment, err := client.GetDeployment(ctx, &appconfigsvc42.GetDeploymentInput{
		ApplicationId:    aws.String(appID),
		EnvironmentId:    aws.String(envID),
		DeploymentNumber: aws.Int32(deploymentsOut.Items[0].DeploymentNumber),
	})
	require.NoError(t, err, "GetDeployment should succeed")
	assert.Equal(t, "mega-batch-42 deployment", aws.ToString(getDeployment.Description))

	extsOut, err := client.ListExtensions(ctx, &appconfigsvc42.ListExtensionsInput{})
	require.NoError(t, err, "ListExtensions should succeed")

	var extensionArn string

	for _, e := range extsOut.Items {
		if aws.ToString(e.Name) == "mega-batch-42-extension" {
			extensionArn = aws.ToString(e.Arn)
		}
	}

	require.NotEmpty(t, extensionArn, "extension should be listed")

	assocsOut, err := client.ListExtensionAssociations(ctx, &appconfigsvc42.ListExtensionAssociationsInput{
		ExtensionIdentifier: aws.String(extensionArn),
	})
	require.NoError(t, err, "ListExtensionAssociations should succeed")
	require.NotEmpty(t, assocsOut.Items)
	assert.Contains(t, aws.ToString(assocsOut.Items[0].ResourceArn), appID)
}
