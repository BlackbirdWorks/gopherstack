package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsvc49 "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	grafanasvc49 "github.com/aws/aws-sdk-go-v2/service/grafana"
	inspector2svc49 "github.com/aws/aws-sdk-go-v2/service/inspector2"
	medialivesvc49 "github.com/aws/aws-sdk-go-v2/service/medialive"
	ramsvc49 "github.com/aws/aws-sdk-go-v2/service/ram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_ElasticsearchGrafanaAndRAM provisions a RAM resource share with a principal
// association, a resource association, and a share accepter; a Grafana
// workspace with a license association, a SAML authentication configuration,
// a service account, and a service account token; an Inspector2 delegated
// admin account, a suppression filter, a member association, and an
// organization auto-enable configuration; a MediaLive input security group,
// channel, multiplex, and multiplex program; and an Elasticsearch domain
// with a resource-based access policy, a SAML authentication configuration,
// and a VPC endpoint -- via Terraform, verifying each through its own SDK
// client.
//
// aws_ram_sharing_with_organization is intentionally left out of the fixture
// (see elasticsearch-grafana-and-ram.tf's comment): EnableSharingWithAwsOrganization now
// performs both cross-service side effects its Read depends on (IAM GetRole,
// Organizations ListAWSServiceAccessForOrganization -- services/ram/PARITY.md),
// but this fixture can't safely create or depend on an Organization: it's a
// per-backend singleton and every terraform-fixture test runs against the same
// shared emulator in parallel.
func TestTerraform_ElasticsearchGrafanaAndRAM(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name: "success",
			// macie2ProviderBlock omits skip_requesting_account_id: several
			// resources here (aws_inspector2_organization_configuration) use
			// the caller's account ID as their Terraform resource ID, which
			// comes back empty -- and removes the resource from state right
			// after creation -- under the default providerBlock's
			// skip_requesting_account_id=true.
			fixture:    "elasticsearch-grafana-and-ram",
			providerFn: macie2ProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{"Endpoint": endpoint}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyElasticsearchGrafanaAndRAMRAM(ctx, t)
				verifyElasticsearchGrafanaAndRAMGrafana(ctx, t)
				verifyElasticsearchGrafanaAndRAMInspector2(ctx, t)
				verifyElasticsearchGrafanaAndRAMMediaLive(ctx, t)
				verifyElasticsearchGrafanaAndRAMElasticsearch(ctx, t)
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

func verifyElasticsearchGrafanaAndRAMRAM(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := ramsvc49.NewFromConfig(cfg, func(o *ramsvc49.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	sharesOut, err := client.GetResourceShares(ctx, &ramsvc49.GetResourceSharesInput{
		Name:          aws.String("egar-share"),
		ResourceOwner: "SELF",
	})
	require.NoError(t, err, "GetResourceShares should succeed")
	require.Len(t, sharesOut.ResourceShares, 1)

	shareARN := aws.ToString(sharesOut.ResourceShares[0].ResourceShareArn)

	principalsOut, err := client.ListPrincipals(ctx, &ramsvc49.ListPrincipalsInput{
		ResourceOwner:     "SELF",
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err, "ListPrincipals should succeed")

	var foundPrincipal bool

	for _, p := range principalsOut.Principals {
		if aws.ToString(p.Id) == "999999999999" {
			foundPrincipal = true
		}
	}

	assert.True(t, foundPrincipal, "egar principal association should be listed")

	resourcesOut, err := client.ListResources(ctx, &ramsvc49.ListResourcesInput{
		ResourceOwner:     "SELF",
		ResourceShareArns: []string{shareARN},
	})
	require.NoError(t, err, "ListResources should succeed")
	require.Len(t, resourcesOut.Resources, 1)
	assert.Contains(t, aws.ToString(resourcesOut.Resources[0].Arn), "subnet-")
}

func verifyElasticsearchGrafanaAndRAMGrafana(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := grafanasvc49.NewFromConfig(cfg, func(o *grafanasvc49.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	wsOut, err := client.ListWorkspaces(ctx, &grafanasvc49.ListWorkspacesInput{})
	require.NoError(t, err, "ListWorkspaces should succeed")

	var workspaceID string

	for _, w := range wsOut.Workspaces {
		if aws.ToString(w.Name) == "egar-grafana" {
			workspaceID = aws.ToString(w.Id)
		}
	}

	require.NotEmpty(t, workspaceID, "egar grafana workspace should be listed")

	descOut, err := client.DescribeWorkspace(ctx, &grafanasvc49.DescribeWorkspaceInput{
		WorkspaceId: aws.String(workspaceID),
	})
	require.NoError(t, err, "DescribeWorkspace should succeed")
	assert.Equal(t, "ENTERPRISE", string(descOut.Workspace.LicenseType))

	authOut, err := client.DescribeWorkspaceAuthentication(ctx, &grafanasvc49.DescribeWorkspaceAuthenticationInput{
		WorkspaceId: aws.String(workspaceID),
	})
	require.NoError(t, err, "DescribeWorkspaceAuthentication should succeed")
	require.NotNil(t, authOut.Authentication.Saml)
	require.NotNil(t, authOut.Authentication.Saml.Configuration.RoleValues)
	assert.Contains(t, authOut.Authentication.Saml.Configuration.RoleValues.Admin, "admin")

	saOut, err := client.ListWorkspaceServiceAccounts(ctx, &grafanasvc49.ListWorkspaceServiceAccountsInput{
		WorkspaceId: aws.String(workspaceID),
	})
	require.NoError(t, err, "ListWorkspaceServiceAccounts should succeed")

	var serviceAccountID string

	for _, sa := range saOut.ServiceAccounts {
		if aws.ToString(sa.Name) == "egar-sa" {
			serviceAccountID = aws.ToString(sa.Id)
		}
	}

	require.NotEmpty(t, serviceAccountID, "egar grafana service account should be listed")

	tokensOut, err := client.ListWorkspaceServiceAccountTokens(
		ctx,
		&grafanasvc49.ListWorkspaceServiceAccountTokensInput{
			WorkspaceId:      aws.String(workspaceID),
			ServiceAccountId: aws.String(serviceAccountID),
		},
	)
	require.NoError(t, err, "ListWorkspaceServiceAccountTokens should succeed")

	var foundToken bool

	for _, tok := range tokensOut.ServiceAccountTokens {
		if aws.ToString(tok.Name) == "egar-token" {
			foundToken = true
		}
	}

	assert.True(t, foundToken, "egar grafana service account token should be listed")
}

func verifyElasticsearchGrafanaAndRAMInspector2(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := inspector2svc49.NewFromConfig(cfg, func(o *inspector2svc49.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	adminOut, err := client.GetDelegatedAdminAccount(ctx, &inspector2svc49.GetDelegatedAdminAccountInput{})
	require.NoError(t, err, "GetDelegatedAdminAccount should succeed")
	require.NotNil(t, adminOut.DelegatedAdmin)
	assert.Equal(t, "555566667777", aws.ToString(adminOut.DelegatedAdmin.AccountId))

	filtersOut, err := client.ListFilters(ctx, &inspector2svc49.ListFiltersInput{})
	require.NoError(t, err, "ListFilters should succeed")

	var foundFilter bool

	for _, f := range filtersOut.Filters {
		if aws.ToString(f.Name) == "egar-filter" {
			foundFilter = true
			assert.Equal(t, "SUPPRESS", string(f.Action))
		}
	}

	assert.True(t, foundFilter, "egar inspector2 filter should be listed")

	membersOut, err := client.ListMembers(ctx, &inspector2svc49.ListMembersInput{})
	require.NoError(t, err, "ListMembers should succeed")

	var foundMember bool

	for _, m := range membersOut.Members {
		if aws.ToString(m.AccountId) == "444455556666" {
			foundMember = true
		}
	}

	assert.True(t, foundMember, "egar inspector2 member should be listed")

	cfgOut, err := client.DescribeOrganizationConfiguration(
		ctx,
		&inspector2svc49.DescribeOrganizationConfigurationInput{},
	)
	require.NoError(t, err, "DescribeOrganizationConfiguration should succeed")
	require.NotNil(t, cfgOut.AutoEnable)
	assert.True(t, aws.ToBool(cfgOut.AutoEnable.Ec2))
	assert.True(t, aws.ToBool(cfgOut.AutoEnable.Ecr))
	assert.True(t, aws.ToBool(cfgOut.AutoEnable.Lambda))
}

func verifyElasticsearchGrafanaAndRAMMediaLive(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := medialivesvc49.NewFromConfig(cfg, func(o *medialivesvc49.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	sgOut, err := client.ListInputSecurityGroups(ctx, &medialivesvc49.ListInputSecurityGroupsInput{})
	require.NoError(t, err, "ListInputSecurityGroups should succeed")

	var foundSG bool

	for _, sg := range sgOut.InputSecurityGroups {
		for _, rule := range sg.WhitelistRules {
			if aws.ToString(rule.Cidr) == "10.49.0.0/16" {
				foundSG = true
			}
		}
	}

	assert.True(t, foundSG, "egar medialive input security group should be listed")

	channelsOut, err := client.ListChannels(ctx, &medialivesvc49.ListChannelsInput{})
	require.NoError(t, err, "ListChannels should succeed")

	var channelID string

	for _, c := range channelsOut.Channels {
		if aws.ToString(c.Name) == "egar-channel" {
			channelID = aws.ToString(c.Id)
		}
	}

	require.NotEmpty(t, channelID, "egar medialive channel should be listed")

	descChOut, err := client.DescribeChannel(
		ctx,
		&medialivesvc49.DescribeChannelInput{ChannelId: aws.String(channelID)},
	)
	require.NoError(t, err, "DescribeChannel should succeed")
	assert.Equal(t, "SINGLE_PIPELINE", string(descChOut.ChannelClass))

	multiplexesOut, err := client.ListMultiplexes(ctx, &medialivesvc49.ListMultiplexesInput{})
	require.NoError(t, err, "ListMultiplexes should succeed")

	var multiplexID string

	for _, m := range multiplexesOut.Multiplexes {
		if aws.ToString(m.Name) == "egar-multiplex" {
			multiplexID = aws.ToString(m.Id)
		}
	}

	require.NotEmpty(t, multiplexID, "egar medialive multiplex should be listed")

	programOut, err := client.DescribeMultiplexProgram(ctx, &medialivesvc49.DescribeMultiplexProgramInput{
		MultiplexId: aws.String(multiplexID),
		ProgramName: aws.String("egar-program"),
	})
	require.NoError(t, err, "DescribeMultiplexProgram should succeed")
	require.NotNil(t, programOut.MultiplexProgramSettings.VideoSettings)
	assert.Equal(t, int32(100000), aws.ToInt32(programOut.MultiplexProgramSettings.VideoSettings.ConstantBitrate))
}

func verifyElasticsearchGrafanaAndRAMElasticsearch(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := elasticsearchsvc49.NewFromConfig(cfg, func(o *elasticsearchsvc49.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	domOut, err := client.DescribeElasticsearchDomain(ctx, &elasticsearchsvc49.DescribeElasticsearchDomainInput{
		DomainName: aws.String("egar-es"),
	})
	require.NoError(t, err, "DescribeElasticsearchDomain should succeed")
	require.NotNil(t, domOut.DomainStatus)

	configOut, err := client.DescribeElasticsearchDomainConfig(
		ctx,
		&elasticsearchsvc49.DescribeElasticsearchDomainConfigInput{DomainName: aws.String("egar-es")},
	)
	require.NoError(t, err, "DescribeElasticsearchDomainConfig should succeed")
	require.NotNil(t, configOut.DomainConfig.AccessPolicies)
	assert.Contains(
		t,
		aws.ToString(configOut.DomainConfig.AccessPolicies.Options),
		"ElasticsearchGrafanaAndRAMESPolicy",
	)

	require.NotNil(t, configOut.DomainConfig.AdvancedSecurityOptions)
	require.NotNil(t, configOut.DomainConfig.AdvancedSecurityOptions.Options)
	require.NotNil(t, configOut.DomainConfig.AdvancedSecurityOptions.Options.SAMLOptions)
	assert.True(t, aws.ToBool(configOut.DomainConfig.AdvancedSecurityOptions.Options.SAMLOptions.Enabled))

	domainARN := aws.ToString(domOut.DomainStatus.ARN)

	listOut, err := client.ListVpcEndpointsForDomain(ctx, &elasticsearchsvc49.ListVpcEndpointsForDomainInput{
		DomainName: aws.String("egar-es"),
	})
	require.NoError(t, err, "ListVpcEndpointsForDomain should succeed")
	require.Len(t, listOut.VpcEndpointSummaryList, 1)

	vpcEndpointsOut, err := client.DescribeVpcEndpoints(ctx, &elasticsearchsvc49.DescribeVpcEndpointsInput{
		VpcEndpointIds: []string{aws.ToString(listOut.VpcEndpointSummaryList[0].VpcEndpointId)},
	})
	require.NoError(t, err, "DescribeVpcEndpoints should succeed")
	require.Len(t, vpcEndpointsOut.VpcEndpoints, 1)
	assert.Equal(t, domainARN, aws.ToString(vpcEndpointsOut.VpcEndpoints[0].DomainArn))
}
