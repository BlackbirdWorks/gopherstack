package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsvc37 "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes37 "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	cbsvc37 "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cidpsvc37 "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	ecssvc37 "github.com/aws/aws-sdk-go-v2/service/ecs"
	waf2svc37 "github.com/aws/aws-sdk-go-v2/service/wafv2"
	waf2types37 "github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_CognitoEcsCloudformationAndWafv2 provisions Cognito identity provider/resource
// server/user pool domain/UI customization/risk configuration, ECS account
// setting default/capacity provider/cluster association/tag/task set,
// CloudFormation self-managed StackSet with a stack-set instance, a bulk
// stack-instances deployment, and a private resource type, CodeBuild fleet/
// report group/resource policy/source credential/webhook, and WAFv2 IP set/
// regex pattern set/rule group/API key/web ACL association/logging
// configuration via Terraform, verifying each through its own SDK client.
func TestTerraform_CognitoEcsCloudformationAndWafv2(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "cognito-ecs-cloudformation-and-wafv2",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return vpcCIDRVars(t)
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyCognitoEcsCloudformationAndWafv2Cognito(ctx, t)
				verifyCognitoEcsCloudformationAndWafv2ECS(ctx, t)
				verifyCognitoEcsCloudformationAndWafv2CloudFormation(ctx, t)
				verifyCognitoEcsCloudformationAndWafv2CodeBuild(ctx, t)
				verifyCognitoEcsCloudformationAndWafv2WAFv2(ctx, t)
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

func verifyCognitoEcsCloudformationAndWafv2Cognito(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := cidpsvc37.NewFromConfig(cfg, func(o *cidpsvc37.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	poolsOut, err := client.ListUserPools(ctx, &cidpsvc37.ListUserPoolsInput{MaxResults: aws.Int32(60)})
	require.NoError(t, err, "ListUserPools should succeed")

	var poolID string

	for _, p := range poolsOut.UserPools {
		if aws.ToString(p.Name) == "cecw-pool" {
			poolID = aws.ToString(p.Id)
		}
	}

	require.NotEmpty(t, poolID, "cecw user pool should be listed")

	idpOut, err := client.ListIdentityProviders(ctx, &cidpsvc37.ListIdentityProvidersInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err, "ListIdentityProviders should succeed")
	require.NotEmpty(t, idpOut.Providers)
	assert.Equal(t, "CognitoEcsCloudformationAndWafv2Google", aws.ToString(idpOut.Providers[0].ProviderName))

	rsOut, err := client.DescribeResourceServer(ctx, &cidpsvc37.DescribeResourceServerInput{
		UserPoolId: aws.String(poolID),
		Identifier: aws.String("cecw-api"),
	})
	require.NoError(t, err, "DescribeResourceServer should succeed")
	require.Len(t, rsOut.ResourceServer.Scopes, 2)

	domainOut, err := client.DescribeUserPoolDomain(ctx, &cidpsvc37.DescribeUserPoolDomainInput{
		Domain: aws.String("cecw-domain"),
	})
	require.NoError(t, err, "DescribeUserPoolDomain should succeed")
	require.NotNil(t, domainOut.DomainDescription)
	assert.Equal(t, poolID, aws.ToString(domainOut.DomainDescription.UserPoolId))

	uiOut, err := client.GetUICustomization(ctx, &cidpsvc37.GetUICustomizationInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String("ALL"),
	})
	require.NoError(t, err, "GetUICustomization should succeed")
	assert.Contains(t, aws.ToString(uiOut.UICustomization.CSS), "label-customizable")

	riskOut, err := client.DescribeRiskConfiguration(ctx, &cidpsvc37.DescribeRiskConfigurationInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err, "DescribeRiskConfiguration should succeed")
	require.NotNil(t, riskOut.RiskConfiguration.CompromisedCredentialsRiskConfiguration)
}

func verifyCognitoEcsCloudformationAndWafv2ECS(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := ecssvc37.NewFromConfig(cfg, func(o *ecssvc37.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	settingsOut, err := client.ListAccountSettings(ctx, &ecssvc37.ListAccountSettingsInput{
		Name:              "containerInsights",
		EffectiveSettings: true,
	})
	require.NoError(t, err, "ListAccountSettings should succeed")
	require.NotEmpty(t, settingsOut.Settings)
	assert.Equal(t, "enabled", aws.ToString(settingsOut.Settings[0].Value))

	cpOut, err := client.DescribeCapacityProviders(ctx, &ecssvc37.DescribeCapacityProvidersInput{
		CapacityProviders: []string{"cecw-cp"},
	})
	require.NoError(t, err, "DescribeCapacityProviders should succeed")
	require.NotEmpty(t, cpOut.CapacityProviders)
	require.NotNil(t, cpOut.CapacityProviders[0].AutoScalingGroupProvider)

	clustersOut, err := client.DescribeClusters(ctx, &ecssvc37.DescribeClustersInput{
		Clusters: []string{"cecw-cluster"},
	})
	require.NoError(t, err, "DescribeClusters should succeed")
	require.Len(t, clustersOut.Clusters, 1)
	assert.Contains(t, clustersOut.Clusters[0].CapacityProviders, "cecw-cp")

	clusterArn := aws.ToString(clustersOut.Clusters[0].ClusterArn)

	tagsOut, err := client.ListTagsForResource(ctx, &ecssvc37.ListTagsForResourceInput{
		ResourceArn: aws.String(clusterArn),
	})
	require.NoError(t, err, "ListTagsForResource should succeed")

	var foundTag bool

	for _, tag := range tagsOut.Tags {
		if aws.ToString(tag.Key) == "cecw-key" && aws.ToString(tag.Value) == "cecw-value" {
			foundTag = true
		}
	}

	assert.True(t, foundTag, "cluster tag should be present")

	servicesOut, err := client.DescribeServices(ctx, &ecssvc37.DescribeServicesInput{
		Cluster:  aws.String(clusterArn),
		Services: []string{"cecw-service"},
	})
	require.NoError(t, err, "DescribeServices should succeed")
	require.Len(t, servicesOut.Services, 1)

	taskSetsOut, err := client.DescribeTaskSets(ctx, &ecssvc37.DescribeTaskSetsInput{
		Cluster: aws.String(clusterArn),
		Service: servicesOut.Services[0].ServiceArn,
	})
	require.NoError(t, err, "DescribeTaskSets should succeed")
	require.NotEmpty(t, taskSetsOut.TaskSets)
}

func verifyCognitoEcsCloudformationAndWafv2CloudFormation(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := cfnsvc37.NewFromConfig(cfg, func(o *cfnsvc37.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	ssOut, err := client.DescribeStackSet(ctx, &cfnsvc37.DescribeStackSetInput{
		StackSetName: aws.String("cecw-stackset"),
	})
	require.NoError(t, err, "DescribeStackSet should succeed")
	require.NotNil(t, ssOut.StackSet)

	instOut, err := client.DescribeStackInstance(ctx, &cfnsvc37.DescribeStackInstanceInput{
		StackSetName:         aws.String("cecw-stackset"),
		StackInstanceAccount: aws.String("000000000000"),
		StackInstanceRegion:  aws.String("us-east-1"),
	})
	require.NoError(t, err, "DescribeStackInstance should succeed")
	require.NotNil(t, instOut.StackInstance)

	listOut, err := client.ListStackInstances(ctx, &cfnsvc37.ListStackInstancesInput{
		StackSetName: aws.String("cecw-stackset-bulk"),
	})
	require.NoError(t, err, "ListStackInstances should succeed")

	var foundBulkInstance bool

	for _, inst := range listOut.Summaries {
		if aws.ToString(inst.Region) == "us-west-2" && aws.ToString(inst.Account) == "000000000000" {
			foundBulkInstance = true
		}
	}

	assert.True(t, foundBulkInstance, "bulk stack-instances deployment should be listed")

	typeOut, err := client.DescribeType(ctx, &cfnsvc37.DescribeTypeInput{
		Type:     cfntypes37.RegistryTypeResource,
		TypeName: aws.String("CognitoEcsCloudformationAndWafv2::Example::Resource"),
	})
	require.NoError(t, err, "DescribeType should succeed")
	assert.Equal(t, "CognitoEcsCloudformationAndWafv2::Example::Resource", aws.ToString(typeOut.TypeName))
}

func verifyCognitoEcsCloudformationAndWafv2CodeBuild(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := cbsvc37.NewFromConfig(cfg, func(o *cbsvc37.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	fleetsOut, err := client.BatchGetFleets(ctx, &cbsvc37.BatchGetFleetsInput{
		Names: []string{"cecw-fleet"},
	})
	require.NoError(t, err, "BatchGetFleets should succeed")
	require.Len(t, fleetsOut.Fleets, 1)
	assert.EqualValues(t, 1, aws.ToInt32(fleetsOut.Fleets[0].BaseCapacity))

	rgOut, err := client.BatchGetReportGroups(ctx, &cbsvc37.BatchGetReportGroupsInput{
		ReportGroupArns: []string{
			"arn:aws:codebuild:us-east-1:000000000000:report-group/cecw-report-group",
		},
	})
	require.NoError(t, err, "BatchGetReportGroups should succeed")
	require.Len(t, rgOut.ReportGroups, 1)

	reportGroupArn := aws.ToString(rgOut.ReportGroups[0].Arn)

	policyOut, err := client.GetResourcePolicy(ctx, &cbsvc37.GetResourcePolicyInput{
		ResourceArn: aws.String(reportGroupArn),
	})
	require.NoError(t, err, "GetResourcePolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "cecw-policy")

	credsOut, err := client.ListSourceCredentials(ctx, &cbsvc37.ListSourceCredentialsInput{})
	require.NoError(t, err, "ListSourceCredentials should succeed")

	var foundCred bool

	for _, c := range credsOut.SourceCredentialsInfos {
		if c.ServerType == "GITHUB" && c.AuthType == "PERSONAL_ACCESS_TOKEN" {
			foundCred = true
		}
	}

	assert.True(t, foundCred, "GitHub PAT source credential should be listed")

	projOut, err := client.BatchGetProjects(ctx, &cbsvc37.BatchGetProjectsInput{
		Names: []string{"cecw-project"},
	})
	require.NoError(t, err, "BatchGetProjects should succeed")
	require.Len(t, projOut.Projects, 1)
	require.NotNil(t, projOut.Projects[0].Webhook, "project should have a webhook attached")
}

func verifyCognitoEcsCloudformationAndWafv2WAFv2(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := waf2svc37.NewFromConfig(cfg, func(o *waf2svc37.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	ipSetsOut, err := client.ListIPSets(ctx, &waf2svc37.ListIPSetsInput{Scope: waf2types37.ScopeRegional})
	require.NoError(t, err, "ListIPSets should succeed")

	ipSetID := findWAFv2SummaryID(t, ipSetsOut.IPSets, "cecw-ip-set")

	getIPSet, err := client.GetIPSet(ctx, &waf2svc37.GetIPSetInput{
		Name:  aws.String("cecw-ip-set"),
		Scope: waf2types37.ScopeRegional,
		Id:    aws.String(ipSetID),
	})
	require.NoError(t, err, "GetIPSet should succeed")
	assert.ElementsMatch(t, []string{"10.0.0.0/16", "192.168.0.0/24"}, getIPSet.IPSet.Addresses)

	regexOut, err := client.ListRegexPatternSets(ctx, &waf2svc37.ListRegexPatternSetsInput{
		Scope: waf2types37.ScopeRegional,
	})
	require.NoError(t, err, "ListRegexPatternSets should succeed")

	regexID := findWAFv2SummaryID(t, regexOut.RegexPatternSets, "cecw-regex-pattern-set")

	getRegex, err := client.GetRegexPatternSet(ctx, &waf2svc37.GetRegexPatternSetInput{
		Name:  aws.String("cecw-regex-pattern-set"),
		Scope: waf2types37.ScopeRegional,
		Id:    aws.String(regexID),
	})
	require.NoError(t, err, "GetRegexPatternSet should succeed")
	require.Len(t, getRegex.RegexPatternSet.RegularExpressionList, 1)
	assert.Equal(t, "cecw-.*", aws.ToString(getRegex.RegexPatternSet.RegularExpressionList[0].RegexString))

	rgOut, err := client.ListRuleGroups(ctx, &waf2svc37.ListRuleGroupsInput{Scope: waf2types37.ScopeRegional})
	require.NoError(t, err, "ListRuleGroups should succeed")

	ruleGroupID := findWAFv2SummaryID(t, rgOut.RuleGroups, "cecw-rule-group")

	getRG, err := client.GetRuleGroup(ctx, &waf2svc37.GetRuleGroupInput{
		Name:  aws.String("cecw-rule-group"),
		Scope: waf2types37.ScopeRegional,
		Id:    aws.String(ruleGroupID),
	})
	require.NoError(t, err, "GetRuleGroup should succeed")
	require.Len(t, getRG.RuleGroup.Rules, 2)

	apiKeysOut, err := client.ListAPIKeys(ctx, &waf2svc37.ListAPIKeysInput{Scope: waf2types37.ScopeRegional})
	require.NoError(t, err, "ListAPIKeys should succeed")
	assert.NotEmpty(t, apiKeysOut.APIKeySummaries)

	waclOut, err := client.ListWebACLs(ctx, &waf2svc37.ListWebACLsInput{Scope: waf2types37.ScopeRegional})
	require.NoError(t, err, "ListWebACLs should succeed")

	waclID := findWAFv2SummaryID(t, waclOut.WebACLs, "cecw-web-acl")

	getWACL, err := client.GetWebACL(ctx, &waf2svc37.GetWebACLInput{
		Name:  aws.String("cecw-web-acl"),
		Scope: waf2types37.ScopeRegional,
		Id:    aws.String(waclID),
	})
	require.NoError(t, err, "GetWebACL should succeed")

	resOut, err := client.ListResourcesForWebACL(ctx, &waf2svc37.ListResourcesForWebACLInput{
		WebACLArn:    getWACL.WebACL.ARN,
		ResourceType: waf2types37.ResourceTypeApplicationLoadBalancer,
	})
	require.NoError(t, err, "ListResourcesForWebACL should succeed")
	assert.NotEmpty(t, resOut.ResourceArns, "ALB should be associated with the web ACL")

	loggingOut, err := client.GetLoggingConfiguration(ctx, &waf2svc37.GetLoggingConfigurationInput{
		ResourceArn: getWACL.WebACL.ARN,
	})
	require.NoError(t, err, "GetLoggingConfiguration should succeed")
	require.NotEmpty(t, loggingOut.LoggingConfiguration.LogDestinationConfigs)
	assert.Contains(t, loggingOut.LoggingConfiguration.LogDestinationConfigs[0], "aws-waf-logs-cecw")
}

// wafv2Summary is the shape shared by IPSetSummary/RegexPatternSetSummary/
// RuleGroupSummary/WebACLSummary that findWAFv2SummaryID needs.
type wafv2Summary interface {
	waf2types37.IPSetSummary |
		waf2types37.RegexPatternSetSummary |
		waf2types37.RuleGroupSummary |
		waf2types37.WebACLSummary
}

func findWAFv2SummaryID[T wafv2Summary](t *testing.T, summaries []T, name string) string {
	t.Helper()

	for _, s := range summaries {
		switch v := any(s).(type) {
		case waf2types37.IPSetSummary:
			if aws.ToString(v.Name) == name {
				return aws.ToString(v.Id)
			}
		case waf2types37.RegexPatternSetSummary:
			if aws.ToString(v.Name) == name {
				return aws.ToString(v.Id)
			}
		case waf2types37.RuleGroupSummary:
			if aws.ToString(v.Name) == name {
				return aws.ToString(v.Id)
			}
		case waf2types37.WebACLSummary:
			if aws.ToString(v.Name) == name {
				return aws.ToString(v.Id)
			}
		}
	}

	require.Failf(t, "summary not found", "no summary named %q", name)

	return ""
}
