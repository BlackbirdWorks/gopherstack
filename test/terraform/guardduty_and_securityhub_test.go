package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysvc "github.com/aws/aws-sdk-go-v2/service/guardduty"
	securityhubsvc "github.com/aws/aws-sdk-go-v2/service/securityhub"
	securityhubtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mega19ProviderBlock is providerBlock with guardduty/securityhub endpoints
// added (providerBlock predates both) and skip_requesting_account_id forced
// to false -- aws_securityhub_organization_configuration (like
// aws_backup_global_settings, see mega17ProviderBlock) uses the caller's
// account ID as its Terraform resource ID.
func mega19ProviderBlock(addr string) string {
	base := mega17ProviderBlock(addr)

	const closing = "  }\n}\n"

	trimmed := strings.TrimSuffix(base, closing)
	if trimmed == base {
		return base
	}

	return trimmed + "    guardduty       = " + `"` + addr + `"` + "\n" +
		"    securityhub     = " + `"` + addr + `"` + "\n" + closing
}

// TestTerraform_MegaBatch19 provisions the 12 GuardDuty resource types
// (detector feature, filter, ipset, malware protection plan, member plus
// member detector feature, organization admin account plus configuration
// plus configuration feature, publishing destination, threatintelset) and
// 13 of the 14 Security Hub resource types (action target, automation rule,
// configuration policy plus association, finding aggregator, insight,
// member, organization admin account plus configuration, product
// subscription, standards control plus association, standards subscription)
// that had no Terraform fixture coverage, and verifies each via its own SDK
// client's Get/List path. aws_guardduty_invite_accepter and
// aws_securityhub_invite_accepter are left out: see PARITY.md.
func TestTerraform_MegaBatch19(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "mega-batch-19",
			providerFn: mega19ProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				gd := guarddutysvc.NewFromConfig(cfg, func(o *guarddutysvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				sh := securityhubsvc.NewFromConfig(cfg, func(o *securityhubsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				detectorID := verifyMegaBatch19GuardDuty(ctx, t, gd)
				verifyMegaBatch19SecurityHub(ctx, t, sh, detectorID)
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

func verifyMegaBatch19GuardDuty(ctx context.Context, t *testing.T, c *guarddutysvc.Client) string {
	t.Helper()

	detOut, err := c.ListDetectors(ctx, &guarddutysvc.ListDetectorsInput{})
	require.NoError(t, err, "ListDetectors should succeed")
	require.NotEmpty(t, detOut.DetectorIds, "a detector should exist after apply")
	detectorID := detOut.DetectorIds[0]

	getOut, err := c.GetDetector(ctx, &guarddutysvc.GetDetectorInput{DetectorId: aws.String(detectorID)})
	require.NoError(t, err, "GetDetector should succeed")

	var foundFeature bool

	for _, f := range getOut.Features {
		if string(f.Name) == "S3_DATA_EVENTS" && string(f.Status) == "ENABLED" {
			foundFeature = true
		}
	}

	assert.True(t, foundFeature, "detector feature should be enabled")

	filterOut, err := c.ListFilters(ctx, &guarddutysvc.ListFiltersInput{DetectorId: aws.String(detectorID)})
	require.NoError(t, err, "ListFilters should succeed")
	assert.Contains(t, filterOut.FilterNames, "mega-batch-19-filter")

	ipsetOut, err := c.ListIPSets(ctx, &guarddutysvc.ListIPSetsInput{DetectorId: aws.String(detectorID)})
	require.NoError(t, err, "ListIPSets should succeed")
	require.NotEmpty(t, ipsetOut.IpSetIds, "ipset should be listed")

	tisOut, err := c.ListThreatIntelSets(
		ctx,
		&guarddutysvc.ListThreatIntelSetsInput{DetectorId: aws.String(detectorID)},
	)
	require.NoError(t, err, "ListThreatIntelSets should succeed")
	require.NotEmpty(t, tisOut.ThreatIntelSetIds, "threat intel set should be listed")

	mppOut, err := c.ListMalwareProtectionPlans(ctx, &guarddutysvc.ListMalwareProtectionPlansInput{})
	require.NoError(t, err, "ListMalwareProtectionPlans should succeed")
	assert.NotEmpty(t, mppOut.MalwareProtectionPlans, "malware protection plan should be listed")

	pdOut, err := c.ListPublishingDestinations(ctx, &guarddutysvc.ListPublishingDestinationsInput{
		DetectorId: aws.String(detectorID),
	})
	require.NoError(t, err, "ListPublishingDestinations should succeed")
	assert.NotEmpty(t, pdOut.Destinations, "publishing destination should be listed")

	memOut, err := c.GetMembers(ctx, &guarddutysvc.GetMembersInput{
		DetectorId: aws.String(detectorID),
		AccountIds: []string{"111122223333"},
	})
	require.NoError(t, err, "GetMembers should succeed")
	require.NotEmpty(t, memOut.Members, "member should be listed")

	mdfOut, err := c.GetMemberDetectors(ctx, &guarddutysvc.GetMemberDetectorsInput{
		DetectorId: aws.String(detectorID),
		AccountIds: []string{"111122223333"},
	})
	require.NoError(t, err, "GetMemberDetectors should succeed")
	require.NotEmpty(t, mdfOut.MemberDataSourceConfigurations, "member detector feature should be listed")

	adminOut, err := c.ListOrganizationAdminAccounts(ctx, &guarddutysvc.ListOrganizationAdminAccountsInput{})
	require.NoError(t, err, "ListOrganizationAdminAccounts should succeed")

	var foundAdmin bool

	for _, a := range adminOut.AdminAccounts {
		if aws.ToString(a.AdminAccountId) == "222233334444" {
			foundAdmin = true
		}
	}

	assert.True(t, foundAdmin, "organization admin account should be listed")

	orgConfOut, err := c.DescribeOrganizationConfiguration(ctx, &guarddutysvc.DescribeOrganizationConfigurationInput{
		DetectorId: aws.String(detectorID),
	})
	require.NoError(t, err, "DescribeOrganizationConfiguration should succeed")
	assert.Equal(t, "ALL", string(orgConfOut.AutoEnableOrganizationMembers))

	return detectorID
}

func verifyMegaBatch19SecurityHub(ctx context.Context, t *testing.T, c *securityhubsvc.Client, _ string) {
	t.Helper()

	hubOut, err := c.DescribeHub(ctx, &securityhubsvc.DescribeHubInput{})
	require.NoError(t, err, "DescribeHub should succeed")
	assert.NotEmpty(t, aws.ToString(hubOut.HubArn))

	atOut, err := c.DescribeActionTargets(ctx, &securityhubsvc.DescribeActionTargetsInput{})
	require.NoError(t, err, "DescribeActionTargets should succeed")

	var foundAction bool

	for _, a := range atOut.ActionTargets {
		if aws.ToString(a.Name) == "mega-batch-19-action" {
			foundAction = true
		}
	}

	assert.True(t, foundAction, "action target should be listed")

	faOut, err := c.ListFindingAggregators(ctx, &securityhubsvc.ListFindingAggregatorsInput{})
	require.NoError(t, err, "ListFindingAggregators should succeed")
	require.NotEmpty(t, faOut.FindingAggregators, "finding aggregator should be listed")

	insOut, err := c.GetInsights(ctx, &securityhubsvc.GetInsightsInput{})
	require.NoError(t, err, "GetInsights should succeed")

	var foundInsight bool

	for _, i := range insOut.Insights {
		if aws.ToString(i.Name) == "mega-batch-19-insight" {
			foundInsight = true
		}
	}

	assert.True(t, foundInsight, "insight should be listed")

	arOut, err := c.BatchGetAutomationRules(ctx, &securityhubsvc.BatchGetAutomationRulesInput{
		AutomationRulesArns: automationRuleArns(ctx, t, c),
	})
	require.NoError(t, err, "BatchGetAutomationRules should succeed")
	require.NotEmpty(t, arOut.Rules)
	assert.Equal(t, "mega-batch-19-automation-rule", aws.ToString(arOut.Rules[0].RuleName))

	subOut, err := c.GetEnabledStandards(ctx, &securityhubsvc.GetEnabledStandardsInput{})
	require.NoError(t, err, "GetEnabledStandards should succeed")
	require.NotEmpty(t, subOut.StandardsSubscriptions, "standards subscription should be listed")

	subscriptionArn := aws.ToString(subOut.StandardsSubscriptions[0].StandardsSubscriptionArn)

	ctrlOut, err := c.DescribeStandardsControls(ctx, &securityhubsvc.DescribeStandardsControlsInput{
		StandardsSubscriptionArn: aws.String(subscriptionArn),
	})
	require.NoError(t, err, "DescribeStandardsControls should succeed")
	require.NotEmpty(t, ctrlOut.Controls)
	assert.Equal(t, "DISABLED", string(ctrlOut.Controls[0].ControlStatus))

	standardsArn := "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0"
	assocIn := &securityhubsvc.BatchGetStandardsControlAssociationsInput{
		StandardsControlAssociationIds: []securityhubtypes.StandardsControlAssociationId{
			{
				SecurityControlId: aws.String("IAM.1"),
				StandardsArn:      aws.String(standardsArn),
			},
		},
	}
	assocOut, err := c.BatchGetStandardsControlAssociations(ctx, assocIn)
	require.NoError(t, err, "BatchGetStandardsControlAssociations should succeed")
	require.NotEmpty(t, assocOut.StandardsControlAssociationDetails)
	assert.Equal(t, "DISABLED", string(assocOut.StandardsControlAssociationDetails[0].AssociationStatus))

	prodOut, err := c.DescribeProducts(ctx, &securityhubsvc.DescribeProductsInput{})
	require.NoError(t, err, "DescribeProducts should succeed")
	assert.NotNil(t, prodOut)

	membersOut, err := c.GetMembers(ctx, &securityhubsvc.GetMembersInput{AccountIds: []string{"111122223333"}})
	require.NoError(t, err, "GetMembers should succeed")
	require.NotEmpty(t, membersOut.Members, "security hub member should be listed")

	cpOut, err := c.ListConfigurationPolicies(ctx, &securityhubsvc.ListConfigurationPoliciesInput{})
	require.NoError(t, err, "ListConfigurationPolicies should succeed")

	var foundPolicy bool

	for _, p := range cpOut.ConfigurationPolicySummaries {
		if aws.ToString(p.Name) == "mega-batch-19-config-policy" {
			foundPolicy = true
		}
	}

	assert.True(t, foundPolicy, "configuration policy should be listed")

	orgConfOut, err := c.DescribeOrganizationConfiguration(
		ctx,
		&securityhubsvc.DescribeOrganizationConfigurationInput{},
	)
	require.NoError(t, err, "DescribeOrganizationConfiguration should succeed")
	assert.True(t, aws.ToBool(orgConfOut.AutoEnable))

	adminOut, err := c.ListOrganizationAdminAccounts(ctx, &securityhubsvc.ListOrganizationAdminAccountsInput{})
	require.NoError(t, err, "ListOrganizationAdminAccounts should succeed")

	var foundAdmin bool

	for _, a := range adminOut.AdminAccounts {
		if aws.ToString(a.AccountId) == "222233334444" {
			foundAdmin = true
		}
	}

	assert.True(t, foundAdmin, "securityhub organization admin account should be listed")
}

// automationRuleArns looks up the automation rule ARN by listing rules,
// since BatchGetAutomationRules requires exact ARNs as input.
func automationRuleArns(ctx context.Context, t *testing.T, c *securityhubsvc.Client) []string {
	t.Helper()

	out, err := c.ListAutomationRules(ctx, &securityhubsvc.ListAutomationRulesInput{})
	require.NoError(t, err, "ListAutomationRules should succeed")
	require.NotEmpty(t, out.AutomationRulesMetadata)

	arns := make([]string, 0, len(out.AutomationRulesMetadata))
	for _, m := range out.AutomationRulesMetadata {
		arns = append(arns, aws.ToString(m.RuleArn))
	}

	return arns
}
