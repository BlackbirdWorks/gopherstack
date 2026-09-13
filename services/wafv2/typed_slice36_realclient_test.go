package wafv2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

func newTestWAFV2ClientSlice36(t *testing.T) *wafv2sdk.Client {
	t.Helper()

	backend := wafv2.NewInMemoryBackend("123456789012", "us-east-1")

	return newTestWAFV2Client(t, wafv2.NewHandler(backend))
}

func slice36VisibilityConfig() *types.VisibilityConfig {
	return &types.VisibilityConfig{
		CloudWatchMetricsEnabled: true,
		MetricName:               aws.String("metric"),
		SampledRequestsEnabled:   true,
	}
}

// TestIPSet_TypedLifecycle drives GetIPSet, UpdateIPSet, ListIPSets and
// DeleteIPSet through a real SDK client.
func TestIPSet_TypedLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateIPSet(t.Context(), &wafv2sdk.CreateIPSetInput{
		Name:             aws.String("my-ipset"),
		Scope:            types.ScopeRegional,
		IPAddressVersion: types.IPAddressVersionIpv4,
		Addresses:        []string{"10.0.0.0/8"},
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.Summary.Id)

	getOut, err := client.GetIPSet(t.Context(), &wafv2sdk.GetIPSetInput{
		Id:    aws.String(id),
		Name:  aws.String("my-ipset"),
		Scope: types.ScopeRegional,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.IPSet)
	assert.Equal(t, []string{"10.0.0.0/8"}, getOut.IPSet.Addresses)

	updateOut, err := client.UpdateIPSet(t.Context(), &wafv2sdk.UpdateIPSetInput{
		Id:        aws.String(id),
		Name:      aws.String("my-ipset"),
		Scope:     types.ScopeRegional,
		LockToken: aws.String(aws.ToString(getOut.LockToken)),
		Addresses: []string{"10.0.0.0/8", "192.168.0.0/16"},
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.NextLockToken)

	listOut, err := client.ListIPSets(t.Context(), &wafv2sdk.ListIPSetsInput{
		Scope: types.ScopeRegional,
	})
	require.NoError(t, err)
	var found bool
	for _, s := range listOut.IPSets {
		if aws.ToString(s.Id) == id {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.DeleteIPSet(t.Context(), &wafv2sdk.DeleteIPSetInput{
		Id:        aws.String(id),
		Name:      aws.String("my-ipset"),
		Scope:     types.ScopeRegional,
		LockToken: updateOut.NextLockToken,
	})
	require.NoError(t, err)
}

// TestRegexPatternSet_TypedLifecycle drives GetRegexPatternSet,
// UpdateRegexPatternSet, ListRegexPatternSets and DeleteRegexPatternSet
// through a real SDK client.
func TestRegexPatternSet_TypedLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateRegexPatternSet(t.Context(), &wafv2sdk.CreateRegexPatternSetInput{
		Name:                  aws.String("my-regexset"),
		Scope:                 types.ScopeRegional,
		RegularExpressionList: []types.Regex{{RegexString: aws.String("^abc$")}},
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.Summary.Id)

	getOut, err := client.GetRegexPatternSet(t.Context(), &wafv2sdk.GetRegexPatternSetInput{
		Id:    aws.String(id),
		Name:  aws.String("my-regexset"),
		Scope: types.ScopeRegional,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.RegexPatternSet)
	require.Len(t, getOut.RegexPatternSet.RegularExpressionList, 1)
	assert.Equal(t, "^abc$", aws.ToString(getOut.RegexPatternSet.RegularExpressionList[0].RegexString))

	updateOut, err := client.UpdateRegexPatternSet(t.Context(), &wafv2sdk.UpdateRegexPatternSetInput{
		Id:                    aws.String(id),
		Name:                  aws.String("my-regexset"),
		Scope:                 types.ScopeRegional,
		LockToken:             getOut.LockToken,
		RegularExpressionList: []types.Regex{{RegexString: aws.String("^xyz$")}},
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.NextLockToken)

	listOut, err := client.ListRegexPatternSets(t.Context(), &wafv2sdk.ListRegexPatternSetsInput{
		Scope: types.ScopeRegional,
	})
	require.NoError(t, err)
	var found bool
	for _, s := range listOut.RegexPatternSets {
		if aws.ToString(s.Id) == id {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.DeleteRegexPatternSet(t.Context(), &wafv2sdk.DeleteRegexPatternSetInput{
		Id:        aws.String(id),
		Name:      aws.String("my-regexset"),
		Scope:     types.ScopeRegional,
		LockToken: updateOut.NextLockToken,
	})
	require.NoError(t, err)
}

// TestRuleGroup_TypedDeleteAndList drives ListRuleGroups and DeleteRuleGroup
// through a real SDK client.
func TestRuleGroup_TypedDeleteAndList(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateRuleGroup(t.Context(), &wafv2sdk.CreateRuleGroupInput{
		Name:             aws.String("my-rulegroup"),
		Scope:            types.ScopeRegional,
		Capacity:         aws.Int64(10),
		VisibilityConfig: slice36VisibilityConfig(),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.Summary.Id)

	listOut, err := client.ListRuleGroups(t.Context(), &wafv2sdk.ListRuleGroupsInput{
		Scope: types.ScopeRegional,
	})
	require.NoError(t, err)
	var found bool
	for _, rg := range listOut.RuleGroups {
		if aws.ToString(rg.Id) == id {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.DeleteRuleGroup(t.Context(), &wafv2sdk.DeleteRuleGroupInput{
		Id:        aws.String(id),
		Name:      aws.String("my-rulegroup"),
		Scope:     types.ScopeRegional,
		LockToken: createOut.Summary.LockToken,
	})
	require.NoError(t, err)
}

// TestWebACL_TypedDeleteAndAssociation drives DeleteWebACL,
// DisassociateWebACL and DeleteFirewallManagerRuleGroups through a real SDK
// client.
func TestWebACL_TypedDeleteAndAssociation(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateWebACL(t.Context(), &wafv2sdk.CreateWebACLInput{
		Name:             aws.String("my-acl"),
		Scope:            types.ScopeRegional,
		DefaultAction:    &types.DefaultAction{Allow: &types.AllowAction{}},
		VisibilityConfig: slice36VisibilityConfig(),
	})
	require.NoError(t, err)
	webACLArn := aws.ToString(createOut.Summary.ARN)

	_, err = client.AssociateWebACL(t.Context(), &wafv2sdk.AssociateWebACLInput{
		WebACLArn: aws.String(webACLArn),
		ResourceArn: aws.String(
			"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-lb/50dc6c495c0c9188",
		),
	})
	require.NoError(t, err)

	_, err = client.DisassociateWebACL(t.Context(), &wafv2sdk.DisassociateWebACLInput{
		ResourceArn: aws.String(
			"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-lb/50dc6c495c0c9188",
		),
	})
	require.NoError(t, err)

	fmOut, err := client.DeleteFirewallManagerRuleGroups(t.Context(), &wafv2sdk.DeleteFirewallManagerRuleGroupsInput{
		WebACLArn:       aws.String(webACLArn),
		WebACLLockToken: createOut.Summary.LockToken,
	})
	require.NoError(t, err)
	require.NotNil(t, fmOut.NextWebACLLockToken)

	_, err = client.DeleteWebACL(t.Context(), &wafv2sdk.DeleteWebACLInput{
		Id:        createOut.Summary.Id,
		Name:      aws.String("my-acl"),
		Scope:     types.ScopeRegional,
		LockToken: fmOut.NextWebACLLockToken,
	})
	require.NoError(t, err)
}

// TestAPIKey_TypedLifecycle drives DeleteAPIKey through a real SDK client
// (CreateAPIKey/ListAPIKeys are already covered).
func TestAPIKey_TypedLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateAPIKey(t.Context(), &wafv2sdk.CreateAPIKeyInput{
		Scope:        types.ScopeRegional,
		TokenDomains: []string{"example.com"},
	})
	require.NoError(t, err)
	key := aws.ToString(createOut.APIKey)
	require.NotEmpty(t, key)

	_, err = client.DeleteAPIKey(t.Context(), &wafv2sdk.DeleteAPIKeyInput{
		Scope:  types.ScopeRegional,
		APIKey: aws.String(key),
	})
	require.NoError(t, err)
}

// TestLoggingConfiguration_TypedRoundTrip drives PutLoggingConfiguration,
// GetLoggingConfiguration and DeleteLoggingConfiguration through a real SDK
// client.
func TestLoggingConfiguration_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateWebACL(t.Context(), &wafv2sdk.CreateWebACLInput{
		Name:             aws.String("my-acl"),
		Scope:            types.ScopeRegional,
		DefaultAction:    &types.DefaultAction{Allow: &types.AllowAction{}},
		VisibilityConfig: slice36VisibilityConfig(),
	})
	require.NoError(t, err)
	webACLArn := aws.ToString(createOut.Summary.ARN)

	putOut, err := client.PutLoggingConfiguration(t.Context(), &wafv2sdk.PutLoggingConfigurationInput{
		LoggingConfiguration: &types.LoggingConfiguration{
			ResourceArn:           aws.String(webACLArn),
			LogDestinationConfigs: []string{"arn:aws:s3:::my-log-bucket"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, putOut.LoggingConfiguration)
	assert.Equal(t, webACLArn, aws.ToString(putOut.LoggingConfiguration.ResourceArn))
	assert.Equal(t, []string{"arn:aws:s3:::my-log-bucket"}, putOut.LoggingConfiguration.LogDestinationConfigs)

	getOut, err := client.GetLoggingConfiguration(t.Context(), &wafv2sdk.GetLoggingConfigurationInput{
		ResourceArn: aws.String(webACLArn),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.LoggingConfiguration)
	assert.Equal(t, []string{"arn:aws:s3:::my-log-bucket"}, getOut.LoggingConfiguration.LogDestinationConfigs)

	_, err = client.DeleteLoggingConfiguration(t.Context(), &wafv2sdk.DeleteLoggingConfigurationInput{
		ResourceArn: aws.String(webACLArn),
	})
	require.NoError(t, err)
}

// TestPermissionPolicy_TypedRoundTrip drives PutPermissionPolicy,
// GetPermissionPolicy and DeletePermissionPolicy through a real SDK client.
func TestPermissionPolicy_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateRuleGroup(t.Context(), &wafv2sdk.CreateRuleGroupInput{
		Name:             aws.String("my-rulegroup"),
		Scope:            types.ScopeRegional,
		Capacity:         aws.Int64(10),
		VisibilityConfig: slice36VisibilityConfig(),
	})
	require.NoError(t, err)
	rgArn := aws.ToString(createOut.Summary.ARN)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::999999999999:root"},` +
		`"Action":["wafv2:GetRuleGroup"],"Resource":"` + rgArn + `"}]}`

	_, err = client.PutPermissionPolicy(t.Context(), &wafv2sdk.PutPermissionPolicyInput{
		ResourceArn: aws.String(rgArn),
		Policy:      aws.String(policy),
	})
	require.NoError(t, err)

	getOut, err := client.GetPermissionPolicy(t.Context(), &wafv2sdk.GetPermissionPolicyInput{
		ResourceArn: aws.String(rgArn),
	})
	require.NoError(t, err)
	assert.Equal(t, policy, aws.ToString(getOut.Policy))

	_, err = client.DeletePermissionPolicy(t.Context(), &wafv2sdk.DeletePermissionPolicyInput{
		ResourceArn: aws.String(rgArn),
	})
	require.NoError(t, err)
}

// TestManagedRuleSet_TypedRoundTrip drives PutManagedRuleSetVersions,
// GetManagedRuleSet, ListManagedRuleSets and
// UpdateManagedRuleSetVersionExpiryDate through a real SDK client.
func TestManagedRuleSet_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	putOut, err := client.PutManagedRuleSetVersions(t.Context(), &wafv2sdk.PutManagedRuleSetVersionsInput{
		Id:        aws.String("my-managed-set"),
		Name:      aws.String("my-managed-set"),
		Scope:     types.ScopeRegional,
		LockToken: aws.String(""),
		VersionsToPublish: map[string]types.VersionToPublish{
			"Version_1.0": {
				AssociatedRuleGroupArn: aws.String("arn:aws:wafv2:us-east-1:123456789012:regional/rulegroup/x/y"),
			},
		},
		RecommendedVersion: aws.String("Version_1.0"),
	})
	require.NoError(t, err)
	require.NotNil(t, putOut.NextLockToken)

	getOut, err := client.GetManagedRuleSet(t.Context(), &wafv2sdk.GetManagedRuleSetInput{
		Id:    aws.String("my-managed-set"),
		Name:  aws.String("my-managed-set"),
		Scope: types.ScopeRegional,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.ManagedRuleSet)
	assert.Equal(t, "Version_1.0", aws.ToString(getOut.ManagedRuleSet.RecommendedVersion))

	listOut, err := client.ListManagedRuleSets(t.Context(), &wafv2sdk.ListManagedRuleSetsInput{
		Scope: types.ScopeRegional,
	})
	require.NoError(t, err)
	var found bool
	for _, ms := range listOut.ManagedRuleSets {
		if aws.ToString(ms.Id) == "my-managed-set" {
			found = true
		}
	}
	assert.True(t, found)

	expiryOut, err := client.UpdateManagedRuleSetVersionExpiryDate(
		t.Context(), &wafv2sdk.UpdateManagedRuleSetVersionExpiryDateInput{
			Id:              aws.String("my-managed-set"),
			Name:            aws.String("my-managed-set"),
			Scope:           types.ScopeRegional,
			LockToken:       putOut.NextLockToken,
			VersionToExpire: aws.String("Version_1.0"),
			ExpiryTimestamp: aws.Time(time.Now().Add(24 * time.Hour)),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "Version_1.0", aws.ToString(expiryOut.ExpiringVersion))
}

// TestManagedRuleCatalog_TypedRoundTrip drives
// DescribeManagedProductsByVendor, ListAvailableManagedRuleGroupVersions,
// GenerateMobileSdkReleaseUrl, GetMobileSdkRelease and
// ListMobileSdkReleases through a real SDK client.
func TestManagedRuleCatalog_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	prodOut, err := client.DescribeManagedProductsByVendor(t.Context(), &wafv2sdk.DescribeManagedProductsByVendorInput{
		Scope:      types.ScopeRegional,
		VendorName: aws.String("AWS"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, prodOut.ManagedProducts)
	assert.Equal(t, "AWS", aws.ToString(prodOut.ManagedProducts[0].VendorName))

	versOut, err := client.ListAvailableManagedRuleGroupVersions(
		t.Context(), &wafv2sdk.ListAvailableManagedRuleGroupVersionsInput{
			Scope:      types.ScopeRegional,
			VendorName: aws.String("AWS"),
			Name:       aws.String("AWSManagedRulesCommonRuleSet"),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, versOut.Versions)
	assert.Equal(t, "Version_1.0", aws.ToString(versOut.CurrentDefaultVersion))

	urlOut, err := client.GenerateMobileSdkReleaseUrl(t.Context(), &wafv2sdk.GenerateMobileSdkReleaseUrlInput{
		Platform:       types.PlatformAndroid,
		ReleaseVersion: aws.String("3.1.0"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(urlOut.Url))

	relOut, err := client.GetMobileSdkRelease(t.Context(), &wafv2sdk.GetMobileSdkReleaseInput{
		Platform:       types.PlatformAndroid,
		ReleaseVersion: aws.String("3.1.0"),
	})
	require.NoError(t, err)
	require.NotNil(t, relOut.MobileSdkRelease)
	assert.Equal(t, "3.1.0", aws.ToString(relOut.MobileSdkRelease.ReleaseVersion))

	listOut, err := client.ListMobileSdkReleases(t.Context(), &wafv2sdk.ListMobileSdkReleasesInput{
		Platform: types.PlatformAndroid,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.ReleaseSummaries)
}

// TestRevenueStatistics_TypedRoundTrip drives GetRevenueStatistics,
// GetRevenueStatisticsSummary, GetRevenueStatisticsTimeSeries and
// ListSettlementRecords through a real SDK client.
func TestRevenueStatistics_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	tw := &types.TimeWindow{
		StartTime: aws.Time(time.Now().Add(-time.Hour)),
		EndTime:   aws.Time(time.Now()),
	}

	statsOut, err := client.GetRevenueStatistics(t.Context(), &wafv2sdk.GetRevenueStatisticsInput{
		Currency:      types.CurrencyUsdc,
		Scope:         types.ScopeCloudfront,
		StatisticType: types.RankingStatisticTypeTopPathsByRevenue,
		TimeWindow:    tw,
	})
	require.NoError(t, err)
	assert.NotNil(t, statsOut.RevenuePathStatistics)

	summaryOut, err := client.GetRevenueStatisticsSummary(t.Context(), &wafv2sdk.GetRevenueStatisticsSummaryInput{
		Currency:   types.CurrencyUsdc,
		Scope:      types.ScopeCloudfront,
		TimeWindow: tw,
	})
	require.NoError(t, err)
	require.NotNil(t, summaryOut.RevenueBreakdown)
	assert.Equal(t, types.CurrencyUsdc, summaryOut.RevenueBreakdown.Currency)

	tsOut, err := client.GetRevenueStatisticsTimeSeries(t.Context(), &wafv2sdk.GetRevenueStatisticsTimeSeriesInput{
		Currency:      types.CurrencyUsdc,
		Scope:         types.ScopeCloudfront,
		StatisticType: types.TimeSeriesStatisticTypeDateHistogram,
		Interval:      types.IntervalTypeHourly,
		TimeWindow:    tw,
	})
	require.NoError(t, err)
	assert.Empty(t, tsOut.DataPoints)

	settleOut, err := client.ListSettlementRecords(t.Context(), &wafv2sdk.ListSettlementRecordsInput{
		Currency:   types.CurrencyUsdc,
		Scope:      types.ScopeCloudfront,
		TimeWindow: tw,
	})
	require.NoError(t, err)
	assert.Empty(t, settleOut.Settlements)
}

// TestRateBasedManagedKeysAndSampledRequests_TypedRoundTrip drives
// GetRateBasedStatementManagedKeys and GetSampledRequests through a real SDK
// client.
func TestRateBasedManagedKeysAndSampledRequests_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateWebACL(t.Context(), &wafv2sdk.CreateWebACLInput{
		Name:             aws.String("my-acl"),
		Scope:            types.ScopeRegional,
		DefaultAction:    &types.DefaultAction{Allow: &types.AllowAction{}},
		VisibilityConfig: slice36VisibilityConfig(),
	})
	require.NoError(t, err)
	webACLArn := aws.ToString(createOut.Summary.ARN)
	webACLID := aws.ToString(createOut.Summary.Id)

	keysOut, err := client.GetRateBasedStatementManagedKeys(
		t.Context(),
		&wafv2sdk.GetRateBasedStatementManagedKeysInput{
			Scope:      types.ScopeRegional,
			WebACLName: aws.String("my-acl"),
			WebACLId:   aws.String(webACLID),
			RuleName:   aws.String("my-rate-rule"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, keysOut.ManagedKeysIPV4)
	assert.Equal(t, types.IPAddressVersionIpv4, keysOut.ManagedKeysIPV4.IPAddressVersion)

	sampledOut, err := client.GetSampledRequests(t.Context(), &wafv2sdk.GetSampledRequestsInput{
		Scope:          types.ScopeRegional,
		WebAclArn:      aws.String(webACLArn),
		RuleMetricName: aws.String("my-metric"),
		MaxItems:       aws.Int64(100),
		TimeWindow: &types.TimeWindow{
			StartTime: aws.Time(time.Now().Add(-time.Hour)),
			EndTime:   aws.Time(time.Now()),
		},
	})
	require.NoError(t, err)
	assert.Empty(t, sampledOut.SampledRequests)
	assert.Equal(t, int64(0), sampledOut.PopulationSize)
}

// TestTagResource_TypedRoundTrip drives TagResource and UntagResource
// through a real SDK client.
func TestTagResource_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestWAFV2ClientSlice36(t)

	createOut, err := client.CreateIPSet(t.Context(), &wafv2sdk.CreateIPSetInput{
		Name:             aws.String("my-ipset"),
		Scope:            types.ScopeRegional,
		IPAddressVersion: types.IPAddressVersionIpv4,
		Addresses:        []string{"10.0.0.0/8"},
	})
	require.NoError(t, err)
	arnStr := aws.ToString(createOut.Summary.ARN)

	_, err = client.TagResource(t.Context(), &wafv2sdk.TagResourceInput{
		ResourceARN: aws.String(arnStr),
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(t.Context(), &wafv2sdk.ListTagsForResourceInput{
		ResourceARN: aws.String(arnStr),
	})
	require.NoError(t, err)
	require.NotNil(t, listOut.TagInfoForResource)
	require.Len(t, listOut.TagInfoForResource.TagList, 1)

	_, err = client.UntagResource(t.Context(), &wafv2sdk.UntagResourceInput{
		ResourceARN: aws.String(arnStr),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(t.Context(), &wafv2sdk.ListTagsForResourceInput{
		ResourceARN: aws.String(arnStr),
	})
	require.NoError(t, err)
	require.NotNil(t, listOut2.TagInfoForResource)
	assert.Empty(t, listOut2.TagInfoForResource.TagList)
}
