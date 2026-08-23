package awsconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestListDiscoveredResources_RealClient drives ListDiscoveredResources
// through a real SDK client. Real ListDiscoveredResourcesOutput wraps its
// list under "resourceIdentifiers" (lowercase), unlike this service's
// DescribeXxx wrappers which are PascalCase -- confirmed at
// aws-sdk-go-v2/service/configservice's
// awsAwsjson11_deserializeOpDocumentListDiscoveredResourcesOutput. The
// pre-fix key ("ResourceIdentifiers") does not exist on the real shape at
// all, so a real client's ResourceIdentifiers was always empty regardless
// of what PutResourceConfig had stored.
func TestListDiscoveredResources_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
		ResourceType:    aws.String("AWS::EC2::Instance"),
		ResourceId:      aws.String("i-abc"),
		Configuration:   aws.String(`{"a":1}`),
		SchemaVersionId: aws.String("1.0"),
	})
	require.NoError(t, err)

	out, err := client.ListDiscoveredResources(t.Context(), &configservicesdk.ListDiscoveredResourcesInput{
		ResourceType: "AWS::EC2::Instance",
	})
	require.NoError(t, err)
	require.Len(t, out.ResourceIdentifiers, 1)
	assert.Equal(t, "i-abc", aws.ToString(out.ResourceIdentifiers[0].ResourceId))
	assert.Equal(t, "AWS::EC2::Instance", string(out.ResourceIdentifiers[0].ResourceType))
}

// TestGetResourceConfigHistory_ItemCasing_RealClient drives
// GetResourceConfigHistory through a real SDK client. Real ConfigurationItem
// fields are lowerCamelCase ("resourceType", "resourceId", "configuration",
// "configurationItemCaptureTime" -- confirmed at
// awsAwsjson11_deserializeDocumentConfigurationItem), unlike the outer
// "configurationItems"/DescribeXxx wrapper naming; the pre-fix PascalCase
// item tags meant a real client's ConfigurationItems[i].ResourceType/
// ResourceId/Configuration were always empty even though the wrapper key
// itself was already correct.
func TestGetResourceConfigHistory_ItemCasing_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
		ResourceType:    aws.String("AWS::S3::Bucket"),
		ResourceId:      aws.String("my-bucket"),
		Configuration:   aws.String(`{"Versioning":"Enabled"}`),
		SchemaVersionId: aws.String("1.0"),
	})
	require.NoError(t, err)

	out, err := client.GetResourceConfigHistory(t.Context(), &configservicesdk.GetResourceConfigHistoryInput{
		ResourceType: "AWS::S3::Bucket",
		ResourceId:   aws.String("my-bucket"),
	})
	require.NoError(t, err)
	require.Len(t, out.ConfigurationItems, 1)
	item := out.ConfigurationItems[0]
	assert.Equal(t, "my-bucket", aws.ToString(item.ResourceId))
	assert.Equal(t, "AWS::S3::Bucket", string(item.ResourceType))
	assert.JSONEq(t, `{"Versioning":"Enabled"}`, aws.ToString(item.Configuration))
}

// TestGetDiscoveredResourceCounts_RealClient drives
// GetDiscoveredResourceCounts through a real SDK client. Real
// GetDiscoveredResourceCountsOutput.TotalDiscoveredResources is
// "totalDiscoveredResources" (lowercase -- confirmed at
// awsAwsjson11_deserializeOpDocumentGetDiscoveredResourceCountsOutput); the
// pre-fix PascalCase tag meant a real client's TotalDiscoveredResources was
// always 0 regardless of how many resources had been discovered.
func TestGetDiscoveredResourceCounts_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	for _, id := range []string{"i-1", "i-2", "i-3"} {
		_, err := client.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
			ResourceType:    aws.String("AWS::EC2::Instance"),
			ResourceId:      aws.String(id),
			Configuration:   aws.String(`{}`),
			SchemaVersionId: aws.String("1.0"),
		})
		require.NoError(t, err)
	}

	out, err := client.GetDiscoveredResourceCounts(t.Context(), &configservicesdk.GetDiscoveredResourceCountsInput{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), out.TotalDiscoveredResources)
}

// TestBatchGetResourceConfig_RealClient drives BatchGetResourceConfig
// through a real SDK client. Unlike its BatchGetAggregateResourceConfig
// sibling (PascalCase throughout), the plain op is lowerCamelCase on both
// request ("resourceKeys") and response ("baseConfigurationItems"/
// "unprocessedResourceKeys") -- confirmed at serializers.go's
// awsAwsjson11_serializeOpDocumentBatchGetResourceConfigInput and
// deserializers.go's
// awsAwsjson11_deserializeOpDocumentBatchGetResourceConfigOutput. Reusing
// the aggregate sibling's casing meant a real client's request never
// carried ResourceKeys (always parsed as empty on gopherstack's side) and
// the response's BaseConfigurationItems was always empty regardless.
func TestBatchGetResourceConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
		ResourceType:    aws.String("AWS::EC2::Instance"),
		ResourceId:      aws.String("i-batch"),
		Configuration:   aws.String(`{"x":1}`),
		SchemaVersionId: aws.String("1.0"),
	})
	require.NoError(t, err)

	out, err := client.BatchGetResourceConfig(t.Context(), &configservicesdk.BatchGetResourceConfigInput{
		ResourceKeys: []types.ResourceKey{
			{ResourceType: "AWS::EC2::Instance", ResourceId: aws.String("i-batch")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.BaseConfigurationItems, 1)
	assert.Equal(t, "i-batch", aws.ToString(out.BaseConfigurationItems[0].ResourceId))
	assert.Empty(t, out.UnprocessedResourceKeys)
}

// TestDescribeConformancePackCompliance_NameEcho_RealClient drives
// DescribeConformancePackCompliance through a real SDK client. Real
// DescribeConformancePackComplianceOutput.ConformancePackName is a required
// response member (api_op_DescribeConformancePackCompliance.go) that was
// never emitted at all.
func TestDescribeConformancePackCompliance_NameEcho_RealClient(t *testing.T) {
	t.Parallel()

	b := newCompliancePackBackend(t)
	h := awsconfig.NewHandler(b)
	client := newTestAWSConfigSDKClient(t, h)

	out, err := client.DescribeConformancePackCompliance(
		t.Context(),
		&configservicesdk.DescribeConformancePackComplianceInput{ConformancePackName: aws.String("pack1")},
	)
	require.NoError(t, err)
	assert.Equal(t, "pack1", aws.ToString(out.ConformancePackName))
	assert.NotEmpty(t, out.ConformancePackRuleComplianceList)
}

// TestGetAggregateConfigRuleComplianceSummary_GroupByKeyEcho_RealClient
// drives GetAggregateConfigRuleComplianceSummary through a real SDK client.
// Real GetAggregateConfigRuleComplianceSummaryOutput echoes the request's
// GroupByKey ("the key passed into the request object" per
// api_op_GetAggregateConfigRuleComplianceSummary.go); it was never emitted.
func TestGetAggregateConfigRuleComplianceSummary_GroupByKeyEcho_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutConfigurationAggregator(t.Context(), &configservicesdk.PutConfigurationAggregatorInput{
		ConfigurationAggregatorName: aws.String("agg1"),
	})
	require.NoError(t, err)

	out, err := client.GetAggregateConfigRuleComplianceSummary(
		t.Context(),
		&configservicesdk.GetAggregateConfigRuleComplianceSummaryInput{
			ConfigurationAggregatorName: aws.String("agg1"),
			GroupByKey:                  "AWS_REGION",
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "AWS_REGION", aws.ToString(out.GroupByKey))
}

// TestDescribeAggregationAuthorizations_RealClient drives
// DescribeAggregationAuthorizations through a real SDK client. Real
// AggregationAuthorization.AuthorizedAccountId/AuthorizedAwsRegion are
// PascalCase (confirmed at
// awsAwsjson11_deserializeDocumentAggregationAuthorization,
// aws-sdk-go-v2/service/configservice@v1.68.4/deserializers.go:14466); the
// pre-fix lowercase tags meant a real client's AuthorizedAccountId and
// AuthorizedAwsRegion were always empty, gopherstack-v4a4.
func TestDescribeAggregationAuthorizations_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutAggregationAuthorization(t.Context(), &configservicesdk.PutAggregationAuthorizationInput{
		AuthorizedAccountId: aws.String("123456789012"),
		AuthorizedAwsRegion: aws.String("us-east-1"),
	})
	require.NoError(t, err)

	out, err := client.DescribeAggregationAuthorizations(
		t.Context(), &configservicesdk.DescribeAggregationAuthorizationsInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.AggregationAuthorizations, 1)
	assert.Equal(t, "123456789012", aws.ToString(out.AggregationAuthorizations[0].AuthorizedAccountId))
	assert.Equal(t, "us-east-1", aws.ToString(out.AggregationAuthorizations[0].AuthorizedAwsRegion))
}

// TestDescribeOrganizationConfigRules_RealClient drives
// DescribeOrganizationConfigRules through a real SDK client. Real
// OrganizationConfigRule.OrganizationConfigRuleName is PascalCase (confirmed
// at awsAwsjson11_deserializeDocumentOrganizationConfigRule,
// deserializers.go:21357); the pre-fix lowercase tag meant a real client's
// OrganizationConfigRuleName was always empty. Refs: gopherstack-v4a4.
func TestDescribeOrganizationConfigRules_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutOrganizationConfigRule(t.Context(), &configservicesdk.PutOrganizationConfigRuleInput{
		OrganizationConfigRuleName: aws.String("org-rule1"),
	})
	require.NoError(t, err)

	out, err := client.DescribeOrganizationConfigRules(
		t.Context(), &configservicesdk.DescribeOrganizationConfigRulesInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.OrganizationConfigRules, 1)
	assert.Equal(t, "org-rule1", aws.ToString(out.OrganizationConfigRules[0].OrganizationConfigRuleName))
}

// TestDescribeOrganizationConfigRules_Arn_RealClient drives
// PutOrganizationConfigRule/DescribeOrganizationConfigRules through a real
// SDK client and asserts OrganizationConfigRuleArn is populated. Real
// OrganizationConfigRule.OrganizationConfigRuleArn is declared "This member
// is required" (aws-sdk-go-v2/service/configservice@v1.68.4 types/types.go:
// 2081-2091); gopherstack previously had no Arn field on the type at all, so
// a real client's OrganizationConfigRuleArn was always empty. Refs:
// gopherstack-xit0.
func TestDescribeOrganizationConfigRules_Arn_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	putOut, err := client.PutOrganizationConfigRule(t.Context(), &configservicesdk.PutOrganizationConfigRuleInput{
		OrganizationConfigRuleName: aws.String("org-rule-arn"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.OrganizationConfigRuleArn))

	out, err := client.DescribeOrganizationConfigRules(
		t.Context(), &configservicesdk.DescribeOrganizationConfigRulesInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.OrganizationConfigRules, 1)
	gotArn := aws.ToString(out.OrganizationConfigRules[0].OrganizationConfigRuleArn)
	assert.NotEmpty(t, gotArn)
	assert.Equal(t, putOut.OrganizationConfigRuleArn, out.OrganizationConfigRules[0].OrganizationConfigRuleArn)

	// A second Put on the same name must preserve the same ARN, matching
	// putConfigRuleLocked's create-once-preserve-on-update convention.
	putOut2, err := client.PutOrganizationConfigRule(t.Context(), &configservicesdk.PutOrganizationConfigRuleInput{
		OrganizationConfigRuleName: aws.String("org-rule-arn"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(putOut.OrganizationConfigRuleArn), aws.ToString(putOut2.OrganizationConfigRuleArn))
}

// TestDescribeOrganizationConformancePacks_RealClient drives
// DescribeOrganizationConformancePacks through a real SDK client. Real
// OrganizationConformancePack.OrganizationConformancePackName is PascalCase
// (confirmed at
// awsAwsjson11_deserializeDocumentOrganizationConformancePack,
// deserializers.go:21699); the pre-fix lowercase tag meant a real client's
// OrganizationConformancePackName was always empty. Refs: gopherstack-v4a4.
func TestDescribeOrganizationConformancePacks_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutOrganizationConformancePack(
		t.Context(),
		&configservicesdk.PutOrganizationConformancePackInput{
			OrganizationConformancePackName: aws.String("org-pack1"),
		},
	)
	require.NoError(t, err)

	out, err := client.DescribeOrganizationConformancePacks(
		t.Context(), &configservicesdk.DescribeOrganizationConformancePacksInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.OrganizationConformancePacks, 1)
	assert.Equal(
		t, "org-pack1", aws.ToString(out.OrganizationConformancePacks[0].OrganizationConformancePackName),
	)
}

// TestDescribeDeliveryChannelStatus_RealClient drives
// DescribeDeliveryChannelStatus through a real SDK client. Real
// DeliveryChannelStatus.Name/ConfigHistoryDeliveryInfo/
// ConfigStreamDeliveryInfo, and the nested LastStatus/LastAttemptTime
// members, are all lowerCamelCase (confirmed at
// awsAwsjson11_deserializeDocumentDeliveryChannelStatus,
// deserializers.go:18209); the pre-fix PascalCase tags meant a real
// client's whole DeliveryChannelStatus decoded as the zero value. Refs:
// gopherstack-v4a4.
func TestDescribeDeliveryChannelStatus_RealClient(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
		DeliveryChannel: &types.DeliveryChannel{
			Name:         aws.String("default"),
			S3BucketName: aws.String("my-bucket"),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDeliveryChannelStatus(
		t.Context(), &configservicesdk.DescribeDeliveryChannelStatusInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.DeliveryChannelsStatus, 1)
	status := out.DeliveryChannelsStatus[0]
	assert.Equal(t, "default", aws.ToString(status.Name))
	require.NotNil(t, status.ConfigHistoryDeliveryInfo)
	assert.Equal(t, "SUCCESS", string(status.ConfigHistoryDeliveryInfo.LastStatus))
	require.NotNil(t, status.ConfigStreamDeliveryInfo)
	assert.Equal(t, "SUCCESS", string(status.ConfigStreamDeliveryInfo.LastStatus))
}
