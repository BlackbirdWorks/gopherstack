package wafv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// ListLoggingConfigurationsInput declares a LogScope member (wafv2@v1.77.3
// api_op_ListLoggingConfigurations.go: "The owner of the logging
// configuration... Default: CUSTOMER") that the handler parsed into its
// request struct but never applied to the result -- every LogScope value
// returned every stored configuration regardless of the request.
func TestListLoggingConfigurations_FilterByLogScope_RealClient(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestWAFV2Client(t, wafv2.NewHandler(backend))
	ctx := t.Context()

	_, err := client.PutLoggingConfiguration(ctx, &wafv2sdk.PutLoggingConfigurationInput{
		LoggingConfiguration: &types.LoggingConfiguration{
			ResourceArn:           aws.String("arn:aws:wafv2:us-east-1:123456789012:regional/webacl/customer-wa/id-1"),
			LogDestinationConfigs: []string{"arn:aws:s3:::log-bucket"},
			LogScope:              types.LogScopeCustomer,
		},
	})
	require.NoError(t, err)

	_, err = client.PutLoggingConfiguration(ctx, &wafv2sdk.PutLoggingConfigurationInput{
		LoggingConfiguration: &types.LoggingConfiguration{
			ResourceArn:           aws.String("arn:aws:wafv2:us-east-1:123456789012:regional/webacl/seclake-wa/id-2"),
			LogDestinationConfigs: []string{"arn:aws:s3:::log-bucket"},
			LogScope:              types.LogScopeSecurityLake,
		},
	})
	require.NoError(t, err)

	out, err := client.ListLoggingConfigurations(ctx, &wafv2sdk.ListLoggingConfigurationsInput{
		Scope:    types.ScopeRegional,
		LogScope: types.LogScopeSecurityLake,
	})
	require.NoError(t, err)
	require.Len(t, out.LoggingConfigurations, 1, "LogScope must narrow ListLoggingConfigurations")
	require.Equal(t, "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/seclake-wa/id-2",
		aws.ToString(out.LoggingConfigurations[0].ResourceArn))
}

// ListResourcesForWebACLInput declares a ResourceType member that the
// handler parsed into its request struct but never used at all (wafv2@
// v1.77.3 api_op_ListResourcesForWebACL.go doc comment: "If you don't
// provide a resource type, the call uses the resource type
// APPLICATION_LOAD_BALANCER. Default: APPLICATION_LOAD_BALANCER") -- every
// associated resource ARN was returned regardless of type, and even the
// no-filter case ignored the documented ALB-only default.
func TestListResourcesForWebACL_FilterByResourceType_RealClient(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestWAFV2Client(t, wafv2.NewHandler(backend))
	ctx := t.Context()

	acl, err := client.CreateWebACL(ctx, &wafv2sdk.CreateWebACLInput{
		Name:          aws.String("shared-acl"),
		Scope:         types.ScopeRegional,
		DefaultAction: &types.DefaultAction{Allow: &types.AllowAction{}},
		VisibilityConfig: &types.VisibilityConfig{
			CloudWatchMetricsEnabled: true,
			MetricName:               aws.String("metric"),
			SampledRequestsEnabled:   true,
		},
	})
	require.NoError(t, err)

	const (
		albARN = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/50dc6c495c0c9188"
		apiARN = "arn:aws:apigateway:us-east-1::/restapis/my-api/stages/prod"
	)

	for _, resourceARN := range []string{albARN, apiARN} {
		_, err = client.AssociateWebACL(ctx, &wafv2sdk.AssociateWebACLInput{
			WebACLArn:   acl.Summary.ARN,
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
	}

	defaulted, err := client.ListResourcesForWebACL(ctx, &wafv2sdk.ListResourcesForWebACLInput{
		WebACLArn: acl.Summary.ARN,
	})
	require.NoError(t, err)
	require.Equal(t, []string{albARN}, defaulted.ResourceArns,
		"no ResourceType must default to APPLICATION_LOAD_BALANCER per the SDK doc")

	apiOnly, err := client.ListResourcesForWebACL(ctx, &wafv2sdk.ListResourcesForWebACLInput{
		WebACLArn:    acl.Summary.ARN,
		ResourceType: types.ResourceTypeApiGateway,
	})
	require.NoError(t, err)
	require.Equal(t, []string{apiARN}, apiOnly.ResourceArns)
}

// ListAvailableManagedRuleGroupsInput declares a Limit member that the
// handler parsed into its request struct but never applied at all: every
// call returned the full static catalog (14 entries) regardless of Limit,
// and NextMarker never appeared in the response even when more objects
// remained (wafv2@v1.77.3 api_op_ListAvailableManagedRuleGroups.go doc
// comment: "If you specified a Limit in your request, this might not be
// the full list").
func TestListAvailableManagedRuleGroups_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestWAFV2Client(t, wafv2.NewHandler(backend))
	ctx := t.Context()

	firstPage, err := client.ListAvailableManagedRuleGroups(ctx, &wafv2sdk.ListAvailableManagedRuleGroupsInput{
		Scope: types.ScopeRegional,
		Limit: aws.Int32(5),
	})
	require.NoError(t, err)
	require.Len(t, firstPage.ManagedRuleGroups, 5, "Limit must bound the page size")
	require.NotNil(t, firstPage.NextMarker, "more objects remain, so NextMarker must be set")

	secondPage, err := client.ListAvailableManagedRuleGroups(ctx, &wafv2sdk.ListAvailableManagedRuleGroupsInput{
		Scope:      types.ScopeRegional,
		Limit:      aws.Int32(5),
		NextMarker: firstPage.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, secondPage.ManagedRuleGroups, 5)

	seen := map[string]bool{}
	for _, g := range firstPage.ManagedRuleGroups {
		seen[aws.ToString(g.Name)] = true
	}

	for _, g := range secondPage.ManagedRuleGroups {
		require.False(t, seen[aws.ToString(g.Name)],
			"second page must not repeat a first-page item: %s", aws.ToString(g.Name))
	}
}
