package wafv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRuleGroup_CustomResponseBodies drives the real aws-sdk-go-v2 client:
// CreateRuleGroupInput/UpdateRuleGroupInput.CustomResponseBodies is a real
// member (api_op_CreateRuleGroup.go) that WebACL already models end-to-end in
// this service (handler_web_acls.go) but RuleGroup silently discarded --
// neither CreateRuleGroup nor UpdateRuleGroup accepted it, and GetRuleGroup
// never echoed it back.
func TestRuleGroup_CustomResponseBodies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	vc := &types.VisibilityConfig{
		CloudWatchMetricsEnabled: true,
		MetricName:               aws.String("metric"),
		SampledRequestsEnabled:   true,
	}

	created, err := client.CreateRuleGroup(t.Context(), &wafv2sdk.CreateRuleGroupInput{
		Name:             aws.String("crb-rulegroup"),
		Scope:            types.ScopeRegional,
		Capacity:         aws.Int64(10),
		VisibilityConfig: vc,
		CustomResponseBodies: map[string]types.CustomResponseBody{
			"block-body": {
				Content:     aws.String("blocked"),
				ContentType: types.ResponseContentTypeTextPlain,
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetRuleGroup(t.Context(), &wafv2sdk.GetRuleGroupInput{Id: created.Summary.Id})
	require.NoError(t, err)
	require.Contains(t, got.RuleGroup.CustomResponseBodies, "block-body")
	assert.Equal(t, "blocked", aws.ToString(got.RuleGroup.CustomResponseBodies["block-body"].Content))
	assert.Equal(t, types.ResponseContentTypeTextPlain, got.RuleGroup.CustomResponseBodies["block-body"].ContentType)

	updated, err := client.UpdateRuleGroup(t.Context(), &wafv2sdk.UpdateRuleGroupInput{
		Id:               created.Summary.Id,
		Name:             created.Summary.Name,
		Scope:            types.ScopeRegional,
		LockToken:        created.Summary.LockToken,
		VisibilityConfig: vc,
		CustomResponseBodies: map[string]types.CustomResponseBody{
			"other-body": {
				Content:     aws.String("also blocked"),
				ContentType: types.ResponseContentTypeApplicationJson,
			},
		},
	})
	require.NoError(t, err)

	_ = updated

	got2, err := client.GetRuleGroup(t.Context(), &wafv2sdk.GetRuleGroupInput{Id: created.Summary.Id})
	require.NoError(t, err)
	require.Contains(t, got2.RuleGroup.CustomResponseBodies, "other-body")
	assert.Equal(t, "also blocked", aws.ToString(got2.RuleGroup.CustomResponseBodies["other-body"].Content))
}

// TestDescribeAllManagedProducts_IsVersioningSupported drives the real
// aws-sdk-go-v2 client: ManagedProductDescriptor.IsVersioningSupported is a
// real member (deserializers.go's ManagedProductDescriptor case list) backed
// by this catalog's own VersioningSupported field, which was already tracked
// but never emitted on either DescribeAllManagedProducts or
// DescribeManagedProductsByVendor.
func TestDescribeAllManagedProducts_IsVersioningSupported(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	out, err := client.DescribeAllManagedProducts(
		t.Context(), &wafv2sdk.DescribeAllManagedProductsInput{Scope: types.ScopeRegional},
	)
	require.NoError(t, err)

	byName := make(map[string]bool, len(out.ManagedProducts))
	for _, p := range out.ManagedProducts {
		byName[aws.ToString(p.ManagedRuleSetName)] = p.IsVersioningSupported
	}

	versioned, ok := byName["AWSManagedRulesCommonRuleSet"]
	require.True(t, ok)
	assert.True(t, versioned)

	unversioned, ok := byName["AWSManagedRulesKnownBadInputsRuleSet"]
	require.True(t, ok)
	assert.False(t, unversioned)
}

// TestGetWebACL_RealSDKClient_ARNOptional proves a request identifying the
// WebACL by ARN alone (no Id) succeeds. GetWebACLInput marks no member
// required (wafv2@v1.77.3 api_op_GetWebACL.go) -- ARN is an alternative to
// Name+Scope+Id. The handler previously rejected any request without Id
// with a 400 (gopherstack-4ly2).
func TestGetWebACL_RealSDKClient_ARNOptional(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	created, err := client.CreateWebACL(t.Context(), &wafv2sdk.CreateWebACLInput{
		Name:  aws.String("arn-lookup-acl"),
		Scope: types.ScopeRegional,
		DefaultAction: &types.DefaultAction{
			Allow: &types.AllowAction{},
		},
		VisibilityConfig: &types.VisibilityConfig{
			CloudWatchMetricsEnabled: true,
			MetricName:               aws.String("metric"),
			SampledRequestsEnabled:   true,
		},
	})
	require.NoError(t, err)

	got, err := client.GetWebACL(t.Context(), &wafv2sdk.GetWebACLInput{ARN: created.Summary.ARN})
	require.NoError(t, err)
	assert.Equal(t, "arn-lookup-acl", aws.ToString(got.WebACL.Name))
	assert.Equal(t, aws.ToString(created.Summary.Id), aws.ToString(got.WebACL.Id))
}

// TestGetRuleGroup_RealSDKClient_ARNOptional proves a request identifying
// the RuleGroup by ARN alone (no Id) succeeds. GetRuleGroupInput marks no
// member required (wafv2@v1.77.3 api_op_GetRuleGroup.go) -- ARN is an
// alternative to Name+Scope+Id. The handler previously rejected any request
// without Id with a 400 (gopherstack-4ly2).
func TestGetRuleGroup_RealSDKClient_ARNOptional(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	created, err := client.CreateRuleGroup(t.Context(), &wafv2sdk.CreateRuleGroupInput{
		Name:     aws.String("arn-lookup-rg"),
		Scope:    types.ScopeRegional,
		Capacity: aws.Int64(10),
		VisibilityConfig: &types.VisibilityConfig{
			CloudWatchMetricsEnabled: true,
			MetricName:               aws.String("metric"),
			SampledRequestsEnabled:   true,
		},
	})
	require.NoError(t, err)

	got, err := client.GetRuleGroup(t.Context(), &wafv2sdk.GetRuleGroupInput{ARN: created.Summary.ARN})
	require.NoError(t, err)
	assert.Equal(t, "arn-lookup-rg", aws.ToString(got.RuleGroup.Name))
	assert.Equal(t, aws.ToString(created.Summary.Id), aws.ToString(got.RuleGroup.Id))
}

// TestCheckCapacity_WireKeyCase proves gopherstack-zquj's keycheck sweep
// finding: wafv2@v1.77.3's CheckCapacityOutput has exactly one member,
// "Capacity" (deserializers.go's
// awsAwsjson11_deserializeOpDocumentCheckCapacityOutput, api_op_CheckCapacity.go's
// CheckCapacityOutput.Capacity int64). gopherstack wrote "ConsumedCapacity"
// instead -- a key that appears nowhere in the real deserializer's case
// list. A real SDK client's response deserializer does an exact-case
// switch, so CheckCapacityOutput.Capacity was always zero on every real
// client call regardless of the actual value, independent of the rules
// supplied.
func TestCheckCapacity_WireKeyCase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	vc := &types.VisibilityConfig{
		CloudWatchMetricsEnabled: true,
		MetricName:               aws.String("metric"),
		SampledRequestsEnabled:   true,
	}
	rule := func(name string) types.Rule {
		return types.Rule{
			Name:             aws.String(name),
			Priority:         0,
			Statement:        &types.Statement{GeoMatchStatement: &types.GeoMatchStatement{}},
			VisibilityConfig: vc,
		}
	}

	got, err := client.CheckCapacity(t.Context(), &wafv2sdk.CheckCapacityInput{
		Scope: types.ScopeRegional,
		Rules: []types.Rule{rule("r1"), rule("r2"), rule("r3")},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), got.Capacity,
		"Capacity was silently dropped by the real SDK client's exact-case response "+
			"deserializer before the wire key was fixed from \"ConsumedCapacity\" to \"Capacity\"")
}
