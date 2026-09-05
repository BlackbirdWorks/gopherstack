package wafv2_test

import (
	"fmt"
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

// TestWebACL_OptionalConfigs drives the real aws-sdk-go-v2 client:
// MonetizationConfig, DataProtectionConfig, ApplicationConfig, and
// OnSourceDDoSProtectionConfig are all real CreateWebACLInput/
// UpdateWebACLInput members (wafv2@v1.77.3 api_op_CreateWebACL.go,
// api_op_UpdateWebACL.go) that this handler silently discarded --
// createWebACLRequest/updateWebACLRequest had no fields for them at all, so
// json.Unmarshal dropped them and GetWebACL never echoed them back, unlike
// their sibling opaque configs (AssociationConfig/CaptchaConfig/
// ChallengeConfig) which this handler already round-trips.
func TestWebACL_OptionalConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	vc := &types.VisibilityConfig{
		CloudWatchMetricsEnabled: true,
		MetricName:               aws.String("metric"),
		SampledRequestsEnabled:   true,
	}

	created, err := client.CreateWebACL(t.Context(), &wafv2sdk.CreateWebACLInput{
		Name:             aws.String("optional-configs-acl"),
		Scope:            types.ScopeRegional,
		DefaultAction:    &types.DefaultAction{Allow: &types.AllowAction{}},
		VisibilityConfig: vc,
		MonetizationConfig: &types.MonetizationConfig{
			CurrencyMode: types.CurrencyModeTest,
		},
		DataProtectionConfig: &types.DataProtectionConfig{
			DataProtections: []types.DataProtection{
				{
					Action: types.DataProtectionActionHash,
					Field:  &types.FieldToProtect{FieldType: types.FieldToProtectTypeSingleHeader},
				},
			},
		},
		ApplicationConfig: &types.ApplicationConfig{
			Attributes: []types.ApplicationAttribute{
				{Name: aws.String("attr1"), Values: []string{"v1", "v2"}},
			},
		},
		OnSourceDDoSProtectionConfig: &types.OnSourceDDoSProtectionConfig{
			ALBLowReputationMode: types.LowReputationModeAlwaysOn,
		},
	})
	require.NoError(t, err)

	got, err := client.GetWebACL(t.Context(), &wafv2sdk.GetWebACLInput{Id: created.Summary.Id})
	require.NoError(t, err)

	require.NotNil(t, got.WebACL.MonetizationConfig,
		"MonetizationConfig was silently dropped by createWebACLRequest before it had a field for it")
	assert.Equal(t, types.CurrencyModeTest, got.WebACL.MonetizationConfig.CurrencyMode)

	require.NotNil(t, got.WebACL.DataProtectionConfig)
	require.Len(t, got.WebACL.DataProtectionConfig.DataProtections, 1)
	assert.Equal(t, types.DataProtectionActionHash, got.WebACL.DataProtectionConfig.DataProtections[0].Action)

	require.NotNil(t, got.WebACL.ApplicationConfig)
	require.Len(t, got.WebACL.ApplicationConfig.Attributes, 1)
	assert.Equal(t, "attr1", aws.ToString(got.WebACL.ApplicationConfig.Attributes[0].Name))

	require.NotNil(t, got.WebACL.OnSourceDDoSProtectionConfig)
	assert.Equal(t, types.LowReputationModeAlwaysOn, got.WebACL.OnSourceDDoSProtectionConfig.ALBLowReputationMode)

	updated, err := client.UpdateWebACL(t.Context(), &wafv2sdk.UpdateWebACLInput{
		Id:               created.Summary.Id,
		Name:             created.Summary.Name,
		Scope:            types.ScopeRegional,
		LockToken:        created.Summary.LockToken,
		DefaultAction:    &types.DefaultAction{Allow: &types.AllowAction{}},
		VisibilityConfig: vc,
		MonetizationConfig: &types.MonetizationConfig{
			CurrencyMode: types.CurrencyModeReal,
		},
	})
	require.NoError(t, err)
	_ = updated

	got2, err := client.GetWebACL(t.Context(), &wafv2sdk.GetWebACLInput{Id: created.Summary.Id})
	require.NoError(t, err)
	require.NotNil(t, got2.WebACL.MonetizationConfig)
	assert.Equal(t, types.CurrencyModeReal, got2.WebACL.MonetizationConfig.CurrencyMode,
		"UpdateWebACL's MonetizationConfig was silently dropped before updateWebACLRequest had a field for it")
}

// TestRuleGroup_MonetizationConfig drives the real aws-sdk-go-v2 client:
// MonetizationConfig is a real CreateRuleGroupInput/UpdateRuleGroupInput
// member (wafv2@v1.77.3 api_op_CreateRuleGroup.go,
// api_op_UpdateRuleGroup.go) that this handler silently discarded.
func TestRuleGroup_MonetizationConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	vc := &types.VisibilityConfig{
		CloudWatchMetricsEnabled: true,
		MetricName:               aws.String("metric"),
		SampledRequestsEnabled:   true,
	}

	created, err := client.CreateRuleGroup(t.Context(), &wafv2sdk.CreateRuleGroupInput{
		Name:             aws.String("monetized-rulegroup"),
		Scope:            types.ScopeRegional,
		Capacity:         aws.Int64(10),
		VisibilityConfig: vc,
		MonetizationConfig: &types.MonetizationConfig{
			CurrencyMode: types.CurrencyModeTest,
		},
	})
	require.NoError(t, err)

	got, err := client.GetRuleGroup(t.Context(), &wafv2sdk.GetRuleGroupInput{Id: created.Summary.Id})
	require.NoError(t, err)
	require.NotNil(t, got.RuleGroup.MonetizationConfig,
		"MonetizationConfig was silently dropped by createRuleGroupRequest before it had a field for it")
	assert.Equal(t, types.CurrencyModeTest, got.RuleGroup.MonetizationConfig.CurrencyMode)

	updated, err := client.UpdateRuleGroup(t.Context(), &wafv2sdk.UpdateRuleGroupInput{
		Id:               created.Summary.Id,
		Name:             created.Summary.Name,
		Scope:            types.ScopeRegional,
		LockToken:        created.Summary.LockToken,
		VisibilityConfig: vc,
		MonetizationConfig: &types.MonetizationConfig{
			CurrencyMode: types.CurrencyModeReal,
		},
	})
	require.NoError(t, err)
	_ = updated

	got2, err := client.GetRuleGroup(t.Context(), &wafv2sdk.GetRuleGroupInput{Id: created.Summary.Id})
	require.NoError(t, err)
	require.NotNil(t, got2.RuleGroup.MonetizationConfig)
	assert.Equal(t, types.CurrencyModeReal, got2.RuleGroup.MonetizationConfig.CurrencyMode,
		"UpdateRuleGroup's MonetizationConfig was silently dropped before updateRuleGroupRequest had a field for it")
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

// TestGetWebACL_CapacityAndLabelNamespace proves two real GetWebACLOutput
// members were entirely unmodeled (reverse write-only-state direction: a
// Get/Describe op not reading data this backend can already derive):
//
//   - WebACL.Capacity (wafv2@v1.77.3 types/types.go, "The web ACL capacity
//     units (WCUs) currently being used by this web ACL", Required: No but
//     always genuinely populated by real AWS) -- this backend already has a
//     real per-statement WCU cost model (capacity.go, used by CheckCapacity)
//     but never applied it to GetWebACL/GetWebACLForResource's own response.
//   - WebACL.LabelNamespace (types/types.go), whose exact grammar
//     ("awswaf:<account ID>:webacl:<web ACL name>:") is confirmed via the
//     AWS API reference (the pinned SDK's own godoc comment for this field
//     has its <placeholder> substitutions stripped by a codegen artifact,
//     unlike the doc comment's plain-text mirror on
//     https://docs.aws.amazon.com/waf/latest/APIReference/API_WebACL.html)
//     -- deterministic from data this backend already has (AccountID, Name),
//     not fabricated.
func TestGetWebACL_CapacityAndLabelNamespace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	vc := &types.VisibilityConfig{
		CloudWatchMetricsEnabled: true,
		MetricName:               aws.String("metric"),
		SampledRequestsEnabled:   true,
	}

	created, err := client.CreateWebACL(t.Context(), &wafv2sdk.CreateWebACLInput{
		Name:             aws.String("capacity-labelns-acl"),
		Scope:            types.ScopeRegional,
		DefaultAction:    &types.DefaultAction{Allow: &types.AllowAction{}},
		VisibilityConfig: vc,
		Rules: []types.Rule{
			{
				Name:     aws.String("xss-rule"),
				Priority: 0,
				Statement: &types.Statement{
					XssMatchStatement: &types.XssMatchStatement{
						FieldToMatch: &types.FieldToMatch{AllQueryArguments: &types.AllQueryArguments{}},
						TextTransformations: []types.TextTransformation{
							{Priority: 0, Type: types.TextTransformationTypeNone},
						},
					},
				},
				Action:           &types.RuleAction{Block: &types.BlockAction{}},
				VisibilityConfig: vc,
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetWebACL(t.Context(), &wafv2sdk.GetWebACLInput{Id: created.Summary.Id})
	require.NoError(t, err)

	// XssMatchStatement base 40 WCU + AllQueryArguments surcharge 10 + one
	// TextTransformation 10 == 60, matching capacity.go's documented model.
	assert.Equal(t, int64(60), got.WebACL.Capacity,
		"WebACL.Capacity was never computed/emitted by GetWebACL despite this backend "+
			"already having a real per-statement WCU cost model (capacity.go)")

	wantNamespace := fmt.Sprintf("awswaf:%s:webacl:%s:", "000000000000", "capacity-labelns-acl")
	assert.Equal(t, wantNamespace, aws.ToString(got.WebACL.LabelNamespace),
		"WebACL.LabelNamespace was never emitted by GetWebACL")
}

// TestGetRuleGroup_LabelNamespace proves RuleGroup.LabelNamespace
// (types/types.go, same "awswaf:<account ID>:rulegroup:<rule group name>:"
// grammar as WebACL's, confirmed via
// https://docs.aws.amazon.com/waf/latest/APIReference/API_RuleGroup.html)
// was entirely unmodeled by GetRuleGroup, unlike RuleGroup.Capacity (a
// sibling field this handler already emits correctly).
func TestGetRuleGroup_LabelNamespace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	created, err := client.CreateRuleGroup(t.Context(), &wafv2sdk.CreateRuleGroupInput{
		Name:     aws.String("labelns-rulegroup"),
		Scope:    types.ScopeRegional,
		Capacity: aws.Int64(10),
		VisibilityConfig: &types.VisibilityConfig{
			CloudWatchMetricsEnabled: true,
			MetricName:               aws.String("metric"),
			SampledRequestsEnabled:   true,
		},
	})
	require.NoError(t, err)

	got, err := client.GetRuleGroup(t.Context(), &wafv2sdk.GetRuleGroupInput{Id: created.Summary.Id})
	require.NoError(t, err)

	wantNamespace := fmt.Sprintf("awswaf:%s:rulegroup:%s:", "000000000000", "labelns-rulegroup")
	assert.Equal(t, wantNamespace, aws.ToString(got.RuleGroup.LabelNamespace),
		"RuleGroup.LabelNamespace was never emitted by GetRuleGroup")
}

// TestDescribeManagedRuleGroup_LabelNamespaceAndVersionName proves two real
// DescribeManagedRuleGroupOutput members were entirely unmodeled:
// LabelNamespace (grammar "awswaf:managed:<vendor>:<rule group name>:",
// confirmed via
// https://docs.aws.amazon.com/waf/latest/APIReference/API_DescribeManagedRuleGroup.html,
// deterministic from data this catalog already has) and VersionName (echoes
// the request's VersionName, or the catalog's hardcoded default
// "Version_1.0" for a versioning-supported group -- matching
// ListAvailableManagedRuleGroupVersions' own existing CurrentDefaultVersion
// value, not a new invention).
func TestDescribeManagedRuleGroup_LabelNamespaceAndVersionName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestWAFV2Client(t, h)

	got, err := client.DescribeManagedRuleGroup(t.Context(), &wafv2sdk.DescribeManagedRuleGroupInput{
		Scope:      types.ScopeRegional,
		VendorName: aws.String("AWS"),
		Name:       aws.String("AWSManagedRulesCommonRuleSet"),
	})
	require.NoError(t, err)

	wantNamespace := "awswaf:managed:AWS:AWSManagedRulesCommonRuleSet:"
	assert.Equal(t, wantNamespace, aws.ToString(got.LabelNamespace),
		"DescribeManagedRuleGroupOutput.LabelNamespace was never emitted")
	assert.Equal(t, "Version_1.0", aws.ToString(got.VersionName),
		"DescribeManagedRuleGroupOutput.VersionName was never emitted for a versioning-supported managed rule group")
}
