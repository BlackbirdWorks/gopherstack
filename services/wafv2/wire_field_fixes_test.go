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
