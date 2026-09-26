package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dxsvc "github.com/aws/aws-sdk-go-v2/service/directconnect"
	dxtypes "github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	wafsvc "github.com/aws/aws-sdk-go-v2/service/waf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_WafclassicAndDirectconnect provisions WAF Classic match/rule/rule-group/web-ACL
// resources and Direct Connect virtual interfaces, hosted connections, BGP
// peers, MACsec key association, and a gateway association proposal via
// Terraform and verifies each through its own SDK client's Get/Describe path.
func TestTerraform_WafclassicAndDirectconnect(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "wafclassic-and-directconnect",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyWafclassicAndDirectconnectWAF(ctx, t)
				verifyWafclassicAndDirectconnectDirectConnect(ctx, t)
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

func verifyWafclassicAndDirectconnectWAF(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := wafsvc.NewFromConfig(cfg, func(o *wafsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	byteOut, err := client.ListByteMatchSets(ctx, &wafsvc.ListByteMatchSetsInput{})
	require.NoError(t, err, "ListByteMatchSets should succeed")
	require.NotEmpty(t, byteOut.ByteMatchSets)

	getByte, err := client.GetByteMatchSet(ctx, &wafsvc.GetByteMatchSetInput{
		ByteMatchSetId: byteOut.ByteMatchSets[0].ByteMatchSetId,
	})
	require.NoError(t, err, "GetByteMatchSet should succeed")
	assert.NotEmpty(t, getByte.ByteMatchSet.ByteMatchTuples)

	geoOut, err := client.ListGeoMatchSets(ctx, &wafsvc.ListGeoMatchSetsInput{})
	require.NoError(t, err, "ListGeoMatchSets should succeed")
	require.NotEmpty(t, geoOut.GeoMatchSets)

	getGeo, err := client.GetGeoMatchSet(ctx, &wafsvc.GetGeoMatchSetInput{
		GeoMatchSetId: geoOut.GeoMatchSets[0].GeoMatchSetId,
	})
	require.NoError(t, err, "GetGeoMatchSet should succeed")
	assert.NotEmpty(t, getGeo.GeoMatchSet.GeoMatchConstraints)

	patternOut, err := client.ListRegexPatternSets(ctx, &wafsvc.ListRegexPatternSetsInput{})
	require.NoError(t, err, "ListRegexPatternSets should succeed")
	require.NotEmpty(t, patternOut.RegexPatternSets)

	getPattern, err := client.GetRegexPatternSet(ctx, &wafsvc.GetRegexPatternSetInput{
		RegexPatternSetId: patternOut.RegexPatternSets[0].RegexPatternSetId,
	})
	require.NoError(t, err, "GetRegexPatternSet should succeed")
	assert.Contains(t, getPattern.RegexPatternSet.RegexPatternStrings, "waf-regex-pattern")

	regexMatchOut, err := client.ListRegexMatchSets(ctx, &wafsvc.ListRegexMatchSetsInput{})
	require.NoError(t, err, "ListRegexMatchSets should succeed")
	require.NotEmpty(t, regexMatchOut.RegexMatchSets)

	getRegexMatch, err := client.GetRegexMatchSet(ctx, &wafsvc.GetRegexMatchSetInput{
		RegexMatchSetId: regexMatchOut.RegexMatchSets[0].RegexMatchSetId,
	})
	require.NoError(t, err, "GetRegexMatchSet should succeed")
	assert.NotEmpty(t, getRegexMatch.RegexMatchSet.RegexMatchTuples)

	sizeOut, err := client.ListSizeConstraintSets(ctx, &wafsvc.ListSizeConstraintSetsInput{})
	require.NoError(t, err, "ListSizeConstraintSets should succeed")
	require.NotEmpty(t, sizeOut.SizeConstraintSets)

	getSize, err := client.GetSizeConstraintSet(ctx, &wafsvc.GetSizeConstraintSetInput{
		SizeConstraintSetId: sizeOut.SizeConstraintSets[0].SizeConstraintSetId,
	})
	require.NoError(t, err, "GetSizeConstraintSet should succeed")
	assert.NotEmpty(t, getSize.SizeConstraintSet.SizeConstraints)

	sqliOut, err := client.ListSqlInjectionMatchSets(ctx, &wafsvc.ListSqlInjectionMatchSetsInput{})
	require.NoError(t, err, "ListSqlInjectionMatchSets should succeed")
	require.NotEmpty(t, sqliOut.SqlInjectionMatchSets)

	getSQLi, err := client.GetSqlInjectionMatchSet(ctx, &wafsvc.GetSqlInjectionMatchSetInput{
		SqlInjectionMatchSetId: sqliOut.SqlInjectionMatchSets[0].SqlInjectionMatchSetId,
	})
	require.NoError(t, err, "GetSqlInjectionMatchSet should succeed")
	assert.NotEmpty(t, getSQLi.SqlInjectionMatchSet.SqlInjectionMatchTuples)

	xssOut, err := client.ListXssMatchSets(ctx, &wafsvc.ListXssMatchSetsInput{})
	require.NoError(t, err, "ListXssMatchSets should succeed")
	require.NotEmpty(t, xssOut.XssMatchSets)

	getXSS, err := client.GetXssMatchSet(ctx, &wafsvc.GetXssMatchSetInput{
		XssMatchSetId: xssOut.XssMatchSets[0].XssMatchSetId,
	})
	require.NoError(t, err, "GetXssMatchSet should succeed")
	assert.NotEmpty(t, getXSS.XssMatchSet.XssMatchTuples)

	rateOut, err := client.ListRateBasedRules(ctx, &wafsvc.ListRateBasedRulesInput{})
	require.NoError(t, err, "ListRateBasedRules should succeed")
	require.NotEmpty(t, rateOut.Rules)

	getRate, err := client.GetRateBasedRule(ctx, &wafsvc.GetRateBasedRuleInput{
		RuleId: rateOut.Rules[0].RuleId,
	})
	require.NoError(t, err, "GetRateBasedRule should succeed")
	assert.EqualValues(t, 2000, aws.ToInt64(getRate.Rule.RateLimit))

	groupOut, err := client.ListRuleGroups(ctx, &wafsvc.ListRuleGroupsInput{})
	require.NoError(t, err, "ListRuleGroups should succeed")
	require.NotEmpty(t, groupOut.RuleGroups)

	activated, err := client.ListActivatedRulesInRuleGroup(ctx, &wafsvc.ListActivatedRulesInRuleGroupInput{
		RuleGroupId: groupOut.RuleGroups[0].RuleGroupId,
	})
	require.NoError(t, err, "ListActivatedRulesInRuleGroup should succeed")
	assert.NotEmpty(t, activated.ActivatedRules)

	aclOut, err := client.ListWebACLs(ctx, &wafsvc.ListWebACLsInput{})
	require.NoError(t, err, "ListWebACLs should succeed")
	require.NotEmpty(t, aclOut.WebACLs)

	getACL, err := client.GetWebACL(ctx, &wafsvc.GetWebACLInput{
		WebACLId: aclOut.WebACLs[0].WebACLId,
	})
	require.NoError(t, err, "GetWebACL should succeed")
	assert.NotEmpty(t, getACL.WebACL.Rules)
	assert.Equal(t, dxWAFAllowAction, string(getACL.WebACL.DefaultAction.Type))
}

const dxWAFAllowAction = "ALLOW"

func verifyWafclassicAndDirectconnectDirectConnect(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := dxsvc.NewFromConfig(cfg, func(o *dxsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	connsOut, err := client.DescribeConnections(ctx, &dxsvc.DescribeConnectionsInput{})
	require.NoError(t, err, "DescribeConnections should succeed")

	var hostedConnID string

	for _, c := range connsOut.Connections {
		if aws.ToString(c.ConnectionName) == "wfdc-hosted-conn" {
			hostedConnID = aws.ToString(c.ConnectionId)
		}
	}

	require.NotEmpty(t, hostedConnID, "hosted connection should be listed")

	lagsOut, err := client.DescribeLags(ctx, &dxsvc.DescribeLagsInput{})
	require.NoError(t, err, "DescribeLags should succeed")
	require.NotEmpty(t, lagsOut.Lags)

	var lagID string

	for _, l := range lagsOut.Lags {
		if aws.ToString(l.LagName) == "wfdc-lag" {
			lagID = aws.ToString(l.LagId)
		}
	}

	require.NotEmpty(t, lagID, "lag should be listed")

	hostedOut, err := client.DescribeHostedConnections(ctx, &dxsvc.DescribeHostedConnectionsInput{
		ConnectionId: aws.String(lagID),
	})
	require.NoError(t, err, "DescribeHostedConnections should succeed")
	require.NotEmpty(t, hostedOut.Connections, "hosted connection should appear on the LAG")

	macsecOut, err := client.DescribeConnections(ctx, &dxsvc.DescribeConnectionsInput{})
	require.NoError(t, err, "DescribeConnections (macsec) should succeed")

	var macsecFound bool

	for _, c := range macsecOut.Connections {
		if aws.ToString(c.ConnectionName) == "wfdc-dx-conn" && len(c.MacSecKeys) > 0 {
			macsecFound = true
		}
	}

	assert.True(t, macsecFound, "MACsec key should be associated with the connection")

	vifOut, err := client.DescribeVirtualInterfaces(ctx, &dxsvc.DescribeVirtualInterfacesInput{})
	require.NoError(t, err, "DescribeVirtualInterfaces should succeed")

	byName := map[string]dxtypes.VirtualInterface{}
	for _, v := range vifOut.VirtualInterfaces {
		byName[aws.ToString(v.VirtualInterfaceName)] = v
	}

	for _, name := range []string{
		"wfdc-private-vif",
		"wfdc-public-vif",
		"wfdc-transit-vif",
		"wfdc-hosted-private-vif",
		"wfdc-hosted-public-vif",
		"wfdc-hosted-transit-vif",
	} {
		require.Containsf(t, byName, name, "virtual interface %s should be listed", name)
	}

	privateVIF := byName["wfdc-private-vif"]
	require.NotEmpty(t, privateVIF.BgpPeers, "BGP peer should be attached to the private VIF")

	gwOut, err := client.DescribeDirectConnectGateways(ctx, &dxsvc.DescribeDirectConnectGatewaysInput{})
	require.NoError(t, err, "DescribeDirectConnectGateways should succeed")

	var proposalGatewayID string

	for _, g := range gwOut.DirectConnectGateways {
		if aws.ToString(g.DirectConnectGatewayName) == "wfdc-dxgw-proposal" {
			proposalGatewayID = aws.ToString(g.DirectConnectGatewayId)
		}
	}

	require.NotEmpty(t, proposalGatewayID, "proposal direct connect gateway should be listed")

	proposalsOut, err := client.DescribeDirectConnectGatewayAssociationProposals(
		ctx,
		&dxsvc.DescribeDirectConnectGatewayAssociationProposalsInput{
			DirectConnectGatewayId: aws.String(proposalGatewayID),
		},
	)
	require.NoError(t, err, "DescribeDirectConnectGatewayAssociationProposals should succeed")
	assert.NotEmpty(t, proposalsOut.DirectConnectGatewayAssociationProposals)
}
