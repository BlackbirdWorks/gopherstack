package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecurityGroupVpc(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	sg, _ := b.CreateSecurityGroup("test-sg", "test", "vpc-default")
	vpc, _ := b.CreateVpc("10.0.0.0/16")

	t.Run( //nolint:paralleltest // existing issue.
		"DescribeSecurityGroupReferences returns empty before association",
		func(t *testing.T) {
			refs := b.DescribeSecurityGroupReferences([]string{sg.ID})
			assert.Empty(t, refs)
		},
	)

	t.Run( //nolint:paralleltest // existing issue.
		"DescribeStaleSecurityGroups returns empty with no deleted peering",
		func(t *testing.T) {
			stale := b.DescribeStaleSecurityGroups("vpc-default")
			assert.Empty(t, stale)
		},
	)

	t.Run("associate and describe", func(t *testing.T) { //nolint:paralleltest // existing issue.
		result, err := b.AssociateSecurityGroupVpc(sg.ID, vpc.ID)
		require.NoError(t, err)
		assert.Equal(t, "associated", result.State)

		assocs := b.DescribeSecurityGroupVpcAssociations([]string{sg.ID})
		require.Len(t, assocs, 1)
		assert.Equal(t, sg.ID, assocs[0].SGID)
		assert.Equal(t, vpc.ID, assocs[0].VPCID)
	})

	t.Run("disassociate", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisassociateSecurityGroupVpc(sg.ID, vpc.ID))
	})
}

func TestGetSecurityGroupsForVpc(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("returns default SG for default VPC", func(t *testing.T) { //nolint:paralleltest // existing issue.
		sgs, err := b.GetSecurityGroupsForVpc("vpc-default")
		require.NoError(t, err)
		assert.NotEmpty(t, sgs)
	})

	t.Run("empty VPC returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.GetSecurityGroupsForVpc("")
		require.Error(t, err)
	})
}

// ---- UpdateSecurityGroupRuleDescriptions ---- //nolint:godot // existing issue.
func TestUpdateSGRuleDescriptions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	sg, _ := b.CreateSecurityGroup("test-sg", "test", "vpc-default")

	t.Run("ingress update", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.UpdateSecurityGroupRuleDescriptionsIngress(sg.ID, nil))
	})

	t.Run("egress update", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.UpdateSecurityGroupRuleDescriptionsEgress(sg.ID, nil))
	})

	t.Run("unknown SG returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.UpdateSecurityGroupRuleDescriptionsIngress("sg-missing", nil))
	})

	t.Run("sets description on matching ingress rule", func(t *testing.T) { //nolint:paralleltest // existing issue.
		sg2, err := b.CreateSecurityGroup("desc-sg", "test", "vpc-default")
		require.NoError(t, err)
		require.NoError(t, b.AuthorizeSecurityGroupIngress(sg2.ID, []ec2.SecurityGroupRule{
			{Protocol: "tcp", FromPort: 443, ToPort: 443, IPRange: "10.0.0.0/16"},
		}))

		err = b.UpdateSecurityGroupRuleDescriptionsIngress(sg2.ID, []ec2.SecurityGroupRule{
			{Protocol: "tcp", FromPort: 443, ToPort: 443, IPRange: "10.0.0.0/16", Description: "HTTPS from VPC"},
		})
		require.NoError(t, err)

		rules, err := b.DescribeSecurityGroupRules(sg2.ID)
		require.NoError(t, err)
		rule := findSGRule(t, rules, false, 443)
		assert.Equal(t, "HTTPS from VPC", rule.Description)
	})

	t.Run("sets description on matching egress rule", func(t *testing.T) { //nolint:paralleltest // existing issue.
		sg3, err := b.CreateSecurityGroup("desc-sg-egress", "test", "vpc-default")
		require.NoError(t, err)
		require.NoError(t, b.AuthorizeSecurityGroupEgress(sg3.ID, []ec2.SecurityGroupRule{
			{Protocol: "tcp", FromPort: 8080, ToPort: 8080, IPRange: "0.0.0.0/0"},
		}))

		err = b.UpdateSecurityGroupRuleDescriptionsEgress(sg3.ID, []ec2.SecurityGroupRule{
			{Protocol: "tcp", FromPort: 8080, ToPort: 8080, IPRange: "0.0.0.0/0", Description: "outbound app traffic"},
		})
		require.NoError(t, err)

		rules, err := b.DescribeSecurityGroupRules(sg3.ID)
		require.NoError(t, err)
		rule := findSGRule(t, rules, true, 8080)
		assert.Equal(t, "outbound app traffic", rule.Description)
	})

	t.Run("does not affect rule identity for revoke", func(t *testing.T) { //nolint:paralleltest // existing issue.
		sg4, err := b.CreateSecurityGroup("desc-sg-revoke", "test", "vpc-default")
		require.NoError(t, err)

		rule := ec2.SecurityGroupRule{Protocol: "tcp", FromPort: 22, ToPort: 22, IPRange: "1.2.3.4/32"}
		require.NoError(t, b.AuthorizeSecurityGroupIngress(sg4.ID, []ec2.SecurityGroupRule{rule}))
		require.NoError(t, b.UpdateSecurityGroupRuleDescriptionsIngress(sg4.ID, []ec2.SecurityGroupRule{
			{Protocol: "tcp", FromPort: 22, ToPort: 22, IPRange: "1.2.3.4/32", Description: "ssh"},
		}))

		// Revoking without the description must still find and remove the rule.
		require.NoError(t, b.RevokeSecurityGroupIngress(sg4.ID, []ec2.SecurityGroupRule{rule}))

		rules, err := b.DescribeSecurityGroupRules(sg4.ID)
		require.NoError(t, err)

		for _, r := range rules {
			assert.NotEqual(t, 22, r.FromPort, "revoked ingress rule must be gone: %+v", r)
		}
	})
}

// findSGRule locates the rule matching direction (isEgress) and fromPort in a
// DescribeSecurityGroupRules result, failing the test if none is found.

// TestHTTP_UpdateSGRuleDescriptions verifies the HTTP handler parses the
// request's rule-description fields instead of passing nil (which used to make
// the operation a silent no-op).
func TestHTTP_UpdateSGRuleDescriptions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	sg, err := b.CreateSecurityGroup("http-desc-sg", "test", "vpc-default")
	require.NoError(t, err)
	require.NoError(t, b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", FromPort: 22, ToPort: 22, IPRange: "1.2.3.4/32"},
	}))

	_, err = ec2.ExportDispatch(h, url.Values{
		"Action":                                 []string{"UpdateSecurityGroupRuleDescriptionsIngress"},
		"GroupId":                                []string{sg.ID},
		"IpPermissions.1.IpProtocol":             []string{"tcp"},
		"IpPermissions.1.FromPort":               []string{"22"},
		"IpPermissions.1.ToPort":                 []string{"22"},
		"IpPermissions.1.IpRanges.1.CidrIp":      []string{"1.2.3.4/32"},
		"IpPermissions.1.IpRanges.1.Description": []string{"ssh from office"},
	})
	require.NoError(t, err)

	rules, err := b.DescribeSecurityGroupRules(sg.ID)
	require.NoError(t, err)
	rule := findSGRule(t, rules, false, 22)
	assert.Equal(t, "ssh from office", rule.Description)
}

// ---- HTTP dispatch tests ---- //nolint:godot // existing issue.

// findSGRule locates the rule matching direction (isEgress) and fromPort in a
// DescribeSecurityGroupRules result, failing the test if none is found.
func findSGRule(
	t *testing.T,
	rules []*ec2.SecurityGroupRuleDetail,
	isEgress bool,
	fromPort int,
) *ec2.SecurityGroupRuleDetail {
	t.Helper()

	for _, r := range rules {
		if r.IsEgress == isEgress && r.FromPort == fromPort {
			return r
		}
	}

	t.Fatalf("no rule found with isEgress=%v fromPort=%d in %+v", isEgress, fromPort, rules)

	return nil
}

// TestHandlerModifySecurityGroupRules covers handleModifySecurityGroupRules.
func TestHandlerModifySecurityGroupRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.11.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("test-sg", "test", vpc.ID)
	require.NoError(t, err)

	rec := postForm(t, h, "Action=ModifySecurityGroupRules&Version=2016-11-15"+
		"&GroupId="+sg.ID+
		"&Egress=false"+
		"&IpPermissions.1.IpProtocol=tcp"+
		"&IpPermissions.1.FromPort=443"+
		"&IpPermissions.1.ToPort=443"+
		"&IpPermissions.1.IpRanges.1.CidrIp=10.0.0.0/8")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerReplaceNetworkACLAssociation covers handleReplaceNetworkACLAssociation.
