package ec2_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeSecurityGroupRules verifies SG rule listing.
func TestDescribeSecurityGroupRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	sg, err := b.CreateSecurityGroup("test-sg", "testing", "vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", IPRange: "0.0.0.0/0", FromPort: 80, ToPort: 80},
	}))

	rules, err := b.DescribeSecurityGroupRules(sg.ID)
	require.NoError(t, err)
	// 2 = 1 ingress rule added above + 1 default egress allow-all rule.
	require.Len(t, rules, 2)
	// DescribeSecurityGroupRules appends ingress first.
	assert.False(t, rules[0].IsEgress)
	assert.Equal(t, "tcp", rules[0].Protocol)
}

// TestModifySecurityGroupRules verifies SG rule replacement.

// TestModifySecurityGroupRules verifies SG rule replacement.
func TestModifySecurityGroupRules(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	sg, err := b.CreateSecurityGroup("test-sg", "testing", "vpc-default")
	require.NoError(t, err)

	require.NoError(t, b.AuthorizeSecurityGroupIngress(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", IPRange: "0.0.0.0/0", FromPort: 80, ToPort: 80},
	}))

	// replace with port 443
	require.NoError(t, b.ModifySecurityGroupRules(sg.ID, []ec2.SecurityGroupRule{
		{Protocol: "tcp", IPRange: "0.0.0.0/0", FromPort: 443, ToPort: 443},
	}, false))

	rules, err := b.DescribeSecurityGroupRules(sg.ID)
	require.NoError(t, err)
	// 2 = 1 modified ingress rule + 1 default egress allow-all rule.
	require.Len(t, rules, 2)
	// Ingress rules are returned first.
	assert.Equal(t, 443, rules[0].FromPort)
}

// TestDeleteLaunchTemplate verifies launch template deletion.

func TestSecurityGroupRule_SGReference(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	sg1, err := b.CreateSecurityGroup("sg-source", "source sg", "vpc-default")
	require.NoError(t, err)

	sg2, err := b.CreateSecurityGroup("sg-target", "target sg", "vpc-default")
	require.NoError(t, err)

	rule := ec2.SecurityGroupRule{
		Protocol:      "tcp",
		FromPort:      80,
		ToPort:        80,
		SourceGroupID: sg1.ID,
	}

	err = b.AuthorizeSecurityGroupIngress(sg2.ID, []ec2.SecurityGroupRule{rule})
	require.NoError(t, err)

	sgs := b.DescribeSecurityGroups([]string{sg2.ID})
	require.Len(t, sgs, 1)
	require.Len(t, sgs[0].IngressRules, 1)
	assert.Equal(t, sg1.ID, sgs[0].IngressRules[0].SourceGroupID)
}

// ---- Gap 7: DryRun ----

// TestHTTP_DescribeSecurityGroupRules verifies the HTTP handler.
func TestHTTP_DescribeSecurityGroupRules(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// use the default sg
	rec := postForm(
		t,
		h,
		"Action=DescribeSecurityGroupRules&Version=2016-11-15&Filter.1.Value=sg-default",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeSecurityGroupRulesResponse")
}
