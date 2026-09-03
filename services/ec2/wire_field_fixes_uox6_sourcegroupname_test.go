package ec2_test

// uox6-sourcegroupname: AuthorizeSecurityGroupIngress and
// RevokeSecurityGroupIngress both declare a top-level SourceSecurityGroupName
// field ("[Default VPC] The name of the source security group... The rule
// grants full ICMP, UDP, and TCP access. To create a rule with a specific
// protocol and port range, specify a set of IP permissions instead." --
// api_op_AuthorizeSecurityGroupIngress.go, ec2@v1.319.1) as an alternative to
// IpPermissions, but the handler never read it -- a caller using the classic
// single-rule form got a request that silently added nothing. The
// Authorize/Revoke Egress siblings declare the same field but document it
// "Not supported. Use IP permissions instead.", so they're correctly left
// alone.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthorizeSecurityGroupIngress_BySourceGroupName(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	target, err := b.CreateSecurityGroup("uox6-target-sg", "target", vpc.ID)
	require.NoError(t, err)
	source, err := b.CreateSecurityGroup("uox6-source-sg", "source", vpc.ID)
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId:                 aws.String(target.ID),
		SourceSecurityGroupName: aws.String("uox6-source-sg"),
	})
	require.NoError(t, err)

	out, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{target.ID},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroups, 1)
	require.Len(t, out.SecurityGroups[0].IpPermissions, 1)

	perm := out.SecurityGroups[0].IpPermissions[0]
	assert.Equal(t, "-1", aws.ToString(perm.IpProtocol))
	require.Len(t, perm.UserIdGroupPairs, 1)
	assert.Equal(t, source.ID, aws.ToString(perm.UserIdGroupPairs[0].GroupId))
}

func TestAuthorizeSecurityGroupIngress_BySourceGroupName_NotFound(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	target, err := b.CreateSecurityGroup("uox6-target-sg2", "target", vpc.ID)
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId:                 aws.String(target.ID),
		SourceSecurityGroupName: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidGroup.NotFound", apiErr.ErrorCode())
}

func TestRevokeSecurityGroupIngress_BySourceGroupName(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	target, err := b.CreateSecurityGroup("uox6-target-sg3", "target", vpc.ID)
	require.NoError(t, err)
	_, err = b.CreateSecurityGroup("uox6-source-sg3", "source", vpc.ID)
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId:                 aws.String(target.ID),
		SourceSecurityGroupName: aws.String("uox6-source-sg3"),
	})
	require.NoError(t, err)

	preRevoke, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{target.ID},
	})
	require.NoError(t, err)
	require.Len(t, preRevoke.SecurityGroups, 1)
	require.Len(
		t,
		preRevoke.SecurityGroups[0].IpPermissions,
		1,
		"authorize must have added the rule before revoke can remove it",
	)

	revokeOut, err := client.RevokeSecurityGroupIngress(t.Context(), &ec2sdk.RevokeSecurityGroupIngressInput{
		GroupId:                 aws.String(target.ID),
		SourceSecurityGroupName: aws.String("uox6-source-sg3"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(revokeOut.Return))
	assert.Empty(t, revokeOut.UnknownIpPermissions)

	out, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{target.ID},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroups, 1)
	assert.Empty(t, out.SecurityGroups[0].IpPermissions)
}
