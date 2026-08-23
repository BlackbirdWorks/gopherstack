package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestCreateSecurityGroup_SurfacesArn_RealClient covers
// handleCreateSecurityGroup, which pre-fix never emitted SecurityGroupArn.
// The real CreateSecurityGroupOutput deserializer (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeOpDocumentCreateSecurityGroupOutput)
// matches "groupId", "securityGroupArn", and "tagSet" -- it has no case for
// "return" at all, so the emulator's <return> was always silently skipped
// (harmless), but the missing securityGroupArn left a real client's
// SecurityGroupArn empty regardless of what group was created.
func TestCreateSecurityGroup_SurfacesArn_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep14-sg"),
		Description: aws.String("sweep14 test sg"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.SecurityGroupArn, "pre-fix this field was never rendered")
	assert.Equal(
		t, "arn:aws:ec2:us-east-1:000000000000:security-group/"+aws.ToString(out.GroupId),
		aws.ToString(out.SecurityGroupArn),
	)
}

// TestDeleteSecurityGroup_SurfacesGroupID_RealClient covers
// handleDeleteSecurityGroup, which pre-fix returned only Return. The real
// DeleteSecurityGroupOutput deserializer (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDeleteSecurityGroupOutput) also matches
// "groupId", so a client confirming which group it just deleted got an empty
// string pre-fix.
func TestDeleteSecurityGroup_SurfacesGroupID_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	sg, err := b.CreateSecurityGroup("sweep14-del-sg", "test", "")
	require.NoError(t, err)

	out, err := client.DeleteSecurityGroup(t.Context(), &ec2sdk.DeleteSecurityGroupInput{
		GroupId: aws.String(sg.ID),
	})
	require.NoError(t, err)
	require.NotNil(t, out.GroupId, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, sg.ID, aws.ToString(out.GroupId))
}

// TestAuthorizeSecurityGroupIngress_SurfacesRules_RealClient covers
// handleAuthorizeSecurityGroupIngress, which pre-fix returned only Return.
// The real AuthorizeSecurityGroupIngressOutput deserializer also matches
// "securityGroupRuleSet" (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentAuthorizeSecurityGroupIngressOutput), so a
// client relying on the returned SecurityGroupRuleId (e.g. to later target
// ModifySecurityGroupRules at exactly this rule) always saw an empty slice
// pre-fix, even though the rule was genuinely authorized.
func TestAuthorizeSecurityGroupIngress_SurfacesRules_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep14-auth-in-sg"),
		Description: aws.String("sweep14 auth ingress test sg"),
	})
	require.NoError(t, err)

	out, err := client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId: sg.GroupId,
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(22),
			ToPort:     aws.Int32(22),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("203.0.113.5/32")}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroupRules, 1, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, "tcp", aws.ToString(out.SecurityGroupRules[0].IpProtocol))
	assert.Equal(t, "203.0.113.5/32", aws.ToString(out.SecurityGroupRules[0].CidrIpv4))
	assert.False(t, aws.ToBool(out.SecurityGroupRules[0].IsEgress))
	assert.NotEmpty(t, aws.ToString(out.SecurityGroupRules[0].SecurityGroupRuleId))
}

// TestAuthorizeSecurityGroupEgress_SurfacesRules_RealClient mirrors
// TestAuthorizeSecurityGroupIngress_SurfacesRules_RealClient for the egress
// op (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentAuthorizeSecurityGroupEgressOutput).
func TestAuthorizeSecurityGroupEgress_SurfacesRules_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep14-auth-eg-sg"),
		Description: aws.String("sweep14 auth egress test sg"),
	})
	require.NoError(t, err)

	out, err := client.AuthorizeSecurityGroupEgress(t.Context(), &ec2sdk.AuthorizeSecurityGroupEgressInput{
		GroupId: sg.GroupId,
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(443),
			ToPort:     aws.Int32(443),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("198.51.100.9/32")}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroupRules, 1, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, "198.51.100.9/32", aws.ToString(out.SecurityGroupRules[0].CidrIpv4))
	assert.True(t, aws.ToBool(out.SecurityGroupRules[0].IsEgress))
}

// TestRevokeSecurityGroupIngress_SurfacesRevokedAndUnknown_RealClient covers
// handleRevokeSecurityGroupIngress, which pre-fix returned only Return. The
// real RevokeSecurityGroupIngressOutput deserializer also matches
// "revokedSecurityGroupRuleSet" and "unknownIpPermissionSet" (ec2@v1.319.1
// deserializers.go,
// awsEc2query_deserializeOpDocumentRevokeSecurityGroupIngressOutput), so a
// client could never tell which of several revoke requests actually matched
// an existing rule pre-fix.
func TestRevokeSecurityGroupIngress_SurfacesRevokedAndUnknown_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep14-revoke-sg"),
		Description: aws.String("sweep14 revoke test sg"),
	})
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId: sg.GroupId,
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(22),
			ToPort:     aws.Int32(22),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("203.0.113.6/32")}},
		}},
	})
	require.NoError(t, err)

	out, err := client.RevokeSecurityGroupIngress(t.Context(), &ec2sdk.RevokeSecurityGroupIngressInput{
		GroupId: sg.GroupId,
		IpPermissions: []types.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(22),
				ToPort:     aws.Int32(22),
				IpRanges:   []types.IpRange{{CidrIp: aws.String("203.0.113.6/32")}},
			},
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(9999),
				ToPort:     aws.Int32(9999),
				IpRanges:   []types.IpRange{{CidrIp: aws.String("192.0.2.1/32")}},
			},
		},
	})
	require.NoError(t, err)
	require.Len(
		t, out.RevokedSecurityGroupRules, 1,
		"pre-fix this field was never rendered, only a bare Return bool",
	)
	assert.Equal(t, "203.0.113.6/32", aws.ToString(out.RevokedSecurityGroupRules[0].CidrIpv4))

	require.Len(
		t, out.UnknownIpPermissions, 1,
		"pre-fix this field was never rendered, so a bogus revoke request looked identical to a real one",
	)
	assert.Equal(t, int32(9999), aws.ToInt32(out.UnknownIpPermissions[0].FromPort))
}

// TestAssociateSecurityGroupVpc_WireShape_RealClient covers
// handleAssociateSecurityGroupVpc, which pre-fix wrapped <state> in an extra
// nested <state> element (associateSGVpcResponse's now-removed
// sgVpcAssocStateItem). The real deserializer (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeOpDocumentAssociateSecurityGroupVpcOutput)
// reads the <state> element's value directly via decoder.Value(), which
// cannot decode a child element in place of character data and returns an
// error outright rather than silently dropping the field.
func TestAssociateSecurityGroupVpc_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.51.0.0/16")})
	require.NoError(t, err)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep14-assoc-sg"),
		Description: aws.String("sweep14 assoc test sg"),
		VpcId:       vpc.Vpc.VpcId,
	})
	require.NoError(t, err)

	out, err := client.AssociateSecurityGroupVpc(t.Context(), &ec2sdk.AssociateSecurityGroupVpcInput{
		GroupId: sg.GroupId,
		VpcId:   vpc.Vpc.VpcId,
	})
	require.NoError(t, err, "pre-fix this decode failed outright: "+
		`expected value for state element, got xml.StartElement`)
	assert.Equal(t, "associated", string(out.State))
}

// TestDisassociateSecurityGroupVpc_WireShape_RealClient covers
// handleDisassociateSecurityGroupVpc, which pre-fix rendered a bare
// <return>true</return> via stubResponse. The real
// DisassociateSecurityGroupVpcOutput has no Return member at all -- only
// State (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDisassociateSecurityGroupVpcOutput) -- so
// the real deserializer had no case for "return" and silently skipped it,
// leaving State permanently empty regardless of the disassociation's outcome.
func TestDisassociateSecurityGroupVpc_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.50.0.0/16")})
	require.NoError(t, err)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep14-disassoc-sg"),
		Description: aws.String("sweep14 disassoc test sg"),
		VpcId:       vpc.Vpc.VpcId,
	})
	require.NoError(t, err)

	_, err = client.AssociateSecurityGroupVpc(t.Context(), &ec2sdk.AssociateSecurityGroupVpcInput{
		GroupId: sg.GroupId,
		VpcId:   vpc.Vpc.VpcId,
	})
	require.NoError(t, err)

	out, err := client.DisassociateSecurityGroupVpc(t.Context(), &ec2sdk.DisassociateSecurityGroupVpcInput{
		GroupId: sg.GroupId,
		VpcId:   vpc.Vpc.VpcId,
	})
	require.NoError(t, err)
	assert.Equal(
		t, "disassociated", string(out.State),
		"State empty - real deserializer has no case for <return>, only <state>",
	)
}
