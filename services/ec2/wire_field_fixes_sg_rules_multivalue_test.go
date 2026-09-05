package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// DescribeSecurityGroupRulesInput.Filters documents group-id as a normal
// EC2 Filter (api_op_DescribeSecurityGroupRules.go), and types.Filter's own
// doc comment documents multiple filter values as joined with OR
// (aws-sdk-go-v2/service/ec2/types/types.go). handleDescribeSecurityGroupRules
// (handler_security_groups.go) read only filters["group-id"][0], silently
// dropping every value after the first, so a client asking for rules across
// two groups by listing both group-id values got back only the first
// group's rules.
func TestDescribeSecurityGroupRules_GroupIDFilter_MultipleValues_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	sg1, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName: aws.String("sg-rules-multivalue-1"), Description: aws.String("first"),
	})
	require.NoError(t, err)
	sg2, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName: aws.String("sg-rules-multivalue-2"), Description: aws.String("second"),
	})
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId: sg1.GroupId,
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"), FromPort: aws.Int32(22), ToPort: aws.Int32(22),
			IpRanges: []types.IpRange{{CidrIp: aws.String("203.0.113.1/32")}},
		}},
	})
	require.NoError(t, err)
	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId: sg2.GroupId,
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"), FromPort: aws.Int32(443), ToPort: aws.Int32(443),
			IpRanges: []types.IpRange{{CidrIp: aws.String("203.0.113.2/32")}},
		}},
	})
	require.NoError(t, err)

	out, err := client.DescribeSecurityGroupRules(t.Context(), &ec2sdk.DescribeSecurityGroupRulesInput{
		Filters: []types.Filter{
			{Name: aws.String("group-id"), Values: []string{aws.ToString(sg1.GroupId), aws.ToString(sg2.GroupId)}},
		},
	})
	require.NoError(t, err)

	var sawSG1, sawSG2 bool
	for _, r := range out.SecurityGroupRules {
		switch aws.ToString(r.GroupId) {
		case aws.ToString(sg1.GroupId):
			sawSG1 = true
		case aws.ToString(sg2.GroupId):
			sawSG2 = true
		}
	}
	require.True(t, sawSG1, "group-id filter with multiple values must include the first group's rules")
	require.True(t, sawSG2,
		"group-id filter with multiple values must include the second group's rules, not just filters[\"group-id\"][0]")
}
