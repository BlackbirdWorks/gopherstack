package ec2_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

//nolint:gochecknoglobals // monotonic counter for unique test security group names
var securityGroupQuotaTestSeq atomic.Int64

func createTestSecurityGroup(t *testing.T, client *ec2sdk.Client, vpcID string) string {
	t.Helper()

	n := securityGroupQuotaTestSeq.Add(1)

	out, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String(fmt.Sprintf("sg-quota-test-%d", n)),
		Description: aws.String("quota test group"),
		VpcId:       aws.String(vpcID),
	})
	require.NoError(t, err)

	return aws.ToString(out.GroupId)
}

func createTestVpc(t *testing.T, client *ec2sdk.Client) string {
	t.Helper()

	out, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)

	return aws.ToString(out.Vpc.VpcId)
}

func TestValidateSecurityGroupQuotasForInterface(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)
	vpcID := createTestVpc(t, client)

	t.Run("a handful of groups within quota is valid", func(t *testing.T) {
		t.Parallel()

		sg1 := createTestSecurityGroup(t, client, vpcID)
		sg2 := createTestSecurityGroup(t, client, vpcID)

		out, err := client.ValidateSecurityGroupQuotasForInterface(
			t.Context(), &ec2sdk.ValidateSecurityGroupQuotasForInterfaceInput{
				SecurityGroupIds: []string{sg1, sg2},
			},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(out.Valid))
	})

	t.Run("more than five groups exceeds the per-interface quota", func(t *testing.T) {
		t.Parallel()

		const tooMany = 6

		ids := make([]string, 0, tooMany)
		for range tooMany {
			ids = append(ids, createTestSecurityGroup(t, client, vpcID))
		}

		_, err := client.ValidateSecurityGroupQuotasForInterface(
			t.Context(), &ec2sdk.ValidateSecurityGroupQuotasForInterfaceInput{SecurityGroupIds: ids},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
	})

	t.Run("more than sixty combined rules exceeds the quota", func(t *testing.T) {
		t.Parallel()

		sg := createTestSecurityGroup(t, client, vpcID)

		const tooManyRules = 61

		perms := make([]types.IpPermission, 0, tooManyRules)
		for i := range tooManyRules {
			perms = append(perms, types.IpPermission{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(int32(1000 + i)),
				ToPort:     aws.Int32(int32(1000 + i)),
				IpRanges:   []types.IpRange{{CidrIp: aws.String("10.0.0.0/32")}},
			})
		}

		_, err := client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
			GroupId:       aws.String(sg),
			IpPermissions: perms,
		})
		require.NoError(t, err)

		_, err = client.ValidateSecurityGroupQuotasForInterface(
			t.Context(), &ec2sdk.ValidateSecurityGroupQuotasForInterfaceInput{SecurityGroupIds: []string{sg}},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
	})

	t.Run("unknown security group id is not found", func(t *testing.T) {
		t.Parallel()

		_, err := client.ValidateSecurityGroupQuotasForInterface(
			t.Context(), &ec2sdk.ValidateSecurityGroupQuotasForInterfaceInput{
				SecurityGroupIds: []string{"sg-doesnotexist"},
			},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidGroup.NotFound", apiErr.ErrorCode())
	})

	t.Run("duplicate ids are rejected", func(t *testing.T) {
		t.Parallel()

		sg := createTestSecurityGroup(t, client, vpcID)

		_, err := client.ValidateSecurityGroupQuotasForInterface(
			t.Context(), &ec2sdk.ValidateSecurityGroupQuotasForInterfaceInput{SecurityGroupIds: []string{sg, sg}},
		)
		require.Error(t, err)
	})
}
