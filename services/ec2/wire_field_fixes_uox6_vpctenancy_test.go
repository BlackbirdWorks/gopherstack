package ec2_test

// uox6-vpctenancy: CreateVpc declares InstanceTenancy ("Default: default",
// ec2@v1.319.1 api_op_CreateVpc.go), but the handler never read it, and the
// resulting VPC's tenancy was invisible everywhere -- ModifyVpcTenancy
// already stored a tenancy per VPC, but nothing ever rendered it, so even a
// caller that used ModifyVpcTenancy couldn't observe the result. Both are
// fixed together: DescribeVpcs/CreateVpc now render instanceTenancy, and
// CreateVpc defaults it to "default" when the caller omits it.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateVpc_InstanceTenancy_DefaultsToDefault(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{
		CidrBlock: aws.String("10.0.0.0/16"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Vpc)
	assert.Equal(t, types.TenancyDefault, out.Vpc.InstanceTenancy)

	describeOut, err := client.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{
		VpcIds: []string{aws.ToString(out.Vpc.VpcId)},
	})
	require.NoError(t, err)
	require.Len(t, describeOut.Vpcs, 1)
	assert.Equal(t, types.TenancyDefault, describeOut.Vpcs[0].InstanceTenancy)
}

func TestCreateVpc_InstanceTenancy_Dedicated(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{
		CidrBlock:       aws.String("10.0.0.0/16"),
		InstanceTenancy: types.TenancyDedicated,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Vpc)
	assert.Equal(t, types.TenancyDedicated, out.Vpc.InstanceTenancy)

	describeOut, err := client.DescribeVpcs(t.Context(), &ec2sdk.DescribeVpcsInput{
		VpcIds: []string{aws.ToString(out.Vpc.VpcId)},
	})
	require.NoError(t, err)
	require.Len(t, describeOut.Vpcs, 1)
	assert.Equal(t, types.TenancyDedicated, describeOut.Vpcs[0].InstanceTenancy)
}
