package ec2_test

// gopherstack-j2v5: DescribeDhcpOptions, DescribeEgressOnlyInternetGateways,
// DescribePrefixLists, DescribeManagedPrefixLists, DescribePublicIpv4Pools,
// DescribeBundleTasks, DescribeCarrierGateways, and DescribeFlowLogs declared
// Filters on the wire but no handler code ever read them, so a real client's
// filter was silently ignored and every item came back. DescribeNetworkAcls
// applied only vpc-id of its documented filter set. DescribeInstanceStatus
// never read IncludeAllInstances, so it always returned every instance
// instead of defaulting to running-only. Each test below asserts on the
// decoded response set, not just err == nil, and was confirmed to fail
// against the unmodified handlers (every item came back instead of just the
// filtered one). DescribeInstanceTypes is deliberately not covered here: see
// PARITY.md -- this backend has no instance-type attribute catalog to filter
// against, so its documented filter names can't be honestly implemented.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func filterOf(name string, values ...string) types.Filter {
	return types.Filter{Name: aws.String(name), Values: values}
}

func TestDescribeDhcpOptions_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	want, err := b.CreateDhcpOptions(
		[]ec2.DhcpConfiguration{{Key: "domain-name", Values: []string{"example.com"}}}, nil,
	)
	require.NoError(t, err)
	_, err = b.CreateDhcpOptions(
		[]ec2.DhcpConfiguration{{Key: "netbios-name-servers", Values: []string{"10.0.0.2"}}}, nil,
	)
	require.NoError(t, err)

	out, err := client.DescribeDhcpOptions(t.Context(), &ec2sdk.DescribeDhcpOptionsInput{
		Filters: []types.Filter{filterOf("key", "domain-name")},
	})
	require.NoError(t, err)
	require.Len(t, out.DhcpOptions, 1)
	assert.Equal(t, want.DhcpOptionsID, aws.ToString(out.DhcpOptions[0].DhcpOptionsId))
}

func TestDescribeEgressOnlyInternetGateways_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc1, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)
	vpc2, err := b.CreateVpc("10.1.0.0/16", "default")
	require.NoError(t, err)

	want, err := b.CreateEgressOnlyInternetGateway(vpc1.ID)
	require.NoError(t, err)
	_, err = b.CreateEgressOnlyInternetGateway(vpc2.ID)
	require.NoError(t, err)

	require.NoError(t, b.CreateTags([]string{want.ID}, map[string]string{"Name": "keep"}))

	out, err := client.DescribeEgressOnlyInternetGateways(t.Context(), &ec2sdk.DescribeEgressOnlyInternetGatewaysInput{
		Filters: []types.Filter{filterOf("tag:Name", "keep")},
	})
	require.NoError(t, err)
	require.Len(t, out.EgressOnlyInternetGateways, 1)
	assert.Equal(t, want.ID, aws.ToString(out.EgressOnlyInternetGateways[0].EgressOnlyInternetGatewayId))
}

func TestDescribePrefixLists_Filters_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.DescribePrefixLists(t.Context(), &ec2sdk.DescribePrefixListsInput{
		Filters: []types.Filter{filterOf("prefix-list-name", "com.amazonaws.us-east-1.s3")},
	})
	require.NoError(t, err)
	require.Len(t, out.PrefixLists, 1)
	assert.Equal(t, "com.amazonaws.us-east-1.s3", aws.ToString(out.PrefixLists[0].PrefixListName))
}

func TestDescribeManagedPrefixLists_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	want, err := b.CreateManagedPrefixList("allow-list", "IPv4", 10)
	require.NoError(t, err)
	_, err = b.CreateManagedPrefixList("deny-list", "IPv4", 10)
	require.NoError(t, err)

	out, err := client.DescribeManagedPrefixLists(t.Context(), &ec2sdk.DescribeManagedPrefixListsInput{
		Filters: []types.Filter{filterOf("prefix-list-name", "allow-list")},
	})
	require.NoError(t, err)
	require.Len(t, out.PrefixLists, 1)
	assert.Equal(t, want.PrefixListID, aws.ToString(out.PrefixLists[0].PrefixListId))
}

func TestDescribePublicIpv4Pools_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	want := b.CreatePublicIpv4Pool("us-east-1", map[string]string{"Name": "keep"})
	b.CreatePublicIpv4Pool("us-east-1", nil)

	out, err := client.DescribePublicIpv4Pools(t.Context(), &ec2sdk.DescribePublicIpv4PoolsInput{
		Filters: []types.Filter{filterOf("tag:Name", "keep")},
	})
	require.NoError(t, err)
	require.Len(t, out.PublicIpv4Pools, 1)
	assert.Equal(t, want.PoolID, aws.ToString(out.PublicIpv4Pools[0].PoolId))
}

func TestDescribeBundleTasks_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	insts, err := b.RunInstances("ami-parity-test", "t3.micro", "", 2)
	require.NoError(t, err)

	want, err := b.BundleInstance(insts[0].ID, "my-bucket", "prefix")
	require.NoError(t, err)
	_, err = b.BundleInstance(insts[1].ID, "my-bucket", "prefix")
	require.NoError(t, err)

	out, err := client.DescribeBundleTasks(t.Context(), &ec2sdk.DescribeBundleTasksInput{
		Filters: []types.Filter{filterOf("instance-id", insts[0].ID)},
	})
	require.NoError(t, err)
	require.Len(t, out.BundleTasks, 1)
	assert.Equal(t, want.BundleID, aws.ToString(out.BundleTasks[0].BundleId))
}

func TestDescribeCarrierGateways_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc1, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)
	vpc2, err := b.CreateVpc("10.1.0.0/16", "default")
	require.NoError(t, err)

	want, err := b.CreateCarrierGateway(vpc1.ID)
	require.NoError(t, err)
	_, err = b.CreateCarrierGateway(vpc2.ID)
	require.NoError(t, err)

	out, err := client.DescribeCarrierGateways(t.Context(), &ec2sdk.DescribeCarrierGatewaysInput{
		Filters: []types.Filter{filterOf("vpc-id", vpc1.ID)},
	})
	require.NoError(t, err)
	require.Len(t, out.CarrierGateways, 1)
	assert.Equal(t, want.CarrierGatewayID, aws.ToString(out.CarrierGateways[0].CarrierGatewayId))
}

func TestDescribeFlowLogs_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	want, err := b.CreateFlowLogs([]string{vpc.ID}, "ACCEPT", "cloud-watch-logs", "log-group", nil)
	require.NoError(t, err)
	require.Len(t, want, 1)
	_, err = b.CreateFlowLogs([]string{vpc.ID}, "REJECT", "cloud-watch-logs", "log-group", nil)
	require.NoError(t, err)

	out, err := client.DescribeFlowLogs(t.Context(), &ec2sdk.DescribeFlowLogsInput{
		Filter: []types.Filter{filterOf("traffic-type", "ACCEPT")},
	})
	require.NoError(t, err)
	require.Len(t, out.FlowLogs, 1)
	assert.Equal(t, want[0].FlowLogID, aws.ToString(out.FlowLogs[0].FlowLogId))
}

func TestDescribeNetworkAcls_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	want, err := b.CreateNetworkACL(vpc.ID)
	require.NoError(t, err)
	_, err = b.CreateNetworkACL(vpc.ID)
	require.NoError(t, err)

	require.NoError(t, b.CreateNetworkACLEntry(want.ID, 100, "6", "allow", "192.168.0.0/24", false, 80, 80))

	out, err := client.DescribeNetworkAcls(t.Context(), &ec2sdk.DescribeNetworkAclsInput{
		Filters: []types.Filter{filterOf("entry.cidr", "192.168.0.0/24")},
	})
	require.NoError(t, err)
	require.Len(t, out.NetworkAcls, 1)
	assert.Equal(t, want.ID, aws.ToString(out.NetworkAcls[0].NetworkAclId))
}

func TestDescribeInstanceStatus_IncludeAllInstances_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	insts, err := b.RunInstances("ami-parity-test", "t3.micro", "", 2)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending -> running for insts[0]

	_, err = b.StopInstances([]string{insts[1].ID})
	require.NoError(t, err)

	t.Run("default_excludes_non_running", func(t *testing.T) {
		t.Parallel()

		out, statusErr := client.DescribeInstanceStatus(t.Context(), &ec2sdk.DescribeInstanceStatusInput{})
		require.NoError(t, statusErr)

		gotIDs := make(map[string]bool, len(out.InstanceStatuses))
		for _, s := range out.InstanceStatuses {
			gotIDs[aws.ToString(s.InstanceId)] = true
		}
		assert.True(t, gotIDs[insts[0].ID])
		assert.False(t, gotIDs[insts[1].ID])
	})

	t.Run("include_all_instances_returns_stopped_too", func(t *testing.T) {
		t.Parallel()

		out, statusErr := client.DescribeInstanceStatus(t.Context(), &ec2sdk.DescribeInstanceStatusInput{
			IncludeAllInstances: aws.Bool(true),
		})
		require.NoError(t, statusErr)

		gotIDs := make(map[string]bool, len(out.InstanceStatuses))
		for _, s := range out.InstanceStatuses {
			gotIDs[aws.ToString(s.InstanceId)] = true
		}
		assert.True(t, gotIDs[insts[0].ID])
		assert.True(t, gotIDs[insts[1].ID])
	})
}
