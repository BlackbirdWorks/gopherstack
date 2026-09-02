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

// TestWireFieldFixesSweep28 covers three pre-fix wire-shape bugs, each a
// double-wrapped or misnamed list element that a real client's deserializer
// (ec2@v1.319.1 deserializers.go) could never decode, so a real client
// always saw the field as empty regardless of what the backend returned.
func TestWireFieldFixesSweep28(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *ec2.InMemoryBackend, client *ec2sdk.Client)
		name string
	}{
		{
			// handleDescribeInstanceTopology never wired the backend's
			// per-instance NetworkNodes into the response, and the response
			// struct double-wrapped each string in an extra <item> element
			// instead of the flat <item>value</item>
			// awsEc2query_deserializeDocumentNetworkNodesList expects
			// (deserializers.go:139114).
			name: "instance topology network nodes",
			run: func(t *testing.T, b *ec2.InMemoryBackend, client *ec2sdk.Client) {
				t.Helper()

				insts, err := b.RunInstances("ami-sweep28", "t3.micro", "", 1)
				require.NoError(t, err)
				require.Len(t, insts, 1)
				instanceID := insts[0].ID

				out, err := client.DescribeInstanceTopology(t.Context(), &ec2sdk.DescribeInstanceTopologyInput{
					InstanceIds: []string{instanceID},
				})
				require.NoError(t, err)
				require.Len(t, out.Instances, 1)

				topo := out.Instances[0]
				require.NotNil(t, topo.InstanceId)
				assert.Equal(t, instanceID, *topo.InstanceId)
				assert.NotEmpty(
					t,
					topo.NetworkNodes,
					"NetworkNodes empty - pre-fix never wired and double-<item> wrapped",
				)
			},
		},
		{
			// handleAssignIpv6Addresses/handleUnassignIpv6Addresses
			// double-wrapped each plain string in an extra <item> element
			// instead of the flat <item>value</item> shape
			// awsEc2query_deserializeDocumentIpv6AddressList expects
			// (deserializers.go:139114).
			name: "assign and unassign ipv6 addresses",
			run: func(t *testing.T, b *ec2.InMemoryBackend, client *ec2sdk.Client) {
				t.Helper()

				vpc, err := b.CreateVpc("10.0.0.0/16", "default")
				require.NoError(t, err)
				subnet, err := b.CreateSubnet(vpc.ID, "10.0.0.0/24", "us-east-1a")
				require.NoError(t, err)
				eni, err := b.CreateNetworkInterface(subnet.ID, "sweep28-eni")
				require.NoError(t, err)

				assignOut, err := client.AssignIpv6Addresses(t.Context(), &ec2sdk.AssignIpv6AddressesInput{
					NetworkInterfaceId: &eni.ID,
					Ipv6AddressCount:   aws.Int32(2),
				})
				require.NoError(t, err)
				require.Len(
					t, assignOut.AssignedIpv6Addresses, 2,
					"AssignedIpv6Addresses empty - pre-fix double-<item> wrapped",
				)

				unassignOut, err := client.UnassignIpv6Addresses(t.Context(), &ec2sdk.UnassignIpv6AddressesInput{
					NetworkInterfaceId: &eni.ID,
					Ipv6Addresses:      assignOut.AssignedIpv6Addresses,
				})
				require.NoError(t, err)
				assert.ElementsMatch(
					t, assignOut.AssignedIpv6Addresses, unassignOut.UnassignedIpv6Addresses,
					"UnassignedIpv6Addresses empty - pre-fix double-<item> wrapped",
				)
			},
		},
		{
			// handleRunScheduledInstances wrapped each plain instance-ID
			// string in a named <instanceId> child element instead of the
			// flat <item>value</item> shape
			// awsEc2query_deserializeDocumentInstanceIdSet expects
			// (deserializers.go:112721).
			name: "run scheduled instances instance id set",
			run: func(t *testing.T, b *ec2.InMemoryBackend, client *ec2sdk.Client) {
				t.Helper()

				catalog := b.DescribeScheduledInstanceAvailability(nil, 0, 0)
				require.NotEmpty(t, catalog)
				token := catalog[0].PurchaseToken

				purchased, err := b.PurchaseScheduledInstances(
					[]ec2.ScheduledInstancePurchaseRequest{{PurchaseToken: token, InstanceCount: 1}},
				)
				require.NoError(t, err)
				require.Len(t, purchased, 1)
				scheduledInstanceID := purchased[0].ScheduledInstanceID

				out, err := client.RunScheduledInstances(t.Context(), &ec2sdk.RunScheduledInstancesInput{
					ScheduledInstanceId: &scheduledInstanceID,
					InstanceCount:       aws.Int32(1),
					LaunchSpecification: &types.ScheduledInstancesLaunchSpecification{
						ImageId: aws.String("ami-sweep28"),
					},
				})
				require.NoError(t, err)
				assert.Len(
					t,
					out.InstanceIdSet,
					1,
					"InstanceIdSet empty - pre-fix wrapped each id in a named child element",
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			h := ec2.NewHandler(b)
			client := newTestEC2Client(t, h)

			tc.run(t, b, client)
		})
	}
}
