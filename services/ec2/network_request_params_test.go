package ec2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_CreateDefaultSubnetIpv6Native(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		az         string
		wantErr    string
		ipv6Native bool
	}{
		{"ipv4_default", "us-east-1b", "", false},
		{"ipv6_native", "us-east-1c", "", true},
		{"az_already_has_default", "us-east-1a", "DefaultSubnetAlreadyExistsInAvailabilityZone", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMiscClient(t)

			out, err := client.CreateDefaultSubnet(t.Context(), &ec2sdk.CreateDefaultSubnetInput{
				AvailabilityZone: aws.String(tt.az),
				Ipv6Native:       aws.Bool(tt.ipv6Native),
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ipv6Native, aws.ToBool(out.Subnet.Ipv6Native))

			desc, err := client.DescribeSubnets(t.Context(), &ec2sdk.DescribeSubnetsInput{
				SubnetIds: []string{aws.ToString(out.Subnet.SubnetId)},
			})
			require.NoError(t, err)
			require.Len(t, desc.Subnets, 1)
			assert.Equal(t, tt.ipv6Native, aws.ToBool(desc.Subnets[0].Ipv6Native))
			assert.Equal(t, tt.ipv6Native, aws.ToString(desc.Subnets[0].CidrBlock) == "")
		})
	}
}

func fleetInput(validFrom *time.Time) *ec2sdk.CreateFleetInput {
	return &ec2sdk.CreateFleetInput{
		Type:      types.FleetTypeMaintain,
		ValidFrom: validFrom,
		TargetCapacitySpecification: &types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity: aws.Int32(2),
		},
		LaunchTemplateConfigs: []types.FleetLaunchTemplateConfigRequest{{
			LaunchTemplateSpecification: &types.FleetLaunchTemplateSpecificationRequest{
				LaunchTemplateId: aws.String("lt-validfrom0001"),
			},
		}},
	}
}

func TestRealClient_CreateFleetValidFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantState     types.FleetStateCode
		offset        time.Duration
		wantInstances int
		wantActivated bool
	}{
		{"past_starts_immediately", types.FleetStateCodeActive, -time.Hour, 2, false},
		{"future_is_submitted", types.FleetStateCodeSubmitted, time.Hour, 0, false},
		{"becomes_active_after_valid_from", types.FleetStateCodeSubmitted, 700 * time.Millisecond, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMiscClient(t)

			validFrom := time.Now().Add(tt.offset).UTC().Truncate(time.Millisecond)
			created, err := client.CreateFleet(t.Context(), fleetInput(&validFrom))
			require.NoError(t, err)

			fleetID := aws.ToString(created.FleetId)

			describe := func() types.FleetData {
				out, descErr := client.DescribeFleets(
					t.Context(),
					&ec2sdk.DescribeFleetsInput{FleetIds: []string{fleetID}},
				)
				require.NoError(t, descErr)
				require.Len(t, out.Fleets, 1)

				return out.Fleets[0]
			}

			fleet := describe()
			assert.Equal(t, tt.wantState, fleet.FleetState)
			require.NotNil(t, fleet.ValidFrom)
			assert.WithinDuration(t, validFrom, *fleet.ValidFrom, time.Second)

			active, err := client.DescribeFleetInstances(t.Context(), &ec2sdk.DescribeFleetInstancesInput{
				FleetId: aws.String(fleetID),
			})
			require.NoError(t, err)
			assert.Len(t, active.ActiveInstances, tt.wantInstances)

			if !tt.wantActivated {
				return
			}

			require.Eventually(t, func() bool {
				return describe().FleetState == types.FleetStateCodeActive
			}, 5*time.Second, 50*time.Millisecond)

			active, err = client.DescribeFleetInstances(t.Context(), &ec2sdk.DescribeFleetInstancesInput{
				FleetId: aws.String(fleetID),
			})
			require.NoError(t, err)
			assert.Len(t, active.ActiveInstances, 2)
		})
	}
}

type natFixture struct {
	client   *ec2sdk.Client
	natID    string
	assocID  string
	secondIP string
}

func newNatFixture(t *testing.T) natFixture {
	t.Helper()

	_, client := newMiscClient(t)
	ctx := t.Context()

	vpc, err := client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(ctx, &ec2sdk.CreateSubnetInput{
		VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.0.1.0/24"),
	})
	require.NoError(t, err)
	primary, err := client.AllocateAddress(ctx, &ec2sdk.AllocateAddressInput{Domain: types.DomainTypeVpc})
	require.NoError(t, err)
	extra, err := client.AllocateAddress(ctx, &ec2sdk.AllocateAddressInput{Domain: types.DomainTypeVpc})
	require.NoError(t, err)

	nat, err := client.CreateNatGateway(ctx, &ec2sdk.CreateNatGatewayInput{
		SubnetId: subnet.Subnet.SubnetId, AllocationId: primary.AllocationId,
	})
	require.NoError(t, err)

	natID := aws.ToString(nat.NatGateway.NatGatewayId)

	assoc, err := client.AssociateNatGatewayAddress(ctx, &ec2sdk.AssociateNatGatewayAddressInput{
		NatGatewayId: aws.String(natID), AllocationIds: []string{aws.ToString(extra.AllocationId)},
	})
	require.NoError(t, err)

	assigned, err := client.AssignPrivateNatGatewayAddress(ctx, &ec2sdk.AssignPrivateNatGatewayAddressInput{
		NatGatewayId: aws.String(natID), PrivateIpAddressCount: aws.Int32(1),
	})
	require.NoError(t, err)

	f := natFixture{client: client, natID: natID}

	for _, a := range assoc.NatGatewayAddresses {
		if !aws.ToBool(a.IsPrimary) {
			f.assocID = aws.ToString(a.AssociationId)
		}
	}

	for _, a := range assigned.NatGatewayAddresses {
		if !aws.ToBool(a.IsPrimary) && a.AssociationId == nil {
			f.secondIP = aws.ToString(a.PrivateIp)
		}
	}

	require.NotEmpty(t, f.assocID)
	require.NotEmpty(t, f.secondIP)

	return f
}

func (f natFixture) addresses(t *testing.T) []types.NatGatewayAddress {
	t.Helper()

	out, err := f.client.DescribeNatGateways(t.Context(), &ec2sdk.DescribeNatGatewaysInput{
		NatGatewayIds: []string{f.natID},
	})
	require.NoError(t, err)
	require.Len(t, out.NatGateways, 1)

	return out.NatGateways[0].NatGatewayAddresses
}

func TestRealClient_NatGatewayMaxDrainDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		drain        *int32
		wantErr      string
		wantDraining bool
		wantGone     bool
	}{
		{"immediate_when_absent", nil, "", false, true},
		{"draining_for_an_hour", aws.Int32(3600), "", true, false},
		{"released_after_drain", aws.Int32(1), "", true, true},
		{"negative_rejected", aws.Int32(-5), "InvalidParameterValue", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newNatFixture(t)
			ctx := t.Context()

			_, disassocErr := f.client.DisassociateNatGatewayAddress(ctx, &ec2sdk.DisassociateNatGatewayAddressInput{
				NatGatewayId: aws.String(
					f.natID,
				),
				AssociationIds:          []string{f.assocID},
				MaxDrainDurationSeconds: tt.drain,
			})
			_, unassignErr := f.client.UnassignPrivateNatGatewayAddress(
				ctx,
				&ec2sdk.UnassignPrivateNatGatewayAddressInput{
					NatGatewayId: aws.String(
						f.natID,
					),
					PrivateIpAddresses:      []string{f.secondIP},
					MaxDrainDurationSeconds: tt.drain,
				},
			)

			if tt.wantErr != "" {
				require.ErrorContains(t, disassocErr, tt.wantErr)
				require.ErrorContains(t, unassignErr, tt.wantErr)

				return
			}

			require.NoError(t, disassocErr)
			require.NoError(t, unassignErr)

			statuses := func() map[string]types.NatGatewayAddressStatus {
				m := map[string]types.NatGatewayAddressStatus{}
				for _, a := range f.addresses(t) {
					m[aws.ToString(a.PrivateIp)] = a.Status
				}

				return m
			}

			if tt.wantDraining {
				got := statuses()
				assert.Len(t, got, 3)
				assert.Equal(t, types.NatGatewayAddressStatusUnassigning, got[f.secondIP])
				assert.Contains(t, statusSet(f.addresses(t)), types.NatGatewayAddressStatusDisassociating)
			}

			if tt.wantGone {
				require.Eventually(t, func() bool { return len(statuses()) == 1 }, 5*time.Second, 100*time.Millisecond)
			}
		})
	}
}

func statusSet(addrs []types.NatGatewayAddress) []types.NatGatewayAddressStatus {
	out := make([]types.NatGatewayAddressStatus, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.Status)
	}

	return out
}
