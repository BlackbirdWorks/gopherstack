package ec2_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestRealClient_AvailabilityZoneIDs(t *testing.T) {
	t.Parallel()

	b, client := newMiscClient(t)

	vpc, err := b.CreateVpc("10.50.0.0/16", "")
	require.NoError(t, err)
	subnetA, err := b.CreateSubnet(vpc.ID, "10.50.1.0/24", "us-east-1a")
	require.NoError(t, err)
	subnetB, err := b.CreateSubnet(vpc.ID, "10.50.2.0/24", "us-east-1b")
	require.NoError(t, err)

	t.Run("describe availability zones", func(t *testing.T) {
		t.Parallel()

		runTailCases(t, []tailCase{
			{"id", tailFilter("zone-id", "use1-az2"), []string{"us-east-1b"}},
			{"miss", tailFilter("zone-id", "use1-az9"), nil},
		}, func(ctx context.Context, f []types.Filter) ([]types.AvailabilityZone, error) {
			out, callErr := client.DescribeAvailabilityZones(ctx, &ec2sdk.DescribeAvailabilityZonesInput{Filters: f})
			if callErr != nil {
				return nil, callErr
			}

			return out.AvailabilityZones, nil
		}, func(z types.AvailabilityZone) string { return aws.ToString(z.ZoneName) })
	})

	t.Run("describe instance status", func(t *testing.T) {
		t.Parallel()

		insts, runErr := b.RunInstances("ami-12345678", "t3.micro", subnetB.ID, 1)
		require.NoError(t, runErr)

		id := insts[0].ID

		runTailCases(t, []tailCase{
			{"id", tailFilter("availability-zone-id", "use1-az2"), []string{id}},
			{"miss", tailFilter("availability-zone-id", "use1-az1"), nil},
		}, func(ctx context.Context, f []types.Filter) ([]types.InstanceStatus, error) {
			out, callErr := client.DescribeInstanceStatus(ctx, &ec2sdk.DescribeInstanceStatusInput{
				Filters: f, InstanceIds: []string{id}, IncludeAllInstances: aws.Bool(true),
			})
			if callErr != nil {
				return nil, callErr
			}

			return out.InstanceStatuses, nil
		}, func(s types.InstanceStatus) string { return aws.ToString(s.InstanceId) })
	})

	t.Run("describe subnets", func(t *testing.T) {
		t.Parallel()

		runTailCases(t, []tailCase{
			{"id", tailFilter("availability-zone-id", "use1-az1", "use1-az9"), []string{subnetA.ID}},
			{"miss", tailFilter("availability-zone-id", "use1-az9"), nil},
		}, func(ctx context.Context, f []types.Filter) ([]types.Subnet, error) {
			f = append(f, types.Filter{Name: aws.String("vpc-id"), Values: []string{vpc.ID}})
			out, callErr := client.DescribeSubnets(ctx, &ec2sdk.DescribeSubnetsInput{Filters: f})
			if callErr != nil {
				return nil, callErr
			}

			return out.Subnets, nil
		}, func(s types.Subnet) string { return aws.ToString(s.SubnetId) })

		out, descErr := client.DescribeSubnets(
			t.Context(),
			&ec2sdk.DescribeSubnetsInput{SubnetIds: []string{subnetB.ID}},
		)
		require.NoError(t, descErr)
		require.Len(t, out.Subnets, 1)
		assert.Equal(t, "use1-az2", aws.ToString(out.Subnets[0].AvailabilityZoneId))
	})
}

func TestAvailabilityZoneIDs_ZoneNamesAcrossRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region string
		want   string
	}{
		{"eu-central-1", "euc1-az1"},
		{"ap-southeast-2", "apse2-az1"},
		{"us-gov-west-1", "usgw1-az1"},
		{"sa-east-1", "sae1-az1"},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			t.Parallel()

			client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend(tailAcct, tt.region)))

			out, err := client.DescribeAvailabilityZones(t.Context(), &ec2sdk.DescribeAvailabilityZonesInput{})
			require.NoError(t, err)
			require.NotEmpty(t, out.AvailabilityZones)
			assert.Equal(t, tt.want, aws.ToString(out.AvailabilityZones[0].ZoneId))
		})
	}
}

func TestRealClient_DescribeRegionsOptInStatus(t *testing.T) {
	t.Parallel()

	_, client := newMiscClient(t)

	tests := []struct {
		name   string
		status string
		region string
		other  string
	}{
		{name: "opted in", status: "opted-in", region: "af-south-1", other: "us-east-1"},
		{name: "not required", status: "opt-in-not-required", region: "us-east-1", other: "af-south-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.DescribeRegions(t.Context(), &ec2sdk.DescribeRegionsInput{
				Filters: tailFilter("opt-in-status", tt.status),
			})
			require.NoError(t, err)

			names := make([]string, 0, len(out.Regions))
			for _, r := range out.Regions {
				assert.Equal(t, tt.status, aws.ToString(r.OptInStatus))
				names = append(names, aws.ToString(r.RegionName))
			}

			assert.Contains(t, names, tt.region)
			assert.NotContains(t, names, tt.other)
		})
	}
}
