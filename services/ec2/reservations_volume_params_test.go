package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunInstances_ReservationGrouping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		launches []int32
	}{
		{name: "single launch", launches: []int32{3}},
		{name: "two launches", launches: []int32{2, 1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)
			ctx := t.Context()

			launched := map[string]int{}

			for _, n := range tc.launches {
				out, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
					ImageId: aws.String("ami-12345678"), InstanceType: types.InstanceTypeT3Micro,
					MinCount: aws.Int32(n), MaxCount: aws.Int32(n),
				})
				require.NoError(t, err)
				require.Len(t, out.Instances, int(n))
				launched[aws.ToString(out.ReservationId)] = int(n)

				desc, err := client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{
					Filters: []types.Filter{{
						Name: aws.String("reservation-id"), Values: []string{aws.ToString(out.ReservationId)},
					}},
				})
				require.NoError(t, err)
				require.Len(t, desc.Reservations, 1)
				assert.Len(t, desc.Reservations[0].Instances, int(n))
			}

			assert.Len(t, launched, len(tc.launches))

			all, err := client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{})
			require.NoError(t, err)

			got := map[string]int{}
			for _, r := range all.Reservations {
				got[aws.ToString(r.ReservationId)] = len(r.Instances)
			}

			assert.Equal(t, launched, got)
		})
	}
}

func TestCreateVolume_RequiresSizeOrSnapshot(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input   *ec2sdk.CreateVolumeInput
		name    string
		wantErr bool
	}{
		{input: &ec2sdk.CreateVolumeInput{AvailabilityZone: aws.String("us-east-1a")}, name: "neither", wantErr: true},
		{
			input: &ec2sdk.CreateVolumeInput{AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8)},
			name:  "size",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)

			_, err := client.CreateVolume(t.Context(), tc.input)
			if tc.wantErr {
				require.ErrorContains(t, err, "MissingParameter")

				return
			}

			require.NoError(t, err)
		})
	}
}
