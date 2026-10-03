package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ApplicationStatusCheckHealthCheckPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		paths   []types.HealthCheckPathRequestObject
	}{
		{"none", "", nil},
		{"subnet_to_subnet", "", []types.HealthCheckPathRequestObject{{
			Source:       &types.HealthCheckPathSourceRequestObject{SubnetId: aws.String("subnet-default")},
			Destinations: []types.HealthCheckPathDestinationRequestObject{{SubnetId: aws.String("subnet-default")}},
		}}},
		{"unknown_subnet", "InvalidSubnetID.NotFound", []types.HealthCheckPathRequestObject{{
			Source:       &types.HealthCheckPathSourceRequestObject{SubnetId: aws.String("subnet-missing")},
			Destinations: []types.HealthCheckPathDestinationRequestObject{{SubnetId: aws.String("subnet-default")}},
		}}},
		{"no_destination", "InvalidParameterValue", []types.HealthCheckPathRequestObject{{
			Source: &types.HealthCheckPathSourceRequestObject{SubnetId: aws.String("subnet-default")},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMiscClient(t)

			created, err := client.CreateApplicationStatusCheck(t.Context(), &ec2sdk.CreateApplicationStatusCheckInput{
				Protocol:         types.NetworkProtocolEnumHttp,
				Port:             aws.Int32(80),
				HealthCheckPaths: tt.paths,
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, created.ApplicationStatusCheck.HealthCheckPaths, len(tt.paths))

			desc, err := client.DescribeApplicationStatusChecks(
				t.Context(),
				&ec2sdk.DescribeApplicationStatusChecksInput{
					ApplicationStatusCheckIds: []string{
						aws.ToString(created.ApplicationStatusCheck.ApplicationStatusCheckId),
					},
				},
			)
			require.NoError(t, err)
			require.Len(t, desc.ApplicationStatusChecks, 1)

			got := desc.ApplicationStatusChecks[0].HealthCheckPaths
			require.Len(t, got, len(tt.paths))

			for i, want := range tt.paths {
				assert.Equal(t, aws.ToString(want.Source.SubnetId), aws.ToString(got[i].Source.SubnetId))
				require.Len(t, got[i].Destinations, len(want.Destinations))
				assert.Equal(
					t,
					aws.ToString(want.Destinations[0].SubnetId),
					aws.ToString(got[i].Destinations[0].SubnetId),
				)
			}
		})
	}
}
