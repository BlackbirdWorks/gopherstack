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

const (
	testResourceConfigArn = "arn:aws:vpc-lattice:us-east-1:000000000000:resourceconfiguration/rcfg-0123456789abcdef0"
	testServiceNetworkArn = "arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-0123456789abcdef0"
)

func TestRealClient_VpcEndpointLatticeTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      func(vpcID string) *ec2sdk.CreateVpcEndpointInput
		name    string
		wantErr bool
	}{
		{
			name: "resource",
			in: func(v string) *ec2sdk.CreateVpcEndpointInput {
				return &ec2sdk.CreateVpcEndpointInput{
					VpcId: aws.String(v), VpcEndpointType: types.VpcEndpointTypeResource,
					ResourceConfigurationArn: aws.String(testResourceConfigArn),
				}
			},
		},
		{
			name: "service_network",
			in: func(v string) *ec2sdk.CreateVpcEndpointInput {
				return &ec2sdk.CreateVpcEndpointInput{
					VpcId: aws.String(v), VpcEndpointType: types.VpcEndpointTypeServiceNetwork,
					ServiceNetworkArn: aws.String(testServiceNetworkArn),
				}
			},
		},
		{
			name: "resource_missing_arn",
			in: func(v string) *ec2sdk.CreateVpcEndpointInput {
				return &ec2sdk.CreateVpcEndpointInput{
					VpcId:           aws.String(v),
					VpcEndpointType: types.VpcEndpointTypeResource,
				}
			},
			wantErr: true,
		},
		{
			name: "resource_bad_arn",
			in: func(v string) *ec2sdk.CreateVpcEndpointInput {
				return &ec2sdk.CreateVpcEndpointInput{
					VpcId: aws.String(v), VpcEndpointType: types.VpcEndpointTypeResource,
					ResourceConfigurationArn: aws.String(testServiceNetworkArn),
				}
			},
			wantErr: true,
		},
		{
			name: "service_network_with_service_name",
			in: func(v string) *ec2sdk.CreateVpcEndpointInput {
				return &ec2sdk.CreateVpcEndpointInput{
					VpcId: aws.String(v), VpcEndpointType: types.VpcEndpointTypeServiceNetwork,
					ServiceNetworkArn: aws.String(testServiceNetworkArn),
					ServiceName:       aws.String("com.amazonaws.us-east-1.s3"),
				}
			},
			wantErr: true,
		},
		{
			name: "interface_with_lattice_arn",
			in: func(v string) *ec2sdk.CreateVpcEndpointInput {
				return &ec2sdk.CreateVpcEndpointInput{
					VpcId: aws.String(v), ServiceName: aws.String("com.amazonaws.us-east-1.s3"),
					ServiceNetworkArn: aws.String(testServiceNetworkArn),
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestEC2Client(t, ec2.NewHandler(backend))
			vpcID := natTestVPC(t, client)

			out, err := client.CreateVpcEndpoint(t.Context(), tt.in(vpcID))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			id := aws.ToString(out.VpcEndpoint.VpcEndpointId)
			desc, err := client.DescribeVpcEndpoints(t.Context(), &ec2sdk.DescribeVpcEndpointsInput{
				VpcEndpointIds: []string{id},
			})
			require.NoError(t, err)
			require.Len(t, desc.VpcEndpoints, 1)

			got := desc.VpcEndpoints[0]
			assert.Equal(t, out.VpcEndpoint.VpcEndpointType, got.VpcEndpointType)
			assert.Empty(t, aws.ToString(got.ServiceName))

			if tt.name == "resource" {
				assert.Equal(t, testResourceConfigArn, aws.ToString(got.ResourceConfigurationArn))
				require.Len(t, backend.VpcEndpointsByResourceConfigurationArn(testResourceConfigArn), 1)
				assert.Empty(t, backend.VpcEndpointsByServiceNetworkArn(testResourceConfigArn))
			} else {
				assert.Equal(t, testServiceNetworkArn, aws.ToString(got.ServiceNetworkArn))
				eps := backend.VpcEndpointsByServiceNetworkArn(testServiceNetworkArn)
				require.Len(t, eps, 1)
				assert.Equal(t, id, eps[0].ID)
			}
		})
	}
}
