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

func TestDisassociateVpcEndpointResourceConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		unknown  bool
		network  bool
		wantUnbd bool
		wantErr  bool
	}{
		{name: "resource_endpoint", wantUnbd: true},
		{name: "unknown_endpoint", unknown: true, wantErr: true},
		{name: "service_network_endpoint", network: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestEC2Client(t, ec2.NewHandler(backend))
			vpcID := natTestVPC(t, client)

			in := &ec2sdk.CreateVpcEndpointInput{
				VpcId: aws.String(vpcID), VpcEndpointType: types.VpcEndpointTypeResource,
				ResourceConfigurationArn: aws.String(testResourceConfigArn),
			}
			if tt.network {
				in = &ec2sdk.CreateVpcEndpointInput{
					VpcId: aws.String(vpcID), VpcEndpointType: types.VpcEndpointTypeServiceNetwork,
					ServiceNetworkArn: aws.String(testServiceNetworkArn),
				}
			}

			out, err := client.CreateVpcEndpoint(t.Context(), in)
			require.NoError(t, err)

			id := aws.ToString(out.VpcEndpoint.VpcEndpointId)
			if tt.unknown {
				id = "vpce-missing"
			}

			err = backend.DisassociateVpcEndpointResourceConfiguration(id)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, backend.VpcEndpointsByResourceConfigurationArn(testResourceConfigArn))
		})
	}
}
