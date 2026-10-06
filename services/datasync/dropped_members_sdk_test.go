package datasync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAgent_SDKPrivateLinkConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		in           datasyncsdk.CreateAgentInput
		wantEndpoint types.EndpointType
		wantErr      bool
		wantLink     bool
	}{
		{
			name:         "public",
			in:           datasyncsdk.CreateAgentInput{ActivationKey: aws.String("k")},
			wantEndpoint: types.EndpointTypePublic,
		},
		{
			name: "vpc_endpoint",
			in: datasyncsdk.CreateAgentInput{
				ActivationKey:     aws.String("k"),
				VpcEndpointId:     aws.String("vpce-01234d5aff67890e1"),
				SubnetArns:        []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"},
				SecurityGroupArns: []string{"arn:aws:ec2:us-east-1:123456789012:security-group/sg-1"},
			},
			wantEndpoint: types.EndpointTypePrivateLink,
			wantLink:     true,
		},
		{
			name: "two_subnets_rejected",
			in: datasyncsdk.CreateAgentInput{
				ActivationKey: aws.String("k"),
				SubnetArns:    []string{"a", "b"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			out, err := client.CreateAgent(ctx, &tt.in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			desc, err := client.DescribeAgent(ctx, &datasyncsdk.DescribeAgentInput{AgentArn: out.AgentArn})
			require.NoError(t, err)
			assert.Equal(t, tt.wantEndpoint, desc.EndpointType)

			if !tt.wantLink {
				assert.Nil(t, desc.PrivateLinkConfig)

				return
			}

			require.NotNil(t, desc.PrivateLinkConfig)
			assert.Equal(t, tt.in.VpcEndpointId, desc.PrivateLinkConfig.VpcEndpointId)
			assert.Equal(t, tt.in.SubnetArns, desc.PrivateLinkConfig.SubnetArns)
			assert.Equal(t, tt.in.SecurityGroupArns, desc.PrivateLinkConfig.SecurityGroupArns)
		})
	}
}

func TestObjectStorage_SDKServerCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		create []byte
		update []byte
		want   []byte
	}{
		{name: "create_only", create: []byte("cert-1"), want: []byte("cert-1")},
		{name: "update_replaces", create: []byte("cert-1"), update: []byte("cert-2"), want: []byte("cert-2")},
		{name: "none", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			created, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
				ServerHostname:    aws.String("host.example.com"),
				BucketName:        aws.String("bkt"),
				AgentArns:         nil,
				ServerCertificate: tt.create,
			})
			require.NoError(t, err)

			if tt.update != nil {
				_, err = client.UpdateLocationObjectStorage(ctx, &datasyncsdk.UpdateLocationObjectStorageInput{
					LocationArn:       created.LocationArn,
					ServerCertificate: tt.update,
				})
				require.NoError(t, err)
			}

			desc, err := client.DescribeLocationObjectStorage(ctx, &datasyncsdk.DescribeLocationObjectStorageInput{
				LocationArn: created.LocationArn,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, desc.ServerCertificate)
		})
	}
}
