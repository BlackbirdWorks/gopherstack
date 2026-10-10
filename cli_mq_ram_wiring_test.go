package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/mq"
	mqtypes "github.com/aws/aws-sdk-go-v2/service/mq/types"
	"github.com/aws/aws-sdk-go-v2/service/ram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMQSharedResourcesRAMWiring(t *testing.T) {
	t.Parallel()

	const subnetARN = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-0abc1234"

	tests := []struct {
		name        string
		wantErrCode mqtypes.SharedResourceErrorCode
		wantTypes   []mqtypes.SharedResourceType
		makeShare   bool
	}{
		{
			name: "existing_share", makeShare: true,
			wantTypes: []mqtypes.SharedResourceType{
				mqtypes.SharedResourceTypeResourceShare, mqtypes.SharedResourceTypeResource,
			},
		},
		{
			name:        "missing_share",
			wantTypes:   []mqtypes.SharedResourceType{mqtypes.SharedResourceTypeResourceShare},
			wantErrCode: mqtypes.SharedResourceErrorCodeShareNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/missing"

			if tt.makeShare {
				out, err := ram.NewFromConfig(fx.cfg).CreateResourceShare(t.Context(), &ram.CreateResourceShareInput{
					Name: aws.String("mq-share"), ResourceArns: []string{subnetARN},
				})
				require.NoError(t, err)

				shareARN = aws.ToString(out.ResourceShare.ResourceShareArn)
			}

			c := mq.NewFromConfig(fx.cfg)
			br, err := c.CreateBroker(t.Context(), &mq.CreateBrokerInput{
				BrokerName: aws.String("shared-broker"), EngineType: mqtypes.EngineTypeActivemq,
				EngineVersion: aws.String("5.15.14"), HostInstanceType: aws.String("mq.t3.micro"),
				DeploymentMode: mqtypes.DeploymentModeSingleInstance, PubliclyAccessible: aws.Bool(false),
				Users: []mqtypes.User{{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")}},
			})
			require.NoError(t, err)

			_, err = c.UpdateBroker(t.Context(), &mq.UpdateBrokerInput{
				BrokerId: br.BrokerId, ResourceShareArns: []string{shareARN},
			})
			require.NoError(t, err)

			pre, err := c.DescribeSharedResources(t.Context(), &mq.DescribeSharedResourcesInput{BrokerId: br.BrokerId})
			require.NoError(t, err)
			assert.Empty(t, pre.SharedResources)

			_, err = c.RebootBroker(t.Context(), &mq.RebootBrokerInput{BrokerId: br.BrokerId})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				d, derr := c.DescribeBroker(t.Context(), &mq.DescribeBrokerInput{BrokerId: br.BrokerId})

				return derr == nil && d.BrokerState == mqtypes.BrokerStateRunning
			}, authzDeadline, authzTick)

			out, err := c.DescribeSharedResources(t.Context(), &mq.DescribeSharedResourcesInput{BrokerId: br.BrokerId})
			require.NoError(t, err)
			require.Len(t, out.SharedResources, len(tt.wantTypes))

			for i, want := range tt.wantTypes {
				assert.Equal(t, want, out.SharedResources[i].Type)
				assert.Equal(t, []string{shareARN}, out.SharedResources[i].ResourceShareArns)
			}

			if tt.wantErrCode != "" {
				require.NotNil(t, out.SharedResources[0].Error)
				assert.Equal(t, tt.wantErrCode, out.SharedResources[0].Error.Code)
				assert.Equal(t, mqtypes.SharedResourceStatusError, out.SharedResources[0].Status)

				return
			}

			assert.Equal(t, subnetARN, aws.ToString(out.SharedResources[1].ResourceArn))
			assert.Equal(t, mqtypes.SharedResourceStatusAvailable, out.SharedResources[1].Status)
		})
	}
}
