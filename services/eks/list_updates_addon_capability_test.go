package eks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListUpdates_AddonAndCapabilityFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		addonName   *string
		capability  *string
		wantType    ekstypes.UpdateType
		wantResults int
	}{
		{
			name: "addon_filter", addonName: aws.String("vpc-cni"),
			wantType: ekstypes.UpdateTypeAddonUpdate, wantResults: 1,
		},
		{
			name: "capability_filter", capability: aws.String("my-argocd"),
			wantType: ekstypes.UpdateTypeCapabilityUpdate, wantResults: 1,
		},
		{name: "unknown_addon_filter", addonName: aws.String("coredns"), wantResults: 0},
		{name: "unfiltered", wantResults: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newTestEKSHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:               aws.String("upd-cluster"),
				RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
			})
			require.NoError(t, err)

			_, err = client.CreateAddon(ctx, &ekssdk.CreateAddonInput{
				ClusterName: aws.String("upd-cluster"),
				AddonName:   aws.String("vpc-cni"),
			})
			require.NoError(t, err)

			_, err = client.CreateCapability(ctx, &ekssdk.CreateCapabilityInput{
				ClusterName:             aws.String("upd-cluster"),
				CapabilityName:          aws.String("my-argocd"),
				Type:                    ekstypes.CapabilityTypeArgocd,
				RoleArn:                 aws.String("arn:aws:iam::123456789012:role/capability"),
				DeletePropagationPolicy: ekstypes.CapabilityDeletePropagationPolicyRetain,
				Configuration: &ekstypes.CapabilityConfigurationRequest{ArgoCd: &ekstypes.ArgoCdConfigRequest{
					AwsIdc: &ekstypes.ArgoCdAwsIdcConfigRequest{
						IdcInstanceArn: aws.String("arn:aws:sso:::instance/i-1"),
					},
				}},
			})
			require.NoError(t, err)

			_, err = client.UpdateAddon(ctx, &ekssdk.UpdateAddonInput{
				ClusterName: aws.String("upd-cluster"),
				AddonName:   aws.String("vpc-cni"),
			})
			require.NoError(t, err)

			_, err = client.UpdateCapability(ctx, &ekssdk.UpdateCapabilityInput{
				ClusterName:    aws.String("upd-cluster"),
				CapabilityName: aws.String("my-argocd"),
				RoleArn:        aws.String("arn:aws:iam::123456789012:role/capability2"),
			})
			require.NoError(t, err)

			list, err := client.ListUpdates(ctx, &ekssdk.ListUpdatesInput{
				Name:           aws.String("upd-cluster"),
				AddonName:      tt.addonName,
				CapabilityName: tt.capability,
			})
			require.NoError(t, err)
			require.Len(t, list.UpdateIds, tt.wantResults)

			if tt.wantResults != 1 {
				return
			}

			desc, err := client.DescribeUpdate(ctx, &ekssdk.DescribeUpdateInput{
				Name:     aws.String("upd-cluster"),
				UpdateId: aws.String(list.UpdateIds[0]),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, desc.Update.Type)
		})
	}
}
