package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEKSArgoCdIdcManagedApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		instance string
		wantErr  bool
	}{
		{name: "seeded_instance"},
		{name: "unknown_instance", instance: "arn:aws:sso:::instance/ssoins-missing", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := newWiredSDKConfig(t, CLI{})
			ec := eks.NewFromConfig(cfg)
			sc := ssoadmin.NewFromConfig(cfg)
			ctx := t.Context()

			instances, err := sc.ListInstances(ctx, &ssoadmin.ListInstancesInput{})
			require.NoError(t, err)
			require.NotEmpty(t, instances.Instances)

			instanceArn := aws.ToString(instances.Instances[0].InstanceArn)
			if tt.instance != "" {
				instanceArn = tt.instance
			}

			_, err = ec.CreateCluster(ctx, &eks.CreateClusterInput{
				Name:               aws.String("c1"),
				RoleArn:            aws.String("arn:aws:iam::000000000000:role/eks"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
			})
			require.NoError(t, err)

			out, err := ec.CreateCapability(ctx, &eks.CreateCapabilityInput{
				ClusterName:             aws.String("c1"),
				CapabilityName:          aws.String("argo"),
				Type:                    ekstypes.CapabilityTypeArgocd,
				RoleArn:                 aws.String("arn:aws:iam::000000000000:role/capability"),
				DeletePropagationPolicy: ekstypes.CapabilityDeletePropagationPolicyRetain,
				Configuration: &ekstypes.CapabilityConfigurationRequest{ArgoCd: &ekstypes.ArgoCdConfigRequest{
					AwsIdc: &ekstypes.ArgoCdAwsIdcConfigRequest{IdcInstanceArn: aws.String(instanceArn)},
				}},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			appArn := aws.ToString(out.Capability.Configuration.ArgoCd.AwsIdc.IdcManagedApplicationArn)
			require.NotEmpty(t, appArn)

			app, err := sc.DescribeApplication(
				ctx,
				&ssoadmin.DescribeApplicationInput{ApplicationArn: aws.String(appArn)},
			)
			require.NoError(t, err)
			assert.Equal(t, instanceArn, aws.ToString(app.InstanceArn))

			_, err = ec.DeleteCapability(ctx, &eks.DeleteCapabilityInput{
				ClusterName: aws.String("c1"), CapabilityName: aws.String("argo"),
			})
			require.NoError(t, err)

			_, err = sc.DescribeApplication(ctx, &ssoadmin.DescribeApplicationInput{ApplicationArn: aws.String(appArn)})
			require.Error(t, err, "managed application removed with the capability")
		})
	}
}
