package eks_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errNoIdcInstance = errors.New("instance not found")

type fakeIdcApps struct {
	err     error
	created []string
	deleted []string
	mu      sync.Mutex
}

func (f *fakeIdcApps) CreateManagedApplication(region, instanceArn, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return "", f.err
	}

	f.created = append(f.created, region+"|"+instanceArn+"|"+name)

	return "arn:aws:sso::123456789012:application/ssoins-1/apl-" + name, nil
}

func (f *fakeIdcApps) DeleteManagedApplication(_, appArn string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.deleted = append(f.deleted, appArn)

	return nil
}

func TestCapability_IdcManagedApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		idcRegion   string
		createErr   error
		wantCreated string
		wantErr     bool
	}{
		{
			name: "explicit_region", idcRegion: "eu-west-1",
			wantCreated: "eu-west-1|arn:aws:sso:::instance/ssoins-1|eks-argocd-c1-argo",
		},
		{
			name: "own_region_default", wantCreated: "us-east-1|arn:aws:sso:::instance/ssoins-1|eks-argocd-c1-argo",
		},
		{name: "unknown_instance", createErr: errNoIdcInstance, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			apps := &fakeIdcApps{err: tt.createErr}
			h.Backend.SetIdcApplicationManager(apps)
			client := newTestEKSClient(t, h)
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:               aws.String("c1"),
				RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
			})
			require.NoError(t, err)

			idc := &ekstypes.ArgoCdAwsIdcConfigRequest{IdcInstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1")}
			if tt.idcRegion != "" {
				idc.IdcRegion = aws.String(tt.idcRegion)
			}

			out, err := client.CreateCapability(ctx, &ekssdk.CreateCapabilityInput{
				ClusterName:             aws.String("c1"),
				CapabilityName:          aws.String("argo"),
				Type:                    ekstypes.CapabilityTypeArgocd,
				RoleArn:                 aws.String("arn:aws:iam::123456789012:role/capability"),
				DeletePropagationPolicy: ekstypes.CapabilityDeletePropagationPolicyRetain,
				Configuration: &ekstypes.CapabilityConfigurationRequest{
					ArgoCd: &ekstypes.ArgoCdConfigRequest{AwsIdc: idc},
				},
			})
			if tt.wantErr {
				require.Error(t, err)

				_, derr := client.DescribeCapability(ctx, &ekssdk.DescribeCapabilityInput{
					ClusterName: aws.String("c1"), CapabilityName: aws.String("argo"),
				})
				require.Error(t, derr, "failed create leaves no capability")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []string{tt.wantCreated}, apps.created)

			wantArn := "arn:aws:sso::123456789012:application/ssoins-1/apl-eks-argocd-c1-argo"
			assert.Equal(t, wantArn, aws.ToString(out.Capability.Configuration.ArgoCd.AwsIdc.IdcManagedApplicationArn))

			desc, err := client.DescribeCapability(ctx, &ekssdk.DescribeCapabilityInput{
				ClusterName: aws.String("c1"), CapabilityName: aws.String("argo"),
			})
			require.NoError(t, err)
			assert.Equal(t, wantArn, aws.ToString(desc.Capability.Configuration.ArgoCd.AwsIdc.IdcManagedApplicationArn))

			_, err = client.DeleteCapability(ctx, &ekssdk.DeleteCapabilityInput{
				ClusterName: aws.String("c1"), CapabilityName: aws.String("argo"),
			})
			require.NoError(t, err)
			assert.Equal(t, []string{wantArn}, apps.deleted)
		})
	}
}
