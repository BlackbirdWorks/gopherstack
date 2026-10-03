package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestRegisterJobDefinition_VolumeConfigurations_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input  *batchsdk.RegisterJobDefinitionInput
		verify func(t *testing.T, jd types.JobDefinition)
		name   string
	}{
		{
			name: "efs_and_s3files_container_volumes",
			input: &batchsdk.RegisterJobDefinitionInput{
				Type: types.JobDefinitionTypeContainer,
				ContainerProperties: &types.ContainerProperties{
					Image: aws.String("busybox"),
					ResourceRequirements: []types.ResourceRequirement{
						{Type: types.ResourceTypeVcpu, Value: aws.String("1")},
						{Type: types.ResourceTypeMemory, Value: aws.String("128")},
					},
					Volumes: []types.Volume{
						{
							Name: aws.String("efs"),
							EfsVolumeConfiguration: &types.EFSVolumeConfiguration{
								FileSystemId:          aws.String("fs-12345678"),
								RootDirectory:         aws.String("/data"),
								TransitEncryption:     types.EFSTransitEncryptionEnabled,
								TransitEncryptionPort: aws.Int32(2999),
								AuthorizationConfig: &types.EFSAuthorizationConfig{
									AccessPointId: aws.String("fsap-0123456789abcdef"),
									Iam:           types.EFSAuthorizationConfigIAMEnabled,
								},
							},
						},
						{
							Name: aws.String("s3f"),
							S3filesVolumeConfiguration: &types.S3FilesVolumeConfiguration{
								FileSystemArn:  aws.String("arn:aws:s3files:us-east-1:000000000000:file-system/fs-1"),
								AccessPointArn: aws.String("arn:aws:s3files:us-east-1:000000000000:access-point/ap-1"),
								RootDirectory:  aws.String("/r"),
							},
						},
					},
				},
			},
			verify: func(t *testing.T, jd types.JobDefinition) {
				t.Helper()

				vols := jd.ContainerProperties.Volumes
				require.Len(t, vols, 2)

				efs := vols[0].EfsVolumeConfiguration
				require.NotNil(t, efs)
				assert.Equal(t, "fs-12345678", aws.ToString(efs.FileSystemId))
				assert.Equal(t, "/data", aws.ToString(efs.RootDirectory))
				assert.Equal(t, types.EFSTransitEncryptionEnabled, efs.TransitEncryption)
				assert.Equal(t, int32(2999), aws.ToInt32(efs.TransitEncryptionPort))
				require.NotNil(t, efs.AuthorizationConfig)
				assert.Equal(t, "fsap-0123456789abcdef", aws.ToString(efs.AuthorizationConfig.AccessPointId))
				assert.Equal(t, types.EFSAuthorizationConfigIAMEnabled, efs.AuthorizationConfig.Iam)

				s3f := vols[1].S3filesVolumeConfiguration
				require.NotNil(t, s3f)
				assert.Contains(t, aws.ToString(s3f.FileSystemArn), "file-system/fs-1")
				assert.Contains(t, aws.ToString(s3f.AccessPointArn), "access-point/ap-1")
				assert.Equal(t, "/r", aws.ToString(s3f.RootDirectory))
			},
		},
		{
			name: "eks_persistent_volume_claim",
			input: &batchsdk.RegisterJobDefinitionInput{
				Type: types.JobDefinitionTypeContainer,
				EksProperties: &types.EksProperties{
					PodProperties: &types.EksPodProperties{
						Containers: []types.EksContainer{{Name: aws.String("c"), Image: aws.String("busybox")}},
						Volumes: []types.EksVolume{{
							Name: aws.String("pvc"),
							PersistentVolumeClaim: &types.EksPersistentVolumeClaim{
								ClaimName: aws.String("my-claim"),
								ReadOnly:  aws.Bool(true),
							},
						}},
					},
				},
			},
			verify: func(t *testing.T, jd types.JobDefinition) {
				t.Helper()

				vols := jd.EksProperties.PodProperties.Volumes
				require.Len(t, vols, 1)
				require.NotNil(t, vols[0].PersistentVolumeClaim)
				assert.Equal(t, "my-claim", aws.ToString(vols[0].PersistentVolumeClaim.ClaimName))
				assert.True(t, aws.ToBool(vols[0].PersistentVolumeClaim.ReadOnly))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestBatchClient(t, batch.NewHandler(batch.NewInMemoryBackend("000000000000", "us-east-1")))

			tt.input.JobDefinitionName = aws.String("vol-jd")

			_, err := client.RegisterJobDefinition(ctx, tt.input)
			require.NoError(t, err)

			out, err := client.DescribeJobDefinitions(ctx, &batchsdk.DescribeJobDefinitionsInput{
				JobDefinitionName: aws.String("vol-jd"),
			})
			require.NoError(t, err)
			require.Len(t, out.JobDefinitions, 1)

			tt.verify(t, out.JobDefinitions[0])
		})
	}
}
