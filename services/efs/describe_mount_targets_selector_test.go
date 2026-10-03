package efs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeMountTargets_Selectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   func(fsARN, mtID string) *efssdk.DescribeMountTargetsInput
		name    string
		wantErr string
		wantLen int
	}{
		{
			name:    "no_selector_rejected",
			input:   func(string, string) *efssdk.DescribeMountTargetsInput { return &efssdk.DescribeMountTargetsInput{} },
			wantErr: "BadRequest",
		},
		{
			name: "file_system_arn",
			input: func(fsARN, _ string) *efssdk.DescribeMountTargetsInput {
				return &efssdk.DescribeMountTargetsInput{FileSystemId: aws.String(fsARN)}
			},
			wantLen: 1,
		},
		{
			name: "mount_target_id",
			input: func(_, mtID string) *efssdk.DescribeMountTargetsInput {
				return &efssdk.DescribeMountTargetsInput{MountTargetId: aws.String(mtID)}
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEFSSDKClient(t, newTestEFSHandler())

			fs, err := client.CreateFileSystem(t.Context(), &efssdk.CreateFileSystemInput{
				CreationToken: aws.String("sel-" + tt.name),
			})
			require.NoError(t, err)

			mt, err := client.CreateMountTarget(t.Context(), &efssdk.CreateMountTargetInput{
				FileSystemId: fs.FileSystemId, SubnetId: aws.String("subnet-abc123"),
			})
			require.NoError(t, err)

			out, err := client.DescribeMountTargets(
				t.Context(),
				tt.input(aws.ToString(fs.FileSystemArn), aws.ToString(mt.MountTargetId)),
			)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.MountTargets, tt.wantLen)
		})
	}
}
