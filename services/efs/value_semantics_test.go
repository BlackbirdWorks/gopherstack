package efs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateFileSystem_LeavingProvisionedClearsThroughput(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mode types.ThroughputMode
	}{
		{name: "to bursting", mode: types.ThroughputModeBursting},
		{name: "to elastic", mode: types.ThroughputModeElastic},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newWireTestClient(t)
			ctx := t.Context()

			fs, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
				CreationToken:                aws.String("tok"),
				ThroughputMode:               types.ThroughputModeProvisioned,
				ProvisionedThroughputInMibps: aws.Float64(10),
			})
			require.NoError(t, err)

			upd, err := client.UpdateFileSystem(ctx, &efssdk.UpdateFileSystemInput{
				FileSystemId: fs.FileSystemId, ThroughputMode: tc.mode,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.mode, upd.ThroughputMode)
			assert.Nil(t, upd.ProvisionedThroughputInMibps)

			got, err := client.DescribeFileSystems(ctx, &efssdk.DescribeFileSystemsInput{FileSystemId: fs.FileSystemId})
			require.NoError(t, err)
			require.Len(t, got.FileSystems, 1)
			assert.Nil(t, got.FileSystems[0].ProvisionedThroughputInMibps)
		})
	}
}
