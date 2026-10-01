package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteVolume_FinalBackup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg        *types.DeleteVolumeOntapConfiguration
		name       string
		wantTags   int
		wantBackup bool
	}{
		{name: "default_takes", wantBackup: true},
		{name: "skip", cfg: &types.DeleteVolumeOntapConfiguration{SkipFinalBackup: aws.Bool(true)}},
		{
			name: "tags", wantBackup: true, wantTags: 1,
			cfg: &types.DeleteVolumeOntapConfiguration{
				FinalBackupTags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))
			vol := createTestOntapVolume(t, client, "vol1")
			volID := aws.ToString(vol.Volume.VolumeId)

			out, err := client.DeleteVolume(t.Context(), &fsxsdk.DeleteVolumeInput{
				VolumeId: aws.String(volID), OntapConfiguration: tc.cfg,
			})
			require.NoError(t, err)

			bks, err := client.DescribeBackups(t.Context(), &fsxsdk.DescribeBackupsInput{})
			require.NoError(t, err)

			if !tc.wantBackup {
				assert.Nil(t, out.OntapResponse)
				assert.Empty(t, bks.Backups)

				return
			}

			require.NotNil(t, out.OntapResponse)
			assert.Len(t, out.OntapResponse.FinalBackupTags, tc.wantTags)
			require.Len(t, bks.Backups, 1)
			assert.Equal(t, aws.ToString(out.OntapResponse.FinalBackupId), aws.ToString(bks.Backups[0].BackupId))
			assert.Equal(t, volID, aws.ToString(bks.Backups[0].Volume.VolumeId))
		})
	}
}
