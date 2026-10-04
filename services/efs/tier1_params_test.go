package efs_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

func TestListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagCount  int
		maxResult int32
		wantPages int
	}{
		{name: "single-page-default", tagCount: 5, wantPages: 1},
		{name: "paged-by-2", tagCount: 5, maxResult: 2, wantPages: 3},
		{name: "exact-fit", tagCount: 4, maxResult: 4, wantPages: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fs, err := client.CreateFileSystem(
				ctx,
				&efssdk.CreateFileSystemInput{CreationToken: aws.String("tag-page")},
			)
			require.NoError(t, err)

			tags := make([]efstypes.Tag, 0, tt.tagCount)
			for i := range tt.tagCount {
				tags = append(tags, efstypes.Tag{Key: aws.String("k" + strconv.Itoa(i)), Value: aws.String("v")})
			}

			_, err = client.TagResource(ctx, &efssdk.TagResourceInput{ResourceId: fs.FileSystemId, Tags: tags})
			require.NoError(t, err)

			in := &efssdk.ListTagsForResourceInput{ResourceId: fs.FileSystemId}
			if tt.maxResult > 0 {
				in.MaxResults = aws.Int32(tt.maxResult)
			}

			var got []string
			pages := 0

			for {
				out, listErr := client.ListTagsForResource(ctx, in)
				require.NoError(t, listErr)

				pages++

				for _, tg := range out.Tags {
					got = append(got, aws.ToString(tg.Key))
				}

				if out.NextToken == nil {
					break
				}

				in.NextToken = out.NextToken
			}

			assert.Equal(t, tt.wantPages, pages)
			assert.Len(t, got, tt.tagCount)
			assert.IsIncreasing(t, got)
		})
	}
}

func TestDeleteReplicationConfiguration_DeletionMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    efstypes.DeletionMode
		wantErr bool
	}{
		{name: "default", mode: ""},
		{name: "all", mode: efstypes.DeletionModeAllConfigurations},
		{name: "local-only-same-region-rejected", mode: efstypes.DeletionModeLocalConfigurationOnly, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fs, err := client.CreateFileSystem(
				ctx,
				&efssdk.CreateFileSystemInput{CreationToken: aws.String("del-mode")},
			)
			require.NoError(t, err)

			_, err = client.CreateReplicationConfiguration(ctx, &efssdk.CreateReplicationConfigurationInput{
				SourceFileSystemId: fs.FileSystemId, Destinations: []efstypes.DestinationToCreate{{}},
			})
			require.NoError(t, err)

			_, err = client.DeleteReplicationConfiguration(ctx, &efssdk.DeleteReplicationConfigurationInput{
				SourceFileSystemId: fs.FileSystemId, DeletionMode: tt.mode,
			})

			desc, descErr := client.DescribeReplicationConfigurations(
				ctx,
				&efssdk.DescribeReplicationConfigurationsInput{
					FileSystemId: fs.FileSystemId,
				},
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "BadRequest")
				require.NoError(t, descErr)
				assert.Len(t, desc.Replications, 1, "rejected delete must leave the configuration in place")

				return
			}

			require.NoError(t, err)
			require.NoError(t, descErr)
			assert.Empty(t, desc.Replications)
		})
	}
}
