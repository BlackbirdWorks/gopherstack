package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListTagsForResource_Paging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantSize []int
		max      int32
	}{
		{name: "unbounded", max: 0, wantSize: []int{5}},
		{name: "exact fit", max: 5, wantSize: []int{5}},
		{name: "pages of two", max: 2, wantSize: []int{2, 2, 1}},
		{name: "singles", max: 1, wantSize: []int{1, 1, 1, 1, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))
			fs, err := client.CreateFileSystem(t.Context(), &fsxsdk.CreateFileSystemInput{
				FileSystemType:  types.FileSystemTypeLustre,
				SubnetIds:       []string{"subnet-0123abcd"},
				StorageCapacity: aws.Int32(1200),
				LustreConfiguration: &types.CreateFileSystemLustreConfiguration{
					DeploymentType: types.LustreDeploymentTypeScratch2,
				},
				Tags: []types.Tag{
					{Key: aws.String("e"), Value: aws.String("5")},
					{Key: aws.String("b"), Value: aws.String("2")},
					{Key: aws.String("d"), Value: aws.String("4")},
					{Key: aws.String("a"), Value: aws.String("1")},
					{Key: aws.String("c"), Value: aws.String("3")},
				},
			})
			require.NoError(t, err)

			in := &fsxsdk.ListTagsForResourceInput{ResourceARN: fs.FileSystem.ResourceARN}
			if tt.max > 0 {
				in.MaxResults = aws.Int32(tt.max)
			}

			var sizes []int
			var keys []string

			for {
				out, lErr := client.ListTagsForResource(t.Context(), in)
				require.NoError(t, lErr)

				sizes = append(sizes, len(out.Tags))
				for _, tag := range out.Tags {
					keys = append(keys, aws.ToString(tag.Key))
				}

				if out.NextToken == nil {
					break
				}

				in.NextToken = out.NextToken
			}

			assert.Equal(t, tt.wantSize, sizes)
			assert.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
		})
	}
}
