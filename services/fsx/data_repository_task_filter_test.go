package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeDataRepositoryTasks_FileCacheIDFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   []string
		wantTask bool
	}{
		{name: "unknown_cache_matches_none", values: []string{"fc-0123456789abcdef0"}},
		{name: "fs_filter_still_matches", values: nil, wantTask: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))
			fsID := createTestLustreFS(t, client).FileSystem.FileSystemId

			_, err := client.CreateDataRepositoryTask(t.Context(), &fsxsdk.CreateDataRepositoryTaskInput{
				FileSystemId: fsID,
				Type:         types.DataRepositoryTaskTypeExport,
				Report:       &types.CompletionReport{Enabled: aws.Bool(false)},
			})
			require.NoError(t, err)

			filters := []types.DataRepositoryTaskFilter{{
				Name: types.DataRepositoryTaskFilterNameFileSystemId, Values: []string{aws.ToString(fsID)},
			}}
			if tc.values != nil {
				filters = []types.DataRepositoryTaskFilter{{
					Name: types.DataRepositoryTaskFilterNameFileCacheId, Values: tc.values,
				}}
			}

			out, err := client.DescribeDataRepositoryTasks(t.Context(),
				&fsxsdk.DescribeDataRepositoryTasksInput{Filters: filters})
			require.NoError(t, err)
			assert.Equal(t, tc.wantTask, len(out.DataRepositoryTasks) == 1)
		})
	}
}
