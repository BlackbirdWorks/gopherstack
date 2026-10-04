package fsx_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fsxsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDataRepositoryAssociation_BatchImportMetaDataOnCreate_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		batchImport *bool
		name        string
		wantTasks   int
	}{
		{name: "unset", batchImport: nil, wantTasks: 0},
		{name: "false", batchImport: aws.Bool(false), wantTasks: 0},
		{name: "true", batchImport: aws.Bool(true), wantTasks: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestFSxClient(t, newTestHandler(t))
			fsID := createTestLustreFS(t, client).FileSystem.FileSystemId

			dra, err := client.CreateDataRepositoryAssociation(
				t.Context(),
				&fsxsdk.CreateDataRepositoryAssociationInput{
					FileSystemId:                fsID,
					DataRepositoryPath:          aws.String("s3://import-bucket"),
					FileSystemPath:              aws.String("/data"),
					BatchImportMetaDataOnCreate: tc.batchImport,
				},
			)
			require.NoError(t, err)

			out, err := client.DescribeDataRepositoryTasks(t.Context(), &fsxsdk.DescribeDataRepositoryTasksInput{
				Filters: []types.DataRepositoryTaskFilter{{
					Name:   types.DataRepositoryTaskFilterNameDataRepoAssociationId,
					Values: []string{aws.ToString(dra.Association.AssociationId)},
				}},
			})
			require.NoError(t, err)
			require.Len(t, out.DataRepositoryTasks, tc.wantTasks)

			if tc.wantTasks == 0 {
				return
			}

			task := out.DataRepositoryTasks[0]
			assert.Equal(t, types.DataRepositoryTaskTypeImport, task.Type)
			assert.Equal(t, aws.ToString(fsID), aws.ToString(task.FileSystemId))
			assert.Equal(t, []string{"/data"}, task.Paths)
			assert.Equal(t, types.DataRepositoryTaskLifecycleExecuting, task.Lifecycle)
		})
	}
}
