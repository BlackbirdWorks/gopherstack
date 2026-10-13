package codecommit_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeObjectTypeConflictResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resolution   *types.ConflictResolution
		name         string
		strategy     types.ConflictResolutionStrategyTypeEnum
		wantMergeErr string
		wantDirFile  bool
		wantNested   bool
	}{
		{name: "unresolved_conflicts", wantMergeErr: "ManualMergeRequiredException"},
		{
			name:     "accept_source_keeps_folder",
			strategy: types.ConflictResolutionStrategyTypeEnumAcceptSource, wantNested: true,
		},
		{
			name:     "accept_destination_keeps_file",
			strategy: types.ConflictResolutionStrategyTypeEnumAcceptDestination, wantDirFile: true,
		},
		{
			name: "delete_file_keeps_folder",
			resolution: &types.ConflictResolution{
				DeleteFiles: []types.DeleteFileEntry{{FilePath: aws.String("dir")}},
			},
			wantNested: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newMergeFixture(t, map[string]string{"base.txt": "b"})
			f.branchFeature()
			f.commit("main", map[string]string{"dir": "file"})
			f.commit("feature", map[string]string{"dir/f.txt": "nested"})

			_, err := f.threeWay(&codecommitsdk.MergeBranchesByThreeWayInput{
				ConflictResolutionStrategy: tt.strategy,
				ConflictResolution:         tt.resolution,
				TargetBranch:               aws.String("main"),
			})
			if tt.wantMergeErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantMergeErr)

				return
			}

			require.NoError(t, err)

			_, fileErr := f.client.GetFile(t.Context(), &codecommitsdk.GetFileInput{
				RepositoryName: aws.String(f.repo), CommitSpecifier: aws.String("main"), FilePath: aws.String("dir"),
			})
			_, nestedErr := f.client.GetFile(t.Context(), &codecommitsdk.GetFileInput{
				RepositoryName: aws.String(
					f.repo,
				),
				CommitSpecifier: aws.String("main"),
				FilePath:        aws.String("dir/f.txt"),
			})

			assert.Equal(t, tt.wantDirFile, fileErr == nil)
			assert.Equal(t, tt.wantNested, nestedErr == nil)
		})
	}
}
