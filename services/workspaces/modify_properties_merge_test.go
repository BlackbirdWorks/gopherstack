package workspaces_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModifyWorkspaceProperties_PartialKeepsOmittedMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		modify   *types.WorkspaceProperties
		name     string
		wantMode types.RunningMode
		wantRoot int32
	}{
		{
			name: "running mode only", modify: &types.WorkspaceProperties{RunningMode: types.RunningModeAutoStop},
			wantMode: types.RunningModeAutoStop, wantRoot: 100,
		},
		{
			name: "root volume only", modify: &types.WorkspaceProperties{RootVolumeSizeGib: aws.Int32(200)},
			wantMode: types.RunningModeAlwaysOn, wantRoot: 200,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			_, err := c.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
				DirectoryId: aws.String("d-00000000"), WorkspaceDirectoryName: aws.String("dir"),
			})
			require.NoError(t, err)

			out, err := c.CreateWorkspaces(t.Context(), &wssdk.CreateWorkspacesInput{
				Workspaces: []types.WorkspaceRequest{
					{
						BundleId: aws.String(
							"wsb-00000000",
						),
						DirectoryId: aws.String("d-00000000"),
						UserName:    aws.String("alice"),
						WorkspaceProperties: &types.WorkspaceProperties{
							RunningMode: types.RunningModeAlwaysOn, RootVolumeSizeGib: aws.Int32(100),
							UserVolumeSizeGib: aws.Int32(50), ComputeTypeName: types.ComputeStandard,
						},
					},
				},
			})
			require.NoError(t, err)
			id := out.PendingRequests[0].WorkspaceId

			_, err = c.ModifyWorkspaceProperties(t.Context(), &wssdk.ModifyWorkspacePropertiesInput{
				WorkspaceId: id, WorkspaceProperties: tc.modify,
			})
			require.NoError(t, err)

			got, err := c.DescribeWorkspaces(
				t.Context(),
				&wssdk.DescribeWorkspacesInput{WorkspaceIds: []string{aws.ToString(id)}},
			)
			require.NoError(t, err)
			require.Len(t, got.Workspaces, 1)

			p := got.Workspaces[0].WorkspaceProperties
			assert.Equal(t, tc.wantMode, p.RunningMode)
			assert.Equal(t, tc.wantRoot, aws.ToInt32(p.RootVolumeSizeGib))
			assert.Equal(t, int32(50), aws.ToInt32(p.UserVolumeSizeGib))
			assert.Equal(t, types.ComputeStandard, p.ComputeTypeName)
		})
	}
}
