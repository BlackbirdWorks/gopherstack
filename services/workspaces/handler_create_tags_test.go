package workspaces_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOpsWithTags_RoundTrip drives every workspaces Create*/Register*
// op whose real Input struct accepts Tags (workspaces@v1.73.1:
// api_op_CreateConnectionAlias.go, api_op_CreateIpGroup.go,
// api_op_CreateWorkspaceBundle.go, api_op_CreateWorkspaceImage.go,
// api_op_CreateWorkspacesPool.go, api_op_CreateWorkspaces.go (nested on
// WorkspaceRequest), api_op_RegisterWorkspaceDirectory.go, all
// `Tags []types.Tag`) through the real SDK client and asserts DescribeTags
// sees what was supplied at creation. Real WorkSpaces has no
// TagResource/ListTagsForResource API -- DescribeTags(ResourceId) is the
// read path (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	wantTags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	t.Run("connection alias", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		out, err := client.CreateConnectionAlias(t.Context(), &wssdk.CreateConnectionAliasInput{
			ConnectionString: aws.String("alias.example.com"),
			Tags:             wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeTags(t.Context(), &wssdk.DescribeTagsInput{
			ResourceId: out.AliasId,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.TagList)
	})

	t.Run("ip group", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		out, err := client.CreateIpGroup(t.Context(), &wssdk.CreateIpGroupInput{
			GroupName: aws.String("tagged-ip-group"),
			Tags:      wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeTags(t.Context(), &wssdk.DescribeTagsInput{
			ResourceId: out.GroupId,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.TagList)
	})

	t.Run("workspace bundle", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		// CreateWorkspaceBundle validates that ImageId references a real
		// image, so create one first rather than using a made-up ID.
		imgOut, err := client.CreateWorkspaceImage(t.Context(), &wssdk.CreateWorkspaceImageInput{
			Name:        aws.String("source-image"),
			Description: aws.String("desc"),
			WorkspaceId: aws.String(createSDKWorkspace(t, client)),
		})
		require.NoError(t, err)

		out, err := client.CreateWorkspaceBundle(t.Context(), &wssdk.CreateWorkspaceBundleInput{
			BundleName:        aws.String("tagged-bundle"),
			BundleDescription: aws.String("desc"),
			ComputeType:       &types.ComputeType{Name: types.ComputeValue},
			ImageId:           imgOut.ImageId,
			UserStorage:       &types.UserStorage{Capacity: aws.String("50")},
			Tags:              wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeTags(t.Context(), &wssdk.DescribeTagsInput{
			ResourceId: out.WorkspaceBundle.BundleId,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.TagList)
	})

	t.Run("workspace image", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		// CreateWorkspaceImage validates that WorkspaceId references a real
		// workspace, so create one first rather than using a made-up ID.
		out, err := client.CreateWorkspaceImage(t.Context(), &wssdk.CreateWorkspaceImageInput{
			Name:        aws.String("tagged-image"),
			Description: aws.String("desc"),
			WorkspaceId: aws.String(createSDKWorkspace(t, client)),
			Tags:        wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeTags(t.Context(), &wssdk.DescribeTagsInput{
			ResourceId: out.ImageId,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.TagList)
	})

	t.Run("workspaces pool", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		out, err := client.CreateWorkspacesPool(t.Context(), &wssdk.CreateWorkspacesPoolInput{
			PoolName:    aws.String("tagged-pool"),
			BundleId:    aws.String("wsb-00000000"),
			DirectoryId: aws.String("d-00000000"),
			Description: aws.String("desc"),
			Capacity:    &types.Capacity{DesiredUserSessions: aws.Int32(1)},
			Tags:        wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeTags(t.Context(), &wssdk.DescribeTagsInput{
			ResourceId: out.WorkspacesPool.PoolId,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.TagList)
	})

	t.Run("workspaces (nested tags)", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
			DirectoryId:            aws.String("d-00000000"),
			WorkspaceDirectoryName: aws.String("dir"),
		})
		require.NoError(t, err)

		out, err := client.CreateWorkspaces(t.Context(), &wssdk.CreateWorkspacesInput{
			Workspaces: []types.WorkspaceRequest{
				{
					BundleId:    aws.String("wsb-00000000"),
					DirectoryId: aws.String("d-00000000"),
					UserName:    aws.String("alice"),
					Tags:        wantTags,
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.PendingRequests, 1)

		got, err := client.DescribeTags(t.Context(), &wssdk.DescribeTagsInput{
			ResourceId: out.PendingRequests[0].WorkspaceId,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.TagList)
	})

	// gopherstack-4shm: RegisterWorkspaceDirectoryInput.Tags was decoded and
	// dropped entirely -- every other Create* op in this same file already
	// applies its Tags via the shared b.tags map (see e.g. bundles.go's
	// CreateWorkspaceBundle), but RegisterWorkspaceDirectory never did.
	t.Run("workspace directory", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
			DirectoryId: aws.String("d-tags11111"),
			Tags:        wantTags,
		})
		require.NoError(t, err)

		got, err := client.DescribeTags(t.Context(), &wssdk.DescribeTagsInput{
			ResourceId: aws.String("d-tags11111"),
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.TagList)
	})
}
