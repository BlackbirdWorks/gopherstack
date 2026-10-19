package s3files_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/aws/aws-sdk-go-v2/service/s3files/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAccessPointValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		user     *types.PosixUser
		root     *types.RootDirectory
		wantCode string
	}{
		{name: "empty"},
		{name: "full", user: &types.PosixUser{Uid: aws.Int64(1000), Gid: aws.Int64(1000), SecondaryGids: []int64{5}},
			root: &types.RootDirectory{Path: aws.String("/data"), CreationPermissions: &types.CreationPermissions{
				OwnerUid: aws.Int64(1000), OwnerGid: aws.Int64(1000), Permissions: aws.String("0755"),
			}}},
		{
			name:     "negative uid",
			user:     &types.PosixUser{Uid: aws.Int64(-1), Gid: aws.Int64(1)},
			wantCode: "ValidationException",
		},
		{name: "relative path", root: &types.RootDirectory{Path: aws.String("data")}, wantCode: "ValidationException"},
		{name: "bad permissions", root: &types.RootDirectory{CreationPermissions: &types.CreationPermissions{
			OwnerUid: aws.Int64(1), OwnerGid: aws.Int64(1), Permissions: aws.String("999"),
		}}, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			out, err := f.client.CreateAccessPoint(t.Context(), &s3filessdk.CreateAccessPointInput{
				FileSystemId: aws.String(f.fileSystem(t)), PosixUser: tt.user, RootDirectory: tt.root,
				Tags: []types.Tag{{Key: aws.String("Name"), Value: aws.String("app")}},
			})
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Regexp(t, `^fsap-[0-9a-f]{17}$`, aws.ToString(out.AccessPointId))
			assert.Equal(t, "app", aws.ToString(out.Name))
			assert.Equal(t, types.LifeCycleStateAvailable, out.Status)

			got, err := f.client.GetAccessPoint(
				t.Context(),
				&s3filessdk.GetAccessPointInput{AccessPointId: out.AccessPointArn},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.user, got.PosixUser)
			assert.Equal(t, aws.ToString(out.FileSystemId), aws.ToString(got.FileSystemId))
		})
	}
}

func TestAccessPointListDeleteAndToken(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	fsID := f.fileSystem(t)

	first, err := f.client.CreateAccessPoint(ctx, &s3filessdk.CreateAccessPointInput{
		FileSystemId: aws.String(fsID), ClientToken: aws.String("tok"),
	})
	require.NoError(t, err)

	replay, err := f.client.CreateAccessPoint(ctx, &s3filessdk.CreateAccessPointInput{
		FileSystemId: aws.String(fsID), ClientToken: aws.String("tok"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.AccessPointId), aws.ToString(replay.AccessPointId))

	_, err = f.client.CreateAccessPoint(ctx, &s3filessdk.CreateAccessPointInput{FileSystemId: aws.String(fsID)})
	require.NoError(t, err)

	list, err := f.client.ListAccessPoints(ctx, &s3filessdk.ListAccessPointsInput{FileSystemId: aws.String(fsID)})
	require.NoError(t, err)
	assert.Len(t, list.AccessPoints, 2)

	_, err = f.client.ListAccessPoints(ctx, &s3filessdk.ListAccessPointsInput{FileSystemId: aws.String("fs-none")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.CreateAccessPoint(ctx, &s3filessdk.CreateAccessPointInput{FileSystemId: aws.String("fs-none")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteAccessPoint(ctx, &s3filessdk.DeleteAccessPointInput{AccessPointId: first.AccessPointId})
	require.NoError(t, err)

	_, err = f.client.GetAccessPoint(ctx, &s3filessdk.GetAccessPointInput{AccessPointId: first.AccessPointId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err = f.client.ListAccessPoints(ctx, &s3filessdk.ListAccessPointsInput{FileSystemId: aws.String(fsID)})
	require.NoError(t, err)
	assert.Len(t, list.AccessPoints, 1)
}
