package s3files_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/aws/aws-sdk-go-v2/service/s3files/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3files"
)

func TestCreateFileSystemValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		role     string
		token    string
		wantCode string
	}{
		{name: "ok", bucket: testBucket, role: testRole},
		{name: "bucket not arn", bucket: "data-bucket", role: testRole, wantCode: "ValidationException"},
		{name: "bucket with key", bucket: testBucket + "/key", role: testRole, wantCode: "ValidationException"},
		{name: "missing bucket", bucket: "arn:aws:s3:::nope", role: testRole, wantCode: "ValidationException"},
		{
			name:     "bad role",
			bucket:   testBucket,
			role:     "arn:aws:iam::123456789012:user/x",
			wantCode: "ValidationException",
		},
		{
			name:     "long token",
			bucket:   testBucket,
			role:     testRole,
			token:    string(make([]byte, 65)),
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			in := &s3filessdk.CreateFileSystemInput{Bucket: aws.String(tt.bucket), RoleArn: aws.String(tt.role)}
			if tt.token != "" {
				in.ClientToken = aws.String(tt.token)
			}

			out, err := f.client.CreateFileSystem(t.Context(), in)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Regexp(t, `^fs-[0-9a-f]{17}$`, aws.ToString(out.FileSystemId))
			assert.Equal(t, "arn:aws:s3files:us-east-1:123456789012:file-system/"+aws.ToString(out.FileSystemId),
				aws.ToString(out.FileSystemArn))
			assert.Equal(t, types.LifeCycleStateAvailable, out.Status)
			assert.Equal(t, testAccount, aws.ToString(out.OwnerId))
			assert.False(t, aws.ToTime(out.CreationTime).IsZero())
		})
	}
}

func TestFileSystemGetListDelete(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	named, err := f.client.CreateFileSystem(ctx, &s3filessdk.CreateFileSystemInput{
		Bucket: aws.String(testBucket), RoleArn: aws.String(testRole), Prefix: aws.String("logs/"),
		Tags: []types.Tag{{Key: aws.String("Name"), Value: aws.String("primary")}},
	})
	require.NoError(t, err)

	_, err = f.client.CreateFileSystem(ctx, &s3filessdk.CreateFileSystemInput{
		Bucket: aws.String("arn:aws:s3:::other"), RoleArn: aws.String(testRole),
	})
	require.NoError(t, err)

	got, err := f.client.GetFileSystem(ctx, &s3filessdk.GetFileSystemInput{FileSystemId: named.FileSystemArn})
	require.NoError(t, err)
	assert.Equal(t, "primary", aws.ToString(got.Name))
	assert.Equal(t, "logs/", aws.ToString(got.Prefix))
	require.Len(t, got.Tags, 1)

	all, err := f.client.ListFileSystems(ctx, &s3filessdk.ListFileSystemsInput{})
	require.NoError(t, err)
	assert.Len(t, all.FileSystems, 2)

	filtered, err := f.client.ListFileSystems(ctx, &s3filessdk.ListFileSystemsInput{Bucket: aws.String(testBucket)})
	require.NoError(t, err)
	require.Len(t, filtered.FileSystems, 1)
	assert.Equal(t, "primary", aws.ToString(filtered.FileSystems[0].Name))

	pager := s3filessdk.NewListFileSystemsPaginator(
		f.client,
		&s3filessdk.ListFileSystemsInput{MaxResults: aws.Int32(1)},
	)
	pages, total := 0, 0

	for pager.HasMorePages() {
		p, pErr := pager.NextPage(ctx)
		require.NoError(t, pErr)

		pages++
		total += len(p.FileSystems)
	}

	assert.Equal(t, 2, pages)
	assert.Equal(t, 2, total)

	_, err = f.client.DeleteFileSystem(ctx, &s3filessdk.DeleteFileSystemInput{FileSystemId: named.FileSystemId})
	require.NoError(t, err)

	_, err = f.client.GetFileSystem(ctx, &s3filessdk.GetFileSystemInput{FileSystemId: named.FileSystemId})
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteFileSystem(ctx, &s3filessdk.DeleteFileSystemInput{FileSystemId: named.FileSystemId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestCreateFileSystemIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		wantCode string
		wantSame bool
	}{
		{name: "same params", bucket: testBucket, wantSame: true},
		{name: "different params", bucket: "arn:aws:s3:::other", wantCode: "ConflictException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			mk := func(bucket string) (*s3filessdk.CreateFileSystemOutput, error) {
				return f.client.CreateFileSystem(t.Context(), &s3filessdk.CreateFileSystemInput{
					Bucket: aws.String(bucket), RoleArn: aws.String(testRole), ClientToken: aws.String("tok-1"),
				})
			}

			first, err := mk(testBucket)
			require.NoError(t, err)

			second, err := mk(tt.bucket)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.FileSystemId), aws.ToString(second.FileSystemId))
		})
	}
}

func TestFileSystemLifecycleStates(t *testing.T) {
	t.Parallel()

	backend := s3files.NewInMemoryBackend(testAccount, testRegion)
	backend.SetLifecycleDelay(time.Minute)
	f := newFixtureWith(t, backend, testRegion)
	ctx := t.Context()

	out, err := f.client.CreateFileSystem(ctx, &s3filessdk.CreateFileSystemInput{
		Bucket: aws.String(testBucket), RoleArn: aws.String(testRole),
	})
	require.NoError(t, err)
	assert.Equal(t, types.LifeCycleStateCreating, out.Status)

	_, err = f.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: out.FileSystemId, SubnetId: aws.String(testSubnet),
	})
	require.Error(t, err)
	assert.Equal(t, "ConflictException", apiCode(err))

	f.clock.advance(time.Minute)

	got, err := f.client.GetFileSystem(ctx, &s3filessdk.GetFileSystemInput{FileSystemId: out.FileSystemId})
	require.NoError(t, err)
	assert.Equal(t, types.LifeCycleStateAvailable, got.Status)

	_, err = f.client.DeleteFileSystem(ctx, &s3filessdk.DeleteFileSystemInput{FileSystemId: out.FileSystemId})
	require.NoError(t, err)

	got, err = f.client.GetFileSystem(ctx, &s3filessdk.GetFileSystemInput{FileSystemId: out.FileSystemId})
	require.NoError(t, err)
	assert.Equal(t, types.LifeCycleStateDeleting, got.Status)

	f.clock.advance(time.Minute)

	_, err = f.client.GetFileSystem(ctx, &s3filessdk.GetFileSystemInput{FileSystemId: out.FileSystemId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestDeleteFileSystemWithChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		child func(t *testing.T, f *fixture, fsID string) func()
		name  string
	}{
		{name: "mount target", child: func(t *testing.T, f *fixture, fsID string) func() {
			t.Helper()

			mt, err := f.client.CreateMountTarget(t.Context(), &s3filessdk.CreateMountTargetInput{
				FileSystemId: aws.String(fsID), SubnetId: aws.String(testSubnet),
			})
			require.NoError(t, err)

			return func() {
				_, dErr := f.client.DeleteMountTarget(t.Context(), &s3filessdk.DeleteMountTargetInput{
					MountTargetId: mt.MountTargetId,
				})
				require.NoError(t, dErr)
			}
		}},
		{name: "access point", child: func(t *testing.T, f *fixture, fsID string) func() {
			t.Helper()

			ap, err := f.client.CreateAccessPoint(t.Context(), &s3filessdk.CreateAccessPointInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)

			return func() {
				_, dErr := f.client.DeleteAccessPoint(t.Context(), &s3filessdk.DeleteAccessPointInput{
					AccessPointId: ap.AccessPointId,
				})
				require.NoError(t, dErr)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			fsID := f.fileSystem(t)
			cleanup := tt.child(t, f, fsID)

			_, err := f.client.DeleteFileSystem(t.Context(), &s3filessdk.DeleteFileSystemInput{
				FileSystemId: aws.String(fsID),
			})
			require.Error(t, err)
			assert.Equal(t, "ConflictException", apiCode(err))

			cleanup()

			_, err = f.client.DeleteFileSystem(
				t.Context(),
				&s3filessdk.DeleteFileSystemInput{FileSystemId: aws.String(fsID)},
			)
			require.NoError(t, err)
		})
	}
}

func TestFileSystemRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := s3files.NewInMemoryBackend(testAccount, testRegion)
	west := newFixtureWith(t, backend, "us-west-2")
	id := west.fileSystem(t)

	east := newFixtureWith(t, backend, testRegion)
	_, err := east.client.GetFileSystem(t.Context(), &s3filessdk.GetFileSystemInput{FileSystemId: aws.String(id)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err := east.client.ListFileSystems(t.Context(), &s3filessdk.ListFileSystemsInput{})
	require.NoError(t, err)
	assert.Empty(t, list.FileSystems)
}
