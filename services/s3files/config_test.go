package s3files_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/aws/aws-sdk-go-v2/service/s3files/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSystemPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3files:*"}]}`

	tests := []struct {
		name     string
		policy   string
		wantCode string
	}{
		{name: "valid", policy: policy},
		{name: "not json", policy: "nope", wantCode: "ValidationException"},
		{name: "no statement", policy: `{"Version":"2012-10-17"}`, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ctx := t.Context()
			fsID := f.fileSystem(t)

			_, err := f.client.GetFileSystemPolicy(
				ctx,
				&s3filessdk.GetFileSystemPolicyInput{FileSystemId: aws.String(fsID)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			_, err = f.client.PutFileSystemPolicy(ctx, &s3filessdk.PutFileSystemPolicyInput{
				FileSystemId: aws.String(fsID), Policy: aws.String(tt.policy),
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			got, err := f.client.GetFileSystemPolicy(
				ctx,
				&s3filessdk.GetFileSystemPolicyInput{FileSystemId: aws.String(fsID)},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.policy, aws.ToString(got.Policy))
			assert.Equal(t, fsID, aws.ToString(got.FileSystemId))

			_, err = f.client.DeleteFileSystemPolicy(
				ctx,
				&s3filessdk.DeleteFileSystemPolicyInput{FileSystemId: aws.String(fsID)},
			)
			require.NoError(t, err)

			_, err = f.client.DeleteFileSystemPolicy(
				ctx,
				&s3filessdk.DeleteFileSystemPolicyInput{FileSystemId: aws.String(fsID)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		})
	}
}

func TestSynchronizationConfiguration(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	fsID := aws.String(f.fileSystem(t))

	imp := []types.ImportDataRule{
		{Prefix: aws.String("hot/"), SizeLessThan: aws.Int64(1024), Trigger: types.ImportTriggerOnFileAccess},
	}
	exp := []types.ExpirationDataRule{{DaysAfterLastAccess: aws.Int32(30)}}

	_, err := f.client.GetSynchronizationConfiguration(
		ctx,
		&s3filessdk.GetSynchronizationConfigurationInput{FileSystemId: fsID},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.PutSynchronizationConfiguration(ctx, &s3filessdk.PutSynchronizationConfigurationInput{
		FileSystemId: fsID, ImportDataRules: imp, ExpirationDataRules: exp, LatestVersionNumber: aws.Int32(4),
	})
	assert.Equal(t, "ConflictException", apiCode(err))

	_, err = f.client.PutSynchronizationConfiguration(ctx, &s3filessdk.PutSynchronizationConfigurationInput{
		FileSystemId:        fsID,
		ImportDataRules:     imp,
		ExpirationDataRules: []types.ExpirationDataRule{{DaysAfterLastAccess: aws.Int32(0)}},
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.PutSynchronizationConfiguration(ctx, &s3filessdk.PutSynchronizationConfigurationInput{
		FileSystemId: fsID, ImportDataRules: imp, ExpirationDataRules: exp,
	})
	require.NoError(t, err)

	got, err := f.client.GetSynchronizationConfiguration(
		ctx,
		&s3filessdk.GetSynchronizationConfigurationInput{FileSystemId: fsID},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(1), aws.ToInt32(got.LatestVersionNumber))
	assert.Equal(t, imp, got.ImportDataRules)
	assert.Equal(t, exp, got.ExpirationDataRules)

	_, err = f.client.PutSynchronizationConfiguration(ctx, &s3filessdk.PutSynchronizationConfigurationInput{
		FileSystemId: fsID, ImportDataRules: imp, ExpirationDataRules: exp,
	})
	assert.Equal(t, "ConflictException", apiCode(err), "update without version must conflict")

	_, err = f.client.PutSynchronizationConfiguration(ctx, &s3filessdk.PutSynchronizationConfigurationInput{
		FileSystemId:        fsID,
		ImportDataRules:     []types.ImportDataRule{},
		ExpirationDataRules: exp,
		LatestVersionNumber: got.LatestVersionNumber,
	})
	require.NoError(t, err)

	got, err = f.client.GetSynchronizationConfiguration(
		ctx,
		&s3filessdk.GetSynchronizationConfigurationInput{FileSystemId: fsID},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(got.LatestVersionNumber))
	assert.Empty(t, got.ImportDataRules)
}

func TestTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target func(t *testing.T, f *fixture) string
		name   string
	}{
		{name: "file system", target: func(t *testing.T, f *fixture) string {
			t.Helper()

			return f.fileSystem(t)
		}},
		{name: "access point arn", target: func(t *testing.T, f *fixture) string {
			t.Helper()

			ap, err := f.client.CreateAccessPoint(t.Context(), &s3filessdk.CreateAccessPointInput{
				FileSystemId: aws.String(f.fileSystem(t)),
			})
			require.NoError(t, err)

			return aws.ToString(ap.AccessPointArn)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ctx := t.Context()
			id := aws.String(tt.target(t, f))

			_, err := f.client.TagResource(ctx, &s3filessdk.TagResourceInput{ResourceId: id, Tags: []types.Tag{
				{Key: aws.String("a"), Value: aws.String("1")}, {Key: aws.String("b"), Value: aws.String("2")},
				{Key: aws.String("c"), Value: aws.String("3")},
			}})
			require.NoError(t, err)

			pager := s3filessdk.NewListTagsForResourcePaginator(f.client, &s3filessdk.ListTagsForResourceInput{
				ResourceId: id, MaxResults: aws.Int32(2),
			})

			var keys []string

			for pager.HasMorePages() {
				p, pErr := pager.NextPage(ctx)
				require.NoError(t, pErr)

				for _, tag := range p.Tags {
					keys = append(keys, aws.ToString(tag.Key))
				}
			}

			assert.Equal(t, []string{"a", "b", "c"}, keys)

			_, err = f.client.UntagResource(
				ctx,
				&s3filessdk.UntagResourceInput{ResourceId: id, TagKeys: []string{"a", "zz"}},
			)
			require.NoError(t, err)

			out, err := f.client.ListTagsForResource(ctx, &s3filessdk.ListTagsForResourceInput{ResourceId: id})
			require.NoError(t, err)
			assert.Len(t, out.Tags, 2)

			_, err = f.client.TagResource(ctx, &s3filessdk.TagResourceInput{
				ResourceId: aws.String("fs-none"), Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			_, err = f.client.TagResource(ctx, &s3filessdk.TagResourceInput{
				ResourceId: id, Tags: []types.Tag{{Key: aws.String("aws:reserved"), Value: aws.String("v")}},
			})
			assert.Equal(t, "ValidationException", apiCode(err))
		})
	}
}
