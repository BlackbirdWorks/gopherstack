package s3vectors_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	created, err := f.client.CreateVectorBucket(ctx, &sdk.CreateVectorBucketInput{
		VectorBucketName: aws.String("my-vectors"), Tags: map[string]string{"team": "a"},
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"arn:aws:s3vectors:us-east-1:"+testAccount+":bucket/my-vectors",
		aws.ToString(created.VectorBucketArn),
	)

	got, err := f.client.GetVectorBucket(ctx, &sdk.GetVectorBucketInput{VectorBucketArn: created.VectorBucketArn})
	require.NoError(t, err)
	assert.Equal(t, "my-vectors", aws.ToString(got.VectorBucket.VectorBucketName))
	assert.Equal(t, types.SseTypeAes256, got.VectorBucket.EncryptionConfiguration.SseType)
	assert.Equal(
		t,
		types.IndexModeClassic,
		got.VectorBucket.DefaultIndexMode,
		"buckets before the cutoff default to CLASSIC",
	)
	assert.WithinDuration(
		t,
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		aws.ToTime(got.VectorBucket.CreationTime),
		time.Second,
	)

	_, err = f.client.CreateVectorBucket(ctx, &sdk.CreateVectorBucketInput{VectorBucketName: aws.String("my-vectors")})
	assert.Equal(t, "ConflictException", apiCode(err))

	tags, err := f.client.ListTagsForResource(ctx, &sdk.ListTagsForResourceInput{ResourceArn: created.VectorBucketArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "a"}, tags.Tags)

	_, err = f.client.TagResource(
		ctx,
		&sdk.TagResourceInput{ResourceArn: created.VectorBucketArn, Tags: map[string]string{"env": "dev"}},
	)
	require.NoError(t, err)
	_, err = f.client.UntagResource(
		ctx,
		&sdk.UntagResourceInput{ResourceArn: created.VectorBucketArn, TagKeys: []string{"team"}},
	)
	require.NoError(t, err)

	tags, err = f.client.ListTagsForResource(ctx, &sdk.ListTagsForResourceInput{ResourceArn: created.VectorBucketArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "dev"}, tags.Tags)

	f.index(t, "my-vectors", "idx", "cosine", 3)

	_, err = f.client.DeleteVectorBucket(ctx, &sdk.DeleteVectorBucketInput{VectorBucketName: aws.String("my-vectors")})
	assert.Equal(t, "ConflictException", apiCode(err), "a bucket with indexes cannot be deleted")

	_, err = f.client.DeleteIndex(
		ctx,
		&sdk.DeleteIndexInput{VectorBucketName: aws.String("my-vectors"), IndexName: aws.String("idx")},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteVectorBucket(ctx, &sdk.DeleteVectorBucketInput{VectorBucketName: aws.String("my-vectors")})
	require.NoError(t, err)

	_, err = f.client.GetVectorBucket(ctx, &sdk.GetVectorBucketInput{VectorBucketName: aws.String("my-vectors")})
	assert.Equal(t, "NotFoundException", apiCode(err))
}

func TestBucketValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	tests := []struct {
		name     string
		in       *sdk.CreateVectorBucketInput
		wantCode string
	}{
		{"too short", &sdk.CreateVectorBucketInput{VectorBucketName: aws.String("ab")}, "ValidationException"},
		{"uppercase", &sdk.CreateVectorBucketInput{VectorBucketName: aws.String("Bad-Name")}, "ValidationException"},
		{"underscore", &sdk.CreateVectorBucketInput{VectorBucketName: aws.String("bad_name")}, "ValidationException"},
		{"kms ok", &sdk.CreateVectorBucketInput{
			VectorBucketName: aws.String("kms-bucket"),
			EncryptionConfiguration: &types.EncryptionConfiguration{
				SseType:   types.SseTypeAwsKms,
				KmsKeyArn: aws.String("arn:aws:kms:us-east-1:" + testAccount + ":key/abc"),
			},
		}, ""},
		{"kms bad arn", &sdk.CreateVectorBucketInput{
			VectorBucketName: aws.String("kms-bad"),
			EncryptionConfiguration: &types.EncryptionConfiguration{
				SseType:   types.SseTypeAwsKms,
				KmsKeyArn: aws.String("nope"),
			},
		}, "ValidationException"},
		{"aes with key", &sdk.CreateVectorBucketInput{
			VectorBucketName: aws.String("aes-key"),
			EncryptionConfiguration: &types.EncryptionConfiguration{
				SseType:   types.SseTypeAes256,
				KmsKeyArn: aws.String("arn:aws:kms:us-east-1:" + testAccount + ":key/abc"),
			},
		}, "ValidationException"},
		{
			"reserved tag",
			&sdk.CreateVectorBucketInput{
				VectorBucketName: aws.String("tag-bad"),
				Tags:             map[string]string{"aws:x": "y"},
			},
			"ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.CreateVectorBucket(t.Context(), tt.in)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestBucketListing(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	for i := range 5 {
		f.bucket(t, fmt.Sprintf("list-%d", i))
	}

	f.bucket(t, "other")

	var names []string

	in := &sdk.ListVectorBucketsInput{Prefix: aws.String("list-"), MaxResults: aws.Int32(2)}
	pages := 0

	for {
		out, err := f.client.ListVectorBuckets(t.Context(), in)
		require.NoError(t, err)

		pages++

		for _, b := range out.VectorBuckets {
			names = append(names, aws.ToString(b.VectorBucketName))
		}

		if out.NextToken == nil {
			break
		}

		in.NextToken = out.NextToken
	}

	assert.Equal(t, []string{"list-0", "list-1", "list-2", "list-3", "list-4"}, names)
	assert.Equal(t, 3, pages)

	_, err := f.client.ListVectorBuckets(t.Context(), &sdk.ListVectorBucketsInput{MaxResults: aws.Int32(501)})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestBucketPolicyAndMode(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	f.bucket(t, "policy-bucket")

	name := aws.String("policy-bucket")

	_, err := f.client.GetVectorBucketPolicy(ctx, &sdk.GetVectorBucketPolicyInput{VectorBucketName: name})
	assert.Equal(t, "NotFoundException", apiCode(err))

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
		`"Action":"s3vectors:GetVectors","Resource":"*"}]}`
	_, err = f.client.PutVectorBucketPolicy(
		ctx,
		&sdk.PutVectorBucketPolicyInput{VectorBucketName: name, Policy: aws.String(doc)},
	)
	require.NoError(t, err)

	got, err := f.client.GetVectorBucketPolicy(ctx, &sdk.GetVectorBucketPolicyInput{VectorBucketName: name})
	require.NoError(t, err)
	assert.JSONEq(t, doc, aws.ToString(got.Policy))

	_, err = f.client.PutVectorBucketPolicy(
		ctx,
		&sdk.PutVectorBucketPolicyInput{VectorBucketName: name, Policy: aws.String("{}")},
	)
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.DeleteVectorBucketPolicy(ctx, &sdk.DeleteVectorBucketPolicyInput{VectorBucketName: name})
	require.NoError(t, err)

	_, err = f.client.GetVectorBucketPolicy(ctx, &sdk.GetVectorBucketPolicyInput{VectorBucketName: name})
	assert.Equal(t, "NotFoundException", apiCode(err))

	_, err = f.client.PutVectorBucketDefaultIndexMode(ctx, &sdk.PutVectorBucketDefaultIndexModeInput{
		VectorBucketName: name, DefaultIndexMode: types.IndexModeEnhanced,
	})
	require.NoError(t, err)

	b, err := f.client.GetVectorBucket(ctx, &sdk.GetVectorBucketInput{VectorBucketName: name})
	require.NoError(t, err)
	assert.Equal(t, types.IndexModeEnhanced, b.VectorBucket.DefaultIndexMode)

	f.index(t, "policy-bucket", "inherits", "cosine", 2)

	idx, err := f.client.GetIndex(ctx, &sdk.GetIndexInput{VectorBucketName: name, IndexName: aws.String("inherits")})
	require.NoError(t, err)
	assert.Equal(t, types.IndexModeEnhanced, idx.Index.IndexMode, "new indexes take the bucket default")
}
