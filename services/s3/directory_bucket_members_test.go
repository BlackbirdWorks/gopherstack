package s3_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3Express_ListDirectoryBucketsPagination(t *testing.T) {
	t.Parallel()

	client := newRealS3ExpressClientTest(t)
	ctx := t.Context()

	for _, name := range []string{"pg-a--use1-az4--x-s3", "pg-b--use1-az4--x-s3", "pg-c--use1-az4--x-s3"} {
		_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(name)})
		require.NoError(t, err)
	}

	var names []string

	var token *string

	pages := 0

	for {
		out, err := client.ListDirectoryBuckets(ctx, &sdk_s3.ListDirectoryBucketsInput{
			MaxDirectoryBuckets: aws.Int32(2), ContinuationToken: token,
		})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(out.Buckets), 2)

		for _, b := range out.Buckets {
			names = append(names, aws.ToString(b.Name))
		}

		pages++

		if out.ContinuationToken == nil {
			break
		}

		token = out.ContinuationToken
	}

	assert.Equal(t, []string{"pg-a--use1-az4--x-s3", "pg-b--use1-az4--x-s3", "pg-c--use1-az4--x-s3"}, names)
	assert.Equal(t, 2, pages)
}

func TestS3Express_DeleteObjectConditionalMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		size        *int64
		modShift    *time.Duration
		name        string
		ifMatch     string
		wantDeleted bool
	}{
		{name: "size_match", size: aws.Int64(5), wantDeleted: true},
		{name: "size_mismatch", size: aws.Int64(6)},
		{name: "modified_match", modShift: aws.Duration(0), wantDeleted: true},
		{name: "modified_mismatch", modShift: aws.Duration(-time.Hour)},
		{name: "wildcard_if_match", ifMatch: "*", wantDeleted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ExpressClientTest(t)
			bucket := "cond-" + strings.ReplaceAll(tt.name, "_", "-") + "--use1-az4--x-s3"
			obj := &sdk_s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")}

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: obj.Bucket, Key: obj.Key, Body: strings.NewReader("hello"),
			})
			require.NoError(t, err)

			head, err := client.HeadObject(t.Context(), obj)
			require.NoError(t, err)

			in := &sdk_s3.DeleteObjectInput{Bucket: obj.Bucket, Key: obj.Key, IfMatchSize: tt.size}
			if tt.ifMatch != "" {
				in.IfMatch = aws.String(tt.ifMatch)
			}

			if tt.modShift != nil {
				in.IfMatchLastModifiedTime = aws.Time(aws.ToTime(head.LastModified).Add(*tt.modShift))
			}

			_, err = client.DeleteObject(t.Context(), in)
			_, headErr := client.HeadObject(t.Context(), obj)

			if tt.wantDeleted {
				require.NoError(t, err)
				require.Error(t, headErr)

				return
			}

			require.ErrorContains(t, err, "PreconditionFailed")
			require.NoError(t, headErr)
		})
	}
}
