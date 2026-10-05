package s3_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_CompleteMultipartUploadObjectSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    *int64
		wantErr string
	}{
		{name: "absent", size: nil},
		{name: "match", size: aws.Int64(5)},
		{name: "mismatch", size: aws.Int64(6), wantErr: "InvalidRequest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "mpu-size"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			created, err := client.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String("k"),
			})
			require.NoError(t, err)

			part, err := client.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: created.UploadId,
				PartNumber: aws.Int32(1), Body: strings.NewReader("hello"),
			})
			require.NoError(t, err)

			_, err = client.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: created.UploadId, MpuObjectSize: tt.size,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
				},
			})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			head, err := client.HeadObject(
				t.Context(), &sdk_s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")},
			)
			require.NoError(t, err)
			assert.EqualValues(t, 5, aws.ToInt64(head.ContentLength))
		})
	}
}
