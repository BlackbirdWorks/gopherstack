package s3_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRenameObjectDestinationPreconditions is a regression test for
// gopherstack-qfko: RenameObjectInput declares DestinationIfMatch,
// DestinationIfNoneMatch, DestinationIfModifiedSince and
// DestinationIfUnmodifiedSince but handleRenameObject enforced none of them,
// so a conditional rename that should fail with 412 silently succeeded
// instead. Each case proves both directions: the condition that should block
// the rename does block it with PreconditionFailed, and the condition that
// should let it through does.
func TestRenameObjectDestinationPreconditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build     func(dstETag string, dstModified time.Time) sdk_s3.RenameObjectInput
		name      string
		dstExists bool
		wantFail  bool
	}{
		{
			name:      "if none match star blocks existing destination",
			dstExists: true,
			build: func(string, time.Time) sdk_s3.RenameObjectInput {
				return sdk_s3.RenameObjectInput{DestinationIfNoneMatch: aws.String("*")}
			},
			wantFail: true,
		},
		{
			name:      "if none match star allows missing destination",
			dstExists: false,
			build: func(string, time.Time) sdk_s3.RenameObjectInput {
				return sdk_s3.RenameObjectInput{DestinationIfNoneMatch: aws.String("*")}
			},
			wantFail: false,
		},
		{
			name:      "if match wrong etag blocks",
			dstExists: true,
			build: func(string, time.Time) sdk_s3.RenameObjectInput {
				return sdk_s3.RenameObjectInput{
					DestinationIfMatch: aws.String(`"deadbeefdeadbeefdeadbeefdeadbeef"`),
				}
			},
			wantFail: true,
		},
		{
			name:      "if match correct etag allows",
			dstExists: true,
			build: func(dstETag string, _ time.Time) sdk_s3.RenameObjectInput {
				return sdk_s3.RenameObjectInput{DestinationIfMatch: aws.String(dstETag)}
			},
			wantFail: false,
		},
		{
			name:      "if unmodified since past blocks modified destination",
			dstExists: true,
			build: func(_ string, dstModified time.Time) sdk_s3.RenameObjectInput {
				past := dstModified.Add(-1 * time.Hour)

				return sdk_s3.RenameObjectInput{DestinationIfUnmodifiedSince: &past}
			},
			wantFail: true,
		},
		{
			name:      "if unmodified since future allows unmodified destination",
			dstExists: true,
			build: func(_ string, dstModified time.Time) sdk_s3.RenameObjectInput {
				future := dstModified.Add(1 * time.Hour)

				return sdk_s3.RenameObjectInput{DestinationIfUnmodifiedSince: &future}
			},
			wantFail: false,
		},
		{
			name:      "if modified since future blocks unmodified destination",
			dstExists: true,
			build: func(_ string, dstModified time.Time) sdk_s3.RenameObjectInput {
				future := dstModified.Add(1 * time.Hour)

				return sdk_s3.RenameObjectInput{DestinationIfModifiedSince: &future}
			},
			wantFail: true,
		},
		{
			name:      "if modified since past allows modified destination",
			dstExists: true,
			build: func(_ string, dstModified time.Time) sdk_s3.RenameObjectInput {
				past := dstModified.Add(-1 * time.Hour)

				return sdk_s3.RenameObjectInput{DestinationIfModifiedSince: &past}
			},
			wantFail: false,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := fmt.Sprintf("rename-precond-%d", i)

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("src"), Body: strings.NewReader("source-body"),
			})
			require.NoError(t, err)

			var dstETag string
			var dstModified time.Time
			if tc.dstExists {
				put, putErr := client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("dst"), Body: strings.NewReader("dest-body"),
				})
				require.NoError(t, putErr)
				dstETag = aws.ToString(put.ETag)

				head, headErr := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("dst"),
				})
				require.NoError(t, headErr)
				dstModified = aws.ToTime(head.LastModified)
			}

			in := tc.build(dstETag, dstModified)
			in.Bucket = aws.String(bucket)
			in.Key = aws.String("dst")
			in.RenameSource = aws.String(bucket + "/src")

			_, err = client.RenameObject(t.Context(), &in)

			if tc.wantFail {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "PreconditionFailed")

				return
			}

			require.NoError(t, err)
		})
	}
}
