package s3_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestCompleteMultipartUpload_PartETagMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(etag string) string
		wantErr error
		name    string
	}{
		{name: "quoted", mutate: func(e string) string { return e }},
		{name: "unquoted", mutate: func(e string) string { return strings.Trim(e, `"`) }},
		{
			name:    "wrong",
			mutate:  func(string) string { return `"00000000000000000000000000000000"` },
			wantErr: s3.ErrInvalidPart,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "etag-bkt")
			f := newMPUFixture(t, backend, "etag-bkt", "k")

			_, err := backend.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket: aws.String("etag-bkt"), Key: aws.String("k"), UploadId: f.id,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{ETag: aws.String(tt.mutate(*f.etag)), PartNumber: aws.Int32(1)}},
				},
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}
