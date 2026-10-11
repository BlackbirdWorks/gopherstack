package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestHandler_PutObject_ChecksumHeaderWithoutAlgorithm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		algo   string
		header string
	}{
		{name: "crc32", algo: "CRC32", header: "X-Amz-Checksum-Crc32"},
		{name: "crc32c", algo: "CRC32C", header: "X-Amz-Checksum-Crc32c"},
		{name: "sha1", algo: "SHA1", header: "X-Amz-Checksum-Sha1"},
		{name: "sha256", algo: "SHA256", header: "X-Amz-Checksum-Sha256"},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_stored", func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			body := "inferred checksum body"
			want := s3.CalculateChecksum([]byte(body), tt.algo)

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader(body))
			req.Header.Set(tt.header, want)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			out, err := backend.HeadObject(
				t.Context(),
				&sdk_s3.HeadObjectInput{Bucket: aws.String("bkt"), Key: aws.String("key")},
			)
			require.NoError(t, err)

			got := map[string]*string{
				"CRC32": out.ChecksumCRC32, "CRC32C": out.ChecksumCRC32C,
				"SHA1": out.ChecksumSHA1, "SHA256": out.ChecksumSHA256,
			}[tt.algo]
			require.NotNil(t, got, "checksum must be stored from the value header alone")
			assert.Equal(t, want, *got)
		})

		t.Run(tt.name+"_mismatch", func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			body := "inferred checksum body"
			correct := s3.CalculateChecksum([]byte(body), tt.algo)

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader(body))
			req.Header.Set(tt.header, correct[:len(correct)-1]+"X")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "BadDigest")
		})
	}
}
