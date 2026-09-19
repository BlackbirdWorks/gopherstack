package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestControlPlaneChecksumAlgorithm_RealClientRoundTrips is a regression test
// for gopherstack-xhu2t: PutBucketPolicy/PutBucketEncryption's ChecksumAlgorithm
// field (s3@v1.111.0 serializers.go, X-Amz-Sdk-Checksum-Algorithm) was read
// nowhere. EnableTrailingChecksum is false for these ops
// (service/internal/checksum@v1.11.2 middleware_compute_input_checksum.go), so
// the real SDK computes the checksum client-side and sends it as a plain
// X-Amz-Checksum-<Algo> header; a real typed client setting ChecksumAlgorithm
// must still succeed once the server verifies it.
func TestControlPlaneChecksumAlgorithm_RealClientRoundTrips(t *testing.T) {
	t.Parallel()

	algos := []types.ChecksumAlgorithm{
		types.ChecksumAlgorithmCrc32,
		types.ChecksumAlgorithmCrc32c,
		types.ChecksumAlgorithmSha1,
		types.ChecksumAlgorithmSha256,
	}

	for _, algo := range algos {
		t.Run("putbucketpolicy_"+string(algo), func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "checksum-policy-" + strings.ToLower(string(algo))

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			policy := `{"Version":"2012-10-17","Statement":[{"Sid":"s","Effect":"Allow",` +
				`"Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::` + bucket + `/*"}]}`

			_, err = client.PutBucketPolicy(t.Context(), &sdk_s3.PutBucketPolicyInput{
				Bucket:            aws.String(bucket),
				Policy:            aws.String(policy),
				ChecksumAlgorithm: algo,
			})
			require.NoError(t, err)

			out, err := client.GetBucketPolicy(t.Context(), &sdk_s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)
			assert.Equal(t, policy, aws.ToString(out.Policy))
		})

		t.Run("putbucketencryption_"+string(algo), func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "checksum-enc-" + strings.ToLower(string(algo))

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			_, err = client.PutBucketEncryption(t.Context(), &sdk_s3.PutBucketEncryptionInput{
				Bucket: aws.String(bucket),
				ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
					Rules: []types.ServerSideEncryptionRule{
						{
							ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
								SSEAlgorithm: types.ServerSideEncryptionAes256,
							},
						},
					},
				},
				ChecksumAlgorithm: algo,
			})
			require.NoError(t, err)

			out, err := client.GetBucketEncryption(
				t.Context(),
				&sdk_s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)},
			)
			require.NoError(t, err)
			require.Len(t, out.ServerSideEncryptionConfiguration.Rules, 1)
			assert.Equal(
				t,
				types.ServerSideEncryptionAes256,
				out.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm,
			)
		})
	}
}

// TestControlPlaneChecksumAlgorithm_MismatchRejected proves a tampered/wrong
// checksum is actually verified, not just accepted-and-ignored: a raw request
// declaring CRC32 with a checksum that doesn't match the body must be
// rejected with 400 BadDigest, matching real S3's whole-request-checksum
// validation for these ops.
func TestControlPlaneChecksumAlgorithm_MismatchRejected(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	bucket := "checksum-mismatch-bucket"
	mustCreateBucket(t, backend, bucket)

	policy := `{"Version":"2012-10-17","Statement":[{"Sid":"s","Effect":"Allow",` +
		`"Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::` + bucket + `/*"}]}`

	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?policy", strings.NewReader(policy))
	req.Header.Set("X-Amz-Sdk-Checksum-Algorithm", "CRC32")
	req.Header.Set("X-Amz-Checksum-Crc32", "AAAAAA==") // wrong: doesn't match policy body
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "BadDigest")
}
