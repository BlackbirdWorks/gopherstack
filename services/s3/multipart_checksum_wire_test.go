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

// TestUploadPart_ChecksumEchoedInResponseAndListParts is a regression test for
// gopherstack-3dqa's deep pass: UploadPartOutput.ChecksumCRC32/-CRC32C/-SHA1/-SHA256
// are header-bound (s3@v1.106.5 deserializers.go's
// awsRestxml_deserializeOpHttpBindingsUploadPartOutput reads x-amz-checksum-crc32
// etc from the response headers), and the backend already computes and verifies
// them (multipart.go's UploadPart) -- but the HTTP handler only ever wrote the
// ETag header, discarding the computed checksum entirely, and StoredPart never
// persisted it, so ListParts could never report it either.
func TestUploadPart_ChecksumEchoedInResponseAndListParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		validatePart func(t *testing.T, part *sdk_s3.UploadPartOutput, listed types.Part)
		name         string
		algo         types.ChecksumAlgorithm
	}{
		{
			name: "crc32",
			algo: types.ChecksumAlgorithmCrc32,
			validatePart: func(t *testing.T, part *sdk_s3.UploadPartOutput, listed types.Part) {
				t.Helper()
				require.NotNil(t, part.ChecksumCRC32)
				assert.NotEmpty(t, *part.ChecksumCRC32)
				require.NotNil(t, listed.ChecksumCRC32)
				assert.Equal(t, *part.ChecksumCRC32, *listed.ChecksumCRC32)
			},
		},
		{
			name: "crc32c",
			algo: types.ChecksumAlgorithmCrc32c,
			validatePart: func(t *testing.T, part *sdk_s3.UploadPartOutput, listed types.Part) {
				t.Helper()
				require.NotNil(t, part.ChecksumCRC32C)
				assert.NotEmpty(t, *part.ChecksumCRC32C)
				require.NotNil(t, listed.ChecksumCRC32C)
				assert.Equal(t, *part.ChecksumCRC32C, *listed.ChecksumCRC32C)
			},
		},
		{
			name: "crc64nvme",
			algo: types.ChecksumAlgorithmCrc64nvme,
			validatePart: func(t *testing.T, part *sdk_s3.UploadPartOutput, listed types.Part) {
				t.Helper()
				require.NotNil(t, part.ChecksumCRC64NVME)
				assert.NotEmpty(t, *part.ChecksumCRC64NVME)
				require.NotNil(t, listed.ChecksumCRC64NVME)
				assert.Equal(t, *part.ChecksumCRC64NVME, *listed.ChecksumCRC64NVME)
			},
		},
		{
			name: "sha1",
			algo: types.ChecksumAlgorithmSha1,
			validatePart: func(t *testing.T, part *sdk_s3.UploadPartOutput, listed types.Part) {
				t.Helper()
				require.NotNil(t, part.ChecksumSHA1)
				assert.NotEmpty(t, *part.ChecksumSHA1)
				require.NotNil(t, listed.ChecksumSHA1)
				assert.Equal(t, *part.ChecksumSHA1, *listed.ChecksumSHA1)
			},
		},
		{
			name: "sha256",
			algo: types.ChecksumAlgorithmSha256,
			validatePart: func(t *testing.T, part *sdk_s3.UploadPartOutput, listed types.Part) {
				t.Helper()
				require.NotNil(t, part.ChecksumSHA256)
				assert.NotEmpty(t, *part.ChecksumSHA256)
				require.NotNil(t, listed.ChecksumSHA256)
				assert.Equal(t, *part.ChecksumSHA256, *listed.ChecksumSHA256)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "mp-checksum-bucket-" + tt.name
			key := "obj.bin"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			created, err := client.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)
			uploadID := created.UploadId

			part, err := client.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:            aws.String(bucket),
				Key:               aws.String(key),
				UploadId:          uploadID,
				PartNumber:        aws.Int32(1),
				Body:              strings.NewReader("payload-bytes-for-checksum"),
				ChecksumAlgorithm: tt.algo,
			})
			require.NoError(t, err)

			listed, err := client.ListParts(t.Context(), &sdk_s3.ListPartsInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: uploadID,
			})
			require.NoError(t, err)
			require.Len(t, listed.Parts, 1)
			tt.validatePart(t, part, listed.Parts[0])
		})
	}
}

// TestListObjectVersions_ChecksumAlgorithmPopulated is a regression test for
// gopherstack-3dqa: StoredObjectVersion.ChecksumAlgorithm is tracked per version
// (types.go) and already threaded through ListObjectsV2's ObjectXML, but
// ListObjectVersions' ObjectVersionXML never carried the field at all -- a
// versioned object's ChecksumAlgorithm was reported for its current version and
// silently dropped for the exact same data one API over.
func TestListObjectVersions_ChecksumAlgorithmPopulated(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "list-versions-checksum-bucket"
	key := "obj.bin"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)

	_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              strings.NewReader("payload-bytes-for-checksum"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)

	listed, err := client.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listed.Versions, 1)
	require.Len(t, listed.Versions[0].ChecksumAlgorithm, 1,
		"ListObjectVersions must report the same checksum algorithm ListObjectsV2 already does")
	assert.Equal(t, types.ChecksumAlgorithmSha256, listed.Versions[0].ChecksumAlgorithm[0])
}
