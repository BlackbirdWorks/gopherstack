package s3_test

import (
	"crypto/md5"
	"crypto/sha512"
	"encoding/base64"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func b64Sum(sum []byte) string { return base64.StdEncoding.EncodeToString(sum) }

func TestRealClient_PutObjectMD5SHA512Checksum(t *testing.T) {
	t.Parallel()

	body := "checksum-payload"
	md5Sum := md5.Sum([]byte(body))
	sha512Sum := sha512.Sum512([]byte(body))

	tests := []struct {
		put     func(in *sdk_s3.PutObjectInput)
		get     func(h *sdk_s3.HeadObjectOutput) *string
		attrs   func(a *sdk_s3.GetObjectAttributesOutput) *string
		name    string
		want    string
		wantErr string
	}{
		{
			name: "md5",
			want: b64Sum(md5Sum[:]),
			put: func(in *sdk_s3.PutObjectInput) {
				in.ChecksumMD5 = aws.String(b64Sum(md5Sum[:]))
			},
			get:   func(h *sdk_s3.HeadObjectOutput) *string { return h.ChecksumMD5 },
			attrs: func(a *sdk_s3.GetObjectAttributesOutput) *string { return a.Checksum.ChecksumMD5 },
		},
		{
			name: "sha512",
			want: b64Sum(sha512Sum[:]),
			put: func(in *sdk_s3.PutObjectInput) {
				in.ChecksumSHA512 = aws.String(b64Sum(sha512Sum[:]))
			},
			get:   func(h *sdk_s3.HeadObjectOutput) *string { return h.ChecksumSHA512 },
			attrs: func(a *sdk_s3.GetObjectAttributesOutput) *string { return a.Checksum.ChecksumSHA512 },
		},
		{
			name: "sha512 server computed",
			want: b64Sum(sha512Sum[:]),
			put: func(in *sdk_s3.PutObjectInput) {
				in.ChecksumAlgorithm = types.ChecksumAlgorithmSha512
			},
			get:   func(h *sdk_s3.HeadObjectOutput) *string { return h.ChecksumSHA512 },
			attrs: func(a *sdk_s3.GetObjectAttributesOutput) *string { return a.Checksum.ChecksumSHA512 },
		},
		{
			name:    "md5 mismatch",
			wantErr: "BadDigest",
			put: func(in *sdk_s3.PutObjectInput) {
				in.ChecksumMD5 = aws.String(b64Sum(make([]byte, md5.Size)))
			},
		},
		{
			name:    "sha512 mismatch",
			wantErr: "BadDigest",
			put: func(in *sdk_s3.PutObjectInput) {
				in.ChecksumSHA512 = aws.String(b64Sum(make([]byte, sha512.Size)))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "md5-sha512-bucket"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			in := &sdk_s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String("k"),
				Body:   strings.NewReader(body),
			}
			tt.put(in)

			_, err = client.PutObject(t.Context(), in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			head, err := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), ChecksumMode: types.ChecksumModeEnabled,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(tt.get(head)))

			attrs, err := client.GetObjectAttributes(t.Context(), &sdk_s3.GetObjectAttributesInput{
				Bucket: aws.String(bucket), Key: aws.String("k"),
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesChecksum},
			})
			require.NoError(t, err)
			require.NotNil(t, attrs.Checksum)
			assert.Equal(t, tt.want, aws.ToString(tt.attrs(attrs)))
		})
	}
}

func TestRealClient_MultipartObjectChecksum(t *testing.T) {
	t.Parallel()

	body := "multipart-checksum-body"
	md5Sum := md5.Sum([]byte(body))
	sha512Sum := sha512.Sum512([]byte(body))
	crc := crc32.ChecksumIEEE([]byte(body))
	crcSum := []byte{byte(crc >> 24), byte(crc >> 16), byte(crc >> 8), byte(crc)}

	md5Outer := md5.Sum(md5Sum[:])
	sha512Outer := sha512.Sum512(sha512Sum[:])

	tests := []struct {
		partValue func(in *sdk_s3.UploadPartInput)
		got       func(h *sdk_s3.HeadObjectOutput) *string
		name      string
		algo      types.ChecksumAlgorithm
		ctype     types.ChecksumType
		want      string
	}{
		{
			name: "md5 composite", algo: types.ChecksumAlgorithmMd5, ctype: types.ChecksumTypeComposite,
			want:      b64Sum(md5Outer[:]) + "-1",
			partValue: func(in *sdk_s3.UploadPartInput) { in.ChecksumMD5 = aws.String(b64Sum(md5Sum[:])) },
			got:       func(h *sdk_s3.HeadObjectOutput) *string { return h.ChecksumMD5 },
		},
		{
			name: "sha512 composite", algo: types.ChecksumAlgorithmSha512, ctype: types.ChecksumTypeComposite,
			want:      b64Sum(sha512Outer[:]) + "-1",
			partValue: func(in *sdk_s3.UploadPartInput) { in.ChecksumSHA512 = aws.String(b64Sum(sha512Sum[:])) },
			got:       func(h *sdk_s3.HeadObjectOutput) *string { return h.ChecksumSHA512 },
		},
		{
			name: "md5 full object", algo: types.ChecksumAlgorithmMd5, ctype: types.ChecksumTypeFullObject,
			want:      b64Sum(md5Sum[:]),
			partValue: func(in *sdk_s3.UploadPartInput) { in.ChecksumMD5 = aws.String(b64Sum(md5Sum[:])) },
			got:       func(h *sdk_s3.HeadObjectOutput) *string { return h.ChecksumMD5 },
		},
		{
			name: "crc32 full object", algo: types.ChecksumAlgorithmCrc32, ctype: types.ChecksumTypeFullObject,
			want:      b64Sum(crcSum),
			partValue: func(in *sdk_s3.UploadPartInput) { in.ChecksumCRC32 = aws.String(b64Sum(crcSum)) },
			got:       func(h *sdk_s3.HeadObjectOutput) *string { return h.ChecksumCRC32 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "mpu-object-checksum"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			created, err := client.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String("k"),
				ChecksumAlgorithm: tt.algo, ChecksumType: tt.ctype,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.algo, created.ChecksumAlgorithm)
			assert.Equal(t, tt.ctype, created.ChecksumType)

			upIn := &sdk_s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: created.UploadId,
				PartNumber: aws.Int32(1), Body: strings.NewReader(body),
			}
			tt.partValue(upIn)

			part, err := client.UploadPart(t.Context(), upIn)
			require.NoError(t, err)

			done, err := client.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), UploadId: created.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.ctype, done.ChecksumType)

			head, err := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("k"), ChecksumMode: types.ChecksumModeEnabled,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(tt.got(head)))
			assert.Equal(t, tt.ctype, head.ChecksumType)
		})
	}
}
