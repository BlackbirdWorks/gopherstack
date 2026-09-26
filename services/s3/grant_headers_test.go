package s3_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestGrantHeaders is a regression test for gopherstack-xhu2t: x-amz-grant-*
// headers (s3@v1.111.0 serializers.go: GrantFullControl/GrantRead/
// GrantReadACP/GrantWrite/GrantWriteACP) were read nowhere in this service --
// PutBucketAcl, PutObjectAcl and CreateMultipartUpload all silently dropped
// them, leaving every grantee out of the resulting ACL. Each subtest grants a
// specific permission to a specific canonical-user ID via the corresponding
// header and asserts the grant is actually present on read-back through the
// real typed client.
func TestGrantHeaders(t *testing.T) {
	t.Parallel()

	const granteeID = "111122223333"

	tests := []struct {
		grantOnCreateMultipart func(*sdk_s3.CreateMultipartUploadInput)
		grantOnPutBucketACL    func(*sdk_s3.PutBucketAclInput)
		grantOnPutObjectACL    func(*sdk_s3.PutObjectAclInput)
		name                   string
		wantPerm               types.Permission
	}{
		{
			name:     "full_control",
			wantPerm: types.PermissionFullControl,
			grantOnCreateMultipart: func(in *sdk_s3.CreateMultipartUploadInput) {
				in.GrantFullControl = aws.String(`id="` + granteeID + `"`)
			},
			grantOnPutBucketACL: func(in *sdk_s3.PutBucketAclInput) {
				in.GrantFullControl = aws.String(`id="` + granteeID + `"`)
			},
			grantOnPutObjectACL: func(in *sdk_s3.PutObjectAclInput) {
				in.GrantFullControl = aws.String(`id="` + granteeID + `"`)
			},
		},
		{
			name:     "read",
			wantPerm: types.PermissionRead,
			grantOnCreateMultipart: func(in *sdk_s3.CreateMultipartUploadInput) {
				in.GrantRead = aws.String(`id="` + granteeID + `"`)
			},
			grantOnPutBucketACL: func(in *sdk_s3.PutBucketAclInput) {
				in.GrantRead = aws.String(`id="` + granteeID + `"`)
			},
			grantOnPutObjectACL: func(in *sdk_s3.PutObjectAclInput) {
				in.GrantRead = aws.String(`id="` + granteeID + `"`)
			},
		},
		{
			name:     "read_acp",
			wantPerm: types.PermissionReadAcp,
			grantOnCreateMultipart: func(in *sdk_s3.CreateMultipartUploadInput) {
				in.GrantReadACP = aws.String(`id="` + granteeID + `"`)
			},
			grantOnPutBucketACL: func(in *sdk_s3.PutBucketAclInput) {
				in.GrantReadACP = aws.String(`id="` + granteeID + `"`)
			},
			grantOnPutObjectACL: func(in *sdk_s3.PutObjectAclInput) {
				in.GrantReadACP = aws.String(`id="` + granteeID + `"`)
			},
		},
		{
			name:     "write_acp",
			wantPerm: types.PermissionWriteAcp,
			grantOnCreateMultipart: func(in *sdk_s3.CreateMultipartUploadInput) {
				in.GrantWriteACP = aws.String(`id="` + granteeID + `"`)
			},
			grantOnPutBucketACL: func(in *sdk_s3.PutBucketAclInput) {
				in.GrantWriteACP = aws.String(`id="` + granteeID + `"`)
			},
			grantOnPutObjectACL: func(in *sdk_s3.PutObjectAclInput) {
				in.GrantWriteACP = aws.String(`id="` + granteeID + `"`)
			},
		},
	}

	for _, tt := range tests {
		t.Run("createmultipartupload_"+tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "grant-cmu-" + strings.ReplaceAll(tt.name, "_", "-")
			key := "obj"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			in := &sdk_s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)}
			tt.grantOnCreateMultipart(in)

			created, err := client.CreateMultipartUpload(t.Context(), in)
			require.NoError(t, err)

			partOut, err := client.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   created.UploadId,
				PartNumber: aws.Int32(1),
				Body:       strings.NewReader("body"),
			})
			require.NoError(t, err)

			_, err = client.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: created.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{ETag: partOut.ETag, PartNumber: aws.Int32(1)}},
				},
			})
			require.NoError(t, err)

			out, err := client.GetObjectAcl(t.Context(), &sdk_s3.GetObjectAclInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)
			requireGrant(t, out.Grants, granteeID, tt.wantPerm)
		})

		t.Run("putbucketacl_"+tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "grant-pba-" + strings.ReplaceAll(tt.name, "_", "-")

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			in := &sdk_s3.PutBucketAclInput{Bucket: aws.String(bucket)}
			tt.grantOnPutBucketACL(in)

			_, err = client.PutBucketAcl(t.Context(), in)
			require.NoError(t, err)

			out, err := client.GetBucketAcl(t.Context(), &sdk_s3.GetBucketAclInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)
			requireGrant(t, out.Grants, granteeID, tt.wantPerm)
		})

		t.Run("putobjectacl_"+tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "grant-poa-" + strings.ReplaceAll(tt.name, "_", "-")
			key := "obj"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("body"),
			})
			require.NoError(t, err)

			in := &sdk_s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)}
			tt.grantOnPutObjectACL(in)

			_, err = client.PutObjectAcl(t.Context(), in)
			require.NoError(t, err)

			out, err := client.GetObjectAcl(t.Context(), &sdk_s3.GetObjectAclInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)
			requireGrant(t, out.Grants, granteeID, tt.wantPerm)
		})
	}
}

func requireGrant(t *testing.T, grants []types.Grant, wantID string, wantPerm types.Permission) {
	t.Helper()

	for _, g := range grants {
		if g.Grantee != nil && aws.ToString(g.Grantee.ID) == wantID && g.Permission == wantPerm {
			return
		}
	}

	t.Fatalf("no grant found for id=%s permission=%s in %+v", wantID, wantPerm, grants)
}
