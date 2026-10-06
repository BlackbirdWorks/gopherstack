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

type headerWant struct {
	meta                                                                                      map[string]string
	cacheControl, contentLanguage, contentDisposition, contentEncoding, contentType, redirect string
}

func assertHeadMatches(t *testing.T, client *sdk_s3.Client, bucket, key string, want headerWant) {
	t.Helper()

	head, err := client.HeadObject(
		t.Context(), &sdk_s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)},
	)
	require.NoError(t, err)
	assert.Equal(t, want.cacheControl, aws.ToString(head.CacheControl), "cache-control")
	assert.Equal(t, want.contentLanguage, aws.ToString(head.ContentLanguage), "content-language")
	assert.Equal(t, want.contentDisposition, aws.ToString(head.ContentDisposition), "content-disposition")
	assert.Equal(t, want.contentEncoding, aws.ToString(head.ContentEncoding), "content-encoding")
	assert.Equal(t, want.contentType, aws.ToString(head.ContentType), "content-type")
	assert.Equal(t, want.redirect, aws.ToString(head.WebsiteRedirectLocation), "redirect")
	assert.Equal(t, want.meta, head.Metadata, "metadata")

	got, err := client.GetObject(t.Context(), &sdk_s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)

	defer got.Body.Close()

	assert.Equal(t, want.cacheControl, aws.ToString(got.CacheControl), "get cache-control")
	assert.Equal(t, want.contentLanguage, aws.ToString(got.ContentLanguage), "get content-language")
	assert.Equal(t, want.redirect, aws.ToString(got.WebsiteRedirectLocation), "get redirect")
}

func fullPutInput(bucket, key string, w headerWant) *sdk_s3.PutObjectInput {
	return &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("x"),
		CacheControl: aws.String(w.cacheControl), ContentLanguage: aws.String(w.contentLanguage),
		ContentDisposition: aws.String(w.contentDisposition), ContentEncoding: aws.String(w.contentEncoding),
		ContentType: aws.String(w.contentType), WebsiteRedirectLocation: aws.String(w.redirect),
		Metadata: w.meta,
	}
}

func TestRealClient_ObjectMetadataHeaders(t *testing.T) {
	t.Parallel()

	full := headerWant{
		cacheControl: "max-age=60", contentLanguage: "en-US", contentDisposition: "attachment; filename=a.txt",
		contentEncoding: "gzip", contentType: "text/plain", redirect: "/other",
		meta: map[string]string{"owner": "me"},
	}

	tests := []struct {
		run  func(t *testing.T, client *sdk_s3.Client, bucket string)
		name string
	}{
		{name: "put_object", run: func(t *testing.T, client *sdk_s3.Client, bucket string) {
			t.Helper()

			_, err := client.PutObject(t.Context(), fullPutInput(bucket, "k", full))
			require.NoError(t, err)
			assertHeadMatches(t, client, bucket, "k", full)
		}},
		{name: "copy_directive_copies_source_headers", run: func(t *testing.T, client *sdk_s3.Client, bucket string) {
			t.Helper()

			_, err := client.PutObject(t.Context(), fullPutInput(bucket, "src", full))
			require.NoError(t, err)

			_, err = client.CopyObject(t.Context(), &sdk_s3.CopyObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("dst"), CopySource: aws.String(bucket + "/src"),
			})
			require.NoError(t, err)

			want := full
			want.redirect = ""

			assertHeadMatches(t, client, bucket, "dst", want)
		}},
		{name: "copy_replace_uses_request_headers", run: func(t *testing.T, client *sdk_s3.Client, bucket string) {
			t.Helper()

			_, err := client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("src"), Body: strings.NewReader("x"),
				CacheControl: aws.String(full.cacheControl), ContentLanguage: aws.String(full.contentLanguage),
			})
			require.NoError(t, err)

			_, err = client.CopyObject(t.Context(), &sdk_s3.CopyObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("dst"), CopySource: aws.String(bucket + "/src"),
				MetadataDirective: types.MetadataDirectiveReplace, CacheControl: aws.String("no-cache"),
				ContentDisposition: aws.String("inline"), ContentEncoding: aws.String("br"),
				ContentLanguage: aws.String("fr"), ContentType: aws.String("text/html"),
				WebsiteRedirectLocation: aws.String("/new"), Metadata: map[string]string{"k": "v"},
			})
			require.NoError(t, err)

			assertHeadMatches(t, client, bucket, "dst", headerWant{
				cacheControl: "no-cache", contentLanguage: "fr", contentDisposition: "inline",
				contentEncoding: "br", contentType: "text/html", redirect: "/new", meta: map[string]string{"k": "v"},
			})
		}},
		{name: "multipart_upload", run: func(t *testing.T, client *sdk_s3.Client, bucket string) {
			t.Helper()

			put := fullPutInput(bucket, "mp", full)
			created, err := client.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
				Bucket: put.Bucket, Key: put.Key, CacheControl: put.CacheControl,
				ContentLanguage: put.ContentLanguage, ContentDisposition: put.ContentDisposition,
				ContentEncoding: put.ContentEncoding, ContentType: put.ContentType,
				WebsiteRedirectLocation: put.WebsiteRedirectLocation, Metadata: put.Metadata,
			})
			require.NoError(t, err)

			part, err := client.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String("mp"), UploadId: created.UploadId,
				PartNumber: aws.Int32(1), Body: strings.NewReader("x"),
			})
			require.NoError(t, err)

			_, err = client.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String("mp"), UploadId: created.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
				},
			})
			require.NoError(t, err)
			assertHeadMatches(t, client, bucket, "mp", full)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := "meta-headers"

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			tt.run(t, client, bucket)
		})
	}
}
