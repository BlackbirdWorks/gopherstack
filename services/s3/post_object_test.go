package s3_test

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/require"
)

func TestHandler_PostObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fields             map[string]string
		name               string
		filename           string
		contents           []byte
		wantLocationParts  []string
		wantBodyParts      []string
		wantObjectKey      string
		wantObjectType     string
		wantObjectContents []byte
		wantStatus         int
		wantVerifyObject   bool
	}{
		{
			name: "basic_upload_defaults_to_204",
			fields: map[string]string{
				"key":          "uploads/${filename}",
				"Content-Type": "text/plain",
			},
			filename:           "hello.txt",
			contents:           []byte("hello world"),
			wantObjectKey:      "uploads/hello.txt",
			wantObjectType:     "text/plain",
			wantObjectContents: []byte("hello world"),
			wantStatus:         http.StatusNoContent,
			wantVerifyObject:   true,
		},
		{
			name: "success_action_status_201_returns_post_response",
			fields: map[string]string{
				"key":                   "dst.txt",
				"success_action_status": "201",
			},
			filename:      "dst.txt",
			contents:      []byte("body"),
			wantBodyParts: []string{"<PostResponse>", "<Bucket>form-bkt</Bucket>"},
			wantStatus:    http.StatusCreated,
		},
		{
			name: "success_action_redirect_returns_303_location",
			fields: map[string]string{
				"key":                     "redir.txt",
				"success_action_redirect": "https://app.example.com/done",
			},
			filename:          "redir.txt",
			contents:          []byte("body"),
			wantLocationParts: []string{"bucket=form-bkt", "key=redir.txt"},
			wantStatus:        http.StatusSeeOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "form-bkt")

			body, contentType := buildPostForm(t, tt.fields, tt.filename, tt.contents)

			req := httptest.NewRequest(http.MethodPost, "/form-bkt", body)
			req.Header.Set("Content-Type", contentType)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, tt.wantStatus, rec.Code)

			for _, part := range tt.wantBodyParts {
				require.Contains(t, rec.Body.String(), part)
			}
			for _, part := range tt.wantLocationParts {
				require.Contains(t, rec.Header().Get("Location"), part)
			}

			if tt.wantVerifyObject {
				out, err := backend.GetObject(context.Background(), &sdk_s3.GetObjectInput{
					Bucket: aws.String("form-bkt"),
					Key:    aws.String(tt.wantObjectKey),
				})
				require.NoError(t, err)

				got, err := io.ReadAll(out.Body)
				require.NoError(t, err)
				require.Equal(t, tt.wantObjectContents, got)
				require.Equal(t, tt.wantObjectType, aws.ToString(out.ContentType))
			}
		})
	}
}

// TestHandler_PostObject_FormFieldPassthrough covers three presigned-POST form
// fields applied to the uploaded object exactly like their PutObject header
// equivalents: x-amz-server-side-encryption(-aws-kms-key-id), x-amz-storage-class,
// and x-amz-checksum-algorithm. Each case uses its own bucket/key so subtests run
// fully in parallel.
func TestHandler_PostObject_FormFieldPassthrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fields   map[string]string
		check    func(t *testing.T, rec *httptest.ResponseRecorder, backend *s3.InMemoryBackend)
		name     string
		bucket   string
		key      string
		contents []byte
	}{
		{
			name:     "x-amz-server-side-encryption applies SSE-KMS and echoes response headers",
			bucket:   "form-sse-bkt",
			key:      "encrypted.txt",
			contents: []byte("secret contents"),
			fields: map[string]string{
				"key":                          "encrypted.txt",
				"x-amz-server-side-encryption": "aws:kms",
				"x-amz-server-side-encryption-aws-kms-key-id": "arn:aws:kms:us-east-1:000000000000:key/test-key",
			},
			check: func(t *testing.T, rec *httptest.ResponseRecorder, backend *s3.InMemoryBackend) {
				t.Helper()

				require.Equal(t, "aws:kms", rec.Header().Get("X-Amz-Server-Side-Encryption"))
				require.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/test-key",
					rec.Header().Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"))

				out, err := backend.GetObject(context.Background(), &sdk_s3.GetObjectInput{
					Bucket: aws.String("form-sse-bkt"),
					Key:    aws.String("encrypted.txt"),
				})
				require.NoError(t, err)
				got, err := io.ReadAll(out.Body)
				require.NoError(t, err)
				require.Equal(t, []byte("secret contents"), got)
				require.Equal(t, "aws:kms", string(out.ServerSideEncryption))
			},
		},
		{
			name:     "x-amz-storage-class applies to the stored object",
			bucket:   "form-sc-bkt",
			key:      "archive.txt",
			contents: []byte("cold data"),
			fields: map[string]string{
				"key":                 "archive.txt",
				"x-amz-storage-class": "STANDARD_IA",
			},
			check: func(t *testing.T, _ *httptest.ResponseRecorder, backend *s3.InMemoryBackend) {
				t.Helper()

				out, err := backend.HeadObject(context.Background(), &sdk_s3.HeadObjectInput{
					Bucket: aws.String("form-sc-bkt"),
					Key:    aws.String("archive.txt"),
				})
				require.NoError(t, err)
				require.Equal(t, "STANDARD_IA", string(out.StorageClass))
			},
		},
		{
			name:     "x-amz-checksum-algorithm computes a checksum server-side",
			bucket:   "form-checksum-bkt",
			key:      "checked.txt",
			contents: []byte("checksum me"),
			fields: map[string]string{
				"key":                      "checked.txt",
				"x-amz-checksum-algorithm": "CRC32",
			},
			check: func(t *testing.T, _ *httptest.ResponseRecorder, backend *s3.InMemoryBackend) {
				t.Helper()

				out, err := backend.HeadObject(context.Background(), &sdk_s3.HeadObjectInput{
					Bucket: aws.String("form-checksum-bkt"),
					Key:    aws.String("checked.txt"),
				})
				require.NoError(t, err)
				require.NotNil(t, out.ChecksumCRC32)
				require.NotEmpty(t, *out.ChecksumCRC32)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			body, contentType := buildPostForm(t, tt.fields, tt.key, tt.contents)

			req := httptest.NewRequest(http.MethodPost, "/"+tt.bucket, body)
			req.Header.Set("Content-Type", contentType)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusNoContent, rec.Code)

			tt.check(t, rec, backend)
		})
	}
}

// buildPostForm writes a multipart/form-data body with the supplied form
// fields followed by a "file" part. AWS requires "file" to be last; we
// preserve that ordering so the body looks identical to a real browser POST.
func buildPostForm(
	t *testing.T,
	fields map[string]string,
	filename string,
	contents []byte,
) (io.Reader, string) {
	t.Helper()

	buf := &bytes.Buffer{}
	w := multipart.NewWriter(buf)

	for k, v := range fields {
		require.NoError(t, w.WriteField(k, v))
	}

	hdr := textproto.MIMEHeader{}
	hdr.Set("Content-Disposition", `form-data; name="file"; filename="`+filename+`"`)
	hdr.Set("Content-Type", "application/octet-stream")
	part, err := w.CreatePart(hdr)
	require.NoError(t, err)
	_, err = part.Write(contents)
	require.NoError(t, err)

	require.NoError(t, w.Close())

	return buf, w.FormDataContentType()
}
