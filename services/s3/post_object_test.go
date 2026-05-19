package s3_test

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
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

			if tt.wantObjectKey == "" {
				return
			}

			out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String("form-bkt"),
				Key:    aws.String(tt.wantObjectKey),
			})
			require.NoError(t, err)

			got, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			require.Equal(t, tt.wantObjectContents, got)
			require.Equal(t, tt.wantObjectType, aws.ToString(out.ContentType))
		})
	}
}

// buildPostForm writes a multipart/form-data body with the supplied form
// fields followed by a "file" part. AWS requires "file" to be last; we
// preserve that ordering so the body looks identical to a real browser POST.
func buildPostForm(t *testing.T, fields map[string]string, filename string, contents []byte) (io.Reader, string) {
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
