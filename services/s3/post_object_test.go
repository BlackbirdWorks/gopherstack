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
	"github.com/stretchr/testify/require"
)

// TestHandler_PostObject_Basic exercises the POST /bucket form-upload path:
// the file body is stored at the supplied key with the supplied Content-Type,
// and the default response is 204.
func TestHandler_PostObject_Basic(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "form-bkt")

	body, contentType := buildPostForm(t, map[string]string{
		"key":          "uploads/${filename}",
		"Content-Type": "text/plain",
	}, "hello.txt", []byte("hello world"))

	req := httptest.NewRequest(http.MethodPost, "/form-bkt", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	out, err := backend.GetObject(context.Background(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("form-bkt"),
		Key:    aws.String("uploads/hello.txt"),
	})
	require.NoError(t, err)
	got, _ := io.ReadAll(out.Body)
	require.Equal(t, []byte("hello world"), got)
	require.Equal(t, "text/plain", aws.ToString(out.ContentType))
}

// TestHandler_PostObject_SuccessActionStatus201 verifies that the form's
// success_action_status field is honoured and that the 201 response carries
// the PostResponse XML body.
func TestHandler_PostObject_SuccessActionStatus201(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "form-bkt")

	body, contentType := buildPostForm(t, map[string]string{
		"key":                   "dst.txt",
		"success_action_status": "201",
	}, "dst.txt", []byte("body"))

	req := httptest.NewRequest(http.MethodPost, "/form-bkt", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Contains(t, rec.Body.String(), "<PostResponse>")
	require.Contains(t, rec.Body.String(), "<Bucket>form-bkt</Bucket>")
}

// TestHandler_PostObject_SuccessActionRedirect verifies that the redirect
// form variant returns 303 with bucket/key/etag appended to the Location.
func TestHandler_PostObject_SuccessActionRedirect(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "form-bkt")

	body, contentType := buildPostForm(t, map[string]string{
		"key":                     "redir.txt",
		"success_action_redirect": "https://app.example.com/done",
	}, "redir.txt", []byte("body"))

	req := httptest.NewRequest(http.MethodPost, "/form-bkt", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusSeeOther, rec.Code)
	require.Contains(t, rec.Header().Get("Location"), "bucket=form-bkt")
	require.Contains(t, rec.Header().Get("Location"), "key=redir.txt")
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
