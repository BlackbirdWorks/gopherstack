package s3_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildDeleteBody constructs a DeleteObjects (bulk delete) request body
// containing n <Object> entries.
func buildDeleteBody(n int) string {
	var sb strings.Builder
	sb.WriteString("<Delete>")
	for i := range n {
		sb.WriteString("<Object><Key>key-")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString("</Key></Object>")
	}
	sb.WriteString("</Delete>")

	return sb.String()
}

// TestDeleteObjects_KeyLimit verifies that a DeleteObjects request exceeding
// the 1000-key limit fails with HTTP 400 and the MalformedXML error code
// (matching AWS), while a request at exactly the limit is accepted.
func TestDeleteObjects_KeyLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantBodyHas string
		keyCount    int
		wantCode    int
	}{
		{
			name:        "over_limit_rejected",
			keyCount:    1001,
			wantCode:    http.StatusBadRequest,
			wantBodyHas: "MalformedXML",
		},
		{
			name:     "at_limit_succeeds",
			keyCount: 1000,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			body := buildDeleteBody(tt.keyCount)

			req := httptest.NewRequest(http.MethodPost, "/bkt?delete", strings.NewReader(body))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantBodyHas != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyHas)
			}
		})
	}
}

// TestHandler_DeleteObjects_BulkOps verifies bulk DeleteObjects behavior,
// including mixed existing/nonexistent keys and malformed request bodies.
func TestHandler_DeleteObjects_BulkOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		deleteBody   string
		wantBody     string
		setupObjects []string
		wantCode     int
	}{
		{
			name:         "mixed_objects_including_nonexistent",
			bucket:       "del-objects-bucket",
			setupObjects: []string{"obj1", "obj2"},
			deleteBody: `<Delete>` +
				`<Object><Key>obj1</Key></Object>` +
				`<Object><Key>obj2</Key></Object>` +
				`<Object><Key>nonexistent</Key></Object>` +
				`</Delete>`,
			wantCode: http.StatusOK,
			wantBody: "DeleteResult",
		},
		{
			name:       "invalid_xml_body",
			bucket:     "del-bad-xml",
			deleteBody: "not-xml",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			for _, key := range tt.setupObjects {
				putReq := httptest.NewRequest(
					http.MethodPut,
					"/"+tt.bucket+"/"+key,
					strings.NewReader("data"),
				)
				putRec := httptest.NewRecorder()
				serveS3Handler(handler, putRec, putReq)
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			req := httptest.NewRequest(
				http.MethodPost,
				"/"+tt.bucket+"?delete",
				strings.NewReader(tt.deleteBody),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestHandler_DeleteObject_Versioned verifies that DeleteObject on an existing
// object returns 204 No Content.
func TestHandler_DeleteObject_Versioned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		key      string
		wantCode int
	}{
		{
			name:     "delete_existing_object",
			bucket:   "del-versioned",
			key:      "obj",
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			putReq := httptest.NewRequest(
				http.MethodPut,
				"/"+tt.bucket+"/"+tt.key,
				strings.NewReader("data"),
			)
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			require.Equal(t, http.StatusOK, putRec.Code)

			delReq := httptest.NewRequest(http.MethodDelete, "/"+tt.bucket+"/"+tt.key, nil)
			delRec := httptest.NewRecorder()
			serveS3Handler(handler, delRec, delReq)

			assert.Equal(t, tt.wantCode, delRec.Code)
		})
	}
}

func TestHandler_DeleteObject_NonExistent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		wantStatus int
	}{
		{
			name:       "delete non-existent key returns 204",
			key:        "no-key",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodDelete, "/bkt/"+tt.key, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete existing object returns 204"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("data"))

			req := httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

func TestHandler_DeleteObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		xmlBody    string
		wantStatus int
	}{
		{
			name:   "delete multiple objects",
			bucket: "bkt",
			xmlBody: `<Delete>
				<Object><Key>k1</Key></Object>
				<Object><Key>k2</Key></Object>
			</Delete>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k1", []byte("d1"))
				mustPutObject(t, b, "bkt", "k2", []byte("d2"))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "delete objects with versions",
			bucket: "bkt",
			xmlBody: `<Delete>
				<Object><Key>k1</Key><VersionId>null</VersionId></Object>
			</Delete>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k1", []byte("d1"))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "quiet mode",
			bucket: "bkt",
			xmlBody: `<Delete>
				<Quiet>true</Quiet>
				<Object><Key>k1</Key></Object>
			</Delete>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "k1", []byte("d1"))
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(
				http.MethodPost,
				"/"+tt.bucket+"?delete",
				strings.NewReader(tt.xmlBody),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus == http.StatusOK {
				assert.Contains(t, rec.Header().Get("Content-Type"), "application/xml")
			}
		})
	}
}

func TestHandler_DeleteObjects_Versioning(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	bucket := "versioned-bucket"
	mustCreateBucket(t, backend, bucket)
	enableVersioning(t, handler, bucket)

	// Create 3 versions of k1
	v1, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), Body: strings.NewReader("v1"),
	})
	require.NoError(t, err)
	v1ID := *v1.VersionId

	v2, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), Body: strings.NewReader("v2"),
	})
	require.NoError(t, err)
	v2ID := *v2.VersionId

	v3, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), Body: strings.NewReader("v3"),
	})
	require.NoError(t, err)
	v3ID := *v3.VersionId

	// Delete v1 and v3, leave v2
	xmlBody := fmt.Sprintf(`<Delete>
		<Object><Key>k1</Key><VersionId>%s</VersionId></Object>
		<Object><Key>k1</Key><VersionId>%s</VersionId></Object>
	</Delete>`, v1ID, v3ID)

	req := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(xmlBody))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify v1 and v3 are gone, v2 remains
	_, err = backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), VersionId: aws.String(v1ID),
	})
	require.Error(t, err)

	_, err = backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), VersionId: aws.String(v3ID),
	})
	require.Error(t, err)

	out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k1"), VersionId: aws.String(v2ID),
	})
	require.NoError(t, err)
	data, _ := io.ReadAll(out.Body)
	assert.Equal(t, "v2", string(data))

	// Now delete without version ID - should create a delete marker
	xmlBody = `<Delete><Object><Key>k1</Key></Object></Delete>`
	req = httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(xmlBody))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Head object should now return 404 (due to delete marker)
	req = httptest.NewRequest(http.MethodHead, "/"+bucket+"/k1", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
