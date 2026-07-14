package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

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
