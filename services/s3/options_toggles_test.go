package s3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeKMSKeys map[string]bool

func (f fakeKMSKeys) KMSKeyExists(_ context.Context, keyID string) bool { return f[keyID] }

func TestBucketCORSDisableToggle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		method        string
		disable       bool
		wantPreflight int
		wantOrigin    string
	}{
		{name: "preflight on", method: http.MethodOptions, wantPreflight: http.StatusOK, wantOrigin: "https://example.com"},
		{name: "preflight off", method: http.MethodOptions, disable: true, wantPreflight: http.StatusForbidden},
		{name: "actual on", method: http.MethodGet, wantPreflight: http.StatusOK, wantOrigin: "https://example.com"},
		{name: "actual off", method: http.MethodGet, disable: true, wantPreflight: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			handler.DisableBucketCORS = tt.disable
			mustCreateBucket(t, backend, "cors-toggle")

			corsXML := `<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod>` +
				`<AllowedOrigin>https://example.com</AllowedOrigin></CORSRule></CORSConfiguration>`
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, httptest.NewRequest(http.MethodPut, "/cors-toggle?cors", strings.NewReader(corsXML)))
			require.Equal(t, http.StatusOK, rec.Code)

			path := "/cors-toggle"
			if tt.method == http.MethodGet {
				path += "?location"
			}

			req := httptest.NewRequest(tt.method, path, nil)
			req.Header.Set("Origin", "https://example.com")
			req.Header.Set("Access-Control-Request-Method", "GET")
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantPreflight, rec.Code)
			assert.Equal(t, tt.wantOrigin, rec.Header().Get("Access-Control-Allow-Origin"))
		})
	}
}

func TestSSEKMSKeyValidation(t *testing.T) {
	t.Parallel()

	const known = "arn:aws:kms:us-east-1:000000000000:key/known"

	tests := []struct {
		name     string
		keyID    string
		algo     string
		skip     bool
		noKMS    bool
		wantCode int
		wantErr  string
	}{
		{name: "known key", keyID: known, algo: "aws:kms", wantCode: http.StatusOK},
		{name: "unknown key", keyID: "missing", algo: "aws:kms", wantCode: http.StatusBadRequest, wantErr: "KMS.NotFoundException"},
		{name: "unknown dsse key", keyID: "missing", algo: "aws:kms:dsse", wantCode: http.StatusBadRequest, wantErr: "KMS.NotFoundException"},
		{name: "unknown key skipped", keyID: "missing", algo: "aws:kms", skip: true, wantCode: http.StatusOK},
		{name: "no kms wired", keyID: "missing", algo: "aws:kms", noKMS: true, wantCode: http.StatusOK},
		{name: "aes256 ignores key id", keyID: "missing", algo: "AES256", wantCode: http.StatusOK},
	}

	ops := []struct{ name string }{{name: "put"}, {name: "copy"}, {name: "multipart"}}

	for _, tt := range tests {
		for _, op := range ops {
			t.Run(tt.name+"_"+op.name, func(t *testing.T) {
				t.Parallel()

				handler, backend := newTestHandler(t)
				if !tt.noKMS {
					handler.SetKMSKeyChecker(fakeKMSKeys{known: true})
				}

				handler.SkipKMSKeyValidation = tt.skip
				mustCreateBucket(t, backend, "kms-bkt")
				mustPutObject(t, backend, "kms-bkt", "src", []byte("data"))

				var req *http.Request

				switch op.name {
				case "put":
					req = httptest.NewRequest(http.MethodPut, "/kms-bkt/obj", strings.NewReader("body"))
				case "copy":
					req = httptest.NewRequest(http.MethodPut, "/kms-bkt/dst", nil)
					req.Header.Set("X-Amz-Copy-Source", "/kms-bkt/src")
				default:
					req = httptest.NewRequest(http.MethodPost, "/kms-bkt/mp?uploads", nil)
				}

				req.Header.Set("X-Amz-Server-Side-Encryption", tt.algo)
				req.Header.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", tt.keyID)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)

				assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

				if tt.wantErr != "" {
					assert.Contains(t, rec.Body.String(), "<Code>"+tt.wantErr+"</Code>")
				}
			})
		}
	}
}
