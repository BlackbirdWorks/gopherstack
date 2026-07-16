package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestHandler_BucketACL verifies the PutBucketACL / GetBucketACL HTTP handlers.
func TestHandler_BucketACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bucket    string
		method    string
		url       string
		aclHeader string
		wantCode  int
	}{
		{
			name:      "put_bucket_acl",
			bucket:    "acl-put-test",
			method:    http.MethodPut,
			url:       "/acl-put-test?acl",
			aclHeader: "public-read",
			wantCode:  http.StatusOK,
		},
		{
			name:     "get_bucket_acl",
			bucket:   "acl-get-test",
			method:   http.MethodGet,
			url:      "/acl-get-test?acl",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(tt.method, tt.url, nil)
			if tt.aclHeader != "" {
				req.Header.Set("X-Amz-Acl", tt.aclHeader)
			}
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestPutGetBucketAbac verifies that PutBucketAbac stores the ABAC
// configuration and GetBucketAbac returns it via the HTTP handlers.
func TestPutGetBucketAbac(t *testing.T) {
	t.Parallel()

	const abacXML = `<AbacConfiguration><Status>Enabled</Status></AbacConfiguration>`

	tests := []struct {
		name          string
		bucket        string
		putBody       string
		getWantStatus string
		putWantCode   int
		getWantCode   int
	}{
		{
			name:          "enabled_config_round_trips",
			bucket:        "bkt",
			putBody:       abacXML,
			putWantCode:   http.StatusOK,
			getWantCode:   http.StatusOK,
			getWantStatus: "Enabled",
		},
		{
			name:          "empty_body_accepted",
			bucket:        "bkt2",
			putBody:       "",
			putWantCode:   http.StatusOK,
			getWantCode:   http.StatusOK,
			getWantStatus: "",
		},
		{
			name:        "put_missing_bucket_404",
			bucket:      "nosuchbucket",
			putBody:     abacXML,
			putWantCode: http.StatusNotFound,
			getWantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			if tt.putWantCode != http.StatusNotFound {
				mustCreateBucket(t, backend, tt.bucket)
			}

			// PUT
			putReq := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"?abac",
				strings.NewReader(tt.putBody))
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			assert.Equal(t, tt.putWantCode, putRec.Code, "PUT abac status")

			// GET (only when PUT succeeded or to verify 404 on missing bucket)
			getReq := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"?abac", nil)
			getRec := httptest.NewRecorder()
			serveS3Handler(handler, getRec, getReq)
			assert.Equal(t, tt.getWantCode, getRec.Code, "GET abac status")

			if tt.getWantStatus != "" {
				assert.Contains(t, getRec.Body.String(), tt.getWantStatus,
					"GET abac should return stored status")
			}
		})
	}
}

// TestBackendAbac verifies the backend-level PutBucketAbac / GetBucketAbac methods.
func TestBackendAbac(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		putXML     string
		wantXML    string
		wantErrPut bool
		wantErrGet bool
		missingBkt bool
	}{
		{
			name:    "put_and_get_roundtrip",
			putXML:  `<AbacConfiguration><Status>Enabled</Status></AbacConfiguration>`,
			wantXML: `<AbacConfiguration><Status>Enabled</Status></AbacConfiguration>`,
		},
		{
			name:    "overwrite_replaces_previous",
			putXML:  `<AbacConfiguration><Status>Disabled</Status></AbacConfiguration>`,
			wantXML: `<AbacConfiguration><Status>Disabled</Status></AbacConfiguration>`,
		},
		{
			name:       "missing_bucket_returns_error",
			putXML:     `<AbacConfiguration/>`,
			wantErrPut: true,
			wantErrGet: true,
			missingBkt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})

			if !tt.missingBkt {
				_, err := backend.CreateBucket(t.Context(),
					&sdk_s3.CreateBucketInput{Bucket: aws.String("bkt")})
				require.NoError(t, err)
			}

			bucketName := "bkt"
			if tt.missingBkt {
				bucketName = "no-such-bucket"
			}

			errPut := backend.PutBucketAbac(t.Context(), bucketName, tt.putXML)
			if tt.wantErrPut {
				require.Error(t, errPut)
			} else {
				require.NoError(t, errPut)
			}

			got, errGet := backend.GetBucketAbac(t.Context(), bucketName)
			if tt.wantErrGet {
				require.Error(t, errGet)
			} else {
				require.NoError(t, errGet)
				assert.Equal(t, tt.wantXML, got)
			}
		})
	}
}
