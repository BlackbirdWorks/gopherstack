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

// TestHandler_GetObjectAcl_NotFound verifies that GetObjectAcl returns 404 when
// the object does not exist.
func TestHandler_GetObjectAcl_NotFound(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodGet, "/bkt/nonexistent?acl", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_GetObjectAcl_ReturnsOwnerGrant verifies that GetObjectAcl returns a
// valid XML ACL with FULL_CONTROL for an existing object.
func TestHandler_GetObjectAcl_ReturnsOwnerGrant(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "key", []byte("data"))

	req := httptest.NewRequest(http.MethodGet, "/bkt/key?acl", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "AccessControlPolicy")
	assert.Contains(t, body, "FULL_CONTROL")
	assert.Contains(t, body, "gopherstack")
	assert.Contains(t, body, "CanonicalUser")
}

func TestPutBucketACL_GetBucketACL(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)

	_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
		Bucket: aws.String("acl-test-bucket"),
	})
	require.NoError(t, err)

	// Default ACL is "private".
	acl, err := backend.GetBucketACL(t.Context(), "acl-test-bucket")
	require.NoError(t, err)
	assert.Equal(t, "private", acl)

	// Set a new ACL.
	err = backend.PutBucketACL(t.Context(), "acl-test-bucket", "public-read")
	require.NoError(t, err)

	acl, err = backend.GetBucketACL(t.Context(), "acl-test-bucket")
	require.NoError(t, err)
	assert.Equal(t, "public-read", acl)
}

func TestPutBucketACL_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.PutBucketACL(t.Context(), "nonexistent-bucket", "private")
	assert.Error(t, err)
}

func TestGetBucketACL_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	_, err := backend.GetBucketACL(t.Context(), "nonexistent-bucket")
	assert.Error(t, err)
}

func TestS3BucketPolicyCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "policy-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	// PutBucketPolicy
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?policy", strings.NewReader(policy))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketPolicy
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?policy", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, policy, rec.Body.String())

	// DeleteBucketPolicy
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?policy", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketPolicy after delete → NoSuchBucketPolicy
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?policy", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestS3BucketCORSCRUD verifies put/get/delete bucket CORS + OPTIONS preflight.

func TestS3PublicAccessBlockCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configXML  string
		wantBody   string
		wantPut    int
		wantGet    int
		wantDelete int
	}{
		{
			name: "full-block-all",
			configXML: `<PublicAccessBlockConfiguration>` +
				`<BlockPublicAcls>true</BlockPublicAcls>` +
				`<IgnorePublicAcls>true</IgnorePublicAcls>` +
				`<BlockPublicPolicy>true</BlockPublicPolicy>` +
				`<RestrictPublicBuckets>true</RestrictPublicBuckets>` +
				`</PublicAccessBlockConfiguration>`,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "BlockPublicAcls",
		},
		{
			name: "partial-block",
			configXML: `<PublicAccessBlockConfiguration>` +
				`<BlockPublicAcls>false</BlockPublicAcls>` +
				`<IgnorePublicAcls>false</IgnorePublicAcls>` +
				`<BlockPublicPolicy>true</BlockPublicPolicy>` +
				`<RestrictPublicBuckets>false</RestrictPublicBuckets>` +
				`</PublicAccessBlockConfiguration>`,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "BlockPublicPolicy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sdkClient := newTestHandler(t)
			bucket := "pab-test-" + tt.name

			_, err := sdkClient.CreateBucket(
				t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: &bucket},
			)
			require.NoError(t, err)

			// GetPublicAccessBlock before put → 404
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?publicAccessBlock", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// PutPublicAccessBlock
			req = httptest.NewRequest(
				http.MethodPut,
				"/"+bucket+"?publicAccessBlock",
				strings.NewReader(tt.configXML),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// GetPublicAccessBlock
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?publicAccessBlock", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantGet, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			// DeletePublicAccessBlock
			req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?publicAccessBlock", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantDelete, rec.Code)

			// GetPublicAccessBlock after delete → 404
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?publicAccessBlock", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestS3PublicAccessBlock_MalformedXML verifies that PutPublicAccessBlock rejects invalid XML.

func TestS3PublicAccessBlock_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "pab-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(
		http.MethodPut,
		"/"+bucket+"?publicAccessBlock",
		strings.NewReader("not-valid-xml"),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3BucketOwnershipControlsCRUD verifies put/get/delete ownership controls.

func TestS3BucketOwnershipControlsCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configXML  string
		wantBody   string
		wantPut    int
		wantGet    int
		wantDelete int
	}{
		{
			name: "bucket-owner-preferred",
			configXML: `<OwnershipControls>` +
				`<Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule>` +
				`</OwnershipControls>`,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "BucketOwnerPreferred",
		},
		{
			name: "bucket-owner-enforced",
			configXML: `<OwnershipControls>` +
				`<Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule>` +
				`</OwnershipControls>`,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "BucketOwnerEnforced",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sdkClient := newTestHandler(t)
			bucket := "ownership-test-" + tt.name

			_, err := sdkClient.CreateBucket(
				t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: &bucket},
			)
			require.NoError(t, err)

			// GetBucketOwnershipControls before put → 404
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?ownershipControls", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			// PutBucketOwnershipControls
			req = httptest.NewRequest(
				http.MethodPut,
				"/"+bucket+"?ownershipControls",
				strings.NewReader(tt.configXML),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// GetBucketOwnershipControls
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?ownershipControls", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantGet, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			// DeleteBucketOwnershipControls
			req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?ownershipControls", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantDelete, rec.Code)

			// GetBucketOwnershipControls after delete → 404
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?ownershipControls", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestS3BucketOwnershipControls_MalformedXML verifies that PutBucketOwnershipControls rejects invalid XML.

func TestS3BucketOwnershipControls_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "ownership-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(
		http.MethodPut,
		"/"+bucket+"?ownershipControls",
		strings.NewReader("not-valid-xml"),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3BucketLoggingCRUD verifies put/get bucket logging configuration.

func TestHandler_PutBucketACL_InvalidValue(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	req := httptest.NewRequest(http.MethodPut, "/bkt?acl", nil)
	req.Header.Set("X-Amz-Acl", "invalid-acl-value")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_PutBucketACL_RejectsObjectOnlyCannedACLs verifies that the
// object-only canned ACLs (bucket-owner-read / bucket-owner-full-control) are
// rejected on PutBucketAcl with 400 InvalidArgument, matching real S3
// (types.BucketCannedACL does not include them).

func TestHandler_PutBucketACL_RejectsObjectOnlyCannedACLs(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	for _, acl := range []string{"bucket-owner-read", "bucket-owner-full-control"} {
		req := httptest.NewRequest(http.MethodPut, "/bkt?acl", nil)
		req.Header.Set("X-Amz-Acl", acl)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code, "canned ACL %q must be rejected", acl)
		assert.Contains(t, rec.Body.String(), "InvalidArgument")
	}
}

func TestHandler_ObjectACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{name: "PUT ACL returns 200", method: http.MethodPut, wantStatus: http.StatusOK},
		{
			name:       "GET ACL returns 200 with owner grant",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("data"))

			req := httptest.NewRequest(tt.method, "/bkt/key?acl", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.method == http.MethodGet {
				assert.Contains(t, rec.Body.String(), "FULL_CONTROL")
				assert.Contains(t, rec.Body.String(), "gopherstack")
			}
		})
	}
}
