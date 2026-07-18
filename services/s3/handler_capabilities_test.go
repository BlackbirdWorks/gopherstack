package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_GetSupportedOperations_HighPriorityS3Ops(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "includes GetBucketLocation", op: "GetBucketLocation"},
		{name: "includes PutBucketTagging", op: "PutBucketTagging"},
		{name: "includes GetBucketTagging", op: "GetBucketTagging"},
		{name: "includes DeleteBucketTagging", op: "DeleteBucketTagging"},
		{name: "includes PutObjectAcl", op: "PutObjectAcl"},
		{name: "includes GetObjectAcl", op: "GetObjectAcl"},
		{name: "includes UploadPartCopy", op: "UploadPartCopy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)
			assert.Contains(t, handler.GetSupportedOperations(), tt.op)
		})
	}
}

func TestHandler_NonExistentBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "list object versions",
			method:     http.MethodGet,
			path:       "/no-bucket?versions",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "complete multipart upload",
			method:     http.MethodPost,
			path:       "/no-bucket/key?uploadId=ui",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get object tagging",
			method:     http.MethodGet,
			path:       "/no-bucket/key?tagging",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get bucket versioning",
			method:     http.MethodGet,
			path:       "/no-bucket?versioning",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)

			var body strings.Reader
			if tt.method == http.MethodPost {
				body = *strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>")
			}

			req := httptest.NewRequest(tt.method, tt.path, &body)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantNotEmpty bool
	}{
		{
			name:         "returns non-empty list including PutObject",
			wantContains: "PutObject",
			wantNotEmpty: true,
		},
		{
			name:         "includes PutBucketWebsite",
			wantContains: "PutBucketWebsite",
		},
		{
			name:         "includes GetBucketWebsite",
			wantContains: "GetBucketWebsite",
		},
		{
			name:         "includes DeleteBucketWebsite",
			wantContains: "DeleteBucketWebsite",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)
			ops := handler.GetSupportedOperations()

			if tt.wantNotEmpty {
				assert.NotEmpty(t, ops)
			}

			assert.Contains(t, ops, tt.wantContains)
		})
	}
}

// TestHandler_ListObjectsV2Error exercises handleListObjectsV2Error via
// ListObjectsV2 on a non-existent bucket (NoSuchBucket) and a generic backend error.

func TestGetSupportedOperations_PublicAccessAndReplicationOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "includes PutPublicAccessBlock", want: "PutPublicAccessBlock"},
		{name: "includes GetPublicAccessBlock", want: "GetPublicAccessBlock"},
		{name: "includes DeletePublicAccessBlock", want: "DeletePublicAccessBlock"},
		{name: "includes PutBucketOwnershipControls", want: "PutBucketOwnershipControls"},
		{name: "includes GetBucketOwnershipControls", want: "GetBucketOwnershipControls"},
		{name: "includes DeleteBucketOwnershipControls", want: "DeleteBucketOwnershipControls"},
		{name: "includes PutBucketLogging", want: "PutBucketLogging"},
		{name: "includes GetBucketLogging", want: "GetBucketLogging"},
		{name: "includes PutBucketReplication", want: "PutBucketReplication"},
		{name: "includes GetBucketReplication", want: "GetBucketReplication"},
		{name: "includes DeleteBucketReplication", want: "DeleteBucketReplication"},
		{name: "includes PutBucketEncryption", want: "PutBucketEncryption"},
		{name: "includes GetBucketEncryption", want: "GetBucketEncryption"},
		{name: "includes DeleteBucketEncryption", want: "DeleteBucketEncryption"},
		{name: "includes PutObjectLockConfiguration", want: "PutObjectLockConfiguration"},
		{name: "includes GetObjectLockConfiguration", want: "GetObjectLockConfiguration"},
		{name: "includes PutObjectRetention", want: "PutObjectRetention"},
		{name: "includes GetObjectRetention", want: "GetObjectRetention"},
		{name: "includes PutObjectLegalHold", want: "PutObjectLegalHold"},
		{name: "includes GetObjectLegalHold", want: "GetObjectLegalHold"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)
			ops := handler.GetSupportedOperations()
			assert.Contains(t, ops, tt.want)
		})
	}
}

// TestNonExistentBucket_PublicAccessAndReplicationOps verifies that new
// operations return NoSuchBucket for missing buckets.
func TestNonExistentBucket_PublicAccessAndReplicationOps(t *testing.T) {
	t.Parallel()

	publicAccessXML := `<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls>` +
		`<IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy>` +
		`<RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>`
	ownershipXML := `<OwnershipControls>` +
		`<Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule>` +
		`</OwnershipControls>`
	loggingXML := `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></BucketLoggingStatus>`
	replicationXML := `<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/r</Role>` +
		`<Rule><Status>Enabled</Status><Prefix></Prefix>` +
		`<Destination><Bucket>arn:aws:s3:::dest</Bucket></Destination></Rule></ReplicationConfiguration>`

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{
			name:   "GetPublicAccessBlock_NoSuchBucket",
			method: http.MethodGet,
			path:   "/missing?publicAccessBlock",
		},
		{
			name:   "PutPublicAccessBlock_NoSuchBucket",
			method: http.MethodPut,
			path:   "/missing?publicAccessBlock",
			body:   publicAccessXML,
		},
		{
			name:   "DeletePublicAccessBlock_NoSuchBucket",
			method: http.MethodDelete,
			path:   "/missing?publicAccessBlock",
		},
		{
			name:   "GetOwnershipControls_NoSuchBucket",
			method: http.MethodGet,
			path:   "/missing?ownershipControls",
		},
		{
			name:   "PutOwnershipControls_NoSuchBucket",
			method: http.MethodPut,
			path:   "/missing?ownershipControls",
			body:   ownershipXML,
		},
		{
			name:   "DeleteOwnershipControls_NoSuchBucket",
			method: http.MethodDelete,
			path:   "/missing?ownershipControls",
		},
		{name: "GetLogging_NoSuchBucket", method: http.MethodGet, path: "/missing?logging"},
		{
			name:   "PutLogging_NoSuchBucket",
			method: http.MethodPut,
			path:   "/missing?logging",
			body:   loggingXML,
		},
		{name: "GetReplication_NoSuchBucket", method: http.MethodGet, path: "/missing?replication"},
		{
			name:   "PutReplication_NoSuchBucket",
			method: http.MethodPut,
			path:   "/missing?replication",
			body:   replicationXML,
		},
		{
			name:   "DeleteReplication_NoSuchBucket",
			method: http.MethodDelete,
			path:   "/missing?replication",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
			assert.Contains(t, rec.Body.String(), "NoSuchBucket")
		})
	}
}

// TestHandler_ServeWebsite verifies the ServeWebsite method for website hosting.

func TestS3_CreateSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucket     string
		path       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "CreateSession returns mock credentials",
			bucket:     "session-bucket",
			path:       "/session-bucket?session",
			wantStatus: http.StatusOK,
			wantBody:   "gopherstack-mock-session-token",
		},
		{
			name:       "CreateSession on missing bucket returns 404",
			bucket:     "",
			path:       "/no-such-bucket?session",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchBucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.bucket != "" {
				mustCreateBucket(t, backend, tt.bucket)
			}

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestS3_NewOperations_SupportedOperations verifies all 24 new operations appear in GetSupportedOperations.

func TestGetSupportedOperations_MetadataAndAnalyticsOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{
			name: "includes CreateBucketMetadataConfiguration",
			want: "CreateBucketMetadataConfiguration",
		},
		{name: "includes GetBucketMetadataConfiguration", want: "GetBucketMetadataConfiguration"},
		{
			name: "includes DeleteBucketMetadataConfiguration",
			want: "DeleteBucketMetadataConfiguration",
		},
		{
			name: "includes CreateBucketMetadataTableConfiguration",
			want: "CreateBucketMetadataTableConfiguration",
		},
		{
			name: "includes GetBucketMetadataTableConfiguration",
			want: "GetBucketMetadataTableConfiguration",
		},
		{
			name: "includes DeleteBucketMetadataTableConfiguration",
			want: "DeleteBucketMetadataTableConfiguration",
		},
		{name: "includes CreateSession", want: "CreateSession"},
		{name: "includes PutBucketAnalyticsConfiguration", want: "PutBucketAnalyticsConfiguration"},
		{name: "includes GetBucketAnalyticsConfiguration", want: "GetBucketAnalyticsConfiguration"},
		{
			name: "includes DeleteBucketAnalyticsConfiguration",
			want: "DeleteBucketAnalyticsConfiguration",
		},
		{
			name: "includes ListBucketAnalyticsConfigurations",
			want: "ListBucketAnalyticsConfigurations",
		},
		{
			name: "includes PutBucketIntelligentTieringConfiguration",
			want: "PutBucketIntelligentTieringConfiguration",
		},
		{
			name: "includes GetBucketIntelligentTieringConfiguration",
			want: "GetBucketIntelligentTieringConfiguration",
		},
		{
			name: "includes DeleteBucketIntelligentTieringConfiguration",
			want: "DeleteBucketIntelligentTieringConfiguration",
		},
		{
			name: "includes ListBucketIntelligentTieringConfigurations",
			want: "ListBucketIntelligentTieringConfigurations",
		},
		{name: "includes PutBucketInventoryConfiguration", want: "PutBucketInventoryConfiguration"},
		{name: "includes GetBucketInventoryConfiguration", want: "GetBucketInventoryConfiguration"},
		{
			name: "includes DeleteBucketInventoryConfiguration",
			want: "DeleteBucketInventoryConfiguration",
		},
		{
			name: "includes ListBucketInventoryConfigurations",
			want: "ListBucketInventoryConfigurations",
		},
		{name: "includes DeleteBucketLifecycle", want: "DeleteBucketLifecycle"},
		{name: "includes PutBucketMetricsConfiguration", want: "PutBucketMetricsConfiguration"},
		{name: "includes GetBucketMetricsConfiguration", want: "GetBucketMetricsConfiguration"},
		{
			name: "includes DeleteBucketMetricsConfiguration",
			want: "DeleteBucketMetricsConfiguration",
		},
		{name: "includes ListBucketMetricsConfigurations", want: "ListBucketMetricsConfigurations"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)
			ops := handler.GetSupportedOperations()
			assert.Contains(t, ops, tt.want)
		})
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "PATCH on bucket",
			method: http.MethodPatch,
			path:   "/bkt",
		},
		{
			name:   "PATCH on object",
			method: http.MethodPatch,
			path:   "/bkt/key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

func TestHandler_InvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "invalid bucket name (too short)",
			method:     http.MethodPut,
			path:       "/ab",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid bucket name (uppercase)",
			method:     http.MethodPut,
			path:       "/InvalidBucket",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid bucket name (underscore)",
			method:     http.MethodPut,
			path:       "/my_bucket",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid bucket name (starts with hyphen)",
			method:     http.MethodPut,
			path:       "/-bucket",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid bucket name (IP address)",
			method:     http.MethodPut,
			path:       "/192.168.1.1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid object key (too long)",
			method:     http.MethodPut,
			path:       "/valid-bucket/" + strings.Repeat("a", 1025),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if strings.Contains(tt.path, "/valid-bucket/") {
				mustCreateBucket(t, backend, "valid-bucket")
			}

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, "case: %s", tt.name)
		})
	}
}
