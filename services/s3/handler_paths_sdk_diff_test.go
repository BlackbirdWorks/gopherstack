package s3_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// sdkRouteCase is one real S3 operation's authoritative method/path/query
// shape, taken directly from s3@v1.106.5 serializers.go: each op's
// awsRestxml_serializeOp<Op>.HandleSerialize sets request.Method and calls
// httpbinding.SplitURI(<template>); "query" below is that template's query
// portion (always including the op's own literal "x-id=<Op>" component when
// the serializer emits one -- x-id is real wire content every pinned SDK
// client sends, even though gopherstack's router does not key on it).
//
// A handful of ops share an identical (method, static-template) pair and
// are only told apart by a REQUIRED dynamic member the static template
// can't show (bound via encoder.SetQuery/SetHeader elsewheer in the same
// HttpBindings function, not by SplitURI's literal string): UploadPart/
// UploadPartCopy/ListParts/AbortMultipartUpload/CompleteMultipartUpload all
// require UploadId; UploadPart/UploadPartCopy also require PartNumber;
// CopyObject/UploadPartCopy require the X-Amz-Copy-Source header;
// GetObjectAnnotation requires AnnotationName (vs. ListObjectAnnotations,
// which lacks it); GetBucketAnalyticsConfiguration/
// GetBucketIntelligentTieringConfiguration/GetBucketInventoryConfiguration/
// GetBucketMetricsConfiguration all require Id (vs. their List* siblings,
// which lack it). Those required members are added to "query"/"headers"
// below so each case reproduces what a real SDK call actually puts on the
// wire, not just the static template.
//
// ListDirectoryBuckets is recorded unreachable rather than given a route:
// real S3 tells it apart from ListBuckets purely by hostname
// (s3express-control.* vs s3.*), which a single-endpoint emulator has no
// way to key on -- see PARITY.md and gopherstack-0bq8.
//
// Regenerate by listing api_op_*.go in the pinned s3 module and, for each,
// pulling request.Method and the httpbinding.SplitURI(...) argument from
// its awsRestxml_serializeOp<Op>.HandleSerialize body in serializers.go.
type sdkRouteCase struct {
	op          string
	method      string
	kind        string // "bucket", "object", "root", or "literal"
	query       string
	headers     map[string]string
	unreachable string
}

func sdkRouteCases() []sdkRouteCase {
	return []sdkRouteCase{
		{
			op:          "AbortMultipartUpload",
			method:      "DELETE",
			kind:        "object",
			query:       "x-id=AbortMultipartUpload&uploadId=test-upload-id",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "CompleteMultipartUpload",
			method:      "POST",
			kind:        "object",
			query:       "uploadId=test-upload-id",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "CopyObject",
			method:      "PUT",
			kind:        "object",
			query:       "x-id=CopyObject",
			headers:     map[string]string{"X-Amz-Copy-Source": "/src-bucket/src-key"},
			unreachable: "",
		},
		{op: "CreateBucket", method: "PUT", kind: "bucket", query: "", headers: nil, unreachable: ""},
		{
			op:          "CreateBucketMetadataConfiguration",
			method:      "POST",
			kind:        "bucket",
			query:       "metadataConfiguration",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "CreateBucketMetadataTableConfiguration",
			method:      "POST",
			kind:        "bucket",
			query:       "metadataTable",
			headers:     nil,
			unreachable: "",
		},
		{op: "CreateMultipartUpload", method: "POST", kind: "object", query: "uploads", headers: nil, unreachable: ""},
		{op: "CreateSession", method: "GET", kind: "bucket", query: "session", headers: nil, unreachable: ""},
		{op: "DeleteBucket", method: "DELETE", kind: "bucket", query: "", headers: nil, unreachable: ""},
		{
			op:          "DeleteBucketAnalyticsConfiguration",
			method:      "DELETE",
			kind:        "bucket",
			query:       "analytics",
			headers:     nil,
			unreachable: "",
		},
		{op: "DeleteBucketCors", method: "DELETE", kind: "bucket", query: "cors", headers: nil, unreachable: ""},
		{
			op:          "DeleteBucketEncryption",
			method:      "DELETE",
			kind:        "bucket",
			query:       "encryption",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "DeleteBucketIntelligentTieringConfiguration",
			method:      "DELETE",
			kind:        "bucket",
			query:       "intelligent-tiering",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "DeleteBucketInventoryConfiguration",
			method:      "DELETE",
			kind:        "bucket",
			query:       "inventory",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "DeleteBucketLifecycle",
			method:      "DELETE",
			kind:        "bucket",
			query:       "lifecycle",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "DeleteBucketMetadataConfiguration",
			method:      "DELETE",
			kind:        "bucket",
			query:       "metadataConfiguration",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "DeleteBucketMetadataTableConfiguration",
			method:      "DELETE",
			kind:        "bucket",
			query:       "metadataTable",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "DeleteBucketMetricsConfiguration",
			method:      "DELETE",
			kind:        "bucket",
			query:       "metrics",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "DeleteBucketOwnershipControls",
			method:      "DELETE",
			kind:        "bucket",
			query:       "ownershipControls",
			headers:     nil,
			unreachable: "",
		},
		{op: "DeleteBucketPolicy", method: "DELETE", kind: "bucket", query: "policy", headers: nil, unreachable: ""},
		{
			op:          "DeleteBucketReplication",
			method:      "DELETE",
			kind:        "bucket",
			query:       "replication",
			headers:     nil,
			unreachable: "",
		},
		{op: "DeleteBucketTagging", method: "DELETE", kind: "bucket", query: "tagging", headers: nil, unreachable: ""},
		{op: "DeleteBucketWebsite", method: "DELETE", kind: "bucket", query: "website", headers: nil, unreachable: ""},
		{
			op:          "DeleteObject",
			method:      "DELETE",
			kind:        "object",
			query:       "x-id=DeleteObject",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "DeleteObjectAnnotation",
			method:      "DELETE",
			kind:        "object",
			query:       "annotation",
			headers:     nil,
			unreachable: "",
		},
		{op: "DeleteObjects", method: "POST", kind: "bucket", query: "delete", headers: nil, unreachable: ""},
		{op: "DeleteObjectTagging", method: "DELETE", kind: "object", query: "tagging", headers: nil, unreachable: ""},
		{
			op:          "DeletePublicAccessBlock",
			method:      "DELETE",
			kind:        "bucket",
			query:       "publicAccessBlock",
			headers:     nil,
			unreachable: "",
		},
		{op: "GetBucketAbac", method: "GET", kind: "bucket", query: "abac", headers: nil, unreachable: ""},
		{
			op:          "GetBucketAccelerateConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "accelerate",
			headers:     nil,
			unreachable: "",
		},
		{op: "GetBucketAcl", method: "GET", kind: "bucket", query: "acl", headers: nil, unreachable: ""},
		{
			op:          "GetBucketAnalyticsConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "analytics&x-id=GetBucketAnalyticsConfiguration&id=test-id",
			headers:     nil,
			unreachable: "",
		},
		{op: "GetBucketCors", method: "GET", kind: "bucket", query: "cors", headers: nil, unreachable: ""},
		{op: "GetBucketEncryption", method: "GET", kind: "bucket", query: "encryption", headers: nil, unreachable: ""},
		{
			op:          "GetBucketIntelligentTieringConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "intelligent-tiering&x-id=GetBucketIntelligentTieringConfiguration&id=test-id",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "GetBucketInventoryConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "inventory&x-id=GetBucketInventoryConfiguration&id=test-id",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "GetBucketLifecycleConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "lifecycle",
			headers:     nil,
			unreachable: "",
		},
		{op: "GetBucketLocation", method: "GET", kind: "bucket", query: "location", headers: nil, unreachable: ""},
		{op: "GetBucketLogging", method: "GET", kind: "bucket", query: "logging", headers: nil, unreachable: ""},
		{
			op:          "GetBucketMetadataConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "metadataConfiguration",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "GetBucketMetadataTableConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "metadataTable",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "GetBucketMetricsConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "metrics&x-id=GetBucketMetricsConfiguration&id=test-id",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "GetBucketNotificationConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "notification",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "GetBucketOwnershipControls",
			method:      "GET",
			kind:        "bucket",
			query:       "ownershipControls",
			headers:     nil,
			unreachable: "",
		},
		{op: "GetBucketPolicy", method: "GET", kind: "bucket", query: "policy", headers: nil, unreachable: ""},
		{
			op:          "GetBucketPolicyStatus",
			method:      "GET",
			kind:        "bucket",
			query:       "policyStatus",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "GetBucketReplication",
			method:      "GET",
			kind:        "bucket",
			query:       "replication",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "GetBucketRequestPayment",
			method:      "GET",
			kind:        "bucket",
			query:       "requestPayment",
			headers:     nil,
			unreachable: "",
		},
		{op: "GetBucketTagging", method: "GET", kind: "bucket", query: "tagging", headers: nil, unreachable: ""},
		{op: "GetBucketVersioning", method: "GET", kind: "bucket", query: "versioning", headers: nil, unreachable: ""},
		{op: "GetBucketWebsite", method: "GET", kind: "bucket", query: "website", headers: nil, unreachable: ""},
		{op: "GetObject", method: "GET", kind: "object", query: "x-id=GetObject", headers: nil, unreachable: ""},
		{op: "GetObjectAcl", method: "GET", kind: "object", query: "acl", headers: nil, unreachable: ""},
		{
			op:          "GetObjectAnnotation",
			method:      "GET",
			kind:        "object",
			query:       "annotation&x-id=GetObjectAnnotation&annotationName=test-annotation",
			headers:     nil,
			unreachable: "",
		},
		{op: "GetObjectAttributes", method: "GET", kind: "object", query: "attributes", headers: nil, unreachable: ""},
		{op: "GetObjectLegalHold", method: "GET", kind: "object", query: "legal-hold", headers: nil, unreachable: ""},
		{
			op:          "GetObjectLockConfiguration",
			method:      "GET",
			kind:        "bucket",
			query:       "object-lock",
			headers:     nil,
			unreachable: "",
		},
		{op: "GetObjectRetention", method: "GET", kind: "object", query: "retention", headers: nil, unreachable: ""},
		{op: "GetObjectTagging", method: "GET", kind: "object", query: "tagging", headers: nil, unreachable: ""},
		{op: "GetObjectTorrent", method: "GET", kind: "object", query: "torrent", headers: nil, unreachable: ""},
		{
			op:          "GetPublicAccessBlock",
			method:      "GET",
			kind:        "bucket",
			query:       "publicAccessBlock",
			headers:     nil,
			unreachable: "",
		},
		{op: "HeadBucket", method: "HEAD", kind: "bucket", query: "", headers: nil, unreachable: ""},
		{op: "HeadObject", method: "HEAD", kind: "object", query: "", headers: nil, unreachable: ""},
		{
			op:          "ListBucketAnalyticsConfigurations",
			method:      "GET",
			kind:        "bucket",
			query:       "analytics&x-id=ListBucketAnalyticsConfigurations",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "ListBucketIntelligentTieringConfigurations",
			method:      "GET",
			kind:        "bucket",
			query:       "intelligent-tiering&x-id=ListBucketIntelligentTieringConfigurations",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "ListBucketInventoryConfigurations",
			method:      "GET",
			kind:        "bucket",
			query:       "inventory&x-id=ListBucketInventoryConfigurations",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "ListBucketMetricsConfigurations",
			method:      "GET",
			kind:        "bucket",
			query:       "metrics&x-id=ListBucketMetricsConfigurations",
			headers:     nil,
			unreachable: "",
		},
		{op: "ListBuckets", method: "GET", kind: "root", query: "x-id=ListBuckets", headers: nil, unreachable: ""},
		{
			op:          "ListDirectoryBuckets",
			method:      "GET",
			kind:        "root",
			query:       "x-id=ListDirectoryBuckets",
			headers:     nil,
			unreachable: "hostname-only discriminator (s3express-control vs s3); see PARITY.md/gopherstack-0bq8",
		},
		{op: "ListMultipartUploads", method: "GET", kind: "bucket", query: "uploads", headers: nil, unreachable: ""},
		{
			op:          "ListObjectAnnotations",
			method:      "GET",
			kind:        "object",
			query:       "annotation&x-id=ListObjectAnnotations",
			headers:     nil,
			unreachable: "",
		},
		{op: "ListObjects", method: "GET", kind: "bucket", query: "", headers: nil, unreachable: ""},
		{op: "ListObjectsV2", method: "GET", kind: "bucket", query: "list-type=2", headers: nil, unreachable: ""},
		{op: "ListObjectVersions", method: "GET", kind: "bucket", query: "versions", headers: nil, unreachable: ""},
		{
			op:          "ListParts",
			method:      "GET",
			kind:        "object",
			query:       "x-id=ListParts&uploadId=test-upload-id",
			headers:     nil,
			unreachable: "",
		},
		{op: "PutBucketAbac", method: "PUT", kind: "bucket", query: "abac", headers: nil, unreachable: ""},
		{
			op:          "PutBucketAccelerateConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "accelerate",
			headers:     nil,
			unreachable: "",
		},
		{op: "PutBucketAcl", method: "PUT", kind: "bucket", query: "acl", headers: nil, unreachable: ""},
		{
			op:          "PutBucketAnalyticsConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "analytics",
			headers:     nil,
			unreachable: "",
		},
		{op: "PutBucketCors", method: "PUT", kind: "bucket", query: "cors", headers: nil, unreachable: ""},
		{op: "PutBucketEncryption", method: "PUT", kind: "bucket", query: "encryption", headers: nil, unreachable: ""},
		{
			op:          "PutBucketIntelligentTieringConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "intelligent-tiering",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "PutBucketInventoryConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "inventory",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "PutBucketLifecycleConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "lifecycle",
			headers:     nil,
			unreachable: "",
		},
		{op: "PutBucketLogging", method: "PUT", kind: "bucket", query: "logging", headers: nil, unreachable: ""},
		{
			op:          "PutBucketMetricsConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "metrics",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "PutBucketNotificationConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "notification",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "PutBucketOwnershipControls",
			method:      "PUT",
			kind:        "bucket",
			query:       "ownershipControls",
			headers:     nil,
			unreachable: "",
		},
		{op: "PutBucketPolicy", method: "PUT", kind: "bucket", query: "policy", headers: nil, unreachable: ""},
		{
			op:          "PutBucketReplication",
			method:      "PUT",
			kind:        "bucket",
			query:       "replication",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "PutBucketRequestPayment",
			method:      "PUT",
			kind:        "bucket",
			query:       "requestPayment",
			headers:     nil,
			unreachable: "",
		},
		{op: "PutBucketTagging", method: "PUT", kind: "bucket", query: "tagging", headers: nil, unreachable: ""},
		{op: "PutBucketVersioning", method: "PUT", kind: "bucket", query: "versioning", headers: nil, unreachable: ""},
		{op: "PutBucketWebsite", method: "PUT", kind: "bucket", query: "website", headers: nil, unreachable: ""},
		{op: "PutObject", method: "PUT", kind: "object", query: "x-id=PutObject", headers: nil, unreachable: ""},
		{op: "PutObjectAcl", method: "PUT", kind: "object", query: "acl", headers: nil, unreachable: ""},
		{op: "PutObjectAnnotation", method: "PUT", kind: "object", query: "annotation", headers: nil, unreachable: ""},
		{op: "PutObjectLegalHold", method: "PUT", kind: "object", query: "legal-hold", headers: nil, unreachable: ""},
		{
			op:          "PutObjectLockConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "object-lock",
			headers:     nil,
			unreachable: "",
		},
		{op: "PutObjectRetention", method: "PUT", kind: "object", query: "retention", headers: nil, unreachable: ""},
		{op: "PutObjectTagging", method: "PUT", kind: "object", query: "tagging", headers: nil, unreachable: ""},
		{
			op:          "PutPublicAccessBlock",
			method:      "PUT",
			kind:        "bucket",
			query:       "publicAccessBlock",
			headers:     nil,
			unreachable: "",
		},
		{op: "RenameObject", method: "PUT", kind: "object", query: "renameObject", headers: nil, unreachable: ""},
		{op: "RestoreObject", method: "POST", kind: "object", query: "restore", headers: nil, unreachable: ""},
		{
			op:          "SelectObjectContent",
			method:      "POST",
			kind:        "object",
			query:       "select&select-type=2",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "UpdateBucketMetadataAnnotationTableConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "metadataAnnotationTable",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "UpdateBucketMetadataInventoryTableConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "metadataInventoryTable",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "UpdateBucketMetadataJournalTableConfiguration",
			method:      "PUT",
			kind:        "bucket",
			query:       "metadataJournalTable",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "UpdateObjectEncryption",
			method:      "PUT",
			kind:        "object",
			query:       "encryption",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "UploadPart",
			method:      "PUT",
			kind:        "object",
			query:       "x-id=UploadPart&partNumber=1&uploadId=test-upload-id",
			headers:     nil,
			unreachable: "",
		},
		{
			op:          "UploadPartCopy",
			method:      "PUT",
			kind:        "object",
			query:       "x-id=UploadPartCopy&partNumber=1&uploadId=test-upload-id",
			headers:     map[string]string{"X-Amz-Copy-Source": "/src-bucket/src-key"},
			unreachable: "",
		},
		{op: "WriteGetObjectResponse", method: "POST", kind: "literal", query: "", headers: nil, unreachable: ""},
	}
}

// buildRouteRequest turns a sdkRouteCase into the HTTP request a real
// aws-sdk-go-v2 S3 client would send for that operation (path-style
// addressing: /{bucket}/{key}), so ExtractOperation and Handler() are
// driven by the same wire shape production traffic uses.
func buildRouteRequest(tc sdkRouteCase) *http.Request {
	var path string

	switch tc.kind {
	case "root":
		path = "/"
	case "bucket":
		path = "/test-bucket"
	case "object":
		path = "/test-bucket/test-key"
	case "literal":
		path = "/WriteGetObjectResponse"
	}

	target := path
	if tc.query != "" {
		target += "?" + tc.query
	}

	req := httptest.NewRequest(tc.method, target, nil)
	for k, v := range tc.headers {
		req.Header.Set(k, v)
	}

	return req
}

// TestExtractOperation_SDKRouteTable drives every real S3 operation's
// authoritative method/path/query/header shape through the real Handler()
// and then reads ExtractOperation back from the same request context.
// Unlike the path-based services (apigateway, lambda, cloudfront), S3's
// ExtractOperation is not computed independently of dispatch -- it reads an
// s3Metrics.operation field that each leaf handler function tags itself
// with via setOperation(ctx, "<Op>") as the very first statement in its
// body (see e.g. bucket_ops_acl_policy.go's putBucketACL). So a single
// post-dispatch read of ExtractOperation verifies both that routing reached
// the correct leaf handler AND that the leaf handler identifies itself
// correctly -- there is no separate "unmatched route" sentinel to check
// for, because reaching the wrong leaf handler (or none) shows up directly
// as the wrong (or unset "Unknown") operation name.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			if tc.unreachable != "" {
				t.Skipf("structurally unreachable: %s", tc.unreachable)
			}

			h, _ := newTestHandler(t)

			req := buildRouteRequest(tc)
			ctx := logger.Save(req.Context(), slog.Default())
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)

			require.NoError(t, h.Handler()(c))

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got,
				"method=%s query=%s: Handler() dispatched to op %q, want %q",
				tc.method, tc.query, got, tc.op)
		})
	}
}
