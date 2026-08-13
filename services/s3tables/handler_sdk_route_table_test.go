package s3tables_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real S3 Tables
// operation, extracted from s3tables@v1.18.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op. None of
// this service's URI labels are greedy ({param+}), so a real tableBucketARN
// (which itself contains "/") always arrives percent-encoded and never adds
// extra path segments -- confirmed both in serializers.go (no "+" labels)
// and against the handler's rawPathSegments, which splits on RawPath (still
// percent-encoded) rather than the decoded Path for exactly this reason.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateNamespace", "PUT", "/namespaces/PLACEHOLDER"},
		{"CreateTable", "PUT", "/tables/PLACEHOLDER/PLACEHOLDER"},
		{"CreateTableBucket", "PUT", "/buckets"},
		{"DeleteNamespace", "DELETE", "/namespaces/PLACEHOLDER/PLACEHOLDER"},
		{"DeleteTable", "DELETE", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER"},
		{"DeleteTableBucket", "DELETE", "/buckets/PLACEHOLDER"},
		{"DeleteTableBucketEncryption", "DELETE", "/buckets/PLACEHOLDER/encryption"},
		{"DeleteTableBucketMetricsConfiguration", "DELETE", "/buckets/PLACEHOLDER/metrics"},
		{"DeleteTableBucketPolicy", "DELETE", "/buckets/PLACEHOLDER/policy"},
		{"DeleteTableBucketReplication", "DELETE", "/table-bucket-replication"},
		{"DeleteTablePolicy", "DELETE", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/policy"},
		{"DeleteTableReplication", "DELETE", "/table-replication"},
		{"GetNamespace", "GET", "/namespaces/PLACEHOLDER/PLACEHOLDER"},
		{"GetTable", "GET", "/get-table"},
		{"GetTableBucket", "GET", "/buckets/PLACEHOLDER"},
		{"GetTableBucketEncryption", "GET", "/buckets/PLACEHOLDER/encryption"},
		{"GetTableBucketMaintenanceConfiguration", "GET", "/buckets/PLACEHOLDER/maintenance"},
		{"GetTableBucketMetricsConfiguration", "GET", "/buckets/PLACEHOLDER/metrics"},
		{"GetTableBucketPolicy", "GET", "/buckets/PLACEHOLDER/policy"},
		{"GetTableBucketReplication", "GET", "/table-bucket-replication"},
		{"GetTableBucketStorageClass", "GET", "/buckets/PLACEHOLDER/storage-class"},
		{"GetTableEncryption", "GET", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/encryption"},
		{"GetTableMaintenanceConfiguration", "GET", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/maintenance"},
		{"GetTableMaintenanceJobStatus", "GET", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/maintenance-job-status"},
		{"GetTableMetadataLocation", "GET", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/metadata-location"},
		{"GetTablePolicy", "GET", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/policy"},
		{"GetTableRecordExpirationConfiguration", "GET", "/table-record-expiration"},
		{"GetTableRecordExpirationJobStatus", "GET", "/table-record-expiration-job-status"},
		{"GetTableReplication", "GET", "/table-replication"},
		{"GetTableReplicationStatus", "GET", "/replication-status"},
		{"GetTableStorageClass", "GET", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/storage-class"},
		{"ListNamespaces", "GET", "/namespaces/PLACEHOLDER"},
		{"ListTableBuckets", "GET", "/buckets"},
		{"ListTables", "GET", "/tables/PLACEHOLDER"},
		{"ListTagsForResource", "GET", "/tag/PLACEHOLDER"},
		{"PutTableBucketEncryption", "PUT", "/buckets/PLACEHOLDER/encryption"},
		{"PutTableBucketMaintenanceConfiguration", "PUT", "/buckets/PLACEHOLDER/maintenance/PLACEHOLDER"},
		{"PutTableBucketMetricsConfiguration", "PUT", "/buckets/PLACEHOLDER/metrics"},
		{"PutTableBucketPolicy", "PUT", "/buckets/PLACEHOLDER/policy"},
		{"PutTableBucketReplication", "PUT", "/table-bucket-replication"},
		{"PutTableBucketStorageClass", "PUT", "/buckets/PLACEHOLDER/storage-class"},
		{
			"PutTableMaintenanceConfiguration", "PUT",
			"/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/maintenance/PLACEHOLDER",
		},
		{"PutTablePolicy", "PUT", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/policy"},
		{"PutTableRecordExpirationConfiguration", "PUT", "/table-record-expiration"},
		{"PutTableReplication", "PUT", "/table-replication"},
		{"RenameTable", "PUT", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/rename"},
		{"TagResource", "POST", "/tag/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tag/PLACEHOLDER"},
		{"UpdateTableMetadataLocation", "PUT", "/tables/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER/metadata-location"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real S3 Tables op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 3: re-extracted all 49 s3tables ops from the pinned SDK and confirmed the
// existing routeRequest table already correct, including the standalone
// "/get-table" path (distinct from "/tables/{bucket}/{ns}/{name}") and the
// several same-path/different-method collisions this service's routing
// depends on (/buckets/{arn}/{encryption,metrics,policy,storage-class},
// /table-bucket-replication, /table-replication).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)
		})
	}
}
