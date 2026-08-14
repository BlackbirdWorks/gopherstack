package efs_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real EFS
// operation, extracted from efs@v1.44.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// DescribeReplicationConfigurations ("GET .../file-systems/replication-configurations",
// a literal path segment) versus CreateReplicationConfiguration/
// DeleteReplicationConfiguration ("PLACEHOLDER/replication-configuration",
// singular, with a real file system ID in between) is this service's one
// path-shape trap comparable to the s3/glacier discriminator class -- both
// forms are kept here rather than collapsed, and parseEFSPath's explicit
// `id == "replication-configurations"` case (handler.go) already resolves it
// ahead of the generic file-system-ID branch.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateAccessPoint", "POST", "/2015-02-01/access-points"},
		{"CreateFileSystem", "POST", "/2015-02-01/file-systems"},
		{"CreateMountTarget", "POST", "/2015-02-01/mount-targets"},
		{"CreateReplicationConfiguration", "POST", "/2015-02-01/file-systems/PLACEHOLDER/replication-configuration"},
		{"CreateTags", "POST", "/2015-02-01/create-tags/PLACEHOLDER"},
		{"DeleteAccessPoint", "DELETE", "/2015-02-01/access-points/PLACEHOLDER"},
		{"DeleteFileSystem", "DELETE", "/2015-02-01/file-systems/PLACEHOLDER"},
		{"DeleteFileSystemPolicy", "DELETE", "/2015-02-01/file-systems/PLACEHOLDER/policy"},
		{"DeleteMountTarget", "DELETE", "/2015-02-01/mount-targets/PLACEHOLDER"},
		{"DeleteReplicationConfiguration", "DELETE", "/2015-02-01/file-systems/PLACEHOLDER/replication-configuration"},
		{"DeleteTags", "POST", "/2015-02-01/delete-tags/PLACEHOLDER"},
		{"DescribeAccessPoints", "GET", "/2015-02-01/access-points"},
		{"DescribeAccountPreferences", "GET", "/2015-02-01/account-preferences"},
		{"DescribeBackupPolicy", "GET", "/2015-02-01/file-systems/PLACEHOLDER/backup-policy"},
		{"DescribeFileSystemPolicy", "GET", "/2015-02-01/file-systems/PLACEHOLDER/policy"},
		{"DescribeFileSystems", "GET", "/2015-02-01/file-systems"},
		{"DescribeLifecycleConfiguration", "GET", "/2015-02-01/file-systems/PLACEHOLDER/lifecycle-configuration"},
		{"DescribeMountTargetSecurityGroups", "GET", "/2015-02-01/mount-targets/PLACEHOLDER/security-groups"},
		{"DescribeMountTargets", "GET", "/2015-02-01/mount-targets"},
		{"DescribeReplicationConfigurations", "GET", "/2015-02-01/file-systems/replication-configurations"},
		{"DescribeTags", "GET", "/2015-02-01/tags/PLACEHOLDER"},
		{"ListTagsForResource", "GET", "/2015-02-01/resource-tags/PLACEHOLDER"},
		{"ModifyMountTargetSecurityGroups", "PUT", "/2015-02-01/mount-targets/PLACEHOLDER/security-groups"},
		{"PutAccountPreferences", "PUT", "/2015-02-01/account-preferences"},
		{"PutBackupPolicy", "PUT", "/2015-02-01/file-systems/PLACEHOLDER/backup-policy"},
		{"PutFileSystemPolicy", "PUT", "/2015-02-01/file-systems/PLACEHOLDER/policy"},
		{"PutLifecycleConfiguration", "PUT", "/2015-02-01/file-systems/PLACEHOLDER/lifecycle-configuration"},
		{"TagResource", "POST", "/2015-02-01/resource-tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/2015-02-01/resource-tags/PLACEHOLDER"},
		{"UpdateFileSystem", "PUT", "/2015-02-01/file-systems/PLACEHOLDER"},
		{"UpdateFileSystemProtection", "PUT", "/2015-02-01/file-systems/PLACEHOLDER/protection"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real EFS op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseEFSPath resolves it to the right op, all 31 ops against efs's
// real op count. It then drives the same request through the real Handler()
// and asserts it did not fall through to the exact "unknown operation: "
// prefix that dispatch's final default case (handler.go) emits under the
// "UnsupportedOperation" error code when parseEFSPath's route reaches no
// dispatch* function -- distinct from the domain not-found errors
// (FileSystemNotFound, MountTargetNotFound, AccessPointNotFound,
// PolicyNotFound), all of which use their own specific error codes.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
