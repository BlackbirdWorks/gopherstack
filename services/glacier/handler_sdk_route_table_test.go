package glacier_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Glacier
// operation, extracted from glacier@v1.35.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {accountId}/{vaultName}/{...Id} URI label -- the router does not
// validate ID shape, so the literal value doesn't matter here, only that
// the path matches Op. glacier@v1.35.4 serializes with awsRestjson1_, not
// REST-XML -- confirmed directly against the pinned SDK source, correcting
// an assumption otherwise.
//
// AddTagsToVault and RemoveTagsFromVault are this service's one genuine
// same-method/same-path collision (POST .../tags), resolved only by the
// literal "?operation=add" vs "?operation=remove" query baked into each
// op's own SplitURI template -- both kept here rather than collapsed, and
// parseTagsPath (handler.go) already discriminates on exactly that query
// substring. No other pair of ops in this table shares a
// method+path-without-query combination, so unlike s3's UploadPart/
// PutObject, no *required dynamic* (non-template) member is needed to
// disambiguate any glacier route.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AbortMultipartUpload", "DELETE", "/PLACEHOLDER/vaults/PLACEHOLDER/multipart-uploads/PLACEHOLDER"},
		{"AbortVaultLock", "DELETE", "/PLACEHOLDER/vaults/PLACEHOLDER/lock-policy"},
		{"AddTagsToVault", "POST", "/PLACEHOLDER/vaults/PLACEHOLDER/tags?operation=add"},
		{"CompleteMultipartUpload", "POST", "/PLACEHOLDER/vaults/PLACEHOLDER/multipart-uploads/PLACEHOLDER"},
		{"CompleteVaultLock", "POST", "/PLACEHOLDER/vaults/PLACEHOLDER/lock-policy/PLACEHOLDER"},
		{"CreateVault", "PUT", "/PLACEHOLDER/vaults/PLACEHOLDER"},
		{"DeleteArchive", "DELETE", "/PLACEHOLDER/vaults/PLACEHOLDER/archives/PLACEHOLDER"},
		{"DeleteVault", "DELETE", "/PLACEHOLDER/vaults/PLACEHOLDER"},
		{"DeleteVaultAccessPolicy", "DELETE", "/PLACEHOLDER/vaults/PLACEHOLDER/access-policy"},
		{"DeleteVaultNotifications", "DELETE", "/PLACEHOLDER/vaults/PLACEHOLDER/notification-configuration"},
		{"DescribeJob", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/jobs/PLACEHOLDER"},
		{"DescribeVault", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER"},
		{"GetDataRetrievalPolicy", "GET", "/PLACEHOLDER/policies/data-retrieval"},
		{"GetJobOutput", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/jobs/PLACEHOLDER/output"},
		{"GetVaultAccessPolicy", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/access-policy"},
		{"GetVaultLock", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/lock-policy"},
		{"GetVaultNotifications", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/notification-configuration"},
		{"InitiateJob", "POST", "/PLACEHOLDER/vaults/PLACEHOLDER/jobs"},
		{"InitiateMultipartUpload", "POST", "/PLACEHOLDER/vaults/PLACEHOLDER/multipart-uploads"},
		{"InitiateVaultLock", "POST", "/PLACEHOLDER/vaults/PLACEHOLDER/lock-policy"},
		{"ListJobs", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/jobs"},
		{"ListMultipartUploads", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/multipart-uploads"},
		{"ListParts", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/multipart-uploads/PLACEHOLDER"},
		{"ListProvisionedCapacity", "GET", "/PLACEHOLDER/provisioned-capacity"},
		{"ListTagsForVault", "GET", "/PLACEHOLDER/vaults/PLACEHOLDER/tags"},
		{"ListVaults", "GET", "/PLACEHOLDER/vaults"},
		{"PurchaseProvisionedCapacity", "POST", "/PLACEHOLDER/provisioned-capacity"},
		{"RemoveTagsFromVault", "POST", "/PLACEHOLDER/vaults/PLACEHOLDER/tags?operation=remove"},
		{"SetDataRetrievalPolicy", "PUT", "/PLACEHOLDER/policies/data-retrieval"},
		{"SetVaultAccessPolicy", "PUT", "/PLACEHOLDER/vaults/PLACEHOLDER/access-policy"},
		{"SetVaultNotifications", "PUT", "/PLACEHOLDER/vaults/PLACEHOLDER/notification-configuration"},
		{"UploadArchive", "POST", "/PLACEHOLDER/vaults/PLACEHOLDER/archives"},
		{"UploadMultipartPart", "PUT", "/PLACEHOLDER/vaults/PLACEHOLDER/multipart-uploads/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Glacier op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseGlacierPath resolves it to the right op, all 33 ops against
// glacier's real op count. It then drives the same request through the real
// Handler() and asserts it did not fall through to the literal
// "unknown operation" text that dispatch's final default case (handler.go)
// emits when parseGlacierPath returns an op with no matching dispatch case --
// distinct from the "not found" text ExtractOperation's own failure emits
// (unreachable here since ExtractOperation is asserted to return tc.op
// first) and distinct from every domain not-found error (e.g. "Archive not
// found"), none of which contain the substring "unknown operation".
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

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
