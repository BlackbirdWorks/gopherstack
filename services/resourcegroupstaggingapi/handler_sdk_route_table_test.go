package resourcegroupstaggingapi_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// Resource Groups Tagging API operation, extracted from
// resourcegroupstaggingapi@v1.35.4/serializers.go's
// awsAwsjson11_serializeOp<Op>.HandleSerialize calls to
// SetHeader("X-Amz-Target").String("ResourceGroupsTaggingAPI_20170126.<Op>"),
// always POSTing to "/" (JSON-RPC 1.1, services/_PROTOCOLS.md).
//
// All 9 real ops are covered. GetSupportedOperations() and buildOps()'s
// map are both hand-written literals (neither built by ranging over the
// other), so this is a genuinely independent diff.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"DescribeReportCreation", "ResourceGroupsTaggingAPI_20170126.DescribeReportCreation"},
		{"GetComplianceSummary", "ResourceGroupsTaggingAPI_20170126.GetComplianceSummary"},
		{"GetResources", "ResourceGroupsTaggingAPI_20170126.GetResources"},
		{"GetTagKeys", "ResourceGroupsTaggingAPI_20170126.GetTagKeys"},
		{"GetTagValues", "ResourceGroupsTaggingAPI_20170126.GetTagValues"},
		{"ListRequiredTags", "ResourceGroupsTaggingAPI_20170126.ListRequiredTags"},
		{"StartReportCreation", "ResourceGroupsTaggingAPI_20170126.StartReportCreation"},
		{"TagResources", "ResourceGroupsTaggingAPI_20170126.TagResources"},
		{"UntagResources", "ResourceGroupsTaggingAPI_20170126.UntagResources"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Resource Groups
// Tagging API operation's authoritative X-Amz-Target through
// ExtractOperation and Handler(), confirming the header resolves to the
// right op name and that dispatch does not fall through to h.dispatch's
// single unmatched-route return (ErrUnknownOperation).
//
// ErrUnknownOperation maps to __type "UnknownOperationException"
// (handler.go's handleError), which is NOT shared with any other mapped
// error in this service -- ErrMissingS3Bucket/ErrValidation map to
// "InvalidParameterException", ErrConcurrentModification to
// "ConcurrentModificationException", ErrPaginationTokenExpired to
// "PaginationTokenExpiredException", and decode failures to
// "SerializationException" (handler.go:153-176). So unlike most of this
// class, the wire __type is a safe, unambiguous sentinel here.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := resourcegroupstaggingapi.NewHandler(
				resourcegroupstaggingapi.NewInMemoryBackend("000000000000", "us-east-1"),
			)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
