package mediastore_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// Elemental MediaStore operation, extracted from
// mediastore@v1.32.4/serializers.go's
// awsAwsjson11_serializeOp<Op>.HandleSerialize calls to
// SetHeader("X-Amz-Target").String("MediaStore_20170901.<Op>"), always
// POSTing to "/" (JSON-RPC 1.1, services/_PROTOCOLS.md).
//
// All 21 real ops are covered. GetSupportedOperations() and the
// mediastoreDispatch package-level map both reference the SAME opMSxxx Go
// constants (handler.go:26-48) -- this is the SHARED-CONSTANT diff kind: a
// typo in a constant's *value* would be invisible to a diff between the two
// structures, since both would silently agree on the wrong string. Only
// omissions would be caught. This table sidesteps that blind spot entirely
// by hardcoding the real SDK target strings independently of gopherstack's
// own opMSxxx constants, so a wrong constant value fails here even though
// it would pass a same-repo cross-check.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateContainer", "MediaStore_20170901.CreateContainer"},
		{"DeleteContainer", "MediaStore_20170901.DeleteContainer"},
		{"DeleteContainerPolicy", "MediaStore_20170901.DeleteContainerPolicy"},
		{"DeleteCorsPolicy", "MediaStore_20170901.DeleteCorsPolicy"},
		{"DeleteLifecyclePolicy", "MediaStore_20170901.DeleteLifecyclePolicy"},
		{"DeleteMetricPolicy", "MediaStore_20170901.DeleteMetricPolicy"},
		{"DescribeContainer", "MediaStore_20170901.DescribeContainer"},
		{"GetContainerPolicy", "MediaStore_20170901.GetContainerPolicy"},
		{"GetCorsPolicy", "MediaStore_20170901.GetCorsPolicy"},
		{"GetLifecyclePolicy", "MediaStore_20170901.GetLifecyclePolicy"},
		{"GetMetricPolicy", "MediaStore_20170901.GetMetricPolicy"},
		{"ListContainers", "MediaStore_20170901.ListContainers"},
		{"ListTagsForResource", "MediaStore_20170901.ListTagsForResource"},
		{"PutContainerPolicy", "MediaStore_20170901.PutContainerPolicy"},
		{"PutCorsPolicy", "MediaStore_20170901.PutCorsPolicy"},
		{"PutLifecyclePolicy", "MediaStore_20170901.PutLifecyclePolicy"},
		{"PutMetricPolicy", "MediaStore_20170901.PutMetricPolicy"},
		{"StartAccessLogging", "MediaStore_20170901.StartAccessLogging"},
		{"StopAccessLogging", "MediaStore_20170901.StopAccessLogging"},
		{"TagResource", "MediaStore_20170901.TagResource"},
		{"UntagResource", "MediaStore_20170901.UntagResource"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real MediaStore
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), confirming the header resolves to the right op name and that
// dispatch does not fall through to dispatch()'s single unmatched-route
// return, which writes __type "UnknownOperationException" (handler.go:218).
// That __type has exactly one production call site (grepped) and is not
// reused by any modeled MediaStore error (ContainerNotFoundException,
// PolicyNotFoundException, CorsPolicyNotFoundException,
// ResourceNotFoundException, ContainerInUseException, ValidationException,
// SerializationException, InternalFailure -- writeBackendError,
// handler.go:624-660), so asserting on wire __type is safe here.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := mediastore.NewHandler(mediastore.NewInMemoryBackend())
			h.AccountID = "000000000000"
			h.DefaultRegion = "us-east-1"

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
