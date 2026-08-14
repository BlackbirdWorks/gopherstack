package identitystore_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// Identity Store operation, extracted from
// identitystore@v1.39.4/serializers.go's
// awsAwsjson11_serializeOp<Op>.HandleSerialize calls to
// SetHeader("X-Amz-Target").String("AWSIdentityStore.<Op>"), always
// POSTing to "/" (JSON-RPC 1.1, services/_PROTOCOLS.md). "AWSIdentityStore"
// is Identity Store's real internal AWS codename -- unrelated to the
// "identitystore" directory name or the "IAM Identity Center" / "Identity
// Store" public branding, confirmed directly from serializers.go, not
// guessed.
//
// All 19 real ops are covered. GetSupportedOperations() and the
// identityStoreDispatch package-level map both reference the SAME opXxx Go
// constants (handler.go:32-50) -- this is the SHARED-CONSTANT diff kind: a
// typo in a constant's *value* would be invisible to a diff between the two
// structures, since both would silently agree on the wrong string. Only
// omissions would be caught. This table sidesteps that blind spot entirely
// by hardcoding the real SDK target strings independently of gopherstack's
// own opXxx constants, so a wrong constant value fails here even though it
// would pass a same-repo cross-check.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateGroup", "AWSIdentityStore.CreateGroup"},
		{"CreateGroupMembership", "AWSIdentityStore.CreateGroupMembership"},
		{"CreateUser", "AWSIdentityStore.CreateUser"},
		{"DeleteGroup", "AWSIdentityStore.DeleteGroup"},
		{"DeleteGroupMembership", "AWSIdentityStore.DeleteGroupMembership"},
		{"DeleteUser", "AWSIdentityStore.DeleteUser"},
		{"DescribeGroup", "AWSIdentityStore.DescribeGroup"},
		{"DescribeGroupMembership", "AWSIdentityStore.DescribeGroupMembership"},
		{"DescribeUser", "AWSIdentityStore.DescribeUser"},
		{"GetGroupId", "AWSIdentityStore.GetGroupId"},
		{"GetGroupMembershipId", "AWSIdentityStore.GetGroupMembershipId"},
		{"GetUserId", "AWSIdentityStore.GetUserId"},
		{"IsMemberInGroups", "AWSIdentityStore.IsMemberInGroups"},
		{"ListGroupMemberships", "AWSIdentityStore.ListGroupMemberships"},
		{"ListGroupMembershipsForMember", "AWSIdentityStore.ListGroupMembershipsForMember"},
		{"ListGroups", "AWSIdentityStore.ListGroups"},
		{"ListUsers", "AWSIdentityStore.ListUsers"},
		{"UpdateGroup", "AWSIdentityStore.UpdateGroup"},
		{"UpdateUser", "AWSIdentityStore.UpdateUser"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Identity Store
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), confirming the header resolves to the right op name and that
// dispatch does not fall through to dispatch()'s single unmatched-route
// return, which writes __type "UnrecognizedClientException" (handler.go:
// 229-230). That __type has exactly two production call sites (grepped):
// this unmatched-route path and the separate missing-X-Amz-Target-header
// path in Handler() (handler.go:150) -- both mean "no operation was
// dispatched", never a legitimate per-op business error (ValidationException,
// ConflictException, ResourceNotFoundException, InternalServerException are
// the only other mapped types, handleBackendError, handler.go:351-376), so
// asserting on wire __type is safe here.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := identitystore.NewHandler(identitystore.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnrecognizedClientException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
