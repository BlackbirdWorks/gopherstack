package verifiedpermissions_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Verified
// Permissions operation, extracted from verifiedpermissions@v1.36.4
// serializers.go: each op's awsAwsjson10_serializeOp<Op>.HandleSerialize
// sets httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "VerifiedPermissions.<Op>") and always POSTs to "/" -- Verified
// Permissions is JSON-RPC 1.0 (services/_PROTOCOLS.md, confirmed directly
// against the awsAwsjson10_ serializer prefix -- not the more common 1.1),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. The target prefix
// ("VerifiedPermissions", bare) is read directly from serializers.go.
// ExtractOperation and Handler() (via buildOps()'s map, dispatched through
// h.dispatch) both derive the action the same way, so the class of bug this
// table catches is a dispatch-table key that doesn't exactly match the real
// op name (typo, wrong case), not a route-template mismatch.
//
// This table covers all 34 real Verified Permissions ops
// (verifiedpermissions@v1.36.4) -- confirmed by diffing both
// GetSupportedOperations() and the actual buildOps() map's key set against
// this exact list: zero mismatches in either direction, no dead or excluded
// keys. GetSupportedOperations() here is a hand-maintained literal slice,
// not built by ranging over the dispatch map, so the two diffs are
// genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("VerifiedPermissions.` and pulling the
// suffix after the dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"BatchGetPolicy", "VerifiedPermissions.BatchGetPolicy"},
		{"BatchIsAuthorized", "VerifiedPermissions.BatchIsAuthorized"},
		{"BatchIsAuthorizedWithToken", "VerifiedPermissions.BatchIsAuthorizedWithToken"},
		{"CreateIdentitySource", "VerifiedPermissions.CreateIdentitySource"},
		{"CreatePolicy", "VerifiedPermissions.CreatePolicy"},
		{"CreatePolicyStore", "VerifiedPermissions.CreatePolicyStore"},
		{"CreatePolicyStoreAlias", "VerifiedPermissions.CreatePolicyStoreAlias"},
		{"CreatePolicyTemplate", "VerifiedPermissions.CreatePolicyTemplate"},
		{"DeleteIdentitySource", "VerifiedPermissions.DeleteIdentitySource"},
		{"DeletePolicy", "VerifiedPermissions.DeletePolicy"},
		{"DeletePolicyStore", "VerifiedPermissions.DeletePolicyStore"},
		{"DeletePolicyStoreAlias", "VerifiedPermissions.DeletePolicyStoreAlias"},
		{"DeletePolicyTemplate", "VerifiedPermissions.DeletePolicyTemplate"},
		{"GetIdentitySource", "VerifiedPermissions.GetIdentitySource"},
		{"GetPolicy", "VerifiedPermissions.GetPolicy"},
		{"GetPolicyStore", "VerifiedPermissions.GetPolicyStore"},
		{"GetPolicyStoreAlias", "VerifiedPermissions.GetPolicyStoreAlias"},
		{"GetPolicyTemplate", "VerifiedPermissions.GetPolicyTemplate"},
		{"GetSchema", "VerifiedPermissions.GetSchema"},
		{"IsAuthorized", "VerifiedPermissions.IsAuthorized"},
		{"IsAuthorizedWithToken", "VerifiedPermissions.IsAuthorizedWithToken"},
		{"ListIdentitySources", "VerifiedPermissions.ListIdentitySources"},
		{"ListPolicies", "VerifiedPermissions.ListPolicies"},
		{"ListPolicyStoreAliases", "VerifiedPermissions.ListPolicyStoreAliases"},
		{"ListPolicyStores", "VerifiedPermissions.ListPolicyStores"},
		{"ListPolicyTemplates", "VerifiedPermissions.ListPolicyTemplates"},
		{"ListTagsForResource", "VerifiedPermissions.ListTagsForResource"},
		{"PutSchema", "VerifiedPermissions.PutSchema"},
		{"TagResource", "VerifiedPermissions.TagResource"},
		{"UntagResource", "VerifiedPermissions.UntagResource"},
		{"UpdateIdentitySource", "VerifiedPermissions.UpdateIdentitySource"},
		{"UpdatePolicy", "VerifiedPermissions.UpdatePolicy"},
		{"UpdatePolicyStore", "VerifiedPermissions.UpdatePolicyStore"},
		{"UpdatePolicyTemplate", "VerifiedPermissions.UpdatePolicyTemplate"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Verified Permissions
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss sentinel
// (errUnknownAction, handler.go's dispatch() single production call site)
// that a dispatch-table key mismatch would produce.
//
// errUnknownAction has its own dedicated case in handleError's switch,
// rendering as "UnknownOperationException" -- a wire type no other sentinel
// in this package maps to (ErrTooManyTags -> "TooManyTagsException";
// awserr.ErrInvalidParameter/errInvalidRequest/syntax/type errors all ->
// "ValidationException"; awserr.ErrNotFound -> "ResourceNotFoundException";
// awserr.ErrConflict -> "ConflictException"), so asserting on the wire type
// is safe here, unlike codedeploy/codecommit/textract, whose dispatch-miss
// sentinel shares its wire type with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
			h := verifiedpermissions.NewHandler(b)

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
