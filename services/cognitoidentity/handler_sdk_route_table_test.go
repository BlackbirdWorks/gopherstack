package cognitoidentity_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Cognito
// Identity operation, extracted from cognitoidentity@v1.36.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSCognitoIdentityService.<Op>")
// and always POSTs to "/" -- Cognito Identity is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header.
// ExtractOperation and Handler() (via h.dispatch's h.ops flat map, built
// once by buildOps()) both derive the action the same way (TrimPrefix on
// "AWSCognitoIdentityService."), so the class of bug this table catches is
// a dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case -- Cognito Identity is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers all 23 real Cognito Identity ops
// (cognitoidentity@v1.36.4) -- confirmed by diffing this SDK-extracted list
// against both GetSupportedOperations() (a hand-written literal) and the
// actual buildOps() dispatch map (also a hand-written literal, not built by
// ranging over anything): zero mismatches in either direction, no dead or
// excluded keys. The two diffs are genuinely independent -- neither is
// derived from the other.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSCognitoIdentityService.` and
// pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateIdentityPool", "AWSCognitoIdentityService.CreateIdentityPool"},
		{"DeleteIdentities", "AWSCognitoIdentityService.DeleteIdentities"},
		{"DeleteIdentityPool", "AWSCognitoIdentityService.DeleteIdentityPool"},
		{"DescribeIdentity", "AWSCognitoIdentityService.DescribeIdentity"},
		{"DescribeIdentityPool", "AWSCognitoIdentityService.DescribeIdentityPool"},
		{"GetCredentialsForIdentity", "AWSCognitoIdentityService.GetCredentialsForIdentity"},
		{"GetId", "AWSCognitoIdentityService.GetId"},
		{"GetIdentityPoolRoles", "AWSCognitoIdentityService.GetIdentityPoolRoles"},
		{"GetOpenIdToken", "AWSCognitoIdentityService.GetOpenIdToken"},
		{
			"GetOpenIdTokenForDeveloperIdentity",
			"AWSCognitoIdentityService.GetOpenIdTokenForDeveloperIdentity",
		},
		{"GetPrincipalTagAttributeMap", "AWSCognitoIdentityService.GetPrincipalTagAttributeMap"},
		{"ListIdentities", "AWSCognitoIdentityService.ListIdentities"},
		{"ListIdentityPools", "AWSCognitoIdentityService.ListIdentityPools"},
		{"ListTagsForResource", "AWSCognitoIdentityService.ListTagsForResource"},
		{"LookupDeveloperIdentity", "AWSCognitoIdentityService.LookupDeveloperIdentity"},
		{"MergeDeveloperIdentities", "AWSCognitoIdentityService.MergeDeveloperIdentities"},
		{"SetIdentityPoolRoles", "AWSCognitoIdentityService.SetIdentityPoolRoles"},
		{"SetPrincipalTagAttributeMap", "AWSCognitoIdentityService.SetPrincipalTagAttributeMap"},
		{"TagResource", "AWSCognitoIdentityService.TagResource"},
		{"UnlinkDeveloperIdentity", "AWSCognitoIdentityService.UnlinkDeveloperIdentity"},
		{"UnlinkIdentity", "AWSCognitoIdentityService.UnlinkIdentity"},
		{"UntagResource", "AWSCognitoIdentityService.UntagResource"},
		{"UpdateIdentityPool", "AWSCognitoIdentityService.UpdateIdentityPool"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Cognito Identity
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to h.dispatch's single unmatched-route
// return (fmt.Errorf("%w: %s", errUnknownAction, action), handler.go's
// dispatch() single production call site).
//
// Unlike ce/support/timestreamwrite/directconnect in this same pass,
// cognitoidentity's dispatch-miss sentinel maps to a wire type
// ("UnknownOperationException", via cognitoIdentitySentinelErrors' last
// entry) that is NOT shared with any other mapped error in this package --
// grepped: the literal `"UnknownOperationException"` (which doubles as
// errUnknownAction's own error message, handler.go:23) appears at exactly
// those two sites, both in this dispatch-miss path. So asserting on the
// wire __type is safe here.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
			h := cognitoidentity.NewHandler(backend, "us-east-1")

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
