package rolesanywhere_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Roles
// Anywhere operation, extracted from rolesanywhere@v1.26.3 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {trustAnchorId}/{profileId}/{crlId}/{subjectId} URI label --
// parseRESTPath and its per-family helpers (handler.go) never validate
// identifier shape, so the literal value doesn't matter here, only path
// depth and static segments. 30 real ops here, matching Roles Anywhere's
// real op count exactly (also matches GetSupportedOperations's own 30
// entries one-for-one). Note the wire shape's genuine oddities, all
// confirmed directly against the SDK source and already routed correctly by
// this handler: ListTagsForResource/TagResource/UntagResource live at
// literal PascalCase paths ("/ListTagsForResource", "/TagResource",
// "/UntagResource") rather than the "/tags/..." convention most sibling
// services use, and UntagResource is bound to POST rather than the more
// common DELETE.
//
// A systematic check for a shared method+path across all 30 ops found zero
// collisions -- every op has its own unique (method, path) pair, so no
// *required dynamic* (non-template) member -- the s3/glacier vacuity-trap
// class -- was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateProfile", "POST", "/profiles"},
		{"CreateTrustAnchor", "POST", "/trustanchors"},
		{"DeleteAttributeMapping", "DELETE", "/profiles/PLACEHOLDER/mappings"},
		{"DeleteCrl", "DELETE", "/crl/PLACEHOLDER"},
		{"DeleteProfile", "DELETE", "/profile/PLACEHOLDER"},
		{"DeleteTrustAnchor", "DELETE", "/trustanchor/PLACEHOLDER"},
		{"DisableCrl", "POST", "/crl/PLACEHOLDER/disable"},
		{"DisableProfile", "POST", "/profile/PLACEHOLDER/disable"},
		{"DisableTrustAnchor", "POST", "/trustanchor/PLACEHOLDER/disable"},
		{"EnableCrl", "POST", "/crl/PLACEHOLDER/enable"},
		{"EnableProfile", "POST", "/profile/PLACEHOLDER/enable"},
		{"EnableTrustAnchor", "POST", "/trustanchor/PLACEHOLDER/enable"},
		{"GetCrl", "GET", "/crl/PLACEHOLDER"},
		{"GetProfile", "GET", "/profile/PLACEHOLDER"},
		{"GetSubject", "GET", "/subject/PLACEHOLDER"},
		{"GetTrustAnchor", "GET", "/trustanchor/PLACEHOLDER"},
		{"ImportCrl", "POST", "/crls"},
		{"ListCrls", "GET", "/crls"},
		{"ListProfiles", "GET", "/profiles"},
		{"ListSubjects", "GET", "/subjects"},
		{"ListTagsForResource", "GET", "/ListTagsForResource"},
		{"ListTrustAnchors", "GET", "/trustanchors"},
		{"PutAttributeMapping", "PUT", "/profiles/PLACEHOLDER/mappings"},
		{"PutNotificationSettings", "PATCH", "/put-notifications-settings"},
		{"ResetNotificationSettings", "PATCH", "/reset-notifications-settings"},
		{"TagResource", "POST", "/TagResource"},
		{"UntagResource", "POST", "/UntagResource"},
		{"UpdateCrl", "PATCH", "/crl/PLACEHOLDER"},
		{"UpdateProfile", "PATCH", "/profile/PLACEHOLDER"},
		{"UpdateTrustAnchor", "PATCH", "/trustanchor/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Roles Anywhere op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseRESTPath (handler.go) resolves it to the right op, all 30
// ops against Roles Anywhere's real op count. It then drives the same
// request through the real Handler() and asserts the response does not
// contain the exact literal "not found" that handleREST emits via
// c.JSON(http.StatusNotFound, errBody("ResourceNotFoundException", "not
// found")) when parseRESTPath returns opUnknown.
//
// "not found" was grepped across every non-test .go file in this package
// and found nowhere else: every domain not-found sentinel in errors.go
// (ErrTrustAnchorNotFound, ErrProfileNotFound, ErrCrlNotFound,
// ErrSubjectNotFound, ErrResourceNotFound) is awserr.New("ResourceNotFound
// Exception", awserr.ErrNotFound) -- its err.Error() is the single literal
// "ResourceNotFoundException" with no message text at all, so it cannot
// collide with the miss sentinel's "not found" substring.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "not found",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
