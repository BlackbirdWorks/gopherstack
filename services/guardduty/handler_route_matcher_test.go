package guardduty_test

// This file exercises Handler.RouteMatcher and Handler.ExtractOperation
// directly -- the two entry points a real echo router uses to decide (a)
// whether a request belongs to this service at all, and (b) which operation
// it maps to.
//
// Every other test in this package drives requests through auditDo, which
// calls h.Handler()(c) directly and so bypasses RouteMatcher and the
// method-aware branch of ExtractOperation's underlying parseRESTPath
// entirely. That means a routing bug -- an op registered for the wrong HTTP
// method, or a path prefix RouteMatcher doesn't recognize -- can hide behind
// 100% green unit tests while being completely unreachable by a real
// aws-sdk-go-v2 client going through echo's router. This exact bug class
// (a whole op silently unroutable) previously hit services/backup and
// services/eks; TestRouteMatcher_MethodSensitivity below is a regression
// guard for the UpdateMalwareProtectionPlan instance of it found in
// GuardDuty: aws-sdk-go-v2/service/guardduty is the one op in this service
// that serializes with PATCH instead of POST.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// routeMatcherContext builds a bare *echo.Context for method+path, with no
// body, matching the minimal input RouteMatcher/ExtractOperation need.
func routeMatcherContext(t *testing.T, method, path string) *echo.Context {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	return c
}

// TestRouteMatcher_RecognizesServicePaths checks that RouteMatcher accepts
// every top-level path prefix a real GuardDuty client can hit and rejects
// paths belonging to other services.
func TestRouteMatcher_RecognizesServicePaths(t *testing.T) {
	t.Parallel()

	h := guardduty.NewHandler(guardduty.NewInMemoryBackend("123456789012", "us-east-1"))

	tests := []struct {
		method string
		path   string
		name   string
		want   bool
	}{
		{name: "detector root", method: http.MethodPost, path: "/detector", want: true},
		{name: "detector nested", method: http.MethodGet, path: "/detector/d1/filter/f1", want: true},
		{name: "malware-protection-plan", method: http.MethodPost, path: "/malware-protection-plan", want: true},
		{name: "malware-scan", method: http.MethodPost, path: "/malware-scan/start", want: true},
		{name: "admin", method: http.MethodGet, path: "/admin", want: true},
		{name: "invitation", method: http.MethodGet, path: "/invitation", want: true},
		{name: "organization", method: http.MethodGet, path: "/organization/statistics", want: true},
		{name: "object-malware-scan", method: http.MethodPost, path: "/object-malware-scan/send", want: true},
		{
			name: "tags for a guardduty ARN", method: http.MethodGet,
			path: "/tags/arn:aws:guardduty:us-east-1:123456789012:detector/d1", want: true,
		},
		{
			name: "tags for a GovCloud-partition guardduty ARN", method: http.MethodGet,
			path: "/tags/arn:aws-us-gov:guardduty:us-gov-west-1:123456789012:detector/d1", want: true,
		},
		{
			name: "tags for a non-guardduty ARN", method: http.MethodGet,
			path: "/tags/arn:aws:s3:::my-bucket", want: false,
		},
		{name: "unrelated service path", method: http.MethodGet, path: "/2015-03-31/functions", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := routeMatcherContext(t, tt.method, tt.path)
			assert.Equal(t, tt.want, h.RouteMatcher()(c), "path %s %s", tt.method, tt.path)
		})
	}
}

// TestRouteMatcher_MethodSensitivity walks (method, path) -> expected
// operation for every family, the way a real echo router + ExtractOperation
// would see an incoming aws-sdk-go-v2 request. Regression guard for
// go-15xt-style bugs: an op registered under the wrong HTTP method is
// unroutable in production even though a body-level test bypassing routing
// would never catch it.
func TestRouteMatcher_MethodSensitivity(t *testing.T) {
	t.Parallel()

	h := guardduty.NewHandler(guardduty.NewInMemoryBackend("123456789012", "us-east-1"))

	tests := []struct {
		method string
		path   string
		wantOp string
		name   string
	}{
		{name: "CreateDetector", method: http.MethodPost, path: "/detector", wantOp: "CreateDetector"},
		{name: "ListDetectors", method: http.MethodGet, path: "/detector", wantOp: "ListDetectors"},
		{name: "GetDetector", method: http.MethodGet, path: "/detector/d1", wantOp: "GetDetector"},
		{name: "UpdateDetector", method: http.MethodPost, path: "/detector/d1", wantOp: "UpdateDetector"},
		{name: "DeleteDetector", method: http.MethodDelete, path: "/detector/d1", wantOp: "DeleteDetector"},
		{name: "CreateFilter", method: http.MethodPost, path: "/detector/d1/filter", wantOp: "CreateFilter"},
		{name: "GetFilter", method: http.MethodGet, path: "/detector/d1/filter/f1", wantOp: "GetFilter"},
		{name: "DeleteFilter", method: http.MethodDelete, path: "/detector/d1/filter/f1", wantOp: "DeleteFilter"},
		{name: "CreateIPSet", method: http.MethodPost, path: "/detector/d1/ipset", wantOp: "CreateIPSet"},
		{name: "DeleteIPSet", method: http.MethodDelete, path: "/detector/d1/ipset/s1", wantOp: "DeleteIPSet"},
		{
			name: "CreateThreatIntelSet", method: http.MethodPost,
			path: "/detector/d1/threatintelset", wantOp: "CreateThreatIntelSet",
		},
		{
			name: "ArchiveFindings", method: http.MethodPost,
			path: "/detector/d1/findings/archive", wantOp: "ArchiveFindings",
		},
		{
			name: "UnarchiveFindings", method: http.MethodPost,
			path: "/detector/d1/findings/unarchive", wantOp: "UnarchiveFindings",
		},
		{
			name: "TagResource", method: http.MethodPost,
			path: "/tags/arn:aws:guardduty:us-east-1:123456789012:detector/d1", wantOp: "TagResource",
		},
		{
			name: "UntagResource", method: http.MethodDelete,
			path: "/tags/arn:aws:guardduty:us-east-1:123456789012:detector/d1", wantOp: "UntagResource",
		},
		{
			name: "ListTagsForResource", method: http.MethodGet,
			path: "/tags/arn:aws:guardduty:us-east-1:123456789012:detector/d1", wantOp: "ListTagsForResource",
		},
		{
			name: "TagResource on a GovCloud-partition ARN", method: http.MethodPost,
			path: "/tags/arn:aws-us-gov:guardduty:us-gov-west-1:123456789012:detector/d1", wantOp: "TagResource",
		},
		{
			name: "CreateMalwareProtectionPlan", method: http.MethodPost,
			path: "/malware-protection-plan", wantOp: "CreateMalwareProtectionPlan",
		},
		{
			name: "DeleteMalwareProtectionPlan", method: http.MethodDelete,
			path: "/malware-protection-plan/p1", wantOp: "DeleteMalwareProtectionPlan",
		},
		{
			name: "GetMalwareProtectionPlan", method: http.MethodGet,
			path: "/malware-protection-plan/p1", wantOp: "GetMalwareProtectionPlan",
		},
		// The regression case: real aws-sdk-go-v2 sends PATCH for this op.
		{
			name: "UpdateMalwareProtectionPlan uses PATCH", method: http.MethodPatch,
			path: "/malware-protection-plan/p1", wantOp: "UpdateMalwareProtectionPlan",
		},
		{
			name: "ListMalwareScans (top-level, not detector-nested)", method: http.MethodPost,
			path: "/malware-scan", wantOp: "ListMalwareScans",
		},
		{name: "StartMalwareScan", method: http.MethodPost, path: "/malware-scan/start", wantOp: "StartMalwareScan"},
		{name: "GetMalwareScan", method: http.MethodGet, path: "/malware-scan/scan1", wantOp: "GetMalwareScan"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := routeMatcherContext(t, tt.method, tt.path)
			require.True(t, h.RouteMatcher()(c), "RouteMatcher should accept %s %s", tt.method, tt.path)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c), "%s %s", tt.method, tt.path)
		})
	}
}

// TestRouteMatcher_UpdateMalwareProtectionPlanRejectsPOST is a focused
// regression guard: before the fix, POST to
// /malware-protection-plan/{id} was (wrongly) accepted as
// UpdateMalwareProtectionPlan. A real client never sends POST here (it
// always sends PATCH), so this path+method combination must not resolve to
// any known operation.
func TestRouteMatcher_UpdateMalwareProtectionPlanRejectsPOST(t *testing.T) {
	t.Parallel()

	h := guardduty.NewHandler(guardduty.NewInMemoryBackend("123456789012", "us-east-1"))

	c := routeMatcherContext(t, http.MethodPost, "/malware-protection-plan/p1")
	assert.Equal(t, "Unknown", h.ExtractOperation(c))
}
