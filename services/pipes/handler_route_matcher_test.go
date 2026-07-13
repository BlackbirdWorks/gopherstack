package pipes_test

// This file exercises Handler.RouteMatcher and Handler.ExtractOperation
// directly -- the two entry points a real echo router uses to decide (a)
// whether a request belongs to this service at all, and (b) which operation
// it maps to.
//
// Every other test in this package drives requests through auditDo/
// TestHandler_* helpers, which call h.Handler()(c) directly and so bypass
// RouteMatcher and ExtractOperation's method-aware branching entirely. That
// means a routing bug -- an op registered for the wrong HTTP method, or a
// path prefix RouteMatcher doesn't recognize -- can hide behind 100% green
// unit tests while being completely unreachable by a real aws-sdk-go-v2
// client going through echo's router. This bug class has previously hit
// several services in this codebase (backup, eks, guardduty).
//
// Pipes' RouteMatcher additionally gates on the SigV4 credential-scope
// service name (via httputils.ExtractServiceFromRequest), since its path
// prefixes -- especially "/tags/" -- are shared with many other services'
// REST APIs. That gate is exercised explicitly below: paths that would
// otherwise match must still be rejected when the request's service isn't
// "pipes".

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// routeMatcherContext builds a bare *echo.Context for method+path with a
// SigV4 Authorization header naming service as the credential-scope service,
// matching the minimal input RouteMatcher/ExtractOperation need.
func routeMatcherContext(t *testing.T, method, path, service string) *echo.Context {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, http.NoBody)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20230101/us-west-2/"+service+"/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	return c
}

// TestRouteMatcher_RecognizesServicePaths checks that RouteMatcher accepts
// every top-level path prefix a real Pipes client can hit (when the request
// is scoped to the pipes service) and rejects everything else, including
// pipes-shaped paths scoped to a different SigV4 service and the shared
// "/tags/" prefix when it belongs to another service.
func TestRouteMatcher_RecognizesServicePaths(t *testing.T) {
	t.Parallel()

	h := pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-west-2"))

	tests := []struct {
		method  string
		path    string
		service string
		name    string
		want    bool
	}{
		{name: "list pipes", method: http.MethodGet, path: "/v1/pipes", service: "pipes", want: true},
		{name: "create pipe", method: http.MethodPost, path: "/v1/pipes/my-pipe", service: "pipes", want: true},
		{name: "describe pipe", method: http.MethodGet, path: "/v1/pipes/my-pipe", service: "pipes", want: true},
		{name: "start pipe", method: http.MethodPost, path: "/v1/pipes/my-pipe/start", service: "pipes", want: true},
		{
			name: "tags for a pipe ARN", method: http.MethodGet,
			path: "/tags/arn:aws:pipes:us-west-2:123456789012:pipe/my-pipe", service: "pipes", want: true,
		},
		{
			name: "pipes path but SigV4 service is a different service", method: http.MethodGet,
			path: "/v1/pipes/my-pipe", service: "sqs", want: false,
		},
		{
			name:   "shared /tags/ prefix scoped to a different service is not claimed",
			method: http.MethodGet,
			path:   "/tags/arn:aws:sqs:us-west-2:123456789012:queue/q1", service: "sqs", want: false,
		},
		{
			name: "unrelated service path", method: http.MethodGet,
			path: "/2015-03-31/functions", service: "pipes", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := routeMatcherContext(t, tt.method, tt.path, tt.service)
			assert.Equal(t, tt.want, h.RouteMatcher()(c), "%s %s (service=%s)", tt.method, tt.path, tt.service)
		})
	}
}

// TestRouteMatcher_MethodSensitivity walks (method, path) -> expected
// operation for every routed op, the way a real echo router +
// ExtractOperation would see an incoming aws-sdk-go-v2 request. Each
// (method, path) pair below was verified against
// aws-sdk-go-v2/service/pipes's generated serializers.go, which is the
// source of truth for what a real SDK client actually sends on the wire.
func TestRouteMatcher_MethodSensitivity(t *testing.T) {
	t.Parallel()

	h := pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-west-2"))

	const pipeARN = "/tags/arn:aws:pipes:us-west-2:123456789012:pipe/my-pipe"

	tests := []struct {
		method string
		path   string
		wantOp string
		name   string
	}{
		{name: "CreatePipe", method: http.MethodPost, path: "/v1/pipes/my-pipe", wantOp: "CreatePipe"},
		{name: "DescribePipe", method: http.MethodGet, path: "/v1/pipes/my-pipe", wantOp: "DescribePipe"},
		{name: "UpdatePipe", method: http.MethodPut, path: "/v1/pipes/my-pipe", wantOp: "UpdatePipe"},
		{name: "DeletePipe", method: http.MethodDelete, path: "/v1/pipes/my-pipe", wantOp: "DeletePipe"},
		{name: "ListPipes", method: http.MethodGet, path: "/v1/pipes", wantOp: "ListPipes"},
		{name: "StartPipe", method: http.MethodPost, path: "/v1/pipes/my-pipe/start", wantOp: "StartPipe"},
		{name: "StopPipe", method: http.MethodPost, path: "/v1/pipes/my-pipe/stop", wantOp: "StopPipe"},
		{name: "TagResource", method: http.MethodPost, path: pipeARN, wantOp: "TagResource"},
		{name: "UntagResource", method: http.MethodDelete, path: pipeARN, wantOp: "UntagResource"},
		{name: "ListTagsForResource", method: http.MethodGet, path: pipeARN, wantOp: "ListTagsForResource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := routeMatcherContext(t, tt.method, tt.path, "pipes")
			require.True(t, h.RouteMatcher()(c), "RouteMatcher should accept %s %s", tt.method, tt.path)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c), "%s %s", tt.method, tt.path)
		})
	}
}

// TestRouteMatcher_UpdatePipeRejectsPOST is a focused regression guard: a
// real aws-sdk-go-v2 client always sends PUT for UpdatePipe on
// /v1/pipes/{Name}, never POST (POST on that exact path is CreatePipe).
// If ExtractPipeCRUDOp's method switch is ever mis-ordered or mis-wired,
// this pins the PUT/POST split so the regression is caught here instead of
// only surfacing as an unroutable op against a real SDK client.
func TestRouteMatcher_UpdatePipeRejectsPOST(t *testing.T) {
	t.Parallel()

	h := pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-west-2"))

	c := routeMatcherContext(t, http.MethodPost, "/v1/pipes/my-pipe", "pipes")
	assert.Equal(t, "CreatePipe", h.ExtractOperation(c), "POST must remain CreatePipe, not UpdatePipe")

	c = routeMatcherContext(t, http.MethodPut, "/v1/pipes/my-pipe", "pipes")
	assert.Equal(t, "UpdatePipe", h.ExtractOperation(c), "PUT must remain UpdatePipe")
}

// TestRouteMatcher_FullDispatch drives one request per op through the exact
// two-step sequence a real echo router uses in production: RouteMatcher(c)
// to decide ownership, then Handler()(c) to actually execute it. This closes
// the gap the rest of the package's h.Handler()(c)-only tests leave open.
func TestRouteMatcher_FullDispatch(t *testing.T) {
	t.Parallel()

	h := pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-west-2"))

	create := auditDo(t, h, http.MethodPost, "/v1/pipes/route-dispatch-pipe", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/pipe-role",
		"Source":  "arn:aws:sqs:us-west-2:123456789012:src",
		"Target":  "arn:aws:sqs:us-west-2:123456789012:dst",
	})
	require.Equal(t, http.StatusOK, create.Code)

	e := echo.New()

	steps := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/v1/pipes/route-dispatch-pipe", ""},
		{http.MethodGet, "/v1/pipes", ""},
		{http.MethodPost, "/v1/pipes/route-dispatch-pipe/stop", ""},
		{http.MethodPost, "/v1/pipes/route-dispatch-pipe/start", ""},
		// UpdatePipe always unmarshals the body, so a real client's PUT
		// (which always carries at least "{}") must be modelled here too.
		{http.MethodPut, "/v1/pipes/route-dispatch-pipe", "{}"},
		{http.MethodDelete, "/v1/pipes/route-dispatch-pipe", ""},
	}

	for _, step := range steps {
		req := httptest.NewRequest(step.method, step.path, strings.NewReader(step.body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-west-2/pipes/aws4_request")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetRequest(req)

		require.True(t, h.RouteMatcher()(c), "RouteMatcher should claim %s %s", step.method, step.path)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusOK, rec.Code, "%s %s: %s", step.method, step.path, rec.Body.String())
	}
}
