package codegurureviewer

import (
	"context"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	signingName   = "codeguru-reviewer"
	uaMarker      = "api/codegurureviewer"
	matchPriority = 87
)

// Handler serves the CodeGuru Reviewer REST-JSON API.
type Handler struct {
	Backend       *InMemoryBackend
	server        *restroute.Server
	idem          *idempotency.Memo
	DefaultRegion string
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend, idem: idempotency.New(serviceName)}
	h.server = &restroute.Server{Router: restroute.New(routes()), Ops: h.ops()}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeGuruReviewer" }

// Reset clears all state.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.idem.Clear()
}

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// GetSupportedOperations lists every routed operation.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.server.Ops))
	for op := range h.server.Ops {
		ops = append(ops, op)
	}

	slices.Sort(ops)

	return ops
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return signingName }

// ChaosOperations lists the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions lists the regions served.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher matches by SigV4 signing name (or SDK user agent) plus a known path shape.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if httputils.ExtractServiceFromRequest(r) != signingName &&
			!service.MatchesUserAgentMarker(r.Header, uaMarker) {
			return false
		}

		return h.server.Router.Handles(restroute.EscapedPath(r))
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := h.server.Operation(c.Request())
	if op == "" {
		return "Unknown"
	}

	return op
}

// ExtractResource returns the first path label for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, params := h.server.Operation(c.Request())
	for _, v := range params {
		return v
	}

	return ""
}

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc { return h.server.Handler() }

func routes() []restroute.Route {
	const (
		get      = http.MethodGet
		post     = http.MethodPost
		put      = http.MethodPut
		del      = http.MethodDelete
		assocArn = "/associations/{AssociationArn}"
		review   = "/codereviews/{CodeReviewArn}"
		feedback = "/feedback/{CodeReviewArn}"
		tagsPath = "/tags/{resourceArn}"
	)

	return []restroute.Route{
		{Method: post, Path: "/associations", Op: "AssociateRepository"},
		{Method: get, Path: "/associations", Op: "ListRepositoryAssociations"},
		{Method: get, Path: assocArn, Op: "DescribeRepositoryAssociation"},
		{Method: del, Path: assocArn, Op: "DisassociateRepository"},
		{Method: post, Path: "/codereviews", Op: "CreateCodeReview"},
		{Method: get, Path: "/codereviews", Op: "ListCodeReviews"},
		{Method: get, Path: review, Op: "DescribeCodeReview"},
		{Method: get, Path: review + "/Recommendations", Op: "ListRecommendations"},
		{Method: put, Path: "/feedback", Op: "PutRecommendationFeedback"},
		{Method: get, Path: feedback, Op: "DescribeRecommendationFeedback"},
		{Method: get, Path: feedback + "/RecommendationFeedback", Op: "ListRecommendationFeedback"},
		{Method: get, Path: tagsPath, Op: "ListTagsForResource"},
		{Method: post, Path: tagsPath, Op: "TagResource"},
		{Method: del, Path: tagsPath, Op: "UntagResource"},
	}
}
