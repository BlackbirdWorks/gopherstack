package oam

import (
	"context"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	signingName   = "oam"
	uaMarker      = "api/oam"
	pathTags      = "/tags/{ResourceArn}"
	matchPriority = 87
)

// Handler serves the CloudWatch Observability Access Manager REST-JSON API.
type Handler struct {
	Backend *InMemoryBackend
	server  *restroute.Server
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.server = &restroute.Server{Router: restroute.New(routes()), Ops: h.ops()}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "OAM" }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations lists every implemented operation.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.server.Ops))
	for op := range h.server.Ops {
		ops = append(ops, op)
	}

	slices.Sort(ops)

	return ops
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return serviceName }

// ChaosOperations returns the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions served.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher matches by SigV4 signing name (or SDK user agent) plus a known path shape.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if !h.server.Router.Handles(restroute.EscapedPath(r)) {
			return false
		}

		return httputils.ExtractServiceFromRequest(r) == signingName ||
			service.MatchesUserAgentMarker(r.Header, uaMarker)
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

// ExtractResource returns the ARN label for metrics.
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
	ops := []string{
		"CreateSink", "GetSink", "ListSinks", "DeleteSink", "PutSinkPolicy", "GetSinkPolicy",
		"CreateLink", "GetLink", "ListLinks", "ListAttachedLinks", "UpdateLink", "DeleteLink",
	}

	rs := make([]restroute.Route, 0, len(ops)+3) //nolint:mnd // three tag routes

	for _, op := range ops {
		rs = append(rs, restroute.Route{Method: http.MethodPost, Path: "/" + op, Op: op})
	}

	return append(rs,
		restroute.Route{Method: http.MethodGet, Path: pathTags, Op: "ListTagsForResource"},
		restroute.Route{Method: http.MethodPut, Path: pathTags, Op: "TagResource"},
		restroute.Route{Method: http.MethodDelete, Path: pathTags, Op: "UntagResource"},
	)
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	return map[string]restroute.OpFunc{
		"CreateSink":          h.createSink,
		"GetSink":             h.getSink,
		"ListSinks":           h.listSinks,
		"DeleteSink":          h.deleteSink,
		"PutSinkPolicy":       h.putSinkPolicy,
		"GetSinkPolicy":       h.getSinkPolicy,
		"CreateLink":          h.createLink,
		"GetLink":             h.getLink,
		"ListLinks":           h.listLinks,
		"ListAttachedLinks":   h.listAttachedLinks,
		"UpdateLink":          h.updateLink,
		"DeleteLink":          h.deleteLink,
		"TagResource":         h.tagResource,
		"UntagResource":       h.untagResource,
		"ListTagsForResource": h.listTags,
	}
}

func decode[T any](req *restroute.Request) (T, error) {
	var in T

	if err := req.Decode(&in); err != nil {
		return in, err
	}

	return in, nil
}

func nextToken[T any](pg page.Page[T]) string { return pg.Next }

func (h *Handler) createSink(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[createSinkReq](req)
	if err != nil {
		return nil, 0, err
	}

	s, err := h.Backend.CreateSink(ctx, in.Name, in.Tags)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	return sinkToResp(s, true), 0, nil
}

func (h *Handler) getSink(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[identifierReq](req)
	if err != nil {
		return nil, 0, err
	}

	s, err := h.Backend.GetSink(ctx, in.Identifier)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	return sinkToResp(s, in.IncludeTags != nil && *in.IncludeTags), 0, nil
}

func (h *Handler) listSinks(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[pageReq](req)
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListSinks(ctx, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	items := make([]listSinksItem, len(pg.Data))
	for i, s := range pg.Data {
		items[i] = listSinksItem{Arn: s.ARN, ID: s.ID, Name: s.Name}
	}

	return &listSinksResp{NextToken: nextToken(pg), Items: items}, 0, nil
}

func (h *Handler) deleteSink(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[identifierReq](req)
	if err != nil {
		return nil, 0, err
	}

	return nil, 0, toAPIErrorOrNil(h.Backend.DeleteSink(ctx, in.Identifier))
}

func toAPIErrorOrNil(err error) error {
	if err == nil {
		return nil
	}

	return toAPIError(err)
}

func (h *Handler) putSinkPolicy(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[sinkPolicyReq](req)
	if err != nil {
		return nil, 0, err
	}

	s, err := h.Backend.PutSinkPolicy(ctx, in.SinkIdentifier, in.Policy)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	return &sinkPolicyResp{Policy: s.Policy, SinkArn: s.ARN, SinkID: s.ID}, 0, nil
}

func (h *Handler) getSinkPolicy(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[sinkPolicyReq](req)
	if err != nil {
		return nil, 0, err
	}

	s, err := h.Backend.GetSinkPolicy(ctx, in.SinkIdentifier)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	return &sinkPolicyResp{Policy: s.Policy, SinkArn: s.ARN, SinkID: s.ID}, 0, nil
}

func (h *Handler) createLink(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[createLinkReq](req)
	if err != nil {
		return nil, 0, err
	}

	input := LinkInput{
		Tags: in.Tags, SinkARN: in.SinkIdentifier, LabelTemplate: in.LabelTemplate, ResourceTypes: in.ResourceTypes,
	}

	if cfg := configFromWire(in.LinkConfiguration); cfg != nil {
		input.Config = *cfg
	}

	l, err := h.Backend.CreateLink(ctx, input)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	return linkToResp(l, true), 0, nil
}

func (h *Handler) getLink(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[identifierReq](req)
	if err != nil {
		return nil, 0, err
	}

	l, err := h.Backend.GetLink(ctx, in.Identifier)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	return linkToResp(l, in.IncludeTags != nil && *in.IncludeTags), 0, nil
}

func (h *Handler) listLinks(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[pageReq](req)
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListLinks(ctx, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	items := make([]listLinksItem, len(pg.Data))
	for i, l := range pg.Data {
		items[i] = listLinksItem{
			Arn:           l.ARN,
			ID:            l.ID,
			Label:         l.Label,
			SinkArn:       l.SinkARN,
			ResourceTypes: l.ResourceTypes,
		}
	}

	return &listLinksResp{NextToken: nextToken(pg), Items: items}, 0, nil
}

func (h *Handler) listAttachedLinks(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[attachedLinksReq](req)
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListAttachedLinks(ctx, in.SinkIdentifier, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	items := make([]attachedLinkItem, len(pg.Data))
	for i, l := range pg.Data {
		items[i] = attachedLinkItem{Label: l.Label, LinkArn: l.ARN, ResourceTypes: l.ResourceTypes}
	}

	return &listAttachedLinksResp{NextToken: nextToken(pg), Items: items}, 0, nil
}

func (h *Handler) updateLink(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[updateLinkReq](req)
	if err != nil {
		return nil, 0, err
	}

	l, err := h.Backend.UpdateLink(ctx, in.Identifier, in.ResourceTypes, configFromWire(in.LinkConfiguration))
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	return linkToResp(l, in.IncludeTags != nil && *in.IncludeTags), 0, nil
}

func (h *Handler) deleteLink(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[identifierReq](req)
	if err != nil {
		return nil, 0, err
	}

	return nil, 0, toAPIErrorOrNil(h.Backend.DeleteLink(ctx, in.Identifier))
}

func (h *Handler) tagResource(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decode[tagReq](req)
	if err != nil {
		return nil, 0, err
	}

	return nil, 0, toAPIErrorOrNil(h.Backend.TagResource(ctx, req.Params["ResourceArn"], in.Tags))
}

func (h *Handler) untagResource(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, toAPIErrorOrNil(h.Backend.UntagResource(ctx, req.Params["ResourceArn"], req.Query["tagKeys"]))
}

func (h *Handler) listTags(ctx context.Context, req *restroute.Request) (any, int, error) {
	m, err := h.Backend.ListTagsForResource(ctx, req.Params["ResourceArn"])
	if err != nil {
		return nil, 0, toAPIError(err)
	}

	return &listTagsResp{Tags: m}, 0, nil
}
