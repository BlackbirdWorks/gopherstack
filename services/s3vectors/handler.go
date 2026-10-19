package s3vectors

import (
	"context"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	signingName   = "s3vectors"
	uaMarker      = "api/s3vectors"
	matchPriority = 87
	pathTags      = "/tags/{resourceArn}"
	tagRoutes     = 3
)

// Handler serves the S3 Vectors REST-JSON API.
type Handler struct {
	Backend       *InMemoryBackend
	server        *restroute.Server
	DefaultRegion string
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend, DefaultRegion: backend.Region()}
	h.server = &restroute.Server{Router: restroute.New(routes()), Ops: h.ops(), Context: h.requestContext}

	return h
}

func (h *Handler) requestContext(r *http.Request) context.Context {
	ctx := r.Context()
	if _, ok := awsmeta.Key.Get(ctx); ok {
		return ctx
	}

	return awsmeta.Set(ctx, awsmeta.FromRequest(r, h.DefaultRegion))
}

// Name returns the service name.
func (h *Handler) Name() string { return "S3Vectors" }

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

// ChaosOperations lists the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions lists the regions this handler serves.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

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

// ExtractOperation returns the operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := h.server.Operation(c.Request())
	if op == "" {
		return "Unknown"
	}

	return op
}

// ExtractResource returns the tagged resource ARN, when the operation has one.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, params := h.server.Operation(c.Request())

	return params["resourceArn"]
}

// Handler returns the echo handler.
func (h *Handler) Handler() echo.HandlerFunc { return h.server.Handler() }

func routes() []restroute.Route {
	posts := []string{
		"CreateIndex", "CreateVectorBucket", "DeleteIndex", "DeleteVectorBucket", "DeleteVectorBucketPolicy",
		"DeleteVectors", "GetIndex", "GetVectorBucket", "GetVectorBucketPolicy", "GetVectors", "ListIndexes",
		"ListVectorBuckets", "ListVectors", "PutVectorBucketDefaultIndexMode", "PutVectorBucketPolicy", "PutVectors",
		"QueryVectors", "UpdateIndexMode",
	}

	out := make([]restroute.Route, 0, len(posts)+tagRoutes)
	for _, op := range posts {
		out = append(out, restroute.Route{Method: http.MethodPost, Path: "/" + op, Op: op})
	}

	return append(out,
		restroute.Route{Method: http.MethodGet, Path: pathTags, Op: "ListTagsForResource"},
		restroute.Route{Method: http.MethodPost, Path: pathTags, Op: "TagResource"},
		restroute.Route{Method: http.MethodDelete, Path: pathTags, Op: "UntagResource"},
	)
}

// call decodes the request body into I and runs fn.
func call[I any](h *Handler, fn func(sc scope, in *I) (any, error)) restroute.OpFunc {
	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		var in I
		if err := req.Decode(&in); err != nil {
			return nil, 0, err
		}

		out, err := fn(h.Backend.scopeOf(ctx), &in)

		return out, 0, err
	}
}

type tagIn struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	b := h.Backend

	return map[string]restroute.OpFunc{
		"CreateVectorBucket": call(h, b.createBucket),
		"GetVectorBucket": call(
			h,
			func(sc scope, in *bucketRef) (any, error) { return b.getBucket(sc, *in) },
		),
		"DeleteVectorBucket": call(
			h,
			func(sc scope, in *bucketRef) (any, error) { return b.deleteBucket(sc, *in) },
		),
		"ListVectorBuckets":     call(h, b.listBuckets),
		"PutVectorBucketPolicy": call(h, b.putBucketPolicy),
		"GetVectorBucketPolicy": call(
			h,
			func(sc scope, in *bucketRef) (any, error) { return b.getBucketPolicy(sc, *in) },
		),
		"DeleteVectorBucketPolicy": call(
			h,
			func(sc scope, in *bucketRef) (any, error) { return b.deleteBucketPolicy(sc, *in) },
		),
		"PutVectorBucketDefaultIndexMode": call(h, b.putDefaultIndexMode),
		"CreateIndex":                     call(h, b.createIndex),
		"GetIndex": call(
			h,
			func(sc scope, in *indexRef) (any, error) { return b.getIndex(sc, *in) },
		),
		"DeleteIndex": call(
			h,
			func(sc scope, in *indexRef) (any, error) { return b.deleteIndex(sc, *in) },
		),
		"ListIndexes":     call(h, b.listIndexes),
		"UpdateIndexMode": call(h, b.updateIndexMode),
		"PutVectors":      call(h, b.putVectors),
		"GetVectors":      call(h, b.getVectors),
		"DeleteVectors":   call(h, b.deleteVectors),
		"ListVectors":     call(h, b.listVectors),
		"QueryVectors":    call(h, b.queryVectors),
		"TagResource": func(_ context.Context, req *restroute.Request) (any, int, error) {
			var in tagIn
			if err := req.Decode(&in); err != nil {
				return nil, 0, err
			}

			out, err := b.tagResource(req.Params["resourceArn"], in.Tags)

			return out, 0, err
		},
		"UntagResource": func(_ context.Context, req *restroute.Request) (any, int, error) {
			out, err := b.untagResource(req.Params["resourceArn"], req.Query["tagKeys"])

			return out, 0, err
		},
		"ListTagsForResource": func(_ context.Context, req *restroute.Request) (any, int, error) {
			out, err := b.listTags(req.Params["resourceArn"])

			return out, 0, err
		},
	}
}
