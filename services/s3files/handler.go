package s3files

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	serviceName    = "S3Files"
	signingName    = "s3files"
	opUnknown      = "Unknown"
	matchPriority  = service.PriorityPathVersioned
	segFileSystems = "file-systems"
	segAccessPts   = "access-points"
	segMountTgts   = "mount-targets"
	segResTags     = "resource-tags"
	segPolicy      = "policy"
	segSync        = "synchronization-configuration"
	minDeleteSegs  = 2
)

const (
	opCreateFileSystem                = "CreateFileSystem"
	opGetFileSystem                   = "GetFileSystem"
	opListFileSystems                 = "ListFileSystems"
	opDeleteFileSystem                = "DeleteFileSystem"
	opPutFileSystemPolicy             = "PutFileSystemPolicy"
	opGetFileSystemPolicy             = "GetFileSystemPolicy"
	opDeleteFileSystemPolicy          = "DeleteFileSystemPolicy"
	opPutSynchronizationConfiguration = "PutSynchronizationConfiguration"
	opGetSynchronizationConfiguration = "GetSynchronizationConfiguration"
	opCreateMountTarget               = "CreateMountTarget"
	opGetMountTarget                  = "GetMountTarget"
	opListMountTargets                = "ListMountTargets"
	opUpdateMountTarget               = "UpdateMountTarget"
	opDeleteMountTarget               = "DeleteMountTarget"
	opCreateAccessPoint               = "CreateAccessPoint"
	opGetAccessPoint                  = "GetAccessPoint"
	opListAccessPoints                = "ListAccessPoints"
	opDeleteAccessPoint               = "DeleteAccessPoint"
	opTagResource                     = "TagResource"
	opUntagResource                   = "UntagResource"
	opListTagsForResource             = "ListTagsForResource"
)

type route struct {
	op       string
	resource string
}

// parseRoute maps a request to an operation using the REST bindings in service/s3files serializers.go.
func parseRoute(method, escapedPath string) route {
	if !knownFirstSegment(escapedPath) {
		return route{op: opUnknown}
	}

	segs := strings.Split(strings.Trim(escapedPath, "/"), "/")
	for i, s := range segs {
		if u, err := url.PathUnescape(s); err == nil {
			segs[i] = u
		}
	}

	id := ""
	if len(segs) > 1 {
		id = segs[1]
	}

	var op string

	switch {
	case len(segs) == 1:
		op = collectionOp(segs[0], method)
	case len(segs) == minDeleteSegs:
		op = itemOp(segs[0], method)
	case len(segs) == minDeleteSegs+1 && segs[0] == segFileSystems:
		op = fileSystemSubOp(segs[2], method)
	}

	if op == "" {
		return route{op: opUnknown}
	}

	return route{op: op, resource: id}
}

// pick returns the op bound to method, or "" when the method has none.
func pick(method, get, put, post, del string) string {
	switch method {
	case http.MethodGet:
		return get
	case http.MethodPut:
		return put
	case http.MethodPost:
		return post
	case http.MethodDelete:
		return del
	}

	return ""
}

func collectionOp(seg, method string) string {
	switch seg {
	case segFileSystems:
		return pick(method, opListFileSystems, opCreateFileSystem, "", "")
	case segAccessPts:
		return pick(method, opListAccessPoints, opCreateAccessPoint, "", "")
	case segMountTgts:
		return pick(method, opListMountTargets, opCreateMountTarget, "", "")
	}

	return ""
}

func itemOp(seg, method string) string {
	switch seg {
	case segFileSystems:
		return pick(method, opGetFileSystem, "", "", opDeleteFileSystem)
	case segAccessPts:
		return pick(method, opGetAccessPoint, "", "", opDeleteAccessPoint)
	case segMountTgts:
		return pick(method, opGetMountTarget, opUpdateMountTarget, "", opDeleteMountTarget)
	case segResTags:
		return pick(method, opListTagsForResource, "", opTagResource, opUntagResource)
	}

	return ""
}

func fileSystemSubOp(sub, method string) string {
	switch sub {
	case segPolicy:
		return pick(method, opGetFileSystemPolicy, opPutFileSystemPolicy, "", opDeleteFileSystemPolicy)
	case segSync:
		return pick(method, opGetSynchronizationConfiguration, opPutSynchronizationConfiguration, "", "")
	}

	return ""
}

func knownFirstSegment(escapedPath string) bool {
	first, _, _ := strings.Cut(strings.TrimPrefix(escapedPath, "/"), "/")

	switch first {
	case segFileSystems, segAccessPts, segMountTgts, segResTags:
		return true
	}

	return strings.IndexByte(first, '%') >= 0
}

type dispatchFunc func(h *Handler, ctx context.Context, r *http.Request, resource string, body []byte) (int, any, error)

// Handler is the Echo handler for the S3 Files REST-JSON API.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]dispatchFunc
}

// NewHandler creates a handler over backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend, ops: operationTable()}
}

// Name returns the service name.
func (h *Handler) Name() string { return serviceName }

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations lists the implemented operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for op := range h.ops {
		ops = append(ops, op)
	}

	return ops
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return signingName }

// ChaosOperations lists the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions lists the regions served.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// RouteMatcher matches requests signed for s3files. Its paths (/file-systems, /access-points, ...) carry no
// service prefix, so the SigV4 signing name or the s3files endpoint host is the discriminator.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()

		if !strings.HasPrefix(r.URL.Path, "/") || parseRoute(r.Method, r.URL.EscapedPath()).op == opUnknown {
			return false
		}

		return httputils.ExtractServiceFromRequest(r) == signingName || strings.HasPrefix(r.Host, signingName+".")
	}
}

// ExtractOperation returns the operation name for the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.EscapedPath()).op
}

// ExtractResource returns the primary resource identifier for the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.EscapedPath()).resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		rt := parseRoute(r.Method, r.URL.EscapedPath())

		fn, ok := h.ops[rt.op]
		if !ok {
			return writeError(c, notFound("unknown operation for "+r.Method+" "+r.URL.Path))
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return writeError(c, validation("failed to read request body"))
		}

		logger.Load(r.Context()).Debug("s3files request", "operation", rt.op, "resource", rt.resource)

		ctx := context.WithValue(
			r.Context(),
			regionContextKey{},
			httputils.ExtractRegionFromRequest(r, h.Backend.Region()),
		)

		status, out, err := fn(h, ctx, r, rt.resource, body)
		if err != nil {
			return writeError(c, err)
		}

		if out == nil {
			return c.NoContent(status)
		}

		return c.JSON(status, out)
	}
}

func writeError(c *echo.Context, err error) error {
	code, status := classify(err)
	body := map[string]string{"message": err.Error()}

	if ce, ok := errors.AsType[*conflictError](err); ok {
		if ce.resourceID != "" {
			body["resourceId"] = ce.resourceID
		}

		body["resourceType"] = ce.resourceType
	}

	c.Response().Header().Set("X-Amzn-Errortype", code)

	return c.JSON(status, body)
}

func decode(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return validation("malformed request body")
	}

	return nil
}

func queryInt(q url.Values, key string) (int, error) {
	raw := q.Get(key)
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, validation(key + " must be a positive integer")
	}

	return n, nil
}

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
