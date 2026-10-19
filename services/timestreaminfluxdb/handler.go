package timestreaminfluxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const targetPrefix = "AmazonTimestreamInfluxDB."

// Handler serves the Timestream for InfluxDB awsjson1.0 API.
type Handler struct {
	Backend       *InMemoryBackend
	DefaultRegion string
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend, DefaultRegion: backend.Region()}
}

type opFunc func(h *Handler, ctx context.Context, body []byte) (any, error)

// bind adapts a typed handler method to an opFunc: decode the JSON body, call, return the response.
func bind[I, O any](fn func(*Handler, context.Context, *I) (*O, error)) opFunc {
	return func(h *Handler, ctx context.Context, body []byte) (any, error) {
		var in I

		if len(body) > 0 {
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, invalid("malformed request body: %v", err)
			}
		}

		return fn(h, ctx, &in)
	}
}

var operations = sync.OnceValue(func() map[string]opFunc { //nolint:gochecknoglobals // immutable dispatch table
	ops := make(map[string]opFunc)
	registerInstanceOps(ops)
	registerClusterOps(ops)
	registerGroupOps(ops)
	registerBackupOps(ops)
	registerTagOps(ops)

	return ops
})

func (h *Handler) requestContext(r *http.Request) context.Context {
	ctx := r.Context()
	if _, ok := awsmeta.Key.Get(ctx); ok {
		return ctx
	}

	return awsmeta.Set(ctx, awsmeta.FromRequest(r, h.DefaultRegion))
}

// Shutdown stops every engine container.
func (h *Handler) Shutdown(_ context.Context) { h.Backend.Close() }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "TimestreamInfluxDB" }

// GetSupportedOperations lists every routed operation.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(operations()))
	for op := range operations() {
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
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher matches the AmazonTimestreamInfluxDB X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from X-Amz-Target.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
}

// ExtractResource returns the operation name; the API has no single resource key.
func (h *Handler) ExtractResource(c *echo.Context) string { return h.ExtractOperation(c) }

// Handler returns the echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := h.requestContext(c.Request())

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			logger.Load(ctx).ErrorContext(ctx, "timestreaminfluxdb: read body failed", "error", err)

			return h.writeError(c, invalid("could not read the request body"))
		}

		op := h.ExtractOperation(c)

		fn, ok := operations()[op]
		if !ok {
			return h.writeError(c, fmt.Errorf("%w: %s", errUnknownOperation, op))
		}

		out, err := fn(h, ctx, body)
		if err != nil {
			return h.writeError(c, err)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

		return c.JSON(http.StatusOK, out)
	}
}

func (h *Handler) writeError(c *echo.Context, err error) error {
	apiErr := awserr.Classify(err, errorTable(), apiErr("InternalServerException", http.StatusInternalServerError))
	if apiErr.HTTPStatus == http.StatusInternalServerError {
		logger.Load(c.Request().Context()).ErrorContext(c.Request().Context(),
			"timestreaminfluxdb: internal error", "error", err)
	}

	return awserr.Write(c, awserr.ProtocolJSON10, apiErr)
}
