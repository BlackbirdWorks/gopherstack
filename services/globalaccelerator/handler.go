package globalaccelerator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const targetPrefix = "GlobalAccelerator_V20180706."

// Handler serves the Global Accelerator awsjson1.1 API.
type Handler struct {
	Backend *InMemoryBackend
	idem    *idempotency.Memo
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend, idem: idempotency.New(serviceName)}
}

type opFunc func(h *Handler, ctx context.Context, body []byte) (any, error)

// bind adapts a typed handler method to an opFunc: decode the JSON body, call, return the response.
func bind[I, O any](fn func(*Handler, context.Context, *I) (*O, error)) opFunc {
	return func(h *Handler, ctx context.Context, body []byte) (any, error) {
		var in I

		if len(body) > 0 {
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, fail(ErrInvalidArgument, "malformed request body: %v", err)
			}
		}

		return fn(h, ctx, &in)
	}
}

var operations = sync.OnceValue(func() map[string]opFunc { //nolint:gochecknoglobals // immutable dispatch table
	ops := make(map[string]opFunc)
	registerAcceleratorOps(ops)
	registerListenerOps(ops)
	registerEndpointGroupOps(ops)
	registerCustomRoutingOps(ops)
	registerAttachmentOps(ops)
	registerByoipOps(ops)
	registerTagOps(ops)

	return ops
})

// Reset clears all state.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.idem.Clear()
}

// Name returns the service name.
func (h *Handler) Name() string { return "GlobalAccelerator" }

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

// ChaosRegions lists the regions this handler serves; Global Accelerator is homed in one region.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher matches the GlobalAccelerator_V20180706 X-Amz-Target prefix.
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
		ctx := c.Request().Context()

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			logger.Load(ctx).ErrorContext(ctx, "globalaccelerator: read body failed", "error", err)

			return h.writeError(c, fail(ErrInvalidArgument, "could not read the request body"))
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

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

		return c.JSON(http.StatusOK, out)
	}
}

func (h *Handler) writeError(c *echo.Context, err error) error {
	apiErr := awserr.Classify(
		err,
		errorTable(),
		apiErr("InternalServiceErrorException", http.StatusInternalServerError),
	)
	if apiErr.HTTPStatus == http.StatusInternalServerError {
		logger.Load(c.Request().Context()).ErrorContext(c.Request().Context(),
			"globalaccelerator: internal error", "error", err)
	}

	return awserr.Write(c, awserr.ProtocolJSON11, apiErr)
}

func callerAccount(ctx context.Context, fallback string) string {
	if a := awsmeta.Account(ctx); a != "" {
		return a
	}

	return fallback
}

// createOnce replays a create made earlier with the same IdempotencyToken and parameters.
func createOnce[T any](
	h *Handler, op, token string, req any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	res, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(req), idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, fail(ErrInvalidArgument, "%v", err)
	}

	return res, err
}
