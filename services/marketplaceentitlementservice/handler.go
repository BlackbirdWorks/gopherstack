package marketplaceentitlementservice

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const targetPrefix = "AWSMPEntitlementService."

// Handler serves the Marketplace Entitlement Service awsjson1.1 API.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler { return &Handler{Backend: backend} }

type opFunc func(h *Handler, body []byte) (any, error)

func operations() map[string]opFunc {
	return map[string]opFunc{
		"GetEntitlements": func(h *Handler, body []byte) (any, error) {
			var in GetEntitlementsInput

			if err := decode(body, &in); err != nil {
				return nil, err
			}

			return h.Backend.GetEntitlements(&in)
		},
	}
}

func decode(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return fail(ErrInvalidParameter, "malformed request body: %v", err)
	}

	return nil
}

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "MarketplaceEntitlementService" }

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

// RouteMatcher matches the AWSMPEntitlementService X-Amz-Target prefix or the RPCv2 CBOR service path.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()

		return strings.HasPrefix(r.Header.Get("X-Amz-Target"), targetPrefix) ||
			service.IsRPCv2CBORRequest(r, cborServicePath)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from X-Amz-Target.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if service.IsRPCv2CBORRequest(r, cborServicePath) {
		return service.ExtractRPCv2CBOROperation(r.URL.Path, cborServicePath)
	}

	return strings.TrimPrefix(r.Header.Get("X-Amz-Target"), targetPrefix)
}

// ExtractResource returns the operation name; the API has no single resource key.
func (h *Handler) ExtractResource(c *echo.Context) string { return h.ExtractOperation(c) }

// Handler returns the echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			logger.Load(ctx).ErrorContext(ctx, "marketplaceentitlementservice: read body failed", "error", err)

			return h.writeError(c, fail(ErrInvalidParameter, "could not read the request body"), false)
		}

		op := h.ExtractOperation(c)
		isCBOR := service.IsRPCv2CBORRequest(c.Request(), cborServicePath)

		fn, ok := operations()[op]
		if !ok {
			return h.writeError(c, fmt.Errorf("%w: %s", errUnknownOperation, op), isCBOR)
		}

		if isCBOR {
			if body, err = cborToJSONBytes(body); err != nil {
				return h.writeError(c, err, true)
			}
		}

		out, err := fn(h, body)
		if err != nil {
			return h.writeError(c, err, isCBOR)
		}

		if isCBOR {
			return h.writeCBOR(c, out)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

		return c.JSON(http.StatusOK, out)
	}
}

func (h *Handler) writeCBOR(c *echo.Context, out any) error {
	raw, err := json.Marshal(out)
	if err == nil {
		var enc []byte

		if enc, err = jsonToCBORBytes(raw); err == nil {
			c.Response().Header().Set("Content-Type", "application/cbor")
			c.Response().Header().Set("Smithy-Protocol", "rpc-v2-cbor")

			return c.Blob(http.StatusOK, "application/cbor", enc)
		}
	}

	return h.writeError(c, err, true)
}

func (h *Handler) writeError(c *echo.Context, err error, isCBOR bool) error {
	e := awserr.Classify(err, errorTable(), apiErr("InternalServiceErrorException", http.StatusInternalServerError))
	if e.HTTPStatus == http.StatusInternalServerError {
		ctx := c.Request().Context()
		logger.Load(ctx).ErrorContext(ctx, "marketplaceentitlementservice: internal error", "error", err)
	}

	if isCBOR {
		return service.WriteRPCv2CBORError(c, e.HTTPStatus, e.Code, e.Message)
	}

	return awserr.Write(c, awserr.ProtocolJSON11, e)
}
