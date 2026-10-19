package marketplacemetering

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const targetPrefix = "AWSMPMeteringService."

// Handler serves the Marketplace Metering awsjson1.1 API.
type Handler struct {
	Backend *InMemoryBackend
	idem    *idempotency.Memo
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend, idem: idempotency.New(serviceName)}
}

type opFunc func(h *Handler, ctx context.Context, body []byte) (any, error)

func bind[I, O any](fn func(*Handler, context.Context, *I) (*O, error)) opFunc {
	return func(h *Handler, ctx context.Context, body []byte) (any, error) {
		var in I

		if err := decode(body, &in); err != nil {
			return nil, err
		}

		return fn(h, ctx, &in)
	}
}

func operations() map[string]opFunc {
	return map[string]opFunc{
		"MeterUsage": bind((*Handler).meterUsage),
		"BatchMeterUsage": bind(
			func(h *Handler, _ context.Context, in *BatchMeterUsageInput) (*BatchMeterUsageOutput, error) {
				return h.Backend.BatchMeterUsage(in)
			},
		),
		"RegisterUsage": bind(
			func(h *Handler, ctx context.Context, in *RegisterUsageInput) (*RegisterUsageOutput, error) {
				return h.Backend.RegisterUsage(ctx, in)
			},
		),
		"ResolveCustomer": bind(
			func(h *Handler, _ context.Context, in *resolveCustomerInput) (*ResolvedCustomer, error) {
				return h.Backend.ResolveCustomer(in.RegistrationToken)
			},
		),
	}
}

type resolveCustomerInput struct {
	RegistrationToken string `json:"RegistrationToken"`
}

func (h *Handler) meterUsage(ctx context.Context, in *MeterUsageInput) (*MeterUsageOutput, error) {
	fp := *in
	fp.ClientToken = ""

	return idempotency.Replay(h.idem, "MeterUsage", in.ClientToken, idempotency.Fingerprint(fp),
		func() (*MeterUsageOutput, error) { return h.Backend.MeterUsage(ctx, in) })
}

func decode(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return fail(ErrValidation, "malformed request body: %v", err)
	}

	return nil
}

// Reset clears all state.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.idem.Clear()
}

// Name returns the service name.
func (h *Handler) Name() string { return "MarketplaceMetering" }

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

// RouteMatcher matches the AWSMPMeteringService X-Amz-Target prefix.
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
			logger.Load(ctx).ErrorContext(ctx, "marketplacemetering: read body failed", "error", err)

			return h.writeError(c, fail(ErrValidation, "could not read the request body"))
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
	e := awserr.Classify(err, errorTable(), apiErr("InternalServiceErrorException", http.StatusInternalServerError))
	if e.HTTPStatus == http.StatusInternalServerError {
		ctx := c.Request().Context()
		logger.Load(ctx).ErrorContext(ctx, "marketplacemetering: internal error", "error", err)
	}

	return awserr.Write(c, awserr.ProtocolJSON11, e)
}
