package cur

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	targetPrefix     = "AWSOrigamiServiceGatewayService."
	deliveryInterval = time.Minute
)

// Handler serves the Cost and Usage Reports awsjson1.1 API.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler { return &Handler{Backend: backend} }

type opFunc func(h *Handler, ctx context.Context, body []byte) (any, error)

func bind[I, O any](fn func(*Handler, context.Context, *I) (*O, error)) opFunc {
	return func(h *Handler, ctx context.Context, body []byte) (any, error) {
		var in I

		if len(body) > 0 {
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, fail(ErrValidation, "malformed request body: %v", err)
			}
		}

		return fn(h, ctx, &in)
	}
}

var operations = sync.OnceValue(func() map[string]opFunc { //nolint:gochecknoglobals // immutable dispatch table
	return map[string]opFunc{
		"PutReportDefinition":       bind((*Handler).putReportDefinition),
		"ModifyReportDefinition":    bind((*Handler).modifyReportDefinition),
		"DescribeReportDefinitions": bind((*Handler).describeReportDefinitions),
		"DeleteReportDefinition":    bind((*Handler).deleteReportDefinition),
		"TagResource":               bind((*Handler).tagResource),
		"UntagResource":             bind((*Handler).untagResource),
		"ListTagsForResource":       bind((*Handler).listTagsForResource),
	}
})

func (h *Handler) putReportDefinition(ctx context.Context, in *putReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.PutReportDefinition(ctx, in.ReportDefinition, in.Tags)
}

func (h *Handler) modifyReportDefinition(ctx context.Context, in *modifyReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.ModifyReportDefinition(ctx, in.ReportName, in.ReportDefinition)
}

func (h *Handler) describeReportDefinitions(_ context.Context, in *describeReq) (*describeResp, error) {
	p, err := h.Backend.DescribeReportDefinitions(in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &describeResp{ReportDefinitions: p.Data, NextToken: p.Next}, nil
}

func (h *Handler) deleteReportDefinition(_ context.Context, in *deleteReq) (*deleteResp, error) {
	if err := h.Backend.DeleteReportDefinition(in.ReportName); err != nil {
		return nil, err
	}

	return &deleteResp{ResponseMessage: fmt.Sprintf("Report %s deleted", in.ReportName)}, nil
}

func (h *Handler) tagResource(_ context.Context, in *tagReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.TagResource(in.ReportName, in.Tags)
}

func (h *Handler) untagResource(_ context.Context, in *tagReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UntagResource(in.ReportName, in.TagKeys)
}

func (h *Handler) listTagsForResource(_ context.Context, in *tagReq) (*listTagsResp, error) {
	t, err := h.Backend.ListTagsForResource(in.ReportName)
	if err != nil {
		return nil, err
	}

	return &listTagsResp{Tags: t}, nil
}

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "CUR" }

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

// ChaosRegions lists the regions this handler serves; CUR is homed in us-east-1.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher matches the AWSOrigamiServiceGatewayService X-Amz-Target prefix.
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

// StartWorker runs scheduled report delivery until ctx is cancelled.
func (h *Handler) StartWorker(ctx context.Context) error {
	g := worker.NewGroup(ctx, serviceName)
	g.Ticker("ReportDelivery", deliveryInterval, 0, h.Backend.DeliverDue, worker.WithImmediate())

	go func() {
		<-ctx.Done()
		g.Stop()
	}()

	return nil
}

// Handler returns the echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			logger.Load(ctx).ErrorContext(ctx, "cur: read body failed", "error", err)

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
	apiErr := awserr.Classify(err, errorTable(), apiErr("InternalErrorException", http.StatusInternalServerError))
	if apiErr.HTTPStatus == http.StatusInternalServerError {
		logger.Load(c.Request().Context()).ErrorContext(c.Request().Context(), "cur: internal error", "error", err)
	}

	return awserr.Write(c, awserr.ProtocolJSON11, apiErr)
}
