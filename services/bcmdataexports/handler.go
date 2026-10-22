package bcmdataexports

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
	targetPrefix     = "AWSBillingAndCostManagementDataExports."
	executionTickGap = 250 * time.Millisecond
)

// Handler serves the Data Exports awsjson1.1 API.
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
				return nil, failReason(reasonCannotParse, "malformed request body: %v", err)
			}
		}

		return fn(h, ctx, &in)
	}
}

var operations = sync.OnceValue(func() map[string]opFunc { //nolint:gochecknoglobals // immutable dispatch table
	return map[string]opFunc{
		"CreateExport":        bind((*Handler).createExport),
		"GetExport":           bind((*Handler).getExport),
		"UpdateExport":        bind((*Handler).updateExport),
		"DeleteExport":        bind((*Handler).deleteExport),
		"ListExports":         bind((*Handler).listExports),
		"GetExecution":        bind((*Handler).getExecution),
		"ListExecutions":      bind((*Handler).listExecutions),
		"GetTable":            bind((*Handler).getTable),
		"ListTables":          bind((*Handler).listTables),
		"TagResource":         bind((*Handler).tagResource),
		"UntagResource":       bind((*Handler).untagResource),
		"ListTagsForResource": bind((*Handler).listTagsForResource),
	}
})

func (h *Handler) createExport(_ context.Context, in *createReq) (*arnResp, error) {
	a, err := h.Backend.CreateExport(in.Export, in.ResourceTags)
	if err != nil {
		return nil, err
	}

	return &arnResp{ExportArn: a}, nil
}

func (h *Handler) updateExport(_ context.Context, in *updateReq) (*arnResp, error) {
	a, err := h.Backend.UpdateExport(in.ExportArn, in.Export)
	if err != nil {
		return nil, err
	}

	return &arnResp{ExportArn: a}, nil
}

func (h *Handler) deleteExport(_ context.Context, in *getExportReq) (*arnResp, error) {
	a, err := h.Backend.DeleteExport(in.ExportArn)
	if err != nil {
		return nil, err
	}

	return &arnResp{ExportArn: a}, nil
}

func (h *Handler) getExport(_ context.Context, in *getExportReq) (*getExportResp, error) {
	e, s, err := h.Backend.GetExport(in.ExportArn)
	if err != nil {
		return nil, err
	}

	return &getExportResp{Export: e, ExportStatus: s}, nil
}

func (h *Handler) listExports(_ context.Context, in *pageReq) (*listExportsResp, error) {
	p, err := h.Backend.ListExports(in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listExportsResp{Exports: p.Data, NextToken: p.Next}, nil
}

func (h *Handler) getExecution(_ context.Context, in *getExecReq) (*getExecResp, error) {
	r, err := h.Backend.GetExecution(in.ExportArn, in.ExecutionID)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (h *Handler) listExecutions(_ context.Context, in *pageReq) (*listExecsResp, error) {
	p, err := h.Backend.ListExecutions(in.ExportArn, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listExecsResp{Executions: p.Data, NextToken: p.Next}, nil
}

func (h *Handler) getTable(_ context.Context, in *getTableReq) (*getTableResp, error) {
	r, err := GetTable(in.TableName, in.TableProperties)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (h *Handler) listTables(_ context.Context, in *pageReq) (*listTablesResp, error) {
	p, err := ListTables(in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listTablesResp{Tables: p.Data, NextToken: p.Next}, nil
}

func (h *Handler) tagResource(_ context.Context, in *tagReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.TagResource(in.ResourceArn, in.ResourceTags)
}

func (h *Handler) untagResource(_ context.Context, in *tagReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UntagResource(in.ResourceArn, in.ResourceTagKeys)
}

func (h *Handler) listTagsForResource(_ context.Context, in *tagReq) (*listTagsResp, error) {
	p, err := h.Backend.ListTagsForResource(in.ResourceArn, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listTagsResp{ResourceTags: p.Data, NextToken: p.Next}, nil
}

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "BCMDataExports" }

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

// ChaosRegions lists the regions this handler serves; Data Exports is homed in us-east-1.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher matches the AWSBillingAndCostManagementDataExports X-Amz-Target prefix.
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

// StartWorker advances export executions until ctx is cancelled.
func (h *Handler) StartWorker(ctx context.Context) error {
	g := worker.NewGroup(ctx, serviceName)
	g.Ticker("ExecutionProcessor", executionTickGap, 0, h.Backend.ProcessExecutions)

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
			logger.Load(ctx).ErrorContext(ctx, "bcmdataexports: read body failed", "error", err)

			return h.writeError(c, failReason(reasonCannotParse, "could not read the request body"))
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
	apiErr := awserr.Classify(err, errorTable(), apiErr("InternalServerException", http.StatusInternalServerError))
	if apiErr.HTTPStatus == http.StatusInternalServerError {
		logger.Load(c.Request().Context()).ErrorContext(c.Request().Context(),
			"bcmdataexports: internal error", "error", err)
	}

	return writeErrorBody(c, apiErr, errorDetail(err))
}
