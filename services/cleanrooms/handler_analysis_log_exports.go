package cleanrooms

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	keyAnalysisLogExport      = "analysisLogExport"
	defaultExportPageSize     = 100
	maxExportPageSize         = 1000
	exportMaxResultsParamName = "maxResults"
)

func (h *Handler) handleStartAnalysisLogExport(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResultConfiguration  map[string]any `json:"resultConfiguration"`
		MembershipIdentifier string         `json:"membershipIdentifier"`
		AnalysisID           string         `json:"analysisId"`
		AnalysisType         string         `json:"analysisType"`
	}
	_ = json.Unmarshal(body, &req)

	e, err := h.Backend.StartAnalysisLogExport(
		req.MembershipIdentifier, req.AnalysisID, req.AnalysisType, req.ResultConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisLogExport: e}), nil
}

func (h *Handler) handleGetAnalysisLogExport(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier        string `json:"membershipIdentifier"`
		AnalysisLogExportIdentifier string `json:"analysisLogExportIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	e, err := h.Backend.GetAnalysisLogExport(req.MembershipIdentifier, req.AnalysisLogExportIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisLogExport: e}), nil
}

func (h *Handler) handleListAnalysisLogExports(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	limit := 0
	if raw := qp(c, exportMaxResultsParamName); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxExportPageSize {
			return nil, ErrValidation
		}

		limit = n
	}

	items, err := h.Backend.ListAnalysisLogExports(
		req.MembershipIdentifier,
		qp(c, "analysisIdentifier"),
		qp(c, "status"),
	)
	if err != nil {
		return nil, err
	}

	p := page.New(items, qp(c, "nextToken"), limit, defaultExportPageSize)
	resp := map[string]any{"analysisLogExports": p.Data}

	if p.Next != "" {
		resp["nextToken"] = p.Next
	}

	return mustJSON(resp), nil
}

func (h *Handler) buildAnalysisLogExportHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opStartAnalysisLogExport: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleStartAnalysisLogExport(ctx, body)
		},
		opGetAnalysisLogExport: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetAnalysisLogExport(ctx, body)
		},
		opListAnalysisLogExports: h.handleListAnalysisLogExports,
	}
}
