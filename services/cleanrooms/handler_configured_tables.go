package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		TableReference map[string]any    `json:"tableReference"`
		Tags           map[string]string `json:"tags"`
		Name           string            `json:"name"`
		Description    string            `json:"description"`
		AnalysisMethod string            `json:"analysisMethod"`
		AllowedColumns []string          `json:"allowedColumns"`
	}
	_ = json.Unmarshal(body, &req)
	ct, err := h.Backend.CreateConfiguredTable(
		req.Name,
		req.Description,
		req.TableReference,
		req.AllowedColumns,
		req.AnalysisMethod,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTable: ct}), nil
}

func (h *Handler) handleGetConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	ct, err := h.Backend.GetConfiguredTable(req.ConfiguredTableIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTable: ct}), nil
}

func (h *Handler) handleListConfiguredTables(
	_ context.Context,
	c *echo.Context,
) ([]byte, error) {
	items, next := h.Backend.ListConfiguredTables(qp(c, "maxResults"), qp(c, "nextToken"))
	resp := map[string]any{"configuredTableSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
		Name                      string `json:"name"`
		Description               string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	ct, err := h.Backend.UpdateConfiguredTable(
		req.ConfiguredTableIdentifier,
		req.Name,
		req.Description,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTable: ct}), nil
}

func (h *Handler) handleDeleteConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredTable(req.ConfiguredTableIdentifier)
}

func (h *Handler) handleCreateConfiguredTableAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy        map[string]any `json:"analysisRulePolicy"`
		ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
		AnalysisRuleType          string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateConfiguredTableAnalysisRule(
		req.ConfiguredTableIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleGetConfiguredTableAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
		AnalysisRuleType          string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetConfiguredTableAnalysisRule(
		req.ConfiguredTableIdentifier,
		req.AnalysisRuleType,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleUpdateConfiguredTableAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy        map[string]any `json:"analysisRulePolicy"`
		ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
		AnalysisRuleType          string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateConfiguredTableAnalysisRule(
		req.ConfiguredTableIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleDeleteConfiguredTableAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
		AnalysisRuleType          string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredTableAnalysisRule(
		req.ConfiguredTableIdentifier,
		req.AnalysisRuleType,
	)
}

func (h *Handler) buildConfiguredTableHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opCreateConfiguredTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateConfiguredTable(ctx, body)
		},
		opGetConfiguredTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredTable(ctx, body)
		},
		opListConfiguredTables: func(ctx context.Context, _ []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListConfiguredTables(ctx, ec)
		},
		opUpdateConfiguredTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateConfiguredTable(ctx, body)
		},
		opDeleteConfiguredTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteConfiguredTable(ctx, body)
		},
		opCreateConfiguredTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateConfiguredTableAnalysisRule(ctx, body)
		},
		opGetConfiguredTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredTableAnalysisRule(ctx, body)
		},
		opUpdateConfiguredTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateConfiguredTableAnalysisRule(ctx, body)
		},
		opDeleteConfiguredTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteConfiguredTableAnalysisRule(ctx, body)
		},
	}
}
