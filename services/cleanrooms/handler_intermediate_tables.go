package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateIntermediateTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		PopulationAnalysisConfiguration map[string]any    `json:"populationAnalysisConfiguration"`
		Tags                            map[string]string `json:"tags"`
		MembershipIdentifier            string            `json:"membershipIdentifier"`
		Name                            string            `json:"name"`
		Description                     string            `json:"description"`
		KmsKeyArn                       string            `json:"kmsKeyArn"`
		RetentionInDays                 int32             `json:"retentionInDays"`
	}
	_ = json.Unmarshal(body, &req)
	it, err := h.Backend.CreateIntermediateTable(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.KmsKeyArn,
		req.PopulationAnalysisConfiguration,
		req.RetentionInDays,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIntermediateTable: it}), nil
}

func (h *Handler) handleGetIntermediateTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier        string `json:"membershipIdentifier"`
		IntermediateTableIdentifier string `json:"intermediateTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	it, err := h.Backend.GetIntermediateTable(req.MembershipIdentifier, req.IntermediateTableIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIntermediateTable: it}), nil
}

func (h *Handler) handleListIntermediateTables(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListIntermediateTables(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"intermediateTableSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateIntermediateTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier        string `json:"membershipIdentifier"`
		IntermediateTableIdentifier string `json:"intermediateTableIdentifier"`
		Description                 string `json:"description"`
		KmsKeyArn                   string `json:"kmsKeyArn"`
	}
	_ = json.Unmarshal(body, &req)
	it, err := h.Backend.UpdateIntermediateTable(
		req.MembershipIdentifier,
		req.IntermediateTableIdentifier,
		req.Description,
		req.KmsKeyArn,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIntermediateTable: it}), nil
}

func (h *Handler) handleDeleteIntermediateTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier        string `json:"membershipIdentifier"`
		IntermediateTableIdentifier string `json:"intermediateTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteIntermediateTable(req.MembershipIdentifier, req.IntermediateTableIdentifier)
}

func (h *Handler) handleListIntermediateTableVersions(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier        string `json:"membershipIdentifier"`
		IntermediateTableIdentifier string `json:"intermediateTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListIntermediateTableVersions(
		req.MembershipIdentifier,
		req.IntermediateTableIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"intermediateTableVersionSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handlePopulateIntermediateTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ComputeConfiguration        map[string]any    `json:"computeConfiguration"`
		Parameters                  map[string]string `json:"parameters"`
		MembershipIdentifier        string            `json:"membershipIdentifier"`
		IntermediateTableIdentifier string            `json:"intermediateTableIdentifier"`
		AnalysisPayerAccountID      string            `json:"analysisPayerAccountId"`
	}
	_ = json.Unmarshal(body, &req)
	result, err := h.Backend.PopulateIntermediateTable(
		req.MembershipIdentifier,
		req.IntermediateTableIdentifier,
		req.AnalysisPayerAccountID,
		req.ComputeConfiguration,
		req.Parameters,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(result), nil
}

func (h *Handler) handleDisallowIntermediateTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		IncludeDescendants    *bool  `json:"includeDescendants"`
		MembershipIdentifier  string `json:"membershipIdentifier"`
		IntermediateTableName string `json:"intermediateTableName"`
	}
	_ = json.Unmarshal(body, &req)
	// IncludeDescendants defaults to true on the wire (see
	// DisallowIntermediateTableInput's doc comment in the real SDK).
	includeDescendants := true
	if req.IncludeDescendants != nil {
		includeDescendants = *req.IncludeDescendants
	}

	return nil, h.Backend.DisallowIntermediateTable(
		req.MembershipIdentifier, req.IntermediateTableName, includeDescendants,
	)
}

func (h *Handler) handleCreateIntermediateTableAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy          map[string]any `json:"analysisRulePolicy"`
		MembershipIdentifier        string         `json:"membershipIdentifier"`
		IntermediateTableIdentifier string         `json:"intermediateTableIdentifier"`
		AnalysisRuleType            string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateIntermediateTableAnalysisRule(
		req.MembershipIdentifier,
		req.IntermediateTableIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleGetIntermediateTableAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier        string `json:"membershipIdentifier"`
		IntermediateTableIdentifier string `json:"intermediateTableIdentifier"`
		AnalysisRuleType            string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetIntermediateTableAnalysisRule(
		req.MembershipIdentifier,
		req.IntermediateTableIdentifier,
		req.AnalysisRuleType,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleUpdateIntermediateTableAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy          map[string]any `json:"analysisRulePolicy"`
		MembershipIdentifier        string         `json:"membershipIdentifier"`
		IntermediateTableIdentifier string         `json:"intermediateTableIdentifier"`
		AnalysisRuleType            string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateIntermediateTableAnalysisRule(
		req.MembershipIdentifier,
		req.IntermediateTableIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleDeleteIntermediateTableAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier        string `json:"membershipIdentifier"`
		IntermediateTableIdentifier string `json:"intermediateTableIdentifier"`
		AnalysisRuleType            string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteIntermediateTableAnalysisRule(
		req.MembershipIdentifier,
		req.IntermediateTableIdentifier,
		req.AnalysisRuleType,
	)
}

func (h *Handler) buildIntermediateTableHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opCreateIntermediateTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateIntermediateTable(ctx, body)
		},
		opGetIntermediateTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetIntermediateTable(ctx, body)
		},
		opListIntermediateTables: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListIntermediateTables(ctx, body, ec)
		},
		opUpdateIntermediateTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateIntermediateTable(ctx, body)
		},
		opDeleteIntermediateTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteIntermediateTable(ctx, body)
		},
		opListIntermediateTableVersions: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListIntermediateTableVersions(ctx, body, ec)
		},
		opPopulateIntermediateTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handlePopulateIntermediateTable(ctx, body)
		},
		opDisallowIntermediateTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDisallowIntermediateTable(ctx, body)
		},
		opCreateIntermediateTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateIntermediateTableAnalysisRule(ctx, body)
		},
		opGetIntermediateTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetIntermediateTableAnalysisRule(ctx, body)
		},
		opUpdateIntermediateTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateIntermediateTableAnalysisRule(ctx, body)
		},
		opDeleteIntermediateTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteIntermediateTableAnalysisRule(ctx, body)
		},
	}
}
