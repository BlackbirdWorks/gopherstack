package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateConfiguredTableAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Tags                      map[string]string `json:"tags"`
		MembershipIdentifier      string            `json:"membershipIdentifier"`
		Name                      string            `json:"name"`
		Description               string            `json:"description"`
		ConfiguredTableIdentifier string            `json:"configuredTableIdentifier"`
		RoleArn                   string            `json:"roleArn"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateConfiguredTableAssociation(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.ConfiguredTableIdentifier,
		req.RoleArn,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTableAssociation: a}), nil
}

func (h *Handler) handleGetConfiguredTableAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetConfiguredTableAssociation(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTableAssociation: a}), nil
}

func (h *Handler) handleListConfiguredTableAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListConfiguredTableAssociations(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"configuredTableAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateConfiguredTableAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		Description                          string `json:"description"`
		RoleArn                              string `json:"roleArn"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateConfiguredTableAssociation(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.Description,
		req.RoleArn,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTableAssociation: a}), nil
}

func (h *Handler) handleDeleteConfiguredTableAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredTableAssociation(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
	)
}

func (h *Handler) handleCreateConfiguredTableAssociationAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy                   map[string]any `json:"analysisRulePolicy"`
		MembershipIdentifier                 string         `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateConfiguredTableAssociationAnalysisRule(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleGetConfiguredTableAssociationAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetConfiguredTableAssociationAnalysisRule(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.AnalysisRuleType,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleUpdateConfiguredTableAssociationAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy                   map[string]any `json:"analysisRulePolicy"`
		MembershipIdentifier                 string         `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateConfiguredTableAssociationAnalysisRule(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleDeleteConfiguredTableAssociationAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredTableAssociationAnalysisRule(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.AnalysisRuleType,
	)
}

func (h *Handler) buildConfiguredTableAssociationHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opCreateConfiguredTableAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateConfiguredTableAssociation(ctx, body)
		},
		opGetConfiguredTableAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredTableAssociation(ctx, body)
		},
		opListConfiguredTableAssociations: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListConfiguredTableAssociations(ctx, body, ec)
		},
		opUpdateConfiguredTableAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateConfiguredTableAssociation(ctx, body)
		},
		opDeleteConfiguredTableAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteConfiguredTableAssociation(ctx, body)
		},
		opCreateConfiguredTableAssociationAnalysisRule: func(
			ctx context.Context, body []byte, _ *echo.Context,
		) ([]byte, error) {
			return h.handleCreateConfiguredTableAssociationAnalysisRule(ctx, body)
		},
		opGetConfiguredTableAssociationAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredTableAssociationAnalysisRule(ctx, body)
		},
		opUpdateConfiguredTableAssociationAnalysisRule: func(
			ctx context.Context, body []byte, _ *echo.Context,
		) ([]byte, error) {
			return h.handleUpdateConfiguredTableAssociationAnalysisRule(ctx, body)
		},
		opDeleteConfiguredTableAssociationAnalysisRule: func(
			ctx context.Context, body []byte, _ *echo.Context,
		) ([]byte, error) {
			return h.handleDeleteConfiguredTableAssociationAnalysisRule(ctx, body)
		},
	}
}
