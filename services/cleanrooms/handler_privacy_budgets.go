package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetCollaborationPrivacyBudgetTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier         string `json:"collaborationIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetCollaborationPrivacyBudgetTemplate(
		req.CollaborationIdentifier,
		req.PrivacyBudgetTemplateIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyPrivacyBudgetTemplate: t}), nil
}

func (h *Handler) handleListCollaborationPrivacyBudgetTemplates(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationPrivacyBudgetTemplates(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleListCollaborationPrivacyBudgets(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationPrivacyBudgets(
		req.CollaborationIdentifier,
		qp(c, "privacyBudgetType"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleCreatePrivacyBudgetTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Parameters           map[string]any    `json:"parameters"`
		Tags                 map[string]string `json:"tags"`
		MembershipIdentifier string            `json:"membershipIdentifier"`
		PrivacyBudgetType    string            `json:"privacyBudgetType"`
		AutoRefresh          string            `json:"autoRefresh"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreatePrivacyBudgetTemplate(
		req.MembershipIdentifier,
		req.PrivacyBudgetType,
		req.AutoRefresh,
		req.Parameters,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyPrivacyBudgetTemplate: t}), nil
}

func (h *Handler) handleGetPrivacyBudgetTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier            string `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetPrivacyBudgetTemplate(
		req.MembershipIdentifier,
		req.PrivacyBudgetTemplateIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyPrivacyBudgetTemplate: t}), nil
}

func (h *Handler) handleListPrivacyBudgetTemplates(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListPrivacyBudgetTemplates(
		req.MembershipIdentifier,
		qp(c, "privacyBudgetType"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdatePrivacyBudgetTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Parameters                      map[string]any `json:"parameters"`
		MembershipIdentifier            string         `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string         `json:"privacyBudgetTemplateIdentifier"`
		AutoRefresh                     string         `json:"autoRefresh"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.UpdatePrivacyBudgetTemplate(
		req.MembershipIdentifier,
		req.PrivacyBudgetTemplateIdentifier,
		req.AutoRefresh,
		req.Parameters,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyPrivacyBudgetTemplate: t}), nil
}

func (h *Handler) handleDeletePrivacyBudgetTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier            string `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeletePrivacyBudgetTemplate(
		req.MembershipIdentifier,
		req.PrivacyBudgetTemplateIdentifier,
	)
}

func (h *Handler) handleListPrivacyBudgets(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListPrivacyBudgets(
		req.MembershipIdentifier,
		qp(c, "privacyBudgetType"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handlePreviewPrivacyImpact(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Parameters           map[string]any `json:"parameters"`
		MembershipIdentifier string         `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	result, err := h.Backend.PreviewPrivacyImpact(req.MembershipIdentifier, req.Parameters)
	if err != nil {
		return nil, err
	}

	return mustJSON(result), nil
}

func (h *Handler) buildPrivacyBudgetHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opGetCollaborationPrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaborationPrivacyBudgetTemplate(ctx, body)
		},
		opListCollaborationPrivacyBudgetTemplates: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationPrivacyBudgetTemplates(ctx, body, ec)
		},
		opListCollaborationPrivacyBudgets: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationPrivacyBudgets(ctx, body, ec)
		},
		opCreatePrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreatePrivacyBudgetTemplate(ctx, body)
		},
		opGetPrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetPrivacyBudgetTemplate(ctx, body)
		},
		opListPrivacyBudgetTemplates: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListPrivacyBudgetTemplates(ctx, body, ec)
		},
		opUpdatePrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdatePrivacyBudgetTemplate(ctx, body)
		},
		opDeletePrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeletePrivacyBudgetTemplate(ctx, body)
		},
		opListPrivacyBudgets: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListPrivacyBudgets(ctx, body, ec)
		},
		opPreviewPrivacyImpact: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handlePreviewPrivacyImpact(ctx, body)
		},
	}
}
