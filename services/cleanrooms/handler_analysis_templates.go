package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetCollaborationAnalysisTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		AnalysisTemplateArn     string `json:"analysisTemplateArn"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetCollaborationAnalysisTemplate(
		req.CollaborationIdentifier,
		req.AnalysisTemplateArn,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaborationAnalysisTemplate: t}), nil
}

func (h *Handler) handleListCollaborationAnalysisTemplates(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationAnalysisTemplates(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"collaborationAnalysisTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleBatchGetCollaborationAnalysisTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string   `json:"collaborationIdentifier"`
		AnalysisTemplateArns    []string `json:"analysisTemplateArns"`
	}
	_ = json.Unmarshal(body, &req)
	items, errs, err := h.Backend.BatchGetCollaborationAnalysisTemplate(
		req.CollaborationIdentifier,
		req.AnalysisTemplateArns,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"collaborationAnalysisTemplates": items, keyErrors: errs}), nil
}

func (h *Handler) handleCreateAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Source               map[string]any    `json:"source"`
		Tags                 map[string]string `json:"tags"`
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		Format               string            `json:"format"`
		AnalysisParameters   []map[string]any  `json:"analysisParameters"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreateAnalysisTemplate(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.Format,
		req.Source,
		req.AnalysisParameters,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisTemplate: t}), nil
}

func (h *Handler) handleGetAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string `json:"membershipIdentifier"`
		AnalysisTemplateIdentifier string `json:"analysisTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetAnalysisTemplate(
		req.MembershipIdentifier,
		req.AnalysisTemplateIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisTemplate: t}), nil
}

func (h *Handler) handleListAnalysisTemplates(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListAnalysisTemplates(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"analysisTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string `json:"membershipIdentifier"`
		AnalysisTemplateIdentifier string `json:"analysisTemplateIdentifier"`
		Description                string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.UpdateAnalysisTemplate(
		req.MembershipIdentifier,
		req.AnalysisTemplateIdentifier,
		req.Description,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisTemplate: t}), nil
}

func (h *Handler) handleDeleteAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string `json:"membershipIdentifier"`
		AnalysisTemplateIdentifier string `json:"analysisTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteAnalysisTemplate(
		req.MembershipIdentifier,
		req.AnalysisTemplateIdentifier,
	)
}

func (h *Handler) buildAnalysisTemplateHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opGetCollaborationAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaborationAnalysisTemplate(ctx, body)
		},
		opListCollaborationAnalysisTemplates: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationAnalysisTemplates(ctx, body, ec)
		},
		opBatchGetCollaborationAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleBatchGetCollaborationAnalysisTemplate(ctx, body)
		},
		opCreateAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateAnalysisTemplate(ctx, body)
		},
		opGetAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetAnalysisTemplate(ctx, body)
		},
		opListAnalysisTemplates: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListAnalysisTemplates(ctx, body, ec)
		},
		opUpdateAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateAnalysisTemplate(ctx, body)
		},
		opDeleteAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteAnalysisTemplate(ctx, body)
		},
	}
}
