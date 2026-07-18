package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleBatchGetSchema(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string   `json:"collaborationIdentifier"`
		Names                   []string `json:"names"`
	}
	_ = json.Unmarshal(body, &req)
	items, errs, err := h.Backend.BatchGetSchema(req.CollaborationIdentifier, req.Names)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"schemas": items, keyErrors: errs}), nil
}

func (h *Handler) handleBatchGetSchemaAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier    string `json:"collaborationIdentifier"`
		SchemaAnalysisRuleRequests []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"schemaAnalysisRuleRequests"`
	}
	_ = json.Unmarshal(body, &req)
	names := make([]string, 0, len(req.SchemaAnalysisRuleRequests))
	var ruleType string
	for _, r := range req.SchemaAnalysisRuleRequests {
		names = append(names, r.Name)
		if ruleType == "" {
			ruleType = r.Type
		}
	}
	items, errs, err := h.Backend.BatchGetSchemaAnalysisRule(
		req.CollaborationIdentifier,
		names,
		ruleType,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"analysisRules": items, keyErrors: errs}), nil
}

func (h *Handler) handleGetSchema(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		Name                    string `json:"name"`
	}
	_ = json.Unmarshal(body, &req)
	s, err := h.Backend.GetSchema(req.CollaborationIdentifier, req.Name)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"schema": s}), nil
}

func (h *Handler) handleListSchemas(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListSchemas(
		req.CollaborationIdentifier,
		qp(c, "schemaType"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"schemaSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleGetSchemaAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		Name                    string `json:"name"`
		Type                    string `json:"type"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetSchemaAnalysisRule(req.CollaborationIdentifier, req.Name, req.Type)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) buildSchemaHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opBatchGetSchema: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleBatchGetSchema(ctx, body)
		},
		opBatchGetSchemaAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleBatchGetSchemaAnalysisRule(ctx, body)
		},
		opGetSchema: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetSchema(ctx, body)
		},
		opListSchemas: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListSchemas(ctx, body, ec)
		},
		opGetSchemaAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetSchemaAnalysisRule(ctx, body)
		},
	}
}
