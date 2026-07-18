package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		InputReferenceConfig map[string]any    `json:"inputReferenceConfig"`
		Tags                 map[string]string `json:"tags"`
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		KmsKeyArn            string            `json:"kmsKeyArn"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreateIDMappingTable(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.InputReferenceConfig,
		req.KmsKeyArn,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDMappingTable: t}), nil
}

func (h *Handler) handleGetIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IDMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetIDMappingTable(req.MembershipIdentifier, req.IDMappingTableIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDMappingTable: t}), nil
}

func (h *Handler) handleListIDMappingTables(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListIDMappingTables(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idMappingTableSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IDMappingTableIdentifier string `json:"idMappingTableIdentifier"`
		Description              string `json:"description"`
		KmsKeyArn                string `json:"kmsKeyArn"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.UpdateIDMappingTable(
		req.MembershipIdentifier,
		req.IDMappingTableIdentifier,
		req.Description,
		req.KmsKeyArn,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDMappingTable: t}), nil
}

func (h *Handler) handleDeleteIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IDMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteIDMappingTable(
		req.MembershipIdentifier,
		req.IDMappingTableIdentifier,
	)
}

func (h *Handler) handlePopulateIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IDMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	result, err := h.Backend.PopulateIDMappingTable(
		req.MembershipIdentifier,
		req.IDMappingTableIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(result), nil
}

func (h *Handler) buildIDMappingTableHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		// IDMappingTable
		opCreateIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateIDMappingTable(ctx, body)
		},
		opGetIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetIDMappingTable(ctx, body)
		},
		opListIDMappingTables: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListIDMappingTables(ctx, body, ec)
		},
		opUpdateIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateIDMappingTable(ctx, body)
		},
		opDeleteIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteIDMappingTable(ctx, body)
		},
		opPopulateIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handlePopulateIDMappingTable(ctx, body)
		},
	}
}
