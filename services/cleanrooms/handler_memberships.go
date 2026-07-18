package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		DefaultResultConfiguration map[string]any    `json:"defaultResultConfiguration"`
		PaymentConfiguration       map[string]any    `json:"paymentConfiguration"`
		Tags                       map[string]string `json:"tags"`
		CollaborationIdentifier    string            `json:"collaborationIdentifier"`
		QueryLogStatus             string            `json:"queryLogStatus"`
		MemberAbilities            []string          `json:"memberAbilities"`
	}
	_ = json.Unmarshal(body, &req)
	m, err := h.Backend.CreateMembership(
		req.CollaborationIdentifier,
		req.QueryLogStatus,
		req.MemberAbilities,
		req.DefaultResultConfiguration,
		req.PaymentConfiguration,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyMembership: m}), nil
}

func (h *Handler) handleGetMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	m, err := h.Backend.GetMembership(req.MembershipIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyMembership: m}), nil
}

func (h *Handler) handleListMemberships(
	_ context.Context,
	c *echo.Context,
) ([]byte, error) {
	items, next := h.Backend.ListMemberships(
		qp(c, "status"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	resp := map[string]any{"membershipSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		DefaultResultConfiguration map[string]any `json:"defaultResultConfiguration"`
		MembershipIdentifier       string         `json:"membershipIdentifier"`
		QueryLogStatus             string         `json:"queryLogStatus"`
	}
	_ = json.Unmarshal(body, &req)
	m, err := h.Backend.UpdateMembership(
		req.MembershipIdentifier,
		req.QueryLogStatus,
		req.DefaultResultConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyMembership: m}), nil
}

func (h *Handler) handleDeleteMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteMembership(req.MembershipIdentifier)
}

func (h *Handler) buildMembershipHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opCreateMembership: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateMembership(ctx, body)
		},
		opGetMembership: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetMembership(ctx, body)
		},
		opListMemberships: func(ctx context.Context, _ []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListMemberships(ctx, ec)
		},
		opUpdateMembership: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateMembership(ctx, body)
		},
		opDeleteMembership: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteMembership(ctx, body)
		},
	}
}
