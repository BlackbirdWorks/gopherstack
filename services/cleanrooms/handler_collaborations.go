package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                   map[string]string `json:"tags"`
		Name                   string            `json:"name"`
		Description            string            `json:"description"`
		CreatorDisplayName     string            `json:"creatorDisplayName"`
		QueryLogStatus         string            `json:"queryLogStatus"`
		CreatorMemberAbilities []string          `json:"creatorMemberAbilities"`
		Members                []MemberSpec      `json:"members"`
	}
	_ = json.Unmarshal(body, &req)
	c, err := h.Backend.CreateCollaboration(
		req.Name,
		req.Description,
		req.CreatorDisplayName,
		req.CreatorMemberAbilities,
		req.Members,
		req.QueryLogStatus,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaboration: c}), nil
}

func (h *Handler) handleGetCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	c, err := h.Backend.GetCollaboration(req.CollaborationIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaboration: c}), nil
}

func (h *Handler) handleListCollaborations(
	_ context.Context,
	c *echo.Context,
) ([]byte, error) {
	items, next := h.Backend.ListCollaborations(
		qp(c, "memberStatus"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	resp := map[string]any{"collaborationList": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		Name                    string `json:"name"`
		Description             string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	col, err := h.Backend.UpdateCollaboration(
		req.CollaborationIdentifier,
		req.Name,
		req.Description,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaboration: col}), nil
}

func (h *Handler) handleDeleteCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteCollaboration(req.CollaborationIdentifier)
}

func (h *Handler) handleListMembers(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListMembers(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"memberList": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleDeleteMember(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		AccountID               string `json:"accountId"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteMember(req.CollaborationIdentifier, req.AccountID)
}

func (h *Handler) handleCreateCollaborationChangeRequest(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string           `json:"collaborationIdentifier"`
		Changes                 []map[string]any `json:"changes"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateCollaborationChangeRequest(
		req.CollaborationIdentifier,
		req.Changes,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaborationChangeRequest: r}), nil
}

func (h *Handler) handleGetCollaborationChangeRequest(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		ChangeRequestIdentifier string `json:"changeRequestIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetCollaborationChangeRequest(
		req.CollaborationIdentifier,
		req.ChangeRequestIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaborationChangeRequest: r}), nil
}

func (h *Handler) handleListCollaborationChangeRequests(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationChangeRequests(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"collaborationChangeRequests": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateCollaborationChangeRequest(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		ChangeRequestIdentifier string `json:"changeRequestIdentifier"`
		Action                  string `json:"action"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateCollaborationChangeRequest(
		req.CollaborationIdentifier,
		req.ChangeRequestIdentifier,
		req.Action,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaborationChangeRequest: r}), nil
}

func (h *Handler) buildCollaborationHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		// Collaboration
		opCreateCollaboration: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateCollaboration(ctx, body)
		},
		opGetCollaboration: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaboration(ctx, body)
		},
		opListCollaborations: func(ctx context.Context, _ []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborations(ctx, ec)
		},
		opUpdateCollaboration: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateCollaboration(ctx, body)
		},
		opDeleteCollaboration: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteCollaboration(ctx, body)
		},
		opListMembers: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListMembers(ctx, body, ec)
		},
		opDeleteMember: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteMember(ctx, body)
		},
		// Collaboration sub-resources
		opCreateCollaborationChangeRequest: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateCollaborationChangeRequest(ctx, body)
		},
		opGetCollaborationChangeRequest: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaborationChangeRequest(ctx, body)
		},
		opListCollaborationChangeRequests: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationChangeRequests(ctx, body, ec)
		},
		opUpdateCollaborationChangeRequest: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateCollaborationChangeRequest(ctx, body)
		},
	}
}
