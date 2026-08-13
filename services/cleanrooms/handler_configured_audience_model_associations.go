package cleanrooms

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetCollaborationConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier                      string `json:"collaborationIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetCollaborationConfiguredAudienceModelAssociation(
		req.CollaborationIdentifier,
		req.ConfiguredAudienceModelAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaborationCAMAAssociation: a}), nil
}

func (h *Handler) handleListCollaborationConfiguredAudienceModelAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationConfiguredAudienceModelAssociations(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"collaborationConfiguredAudienceModelAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleCreateConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Tags                       map[string]string `json:"tags"`
		MembershipIdentifier       string            `json:"membershipIdentifier"`
		ConfiguredAudienceModelArn string            `json:"configuredAudienceModelArn"`
		// Real wire key is configuredAudienceModelAssociationName, NOT "name"
		// (UpdateConfiguredAudienceModelAssociationInput does use plain
		// "name" -- verified against each op's own
		// awsRestjson1_serializeOpDocument*Input, per gopherstack-sdk-shape).
		Name                   string `json:"configuredAudienceModelAssociationName"`
		Description            string `json:"description"`
		ManageResourcePolicies bool   `json:"manageResourcePolicies"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateConfiguredAudienceModelAssociation(
		req.MembershipIdentifier,
		req.ConfiguredAudienceModelArn,
		req.Name,
		req.Description,
		req.ManageResourcePolicies,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCAMAAssociation: a}), nil
}

func (h *Handler) handleGetConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetConfiguredAudienceModelAssociation(
		req.MembershipIdentifier,
		req.ConfiguredAudienceModelAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCAMAAssociation: a}), nil
}

func (h *Handler) handleListConfiguredAudienceModelAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListConfiguredAudienceModelAssociations(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"configuredAudienceModelAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
		Name                                         string `json:"name"`
		Description                                  string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateConfiguredAudienceModelAssociation(
		req.MembershipIdentifier,
		req.ConfiguredAudienceModelAssociationIdentifier,
		req.Name,
		req.Description,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCAMAAssociation: a}), nil
}

func (h *Handler) handleDeleteConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredAudienceModelAssociation(
		req.MembershipIdentifier,
		req.ConfiguredAudienceModelAssociationIdentifier,
	)
}

// buildCAMAAndIDNamespaceHandlers wires both ConfiguredAudienceModelAssociation
// and IDNamespaceAssociation ops. The two op-maps have near-identical shape
// (membership-scoped CRUD + collaboration-scoped Get/List mirrors), so they
// are grouped into one function rather than kept as two structurally-identical
// map literals in separate files, which previously tripped the dupl linter
// (see handler_id_namespace_associations.go, where the IDNamespaceAssociation
// handle* functions still live).
func (h *Handler) buildCAMAAndIDNamespaceHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		// ConfiguredAudienceModelAssociation
		opGetCollaborationConfiguredAudienceModelAssociation: func(
			ctx context.Context, body []byte, _ *echo.Context,
		) ([]byte, error) {
			return h.handleGetCollaborationConfiguredAudienceModelAssociation(ctx, body)
		},
		opListCollaborationConfiguredAudienceModelAssociations: func(
			ctx context.Context, body []byte, ec *echo.Context,
		) ([]byte, error) {
			return h.handleListCollaborationConfiguredAudienceModelAssociations(ctx, body, ec)
		},
		opCreateConfiguredAudienceModelAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateConfiguredAudienceModelAssociation(ctx, body)
		},
		opGetConfiguredAudienceModelAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredAudienceModelAssociation(ctx, body)
		},
		opListConfiguredAudienceModelAssociations: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListConfiguredAudienceModelAssociations(ctx, body, ec)
		},
		opUpdateConfiguredAudienceModelAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateConfiguredAudienceModelAssociation(ctx, body)
		},
		opDeleteConfiguredAudienceModelAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteConfiguredAudienceModelAssociation(ctx, body)
		},
		// IDNamespaceAssociation
		opGetCollaborationIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaborationIDNamespaceAssociation(ctx, body)
		},
		opListCollaborationIDNamespaceAssociations: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationIDNamespaceAssociations(ctx, body, ec)
		},
		opCreateIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateIDNamespaceAssociation(ctx, body)
		},
		opGetIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetIDNamespaceAssociation(ctx, body)
		},
		opListIDNamespaceAssociations: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListIDNamespaceAssociations(ctx, body, ec)
		},
		opUpdateIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateIDNamespaceAssociation(ctx, body)
		},
		opDeleteIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteIDNamespaceAssociation(ctx, body)
		},
	}
}
