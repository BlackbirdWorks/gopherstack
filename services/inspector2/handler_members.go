package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opAssociateMember    = "AssociateMember"
	opDisassociateMember = "DisassociateMember"
	opGetMember          = "GetMember"
	opListMembers        = "ListMembers"

	pathMembersAssociate    = "/members/associate"
	pathMembersDisassociate = "/members/disassociate"
	pathMembersGet          = "/members/get"
	pathMembersList         = "/members/list"
)

func (h *Handler) handleAssociateMember(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountID string `json:"accountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if assocErr := h.Backend.AssociateMember(req.AccountID); assocErr != nil {
		return h.mapError(c, assocErr)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyAccountID: req.AccountID},
	)
}

func (h *Handler) handleDisassociateMember(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountID string `json:"accountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if disassocErr := h.Backend.DisassociateMember(req.AccountID); disassocErr != nil {
		return h.mapError(c, disassocErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAccountID: req.AccountID})
}

func (h *Handler) handleGetMember(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountID string `json:"accountId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	m, getErr := h.Backend.GetMember(req.AccountID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"member": memberToWire(m)})
}

// memberToWire renders a Member in its Inspector2 wire shape. UpdatedAt is a
// Go time.Time internally, but the restjson1 Member.UpdatedAt member is a
// DateTimeTimestamp (epoch-seconds JSON number, see pkgs/awstime) -- the
// domain struct's default JSON marshaling would emit an RFC3339 string and
// break real SDK clients' GetMember/ListMembers deserializers.
func memberToWire(m *Member) map[string]any {
	return map[string]any{
		keyAccountID:             m.AccountID,
		keyDelegatedAdminAccount: m.DelegatedAdminAccountID,
		"relationshipStatus":     m.RelationshipStatus,
		keyUpdatedAt:             awstime.Epoch(m.UpdatedAt),
	}
}

func (h *Handler) handleListMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		OnlyAssociated bool `json:"onlyAssociated"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	members, listErr := h.Backend.ListMembers(req.OnlyAssociated)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	wire := make([]map[string]any, 0, len(members))
	for _, m := range members {
		wire = append(wire, memberToWire(m))
	}

	return c.JSON(http.StatusOK, map[string]any{"members": wire})
}
