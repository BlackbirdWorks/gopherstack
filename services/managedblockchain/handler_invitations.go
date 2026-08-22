package managedblockchain

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListInvitations(c *echo.Context) error {
	invitations, err := h.Backend.ListInvitations()
	if err != nil {
		return h.writeBackendError(c, err)
	}

	pageItems, nextToken := paginate(invitations, c.Request().URL.Query())

	objs := make([]invitationObject, 0, len(pageItems))

	for _, inv := range pageItems {
		objs = append(objs, toInvitationObject(inv))
	}

	return c.JSON(http.StatusOK, listInvitationsResponse{Invitations: objs, NextToken: nextToken})
}

func (h *Handler) handleRejectInvitation(c *echo.Context, invitationID string) error {
	if err := h.Backend.RejectInvitation(invitationID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// toInvitationObject converts an Invitation to its JSON representation.
func toInvitationObject(inv *Invitation) invitationObject {
	obj := invitationObject{
		InvitationID:   inv.InvitationID,
		Arn:            inv.Arn,
		Status:         inv.Status,
		CreationDate:   inv.CreationDate,
		ExpirationDate: inv.ExpirationDate,
	}

	if inv.NetworkSummary != nil {
		obj.NetworkSummary = &invitationNetworkSummaryObject{
			ID:               inv.NetworkSummary.ID,
			Arn:              inv.NetworkSummary.Arn,
			Name:             inv.NetworkSummary.Name,
			Description:      inv.NetworkSummary.Description,
			Framework:        inv.NetworkSummary.Framework,
			FrameworkVersion: inv.NetworkSummary.FrameworkVersion,
			Status:           inv.NetworkSummary.Status,
			CreationDate:     inv.NetworkSummary.CreationDate,
		}
	}

	return obj
}
