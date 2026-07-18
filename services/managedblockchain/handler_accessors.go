package managedblockchain

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateAccessor(c *echo.Context, body []byte) error {
	var req createAccessorRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	accessor, err := h.Backend.CreateAccessor(
		h.DefaultRegion,
		h.AccountID,
		req.AccessorType,
		req.NetworkType,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createAccessorResponse{
		AccessorID:   accessor.ID,
		BillingToken: accessor.BillingToken,
		NetworkType:  accessor.NetworkType,
	})
}

func (h *Handler) handleGetAccessor(c *echo.Context, accessorID string) error {
	accessor, err := h.Backend.GetAccessor(accessorID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getAccessorResponse{
		Accessor: toAccessorObject(accessor),
	})
}

func (h *Handler) handleDeleteAccessor(c *echo.Context, accessorID string) error {
	if err := h.Backend.DeleteAccessor(accessorID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListAccessors(c *echo.Context) error {
	filter := ListAccessorsFilter{
		NetworkType: c.Request().URL.Query().Get("networkType"),
	}

	accessors, err := h.Backend.ListAccessors(filter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]accessorSummaryObject, 0, len(accessors))

	for _, a := range accessors {
		summaries = append(summaries, toAccessorSummaryObject(a))
	}

	return c.JSON(http.StatusOK, listAccessorsResponse{Accessors: summaries})
}

// toAccessorObject converts an Accessor to its JSON representation.
func toAccessorObject(a *Accessor) accessorObject {
	return accessorObject{
		ID:           a.ID,
		Arn:          a.Arn,
		BillingToken: a.BillingToken,
		Type:         a.Type,
		NetworkType:  a.NetworkType,
		Status:       a.Status,
		CreationDate: a.CreationDate,
		Tags:         a.Tags,
	}
}

// toAccessorSummaryObject converts an Accessor to its summary JSON representation.
func toAccessorSummaryObject(a *Accessor) accessorSummaryObject {
	return accessorSummaryObject{
		ID:           a.ID,
		Arn:          a.Arn,
		Type:         a.Type,
		NetworkType:  a.NetworkType,
		Status:       a.Status,
		CreationDate: a.CreationDate,
	}
}
