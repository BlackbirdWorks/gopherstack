package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateShare(c *echo.Context) error {
	var req struct {
		ResourceArn         string `json:"resourceArn"`
		PrincipalSubscriber string `json:"principalSubscriber"`
		Name                string `json:"shareName"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	share, err := h.Backend.CreateShare(req.ResourceArn, req.PrincipalSubscriber, req.Name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		"shareId":   share.ShareID,
		"shareName": share.Name,
		keyStatus:   share.Status,
	})
}

func (h *Handler) handleAcceptShare(c *echo.Context, shareID string) error {
	share, err := h.Backend.AcceptShare(shareID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyStatus: share.Status})
}

func (h *Handler) handleDeleteShare(c *echo.Context, shareID string) error {
	share, err := h.Backend.DeleteShare(shareID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyStatus: share.Status})
}

func (h *Handler) handleGetShare(c *echo.Context, shareID string) error {
	share, err := h.Backend.GetShare(shareID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"share": share})
}

func (h *Handler) handleListShares(c *echo.Context) error {
	var req struct {
		Filter        *ShareFilter `json:"filter"`
		ResourceOwner string       `json:"resourceOwner"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	maxResults, nextToken := listQueryParams(c)

	shares, next, err := h.Backend.ListShares(req.ResourceOwner, req.Filter, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"shares": shares, keyNextToken: next})
}
