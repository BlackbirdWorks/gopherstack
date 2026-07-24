package directoryservice

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateLogSubscription(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: "LogGroupName",
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.CreateLogSubscription(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDeleteLogSubscription(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	if delErr := h.Backend.DeleteLogSubscription(h.contextWithRegion(c), req.DirectoryID); delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListLogSubscriptions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	subs, nextToken, listErr := h.Backend.ListLogSubscriptions(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	subList := make([]map[string]any, 0, len(subs))
	for _, s := range subs {
		subList = append(subList, map[string]any{
			keyDirectoryID:                s.DirectoryID,
			"LogGroupName":                s.LogGroupName,
			"SubscriptionCreatedDateTime": awstime.Epoch(s.CreatedTime),
		})
	}

	resp := map[string]any{"LogSubscriptions": subList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
