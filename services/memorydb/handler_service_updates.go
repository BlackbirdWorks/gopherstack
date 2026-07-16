package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func (h *Handler) handleDescribeServiceUpdates(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeServiceUpdatesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}
	updates, err := h.Backend.DescribeServiceUpdates(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}
	updates, nextToken := paginateItems(
		updates,
		req.NextToken,
		req.MaxResults,
		func(su *ServiceUpdate) string { return su.ServiceUpdateName },
	)
	objs := make([]serviceUpdateObject, 0, len(updates))
	for _, su := range updates {
		objs = append(objs, serviceUpdateObject{
			ServiceUpdateName:   su.ServiceUpdateName,
			ReleaseDate:         awstime.Epoch(su.ReleaseDate),
			Description:         su.Description,
			Status:              su.Status,
			Type:                su.Type,
			AutoUpdateStartDate: awstime.Epoch(su.AutoUpdateStartDate),
		})
	}

	return c.JSON(http.StatusOK, describeServiceUpdatesResponse{ServiceUpdates: objs, NextToken: nextToken})
}

// -- ReservedNode handlers -------------------------------------------------------
