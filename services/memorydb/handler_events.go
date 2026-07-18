package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func (h *Handler) handleDescribeEvents(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeEventsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	events, err := h.Backend.DescribeEvents(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]eventObject, 0, len(events))

	for _, ev := range events {
		objs = append(objs, eventObject{
			Date:       awstime.Epoch(ev.Date),
			SourceName: ev.SourceName,
			SourceType: ev.SourceType,
			Message:    ev.Message,
		})
	}

	return c.JSON(http.StatusOK, describeEventsResponse{Events: objs})
}

// -- MultiRegionCluster handlers -------------------------------------------------
