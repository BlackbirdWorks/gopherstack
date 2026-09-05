package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultDescribeEventsLimit is this backend's default DescribeEvents page
// size; events accumulate up to maxEvents per region (store.go), well past
// one page, so MaxResults/NextToken must be honoured rather than returning
// every stored event in one response.
const defaultDescribeEventsLimit = 100

func (h *Handler) handleDescribeEvents(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeEventsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	events, err := h.Backend.DescribeEvents(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	limit := 0
	if req.MaxResults != nil {
		limit = int(*req.MaxResults)
	}

	p := page.New(events, req.NextToken, limit, defaultDescribeEventsLimit)

	objs := make([]eventObject, 0, len(p.Data))

	for _, ev := range p.Data {
		objs = append(objs, eventObject{
			Date:       awstime.Epoch(ev.Date),
			SourceName: ev.SourceName,
			SourceType: ev.SourceType,
			Message:    ev.Message,
		})
	}

	return c.JSON(http.StatusOK, describeEventsResponse{Events: objs, NextToken: p.Next})
}

// -- MultiRegionCluster handlers -------------------------------------------------
