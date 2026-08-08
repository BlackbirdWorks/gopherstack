package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- AccessLogSubscription handlers -------

func (h *Handler) handleCreateALS(c *echo.Context, body map[string]any) error {
	resourceID, _ := body["resourceIdentifier"].(string)
	destArn, _ := body[keyDestinationARN].(string)
	logType, _ := body["serviceNetworkLogType"].(string)

	if resourceID == "" || destArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{keyMessage: "resourceIdentifier and destinationArn are required"},
		)
	}

	ctx := c.Request().Context()
	tags := extractTags(body)

	als, err := h.Backend.CreateAccessLogSubscription(ctx, resourceID, destArn, logType, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, alsToJSON(als))
}

func (h *Handler) handleGetALS(c *echo.Context, id string) error {
	als, err := h.Backend.GetAccessLogSubscription(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, alsToJSON(als))
}

func (h *Handler) handleUpdateALS(c *echo.Context, id string, body map[string]any) error {
	destArn, _ := body[keyDestinationARN].(string)

	als, err := h.Backend.UpdateAccessLogSubscription(id, destArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, alsToJSON(als))
}

func (h *Handler) handleDeleteALS(c *echo.Context, id string) error {
	if err := h.Backend.DeleteAccessLogSubscription(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListALSs(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")
	resourceID := c.QueryParam("resourceIdentifier")

	items, next, err := h.Backend.ListAccessLogSubscriptions(ctx, resourceID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, a := range items {
		summaries = append(summaries, alsSummaryToJSON(a))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- AccessLogSubscription JSON serialization -------

func alsToJSON(a *AccessLogSubscription) map[string]any {
	return map[string]any{
		keyARN:            a.ARN,
		"id":              a.ID,
		"resourceArn":     a.ResourceARN,
		"resourceId":      a.ResourceID,
		keyDestinationARN: a.DestinationARN,
		keyCreatedAt:      a.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt:  a.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

func alsSummaryToJSON(a *AccessLogSubscriptionSummary) map[string]any {
	return map[string]any{
		keyARN:            a.ARN,
		"id":              a.ID,
		"resourceArn":     a.ResourceARN,
		"resourceId":      a.ResourceID,
		keyDestinationARN: a.DestinationARN,
		keyCreatedAt:      a.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}
