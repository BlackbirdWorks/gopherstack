package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- DomainVerification handlers -------

func (h *Handler) handleStartDomainVerification(c *echo.Context, body map[string]any) error {
	domainName, _ := body[keyDomainName].(string)
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "domainName is required"})
	}

	ctx := c.Request().Context()
	tags := extractTags(body)

	dv, err := h.Backend.StartDomainVerification(ctx, domainName, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, domainVerificationToJSON(dv))
}

func (h *Handler) handleGetDomainVerification(c *echo.Context, id string) error {
	dv, err := h.Backend.GetDomainVerification(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, domainVerificationToJSON(dv))
}

func (h *Handler) handleDeleteDomainVerification(c *echo.Context, id string) error {
	if err := h.Backend.DeleteDomainVerification(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListDomainVerifications(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListDomainVerifications(ctx, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, dv := range items {
		summaries = append(summaries, map[string]any{
			keyARN:        dv.ARN,
			"id":          dv.ID,
			keyDomainName: dv.DomainName,
			keyStatus:     dv.Status,
			keyCreatedAt:  dv.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func domainVerificationToJSON(dv *DomainVerification) map[string]any {
	m := map[string]any{
		keyARN:        dv.ARN,
		"id":          dv.ID,
		keyDomainName: dv.DomainName,
		keyStatus:     dv.Status,
		keyCreatedAt:  dv.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	if dv.LastVerifiedTime != nil {
		m["lastVerifiedTime"] = dv.LastVerifiedTime.UTC().Format("2006-01-02T15:04:05.000Z")
	}

	return m
}
