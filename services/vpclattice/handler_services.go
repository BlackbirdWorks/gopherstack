package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- Service handlers -------

func (h *Handler) handleCreateService(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: keyNameRequired})
	}

	ctx := c.Request().Context()
	authType, _ := body["authType"].(string)
	certArn, _ := body["certificateArn"].(string)
	customDomain, _ := body["customDomainName"].(string)
	tags := extractTags(body)

	svc, err := h.Backend.CreateService(ctx, name, authType, certArn, customDomain, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, serviceToJSON(svc))
}

func (h *Handler) handleGetService(c *echo.Context, id string) error {
	svc, err := h.Backend.GetService(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceToJSON(svc))
}

func (h *Handler) handleUpdateService(c *echo.Context, id string, body map[string]any) error {
	authType, _ := body["authType"].(string)
	certArn, _ := body["certificateArn"].(string)

	svc, err := h.Backend.UpdateService(id, authType, certArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceToJSON(svc))
}

func (h *Handler) handleDeleteService(c *echo.Context, id string) error {
	svc, err := h.Backend.DeleteService(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceToJSON(svc))
}

func (h *Handler) handleListServices(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListServices(ctx, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, serviceSummaryToJSON(s))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- Service JSON serialization -------

func serviceToJSON(s *Service) map[string]any {
	m := map[string]any{
		keyARN:           s.ARN,
		"id":             s.ID,
		keyName:          s.Name,
		"authType":       s.AuthType,
		keyStatus:        s.Status,
		keyCreatedAt:     s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt: s.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
		"dnsEntry":       dnsEntryToJSON(s.DNSName, s.HostedZoneID),
	}

	if s.CertificateArn != "" {
		m["certificateArn"] = s.CertificateArn
	}

	if s.CustomDomainName != "" {
		m["customDomainName"] = s.CustomDomainName
	}

	return m
}

func serviceSummaryToJSON(s *ServiceSummary) map[string]any {
	m := map[string]any{
		keyARN:       s.ARN,
		"id":         s.ID,
		keyName:      s.Name,
		keyStatus:    s.Status,
		keyCreatedAt: s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if s.DNSName != "" {
		m["dnsEntry"] = dnsEntryToJSON(s.DNSName, s.HostedZoneID)
	}

	if s.CustomDomainName != "" {
		m["customDomainName"] = s.CustomDomainName
	}

	return m
}
