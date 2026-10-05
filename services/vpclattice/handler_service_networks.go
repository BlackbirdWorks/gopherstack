package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- ServiceNetwork handlers -------

func (h *Handler) handleCreateServiceNetwork(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	if name == "" {
		return validationError(c, keyNameRequired)
	}

	ctx := c.Request().Context()
	authType, _ := body["authType"].(string)
	tags := extractTags(body)

	sharing := extractSharingConfig(body)

	sn, err := idemCreate(h, "CreateServiceNetwork", "", body,
		func(s *ServiceNetwork) string { return s.ID }, h.Backend.GetServiceNetwork,
		func() (*ServiceNetwork, error) {
			return h.Backend.CreateServiceNetworkWithSharing(ctx, name, authType, tags, sharing)
		})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, serviceNetworkToJSON(sn))
}

func (h *Handler) handleGetServiceNetwork(c *echo.Context, id string) error {
	sn, err := h.Backend.GetServiceNetwork(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceNetworkToJSON(sn))
}

func (h *Handler) handleUpdateServiceNetwork(
	c *echo.Context,
	id string,
	body map[string]any,
) error {
	authType, _ := body["authType"].(string)

	sn, err := h.Backend.UpdateServiceNetwork(id, authType)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceNetworkToJSON(sn))
}

func (h *Handler) handleDeleteServiceNetwork(c *echo.Context, id string) error {
	if err := h.Backend.DeleteServiceNetwork(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListServiceNetworks(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListServiceNetworks(ctx, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, serviceNetworkSummaryToJSON(s))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- ServiceNetwork JSON serialization -------

func extractSharingConfig(body map[string]any) *SharingConfig {
	raw, ok := body["sharingConfig"].(map[string]any)
	if !ok {
		return nil
	}

	enabled, _ := raw["enabled"].(bool)

	return &SharingConfig{Enabled: enabled}
}

func serviceNetworkToJSON(s *ServiceNetwork) map[string]any {
	out := map[string]any{
		keyARN:                       s.ARN,
		"id":                         s.ID,
		keyName:                      s.Name,
		"authType":                   s.AuthType,
		"numberOfAssociatedServices": s.NumberOfAssociatedServices,
		"numberOfAssociatedVPCs":     s.NumberOfAssociatedVPCs,
		keyCreatedAt:                 s.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt:             s.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	if s.SharingConfig != nil {
		out["sharingConfig"] = s.SharingConfig
	}

	return out
}

func serviceNetworkSummaryToJSON(s *ServiceNetworkSummary) map[string]any {
	return map[string]any{
		keyARN:                       s.ARN,
		"id":                         s.ID,
		keyName:                      s.Name,
		"numberOfAssociatedServices": s.NumberOfAssociatedServices,
		"numberOfAssociatedVPCs":     s.NumberOfAssociatedVPCs,
		"numberOfAssociatedResourceConfigurations": s.NumberOfAssociatedResourceConfigurations,
		keyCreatedAt:     s.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt: s.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}
