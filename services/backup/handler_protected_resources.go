package backup

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchProtectedResourceOps handles protected-resource describe/list operations.
func (h *Handler) dispatchProtectedResourceOps(
	c *echo.Context,
	route backupRoute,
) (bool, error) {
	switch route.operation {
	case opDescribeProtectedResource:
		pr, err := h.Backend.DescribeProtectedResource(route.resource)
		if err != nil {
			return true, c.JSON(
				http.StatusBadRequest,
				errResp("ResourceNotFoundException", err.Error()),
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			keyResourceArn:   pr.ResourceArn,
			keyResourceType:  pr.ResourceType,
			"LastBackupTime": epochSeconds(pr.LastBackupTime),
		})
	case opListProtectedResources:
		prs := h.Backend.ListProtectedResources()
		items := make([]map[string]any, 0, len(prs))
		for _, pr := range prs {
			items = append(items, map[string]any{
				keyResourceArn:  pr.ResourceArn,
				keyResourceType: pr.ResourceType,
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"Results": items})
	case opListProtectedResourcesByBackupVault:
		prs := h.Backend.ListProtectedResourcesByBackupVault(route.resource)
		items := make([]map[string]any, 0, len(prs))
		for _, pr := range prs {
			items = append(items, map[string]any{
				keyResourceArn:  pr.ResourceArn,
				keyResourceType: pr.ResourceType,
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"Results": items})
	}

	return false, nil
}
