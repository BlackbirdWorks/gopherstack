package backup

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// protectedResourceToJSON mirrors types.ProtectedResource (backup@v1.64.0
// types.go): ResourceArn, ResourceName, ResourceType, LastBackupTime,
// LastBackupVaultArn, LastRecoveryPointArn. Shared by DescribeProtectedResource
// and both List ops, which previously each built their own narrower,
// inconsistent map.
func protectedResourceToJSON(pr *ProtectedResource) map[string]any {
	m := map[string]any{
		keyResourceArn:   pr.ResourceArn,
		keyResourceType:  pr.ResourceType,
		"LastBackupTime": epochSeconds(pr.LastBackupTime),
	}
	setOptionalStr(m, "ResourceName", pr.ResourceName)
	setOptionalStr(m, "LastBackupVaultArn", pr.LastBackupVaultArn)
	setOptionalStr(m, "LastRecoveryPointArn", pr.LastRecoveryPointArn)

	return m
}

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

		return true, c.JSON(http.StatusOK, protectedResourceToJSON(pr))
	case opListProtectedResources:
		q := c.Request().URL.Query()
		prs, nextToken := h.Backend.ListProtectedResources(parseInt(q.Get("maxResults")), q.Get("nextToken"))
		items := make([]map[string]any, 0, len(prs))
		for _, pr := range prs {
			items = append(items, protectedResourceToJSON(pr))
		}

		resp := map[string]any{"Results": items}
		if nextToken != "" {
			resp["NextToken"] = nextToken
		}

		return true, c.JSON(http.StatusOK, resp)
	case opListProtectedResourcesByBackupVault:
		q := c.Request().URL.Query()
		prs, nextToken := h.Backend.ListProtectedResourcesByBackupVault(
			route.resource, parseInt(q.Get("maxResults")), q.Get("nextToken"),
		)
		items := make([]map[string]any, 0, len(prs))
		for _, pr := range prs {
			items = append(items, protectedResourceToJSON(pr))
		}

		resp := map[string]any{"Results": items}
		if nextToken != "" {
			resp["NextToken"] = nextToken
		}

		return true, c.JSON(http.StatusOK, resp)
	}

	return false, nil
}
