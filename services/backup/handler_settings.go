package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchSettingsOps handles the account-level global/region settings and
// supported-resource-types operations.
func (h *Handler) dispatchSettingsOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opDescribeGlobalSettings:
		settings, lastUpdate := h.Backend.DescribeGlobalSettings()

		return true, c.JSON(http.StatusOK, map[string]any{
			"GlobalSettings": settings,
			"LastUpdateTime": epochSeconds(lastUpdate),
		})
	case opUpdateGlobalSettings:
		var reqGSBody struct {
			GlobalSettings map[string]string `json:"GlobalSettings"`
		}
		if err := json.Unmarshal(body, &reqGSBody); err == nil &&
			reqGSBody.GlobalSettings != nil {
			h.Backend.UpdateGlobalSettings(reqGSBody.GlobalSettings)
		}

		return true, c.NoContent(http.StatusOK)
	case opDescribeRegionSettings:
		rs := h.Backend.DescribeRegionSettings()

		return true, c.JSON(http.StatusOK, map[string]any{
			"ResourceTypeManagementPreference": rs.ResourceTypeManagementPreference,
			"ResourceTypeOptInPreference":      rs.ResourceTypeOptInPreference,
		})
	case opUpdateRegionSettings:
		var reqRSBody struct {
			ResourceTypeManagementPreference map[string]bool `json:"ResourceTypeManagementPreference"`
			ResourceTypeOptInPreference      map[string]bool `json:"ResourceTypeOptInPreference"`
		}
		if err := json.Unmarshal(body, &reqRSBody); err == nil {
			h.Backend.UpdateRegionSettings(
				reqRSBody.ResourceTypeManagementPreference,
				reqRSBody.ResourceTypeOptInPreference,
			)
		}

		return true, c.NoContent(http.StatusOK)
	case opGetSupportedResourceTypes:

		return true, c.JSON(http.StatusOK, map[string]any{
			"ResourceTypes": []string{
				"EBS", "EC2", "RDS", "S3", "DynamoDB", "EFS", "FSx",
				"Aurora", "DocumentDB", "Neptune", "Redshift", "Timestream",
			},
		})
	}

	return false, nil
}
