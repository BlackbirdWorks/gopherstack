package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

const (
	keyTieringConfigurationName = "TieringConfigurationName"
	keyTieringConfigurationArn  = "TieringConfigurationArn"
)

// resourceSelectionJSON is the wire shape of a single ResourceSelection entry
// nested inside a tiering configuration body.
type resourceSelectionJSON struct {
	ResourceType              string   `json:"ResourceType"`
	Resources                 []string `json:"Resources"`
	TieringDownSettingsInDays int64    `json:"TieringDownSettingsInDays"`
}

// tieringConfigBodyJSON is the wire shape shared by
// TieringConfigurationInputForCreate/-ForUpdate.
type tieringConfigBodyJSON struct {
	BackupVaultName          string                  `json:"BackupVaultName"`
	TieringConfigurationName string                  `json:"TieringConfigurationName,omitempty"`
	ResourceSelection        []resourceSelectionJSON `json:"ResourceSelection"`
}

type createTieringConfigBody struct {
	TieringConfigurationTags map[string]string     `json:"TieringConfigurationTags,omitempty"`
	CreatorRequestID         string                `json:"CreatorRequestId,omitempty"`
	TieringConfiguration     tieringConfigBodyJSON `json:"TieringConfiguration"`
}

type updateTieringConfigBody struct {
	TieringConfiguration tieringConfigBodyJSON `json:"TieringConfiguration"`
}

func resourceSelectionFromJSON(in []resourceSelectionJSON) []ResourceSelection {
	out := make([]ResourceSelection, 0, len(in))
	for _, rs := range in {
		out = append(out, ResourceSelection(rs))
	}

	return out
}

func resourceSelectionToJSON(in []ResourceSelection) []resourceSelectionJSON {
	out := make([]resourceSelectionJSON, 0, len(in))
	for _, rs := range in {
		out = append(out, resourceSelectionJSON(rs))
	}

	return out
}

// tieringConfigToJSON renders a TieringConfiguration for GetTieringConfiguration,
// with CreationTime/LastUpdatedTime as epoch-seconds (restjson1 wire format).
func tieringConfigToJSON(tc *TieringConfiguration) map[string]any {
	out := map[string]any{
		keyBackupVaultName:          tc.BackupVaultName,
		keyTieringConfigurationName: tc.TieringConfigurationName,
		keyTieringConfigurationArn:  tc.TieringConfigurationArn,
		"ResourceSelection":         resourceSelectionToJSON(tc.ResourceSelection),
		keyCreationTime:             epochSeconds(tc.CreationTime),
	}
	if tc.LastUpdatedTime != nil {
		out["LastUpdatedTime"] = epochSeconds(*tc.LastUpdatedTime)
	}

	return out
}

// dispatchTieringOps handles tiering configuration operations
// (/tiering-configurations).
func (h *Handler) dispatchTieringOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opCreateTieringConfiguration:

		return true, h.handleCreateTieringConfiguration(c, body)
	case opDeleteTieringConfiguration:

		return true, h.handleDeleteTieringConfiguration(c, route.resource)
	case opGetTieringConfiguration:

		return true, h.handleGetTieringConfiguration(c, route.resource)
	case opListTieringConfigurations:

		return true, h.handleListTieringConfigurations(c)
	case opUpdateTieringConfiguration:

		return true, h.handleUpdateTieringConfiguration(c, route.resource, body)
	}

	return false, nil
}

func (h *Handler) handleCreateTieringConfiguration(c *echo.Context, body []byte) error {
	var in createTieringConfigBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	tc, err := h.Backend.CreateTieringConfiguration(
		in.TieringConfiguration.TieringConfigurationName,
		in.TieringConfiguration.BackupVaultName,
		resourceSelectionFromJSON(in.TieringConfiguration.ResourceSelection),
		in.CreatorRequestID,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCreationTime:             epochSeconds(tc.CreationTime),
		keyTieringConfigurationArn:  tc.TieringConfigurationArn,
		keyTieringConfigurationName: tc.TieringConfigurationName,
	})
}

func (h *Handler) handleDeleteTieringConfiguration(c *echo.Context, name string) error {
	if err := h.Backend.DeleteTieringConfiguration(name); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetTieringConfiguration(c *echo.Context, name string) error {
	tc, err := h.Backend.GetTieringConfiguration(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TieringConfiguration": tieringConfigToJSON(tc),
	})
}

func (h *Handler) handleListTieringConfigurations(c *echo.Context) error {
	tcs := h.Backend.ListTieringConfigurations()
	items := make([]map[string]any, 0, len(tcs))
	for _, tc := range tcs {
		item := map[string]any{
			keyBackupVaultName:          tc.BackupVaultName,
			keyTieringConfigurationName: tc.TieringConfigurationName,
			keyTieringConfigurationArn:  tc.TieringConfigurationArn,
			keyCreationTime:             epochSeconds(tc.CreationTime),
		}
		if tc.LastUpdatedTime != nil {
			item["LastUpdatedTime"] = epochSeconds(*tc.LastUpdatedTime)
		}
		items = append(items, item)
	}

	return c.JSON(http.StatusOK, map[string]any{keyTieringConfigurations: items})
}

func (h *Handler) handleUpdateTieringConfiguration(c *echo.Context, name string, body []byte) error {
	var in updateTieringConfigBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	tc, err := h.Backend.UpdateTieringConfiguration(
		name,
		in.TieringConfiguration.BackupVaultName,
		resourceSelectionFromJSON(in.TieringConfiguration.ResourceSelection),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyCreationTime:             epochSeconds(tc.CreationTime),
		keyTieringConfigurationArn:  tc.TieringConfigurationArn,
		keyTieringConfigurationName: tc.TieringConfigurationName,
	}
	if tc.LastUpdatedTime != nil {
		resp["LastUpdatedTime"] = epochSeconds(*tc.LastUpdatedTime)
	}

	return c.JSON(http.StatusOK, resp)
}
