package backup

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

// dispatchTieringOps handles backup-vault tiering configuration operations.
func (h *Handler) dispatchTieringOps(
	c *echo.Context,
	route backupRoute,
	_ []byte,
) (bool, error) {
	switch route.operation {
	case opCreateTieringConfiguration:
		err := h.Backend.CreateTieringConfiguration(route.resource)
		if err != nil {
			account := awsmeta.Account(c.Request().Context())
			vaultArn := "arn:aws:backup:" + h.Backend.Region() + ":" + account + ":backup-vault:" + route.resource

			return true, c.JSON(http.StatusOK, map[string]any{keyBackupVaultArn: vaultArn})
		}
		tc, _ := h.Backend.GetTieringConfiguration(route.resource)

		return true, c.JSON(http.StatusOK, map[string]any{keyBackupVaultArn: tc.BackupVaultArn})
	case opDeleteTieringConfiguration:
		_ = h.Backend.DeleteTieringConfiguration(route.resource)

		return true, c.NoContent(http.StatusNoContent)
	case opGetTieringConfiguration:
		tc, err := h.Backend.GetTieringConfiguration(route.resource)
		if err != nil {
			return true, c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
		}

		return true, c.JSON(http.StatusOK, map[string]any{
			"TieringConfiguration": map[string]any{
				keyBackupVaultName: tc.BackupVaultName,
				keyBackupVaultArn:  tc.BackupVaultArn,
			},
		})
	case opListTieringConfigurations:
		tcs := h.Backend.ListTieringConfigurations()
		items := make([]map[string]any, 0, len(tcs))
		for _, tc := range tcs {
			items = append(
				items,
				map[string]any{
					keyBackupVaultName: tc.BackupVaultName,
					keyBackupVaultArn:  tc.BackupVaultArn,
				},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{keyTieringConfigurations: items})
	case opUpdateTieringConfiguration:
		_ = h.Backend.UpdateTieringConfiguration(route.resource)

		return true, c.NoContent(http.StatusOK)
	}

	return false, nil
}
