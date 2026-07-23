package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type putVaultAccessPolicyBody struct {
	Policy string `json:"Policy"`
}

func (h *Handler) handlePutBackupVaultAccessPolicy(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	var in putVaultAccessPolicyBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
	}

	if err := h.Backend.PutBackupVaultAccessPolicy(vaultName, in.Policy); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetBackupVaultAccessPolicy(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	pol, err := h.Backend.GetBackupVaultAccessPolicy(vaultName)
	if err != nil {
		return h.handleError(c, err)
	}

	v, err := h.Backend.DescribeBackupVault(vaultName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:  v.BackupVaultArn,
		keyBackupVaultName: vaultName,
		"Policy":           pol.Policy,
	})
}

func (h *Handler) handleDeleteBackupVaultAccessPolicy(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	if err := h.Backend.DeleteBackupVaultAccessPolicy(vaultName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type putVaultLockConfigBody struct {
	MinRetentionDays  int64 `json:"MinRetentionDays,omitempty"`
	MaxRetentionDays  int64 `json:"MaxRetentionDays,omitempty"`
	ChangeableForDays int64 `json:"ChangeableForDays,omitempty"`
}

func (h *Handler) handlePutBackupVaultLockConfiguration(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	var in putVaultLockConfigBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
	}

	cfg := &VaultLockConfig{
		MinRetentionDays:  in.MinRetentionDays,
		MaxRetentionDays:  in.MaxRetentionDays,
		ChangeableForDays: in.ChangeableForDays,
	}

	if err := h.Backend.PutBackupVaultLockConfiguration(vaultName, cfg); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteBackupVaultLockConfiguration(
	c *echo.Context,
	vaultName string,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	if err := h.Backend.DeleteBackupVaultLockConfiguration(vaultName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type putVaultNotificationsBody struct {
	SNSTopicArn       string   `json:"SNSTopicArn"`
	BackupVaultEvents []string `json:"BackupVaultEvents"`
}

func (h *Handler) handlePutBackupVaultNotifications(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	var in putVaultNotificationsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
	}

	cfg := &VaultNotificationConfig{
		SNSTopicArn:       in.SNSTopicArn,
		BackupVaultEvents: in.BackupVaultEvents,
	}

	if err := h.Backend.PutBackupVaultNotifications(vaultName, cfg); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetBackupVaultNotifications(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	cfg, err := h.Backend.GetBackupVaultNotifications(vaultName)
	if err != nil {
		return h.handleError(c, err)
	}

	v, err := h.Backend.DescribeBackupVault(vaultName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:   v.BackupVaultArn,
		keyBackupVaultName:  vaultName,
		"SNSTopicArn":       cfg.SNSTopicArn,
		"BackupVaultEvents": cfg.BackupVaultEvents,
	})
}

func (h *Handler) handleDeleteBackupVaultNotifications(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	if err := h.Backend.DeleteBackupVaultNotifications(vaultName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Backup selection read/delete handlers ---
