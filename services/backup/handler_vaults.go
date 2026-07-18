package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createBackupVaultBody struct {
	BackupVaultTags  map[string]string `json:"BackupVaultTags"`
	EncryptionKeyArn string            `json:"EncryptionKeyArn"`
	CreatorRequestID string            `json:"CreatorRequestId"`
}

func (h *Handler) handleCreateBackupVault(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in createBackupVaultBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	v, err := h.Backend.CreateBackupVault(
		name,
		in.EncryptionKeyArn,
		in.CreatorRequestID,
		in.BackupVaultTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:  v.BackupVaultArn,
		keyBackupVaultName: v.BackupVaultName,
		keyCreationDate:    epochSeconds(v.CreationTime),
	})
}

func (h *Handler) handleDescribeBackupVault(c *echo.Context, name string) error {
	v, err := h.Backend.DescribeBackupVault(name)
	if err != nil {
		return h.handleError(c, err)
	}

	vaultType := v.VaultType
	if vaultType == "" {
		vaultType = VaultTypeBackupVault
	}

	resp := map[string]any{
		keyBackupVaultName:       v.BackupVaultName,
		keyBackupVaultArn:        v.BackupVaultArn,
		keyCreationDate:          epochSeconds(v.CreationTime),
		"NumberOfRecoveryPoints": v.NumberOfRecoveryPoints,
		keyVaultState:            "AVAILABLE",
		keyVaultType:             vaultType,
	}
	setOptionalStr(resp, "EncryptionKeyArn", v.EncryptionKeyArn)
	setOptionalStr(resp, "CreatorRequestId", v.CreatorRequestID)
	if v.Tags != nil {
		if t := v.Tags.Clone(); len(t) > 0 {
			resp["Tags"] = t
		}
	}

	// Include vault lock fields. AWS always returns Locked; when a lock config
	// exists the retention bounds and optional LockDate are also included.
	if cfg, cfgErr := h.Backend.GetBackupVaultLockConfig(name); cfgErr == nil {
		resp["Locked"] = true
		resp["MinRetentionDays"] = cfg.MinRetentionDays
		resp["MaxRetentionDays"] = cfg.MaxRetentionDays
		if cfg.LockDate != nil {
			resp["LockDate"] = epochSeconds(*cfg.LockDate)
		}
	} else {
		resp["Locked"] = false
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListBackupVaults(c *echo.Context) error {
	q := c.Request().URL.Query()
	f := ListVaultsFilter{
		VaultType:  q.Get("byVaultType"),
		NextToken:  q.Get("nextToken"),
		MaxResults: parseInt(q.Get("maxResults")),
	}

	vaults, nextToken := h.Backend.ListBackupVaultsFiltered(f)
	items := make([]map[string]any, 0, len(vaults))

	for _, v := range vaults {
		vt := v.VaultType
		if vt == "" {
			vt = VaultTypeBackupVault
		}

		item := map[string]any{
			keyBackupVaultName:       v.BackupVaultName,
			keyBackupVaultArn:        v.BackupVaultArn,
			keyCreationDate:          epochSeconds(v.CreationTime),
			"NumberOfRecoveryPoints": v.NumberOfRecoveryPoints,
			keyVaultState:            "AVAILABLE",
			keyVaultType:             vt,
		}
		if v.EncryptionKeyArn != "" {
			item["EncryptionKeyArn"] = v.EncryptionKeyArn
		}
		if v.MinRetentionDays > 0 {
			item["MinRetentionDays"] = v.MinRetentionDays
			item["MaxRetentionDays"] = v.MaxRetentionDays
			item[keyVaultState] = statusCreating
		}
		items = append(items, item)
	}

	resp := map[string]any{"BackupVaultList": items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteBackupVault(c *echo.Context, name string) error {
	if err := h.Backend.DeleteBackupVaultChecked(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Plan handlers ---

type associateMpaApprovalTeamBody struct {
	MpaApprovalTeamArn string `json:"MpaApprovalTeamArn"`
	RequesterComment   string `json:"RequesterComment,omitempty"`
}

func (h *Handler) handleAssociateBackupVaultMpaApprovalTeam(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in associateMpaApprovalTeamBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	if err := h.Backend.AssociateBackupVaultMpaApprovalTeam(vaultName, in.MpaApprovalTeamArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type createLogicallyAirGappedBody struct {
	BackupVaultTags  map[string]string `json:"BackupVaultTags,omitempty"`
	CreatorRequestID string            `json:"CreatorRequestId,omitempty"`
	MaxRetentionDays int64             `json:"MaxRetentionDays"`
	MinRetentionDays int64             `json:"MinRetentionDays"`
}

func (h *Handler) handleCreateLogicallyAirGappedBackupVault(
	c *echo.Context,
	name string,
	body []byte,
) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in createLogicallyAirGappedBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	v, err := h.Backend.CreateLogicallyAirGappedBackupVault(
		name, in.CreatorRequestID, in.MinRetentionDays, in.MaxRetentionDays, in.BackupVaultTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:  v.BackupVaultArn,
		keyBackupVaultName: v.BackupVaultName,
		keyCreationDate:    epochSeconds(v.CreationTime),
		keyVaultState:      statusCreating,
		keyVaultType:       VaultTypeAirGapped,
	})
}

type createRestoreAccessVaultBody struct {
	SourceBackupVaultArn string            `json:"SourceBackupVaultArn"`
	BackupVaultName      string            `json:"BackupVaultName,omitempty"`
	BackupVaultTags      map[string]string `json:"BackupVaultTags,omitempty"`
	CreatorRequestID     string            `json:"CreatorRequestId,omitempty"`
	RequesterComment     string            `json:"RequesterComment,omitempty"`
}

func (h *Handler) handleCreateRestoreAccessBackupVault(c *echo.Context, body []byte) error {
	var in createRestoreAccessVaultBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.SourceBackupVaultArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "SourceBackupVaultArn is required"),
		)
	}

	rav, err := h.Backend.CreateRestoreAccessBackupVault(
		in.SourceBackupVaultArn, in.BackupVaultName, in.CreatorRequestID, in.BackupVaultTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreAccessBackupVaultArn":  rav.RestoreAccessBackupVaultArn,
		"RestoreAccessBackupVaultName": rav.RestoreAccessBackupVaultName,
		keyCreationDate:                epochSeconds(rav.CreationDate),
		keyVaultState:                  rav.VaultState,
	})
}

// dispatchRestoreAccessVaultOps handles restore-access-vault and MPA
// approval team operations.
func (h *Handler) dispatchRestoreAccessVaultOps(c *echo.Context, route backupRoute) (bool, error) {
	switch route.operation {
	case opListRestoreAccessBackupVaults:
		vaults := h.Backend.ListRestoreAccessBackupVaults()
		items := make([]map[string]any, 0, len(vaults))
		for _, v := range vaults {
			items = append(items, map[string]any{
				"RestoreAccessBackupVaultName": v.RestoreAccessBackupVaultName,
				"RestoreAccessBackupVaultArn":  v.RestoreAccessBackupVaultArn,
				keyVaultState:                  v.VaultState,
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{"RestoreAccessBackupVaults": items})
	case opRevokeRestoreAccessBackupVault:
		_ = h.Backend.RevokeRestoreAccessBackupVault(route.resource)

		return true, c.NoContent(http.StatusNoContent)
	case opDisassociateBackupVaultMpaApprovalTeam:
		_ = h.Backend.DisassociateBackupVaultMpaApprovalTeam(route.resource)

		return true, c.NoContent(http.StatusNoContent)
	}

	return false, nil
}
