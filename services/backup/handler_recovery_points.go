package backup

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// splitVaultRP splits a "vaultName|recoveryPointArn" resource string.
// Returns ("", "", false) if the resource is not in the expected format.
func splitVaultRP(resource string) (string, string, bool) {
	parts := strings.SplitN(resource, "|", splitTwo)
	if len(parts) != splitTwo || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}

	return parts[0], parts[1], true
}

func (h *Handler) handleListRecoveryPointsByBackupVault(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "BackupVaultName is required"),
		)
	}

	q := c.Request().URL.Query()
	f := ListRPFilter{
		ResourceArn:            q.Get("resourceArn"),
		ResourceType:           q.Get("resourceType"),
		ParentRecoveryPointArn: q.Get("parentRecoveryPointArn"),
		CreatedAfter:           ParseTimeFilter(q.Get("createdAfter")),
		CreatedBefore:          ParseTimeFilter(q.Get("createdBefore")),
		NextToken:              q.Get("nextToken"),
		MaxResults:             parseInt(q.Get("maxResults")),
	}

	pts, nextToken, err := h.Backend.ListRecoveryPointsFiltered(vaultName, f)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(pts))
	for _, rp := range pts {
		item := map[string]any{
			keyRecoveryPointArn: rp.RecoveryPointArn,
			keyBackupVaultName:  rp.BackupVaultName,
			keyBackupVaultArn:   rp.BackupVaultArn,
			keyStatus:           rp.Status,
			keyCreationDate:     epochSeconds(rp.CreationDate),
		}
		setOptionalStr(item, "ResourceArn", rp.ResourceArn)
		setOptionalStr(item, "ResourceType", rp.ResourceType)
		setOptionalStr(item, "IamRoleArn", rp.IAMRoleArn)
		setOptionalStr(item, "StorageClass", rp.StorageClass)
		setOptionalStr(item, "ParentRecoveryPointArn", rp.ParentRecoveryPointArn)
		if rp.BackupSizeInBytes > 0 {
			item["BackupSizeInBytes"] = rp.BackupSizeInBytes
		}
		if rp.IsEncrypted {
			item["IsEncrypted"] = rp.IsEncrypted
		}
		if rp.CompletionDate != nil {
			item["CompletionDate"] = epochSeconds(*rp.CompletionDate)
		}
		if rp.Lifecycle != nil {
			item["Lifecycle"] = lifecycleToJSON(rp.Lifecycle)
		}
		if rp.CalculatedLifecycle != nil {
			item["CalculatedLifecycle"] = calculatedLifecycleToJSON(rp.CalculatedLifecycle)
		}
		items = append(items, item)
	}

	resp := map[string]any{keyRecoveryPoints: items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeRecoveryPoint(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	rp, err := h.Backend.DescribeRecoveryPoint(vaultName, rpArn)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyRecoveryPointArn: rp.RecoveryPointArn,
		keyBackupVaultName:  rp.BackupVaultName,
		keyBackupVaultArn:   rp.BackupVaultArn,
		keyStatus:           rp.Status,
		keyCreationDate:     epochSeconds(rp.CreationDate),
	}
	if rp.ResourceArn != "" {
		resp["ResourceArn"] = rp.ResourceArn
	}
	if rp.ResourceType != "" {
		resp["ResourceType"] = rp.ResourceType
	}
	if rp.IAMRoleArn != "" {
		resp["IamRoleArn"] = rp.IAMRoleArn
	}
	if rp.StorageClass != "" {
		resp["StorageClass"] = rp.StorageClass
	}
	if rp.EncryptionKeyArn != "" {
		resp["EncryptionKeyArn"] = rp.EncryptionKeyArn
	}
	if rp.IsEncrypted {
		resp["IsEncrypted"] = rp.IsEncrypted
	}
	if rp.SourceBackupVaultArn != "" {
		resp["SourceBackupVaultArn"] = rp.SourceBackupVaultArn
	}
	if rp.ParentRecoveryPointArn != "" {
		resp["ParentRecoveryPointArn"] = rp.ParentRecoveryPointArn
	}
	if rp.CompositeMemberIdentifier != "" {
		resp["CompositeMemberIdentifier"] = rp.CompositeMemberIdentifier
	}
	if rp.Lifecycle != nil {
		resp["Lifecycle"] = lifecycleToJSON(rp.Lifecycle)
	}
	if rp.CalculatedLifecycle != nil {
		resp["CalculatedLifecycle"] = calculatedLifecycleToJSON(rp.CalculatedLifecycle)
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetRecoveryPointRestoreMetadata(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	metadata, err := h.Backend.GetRecoveryPointRestoreMetadata(vaultName, rpArn)
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
		keyRecoveryPointArn: rpArn,
		"RestoreMetadata":   metadata,
	})
}

func (h *Handler) handleDeleteRecoveryPoint(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	if err := h.Backend.DeleteRecoveryPoint(vaultName, rpArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDisassociateRecoveryPoint(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	if err := h.Backend.DisassociateRecoveryPoint(vaultName, rpArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDisassociateRecoveryPointFromParent(
	c *echo.Context,
	resource string,
) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterValueException", "invalid resource path"),
		)
	}

	if err := h.Backend.DisassociateRecoveryPointFromParent(vaultName, rpArn); err != nil {
		return h.handleError(c, err)
	}

	// Real AWS: responseCode 204.
	return c.NoContent(http.StatusNoContent)
}

// --- Vault compliance handlers ---

// dispatchRecoveryPointQueryOps handles recovery-point lookups keyed by
// legal hold, protected resource, or index status.
func (h *Handler) dispatchRecoveryPointQueryOps(c *echo.Context, route backupRoute) (bool, error) {
	switch route.operation {
	case opListRecoveryPointsByLegalHold:
		rps := h.Backend.ListRecoveryPointsByLegalHold(route.resource)
		items := make([]map[string]any, 0, len(rps))
		for _, rp := range rps {
			// Real AWS wire shape is RecoveryPointMember: BackupVaultName,
			// RecoveryPointArn, ResourceArn, ResourceType (no Status field).
			items = append(items, map[string]any{
				keyBackupVaultName:  rp.BackupVaultName,
				keyRecoveryPointArn: rp.RecoveryPointArn,
				keyResourceArn:      rp.ResourceArn,
				keyResourceType:     rp.ResourceType,
			})
		}

		return true, c.JSON(http.StatusOK, map[string]any{keyRecoveryPoints: items})
	case opListRecoveryPointsByResource:
		rps := h.Backend.ListRecoveryPointsByResource(route.resource)
		items := make([]map[string]any, 0, len(rps))
		for _, rp := range rps {
			items = append(
				items,
				map[string]any{keyRecoveryPointArn: rp.RecoveryPointArn, keyStatus: rp.Status},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{keyRecoveryPoints: items})
	case opListIndexedRecoveryPoints:
		rps := h.Backend.ListIndexedRecoveryPoints()
		items := make([]map[string]any, 0, len(rps))
		for _, rp := range rps {
			items = append(
				items,
				map[string]any{keyRecoveryPointArn: rp.RecoveryPointArn, keyStatus: rp.Status},
			)
		}

		return true, c.JSON(http.StatusOK, map[string]any{"IndexedRecoveryPoints": items})
	}

	return false, nil
}

// dispatchRecoveryPointIndexOps handles the recovery-point index and
// lifecycle sub-resource operations.
func (h *Handler) dispatchRecoveryPointIndexOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opGetRecoveryPointIndexDetails:

		return true, h.handleGetRecoveryPointIndexDetails(c, route.resource)
	case opUpdateRecoveryPointIndexSettings:

		return true, h.handleUpdateRecoveryPointIndexSettings(c, route.resource, body)
	case opUpdateRecoveryPointLifecycle:

		return true, h.handleUpdateRecoveryPointLifecycle(c, route.resource, body)
	}

	return false, nil
}

func (h *Handler) handleGetRecoveryPointIndexDetails(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid resource path"))
	}

	status, err := h.Backend.GetRecoveryPointIndexDetails(vaultName, rpArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRecoveryPointArn: rpArn,
		"IndexStatus":       status,
	})
}

func (h *Handler) handleUpdateRecoveryPointIndexSettings(
	c *echo.Context, resource string, body []byte,
) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid resource path"))
	}

	var reqBody struct {
		Index string `json:"Index"`
	}
	_ = json.Unmarshal(body, &reqBody)

	if err := h.Backend.UpdateRecoveryPointIndexSettings(vaultName, rpArn, reqBody.Index); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRecoveryPointArn: rpArn,
		"Index":             reqBody.Index,
	})
}

func (h *Handler) handleUpdateRecoveryPointLifecycle(
	c *echo.Context, resource string, body []byte,
) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid resource path"))
	}

	var reqBody struct {
		Lifecycle struct {
			MoveToColdStorageAfterDays int64 `json:"MoveToColdStorageAfterDays"`
			DeleteAfterDays            int64 `json:"DeleteAfterDays"`
		} `json:"Lifecycle"`
	}
	_ = json.Unmarshal(body, &reqBody)

	if err := h.Backend.UpdateRecoveryPointLifecycle(vaultName, rpArn,
		reqBody.Lifecycle.MoveToColdStorageAfterDays, reqBody.Lifecycle.DeleteAfterDays); err != nil {
		return h.handleError(c, err)
	}

	v, err := h.Backend.DescribeBackupVault(vaultName)
	if err != nil {
		return h.handleError(c, err)
	}

	rp, err := h.Backend.DescribeRecoveryPoint(vaultName, rpArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:     v.BackupVaultArn,
		"Lifecycle":           lifecycleToJSON(rp.Lifecycle),
		"CalculatedLifecycle": calculatedLifecycleToJSON(rp.CalculatedLifecycle),
	})
}
