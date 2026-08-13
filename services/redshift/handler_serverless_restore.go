package redshift

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *ServerlessHandler) handleRestoreFromSnapshot(c *echo.Context, body []byte) error {
	var req struct {
		MaintainIntegration         *bool  `json:"maintainIntegration"`
		NamespaceName               string `json:"namespaceName"`
		WorkgroupName               string `json:"workgroupName"`
		SnapshotName                string `json:"snapshotName"`
		SnapshotArn                 string `json:"snapshotArn"`
		OwnerAccount                string `json:"ownerAccount"`
		AdminPasswordSecretKmsKeyID string `json:"adminPasswordSecretKmsKeyId"`
		ManageAdminPassword         bool   `json:"manageAdminPassword"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.NamespaceName == "" || req.WorkgroupName == "" {
		return slBadRequest(c, "namespaceName and workgroupName are required")
	}

	if req.SnapshotName == "" && req.SnapshotArn == "" {
		return slBadRequest(c, "snapshotName or snapshotArn is required")
	}

	ns, snapshotName, err := h.Backend.RestoreFromSnapshotSL(RestoreFromSnapshotParams{
		NamespaceName:               req.NamespaceName,
		WorkgroupName:               req.WorkgroupName,
		SnapshotName:                req.SnapshotName,
		SnapshotArn:                 req.SnapshotArn,
		AdminPasswordSecretKmsKeyID: req.AdminPasswordSecretKmsKeyID,
		ManageAdminPassword:         req.ManageAdminPassword,
		MaintainIntegration:         req.MaintainIntegration,
	})
	if err != nil {
		return slHandleErr(c, err)
	}

	resp := map[string]any{
		slRespNamespace: ns,
		"snapshotName":  snapshotName,
	}

	if req.OwnerAccount != "" {
		resp["ownerAccount"] = req.OwnerAccount
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleConvertRecoveryPointToSnapshot(c *echo.Context, body []byte) error {
	var req struct {
		RecoveryPointID string      `json:"recoveryPointId"`
		SnapshotName    string      `json:"snapshotName"`
		Tags            []slTagWire `json:"tags"`
		RetentionPeriod int         `json:"retentionPeriod"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.RecoveryPointID == "" || req.SnapshotName == "" {
		return slBadRequest(c, "recoveryPointId and snapshotName are required")
	}

	snap, err := h.Backend.ConvertRecoveryPointToSnapshotSL(
		req.RecoveryPointID, req.SnapshotName, req.RetentionPeriod, slTagsFromWire(req.Tags),
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshot: snap})
}
