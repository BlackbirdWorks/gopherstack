package redshift

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// slTableRestoreStatusWire is the wire shape for ServerlessTableRestoreStatus
// responses. RequestTime is an epoch-seconds number on the wire (see
// ServerlessTableRestoreStatus's doc comment in serverless.go), so this
// wrapper does the conversion at the response boundary, same pattern as
// slScheduledActionWire.
type slTableRestoreStatusWire struct {
	RequestTime           *float64 `json:"requestTime,omitempty"`
	Message               string   `json:"message,omitempty"`
	NamespaceName         string   `json:"namespaceName,omitempty"`
	NewTableName          string   `json:"newTableName,omitempty"`
	RecoveryPointID       string   `json:"recoveryPointId,omitempty"`
	SnapshotName          string   `json:"snapshotName,omitempty"`
	SourceDatabaseName    string   `json:"sourceDatabaseName,omitempty"`
	SourceSchemaName      string   `json:"sourceSchemaName,omitempty"`
	SourceTableName       string   `json:"sourceTableName,omitempty"`
	Status                string   `json:"status,omitempty"`
	TableRestoreRequestID string   `json:"tableRestoreRequestId"`
	TargetDatabaseName    string   `json:"targetDatabaseName,omitempty"`
	TargetSchemaName      string   `json:"targetSchemaName,omitempty"`
	WorkgroupName         string   `json:"workgroupName,omitempty"`
	ProgressInMegaBytes   int64    `json:"progressInMegaBytes,omitempty"`
	TotalDataInMegaBytes  int64    `json:"totalDataInMegaBytes,omitempty"`
}

func toTableRestoreStatusWire(tr *ServerlessTableRestoreStatus) *slTableRestoreStatusWire {
	return &slTableRestoreStatusWire{
		Message:               tr.Message,
		NamespaceName:         tr.NamespaceName,
		NewTableName:          tr.NewTableName,
		RecoveryPointID:       tr.RecoveryPointID,
		SnapshotName:          tr.SnapshotName,
		SourceDatabaseName:    tr.SourceDatabaseName,
		SourceSchemaName:      tr.SourceSchemaName,
		SourceTableName:       tr.SourceTableName,
		Status:                tr.Status,
		TableRestoreRequestID: tr.TableRestoreRequestID,
		TargetDatabaseName:    tr.TargetDatabaseName,
		TargetSchemaName:      tr.TargetSchemaName,
		WorkgroupName:         tr.WorkgroupName,
		ProgressInMegaBytes:   tr.ProgressInMegaBytes,
		TotalDataInMegaBytes:  tr.TotalDataInMegaBytes,
		RequestTime:           slEpochPtr(tr.RequestTime),
	}
}

type slTableRestoreReq struct {
	ActivateCaseSensitiveIdentifier *bool  `json:"activateCaseSensitiveIdentifier"`
	NamespaceName                   string `json:"namespaceName"`
	NewTableName                    string `json:"newTableName"`
	SnapshotName                    string `json:"snapshotName"`
	RecoveryPointID                 string `json:"recoveryPointId"`
	SourceDatabaseName              string `json:"sourceDatabaseName"`
	SourceSchemaName                string `json:"sourceSchemaName"`
	SourceTableName                 string `json:"sourceTableName"`
	TargetDatabaseName              string `json:"targetDatabaseName"`
	TargetSchemaName                string `json:"targetSchemaName"`
	WorkgroupName                   string `json:"workgroupName"`
}

func (r slTableRestoreReq) toParams() RestoreTableFromSnapshotParams {
	return RestoreTableFromSnapshotParams(r)
}

func (h *ServerlessHandler) handleRestoreTableFromSnapshot(c *echo.Context, body []byte) error {
	var req slTableRestoreReq

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.NamespaceName == "" || req.NewTableName == "" || req.SnapshotName == "" ||
		req.SourceDatabaseName == "" || req.SourceTableName == "" || req.WorkgroupName == "" {
		return slBadRequest(
			c,
			"namespaceName, newTableName, snapshotName, sourceDatabaseName, sourceTableName and workgroupName are required",
		)
	}

	tr, err := h.Backend.RestoreTableFromSnapshotSL(req.toParams())
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespTableRestoreStatus: toTableRestoreStatusWire(tr)})
}

func (h *ServerlessHandler) handleRestoreTableFromRecoveryPoint(c *echo.Context, body []byte) error {
	var req slTableRestoreReq

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.NamespaceName == "" || req.NewTableName == "" || req.RecoveryPointID == "" ||
		req.SourceDatabaseName == "" || req.SourceTableName == "" || req.WorkgroupName == "" {
		return slBadRequest(
			c,
			"namespaceName, newTableName, recoveryPointId, sourceDatabaseName, sourceTableName and workgroupName are required",
		)
	}

	tr, err := h.Backend.RestoreTableFromRecoveryPointSL(req.toParams())
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespTableRestoreStatus: toTableRestoreStatusWire(tr)})
}

func (h *ServerlessHandler) handleGetTableRestoreStatus(c *echo.Context, body []byte) error {
	var req struct {
		TableRestoreRequestID string `json:"tableRestoreRequestId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.TableRestoreRequestID == "" {
		return slBadRequest(c, "tableRestoreRequestId is required")
	}

	tr, err := h.Backend.GetTableRestoreStatusSL(req.TableRestoreRequestID)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespTableRestoreStatus: toTableRestoreStatusWire(tr)})
}

func (h *ServerlessHandler) handleListTableRestoreStatus(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName string `json:"namespaceName"`
		WorkgroupName string `json:"workgroupName"`
		NextToken     string `json:"nextToken"`
		MaxResults    int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListTableRestoreStatusSL(
		req.NamespaceName,
		req.WorkgroupName,
		req.MaxResults,
		req.NextToken,
	)

	wire := make([]*slTableRestoreStatusWire, 0, len(list))
	for _, tr := range list {
		wire = append(wire, toTableRestoreStatusWire(tr))
	}

	resp := map[string]any{"tableRestoreStatuses": wire}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}
