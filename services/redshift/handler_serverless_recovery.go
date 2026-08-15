package redshift

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *ServerlessHandler) handleGetRecoveryPoint(c *echo.Context, body []byte) error {
	var req struct {
		RecoveryPointID string `json:"recoveryPointId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.RecoveryPointID == "" {
		return slBadRequest(c, "recoveryPointId is required")
	}

	rp, err := h.Backend.GetRecoveryPointSL(req.RecoveryPointID)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespRecoveryPoint: rp})
}

func (h *ServerlessHandler) handleListRecoveryPoints(c *echo.Context, body []byte) error {
	var req struct {
		StartTime     *float64 `json:"startTime"`
		EndTime       *float64 `json:"endTime"`
		NamespaceArn  string   `json:"namespaceArn"`
		NamespaceName string   `json:"namespaceName"`
		NextToken     string   `json:"nextToken"`
		MaxResults    int      `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListRecoveryPointsSL(ListRecoveryPointsParams{
		NamespaceName: req.NamespaceName,
		NamespaceArn:  req.NamespaceArn,
		StartTime:     slEpochFromPtr(req.StartTime),
		EndTime:       slEpochFromPtr(req.EndTime),
		MaxResults:    req.MaxResults,
		NextToken:     req.NextToken,
	})
	resp := map[string]any{"recoveryPoints": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleRestoreFromRecoveryPoint(c *echo.Context, body []byte) error {
	var req struct {
		MaintainIntegration *bool  `json:"maintainIntegration"`
		NamespaceName       string `json:"namespaceName"`
		RecoveryPointID     string `json:"recoveryPointId"`
		WorkgroupName       string `json:"workgroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.NamespaceName == "" || req.RecoveryPointID == "" || req.WorkgroupName == "" {
		return slBadRequest(c, "namespaceName, recoveryPointId and workgroupName are required")
	}

	ns, err := h.Backend.RestoreFromRecoveryPointSL(req.NamespaceName, req.WorkgroupName, req.RecoveryPointID)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		slRespNamespace:   ns,
		"recoveryPointId": req.RecoveryPointID,
	})
}
