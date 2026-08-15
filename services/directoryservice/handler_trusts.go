package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateTrust(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID      string `json:"DirectoryId"`
		RemoteDomainName string `json:"RemoteDomainName"`
		TrustPassword    string `json:"TrustPassword"`
		TrustDirection   string `json:"TrustDirection"`
		TrustType        string `json:"TrustType"`
		SelectiveAuth    string `json:"SelectiveAuth"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.RemoteDomainName == "" || req.TrustPassword == "" || req.TrustDirection == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException",
				"DirectoryId, RemoteDomainName, TrustPassword and TrustDirection are required"),
		)
	}

	if !validEnum(req.TrustDirection,
		string(TrustDirectionOneWayOutgoing), string(TrustDirectionOneWayIncoming), string(TrustDirectionTwoWay)) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid TrustDirection"))
	}
	if !validEnum(req.TrustType, string(TrustTypeForest), string(TrustTypeExternal)) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid TrustType"))
	}
	if !validEnum(req.SelectiveAuth, string(SelectiveAuthEnabled), string(SelectiveAuthDisabled)) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid SelectiveAuth"))
	}

	trustType := req.TrustType
	if trustType == "" {
		trustType = string(TrustTypeForest)
	}

	trustID, createErr := h.Backend.CreateTrust(
		h.contextWithRegion(c),
		req.DirectoryID, req.RemoteDomainName, req.TrustPassword, req.TrustDirection, trustType, req.SelectiveAuth,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: req.DirectoryID,
		"TrustId":      trustID, //nolint:goconst // existing issue.
	})
}

func (h *Handler) handleDeleteTrust(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		TrustID string `json:"TrustId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.TrustID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "TrustId is required"))
	}

	trustID, delErr := h.Backend.DeleteTrust(h.contextWithRegion(c), req.TrustID)
	if delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"TrustId": trustID})
}

func (h *Handler) handleDescribeTrusts(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		NextToken   string   `json:"NextToken"`
		TrustIDs    []string `json:"TrustIds"`
		Limit       int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	trusts, nextToken, descErr := h.Backend.DescribeTrusts(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.TrustIDs,
		req.Limit,
		req.NextToken,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	trustList := make([]map[string]any, 0, len(trusts))
	for _, t := range trusts {
		entry := map[string]any{
			keyDirectoryID:             t.DirectoryID,
			"TrustId":                  t.TrustID,
			"RemoteDomainName":         t.RemoteDomainName,
			"TrustDirection":           t.TrustDirection,
			"TrustType":                t.TrustType,
			"TrustState":               t.TrustState,
			"SelectiveAuth":            t.SelectiveAuth,
			keyCreatedDateTime:         awstime.Epoch(t.CreatedDateTime),
			keyLastUpdatedDateTime:     awstime.Epoch(t.LastUpdatedDateTime),
			"StateLastUpdatedDateTime": awstime.Epoch(t.StateLastUpdatedTime),
		}
		if t.TrustStateReason != "" {
			entry["TrustStateReason"] = t.TrustStateReason
		}
		trustList = append(trustList, entry)
	}

	resp := map[string]any{"Trusts": trustList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateTrust(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		TrustID       string `json:"TrustId"`
		SelectiveAuth string `json:"SelectiveAuth"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.TrustID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "TrustId is required"))
	}
	if !validEnum(req.SelectiveAuth, string(SelectiveAuthEnabled), string(SelectiveAuthDisabled)) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid SelectiveAuth"))
	}

	trustID, updateErr := h.Backend.UpdateTrust(h.contextWithRegion(c), req.TrustID, req.SelectiveAuth)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TrustId":    trustID,
		keyRequestID: uuid.NewString(),
	})
}

func (h *Handler) handleVerifyTrust(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		TrustID string `json:"TrustId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.TrustID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "TrustId is required"))
	}

	trustID, verifyErr := h.Backend.VerifyTrust(h.contextWithRegion(c), req.TrustID)
	if verifyErr != nil {
		return h.mapError(c, verifyErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"TrustId": trustID})
}
