package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateApplicationAssignment(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		PrincipalID    string `json:"PrincipalId"`
		PrincipalType  string `json:"PrincipalType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.CreateApplicationAssignment(
		req.ApplicationArn,
		req.PrincipalID,
		req.PrincipalType,
	); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteApplicationAssignment(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		PrincipalID    string `json:"PrincipalId"`
		PrincipalType  string `json:"PrincipalType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteApplicationAssignment(
		req.ApplicationArn,
		req.PrincipalID,
		req.PrincipalType,
	); err != nil {
		return handleBackendError(c, err, "assignment not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeApplicationAssignment(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		PrincipalID    string `json:"PrincipalId"`
		PrincipalType  string `json:"PrincipalType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	assignment, err := h.Backend.DescribeApplicationAssignment(req.ApplicationArn, req.PrincipalID, req.PrincipalType)
	if err != nil {
		return handleBackendError(c, err, "application assignment not found")
	}

	// Real DescribeApplicationAssignmentOutput is flat (ApplicationArn,
	// PrincipalId, PrincipalType) -- no nested "ApplicationAssignment" wrapper
	// (gopherstack previously invented one here); see
	// awsAwsjson11_deserializeOpDocumentDescribeApplicationAssignmentOutput in
	// the real SDK's deserializers.go.
	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplicationArn: assignment.ApplicationArn,
		"PrincipalId":     assignment.PrincipalID,
		"PrincipalType":   assignment.PrincipalType,
	})
}

func (h *Handler) handleListApplicationAssignments(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		NextToken      string `json:"NextToken"`
		MaxResults     int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	assignments, err := h.Backend.ListApplicationAssignments(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	out := make([]map[string]any, 0, len(assignments))
	for _, assignment := range assignments {
		out = append(out, map[string]any{
			keyApplicationArn: assignment.ApplicationArn,
			"PrincipalId":     assignment.PrincipalID,
			"PrincipalType":   assignment.PrincipalType,
		})
	}

	page, next := paginateBy(out, req.MaxResults, req.NextToken, func(v map[string]any) string {
		principalType, _ := v["PrincipalType"].(string)
		principalID, _ := v["PrincipalId"].(string)

		return principalType + "|" + principalID
	})

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationAssignments": page,
		keyNextToken:             next,
	})
}

func (h *Handler) handlePutApplicationAssignmentConfiguration(c *echo.Context, body []byte) error {
	// Real PutApplicationAssignmentConfigurationInput is exactly
	// {ApplicationArn, AssignmentRequired} -- gopherstack previously also
	// accepted an invented "AssignmentRequiredForAllIdentities" field that
	// doesn't exist anywhere in the real SDK.
	var req struct {
		ApplicationArn     string `json:"ApplicationArn"`
		AssignmentRequired bool   `json:"AssignmentRequired"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.PutApplicationAssignmentConfiguration(req.ApplicationArn, req.AssignmentRequired); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handlePutApplicationSessionConfiguration(c *echo.Context, body []byte) error {
	// Real PutApplicationSessionConfigurationInput is
	// {ApplicationArn, UserBackgroundSessionApplicationStatus} -- there is no
	// "SessionDuration" member on this operation at all (gopherstack
	// previously invented one, confusing it with the unrelated
	// PermissionSet.SessionDuration concept).
	var req struct {
		ApplicationArn                         string `json:"ApplicationArn"`
		UserBackgroundSessionApplicationStatus string `json:"UserBackgroundSessionApplicationStatus"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.PutApplicationSessionConfiguration(
		req.ApplicationArn, req.UserBackgroundSessionApplicationStatus,
	); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetApplicationAssignmentConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}
	required, err := h.Backend.GetApplicationAssignmentConfiguration(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AssignmentRequired": required,
	})
}

func (h *Handler) handleGetApplicationSessionConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}
	status, err := h.Backend.GetApplicationSessionConfiguration(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	// Real GetApplicationSessionConfigurationOutput is flat
	// ({UserBackgroundSessionApplicationStatus}) -- no nested
	// "ApplicationSessionConfiguration" wrapper and no "SessionDuration"
	// member (gopherstack previously invented both).
	return writeJSON(c, http.StatusOK, map[string]any{
		"UserBackgroundSessionApplicationStatus": status,
	})
}

func (h *Handler) handleListApplicationAssignmentsForPrincipal(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn   string `json:"InstanceArn"`
		PrincipalID   string `json:"PrincipalId"`
		PrincipalType string `json:"PrincipalType"`
		Filter        struct {
			ApplicationArn string `json:"ApplicationArn"`
		} `json:"Filter"`
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.PrincipalID == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "PrincipalId is required")
	}
	if req.PrincipalType == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "PrincipalType is required")
	}

	assignments := h.Backend.ListApplicationAssignmentsForPrincipal(req.InstanceArn, req.PrincipalID, req.PrincipalType)

	type appAssignmentView struct {
		ApplicationArn string `json:"ApplicationArn"`
		PrincipalID    string `json:"PrincipalId"`
		PrincipalType  string `json:"PrincipalType"`
	}
	views := make([]appAssignmentView, 0, len(assignments))
	for _, a := range assignments {
		if req.Filter.ApplicationArn != "" && a.ApplicationArn != req.Filter.ApplicationArn {
			continue
		}

		views = append(views, appAssignmentView{
			ApplicationArn: a.ApplicationArn,
			PrincipalID:    a.PrincipalID,
			PrincipalType:  a.PrincipalType,
		})
	}

	page, next := paginateBy(views, req.MaxResults, req.NextToken, func(v appAssignmentView) string {
		return v.ApplicationArn + "|" + v.PrincipalType + "|" + v.PrincipalID
	})

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationAssignments": page,
		keyNextToken:             next,
	})
}
