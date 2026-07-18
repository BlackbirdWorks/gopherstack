package ssoadmin

import (
	"encoding/json"
	"net/http"
	"sort"

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

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationAssignment": map[string]any{
			keyApplicationArn: assignment.ApplicationArn,
			"PrincipalId":     assignment.PrincipalID,
			"PrincipalType":   assignment.PrincipalType,
		},
	})
}

func (h *Handler) handleListApplicationAssignments(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	assignments, err := h.Backend.ListApplicationAssignments(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	sort.Slice(assignments, func(i, j int) bool {
		if assignments[i].PrincipalType != assignments[j].PrincipalType {
			return assignments[i].PrincipalType < assignments[j].PrincipalType
		}

		return assignments[i].PrincipalID < assignments[j].PrincipalID
	})

	out := make([]map[string]any, 0, len(assignments))
	for _, assignment := range assignments {
		out = append(out, map[string]any{
			keyApplicationArn: assignment.ApplicationArn,
			"PrincipalId":     assignment.PrincipalID,
			"PrincipalType":   assignment.PrincipalType,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationAssignments": out,
		keyNextToken:             nil,
	})
}

func (h *Handler) handlePutApplicationAssignmentConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn                     string `json:"ApplicationArn"`
		AssignmentRequired                 bool   `json:"AssignmentRequired"`
		AssignmentRequiredForAllIdentities bool   `json:"AssignmentRequiredForAllIdentities"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	required := req.AssignmentRequired || req.AssignmentRequiredForAllIdentities
	if err := h.Backend.PutApplicationAssignmentConfiguration(req.ApplicationArn, required); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handlePutApplicationSessionConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn  string `json:"ApplicationArn"`
		SessionDuration string `json:"SessionDuration"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.PutApplicationSessionConfiguration(req.ApplicationArn, req.SessionDuration); err != nil {
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
	dur, err := h.Backend.GetApplicationSessionConfiguration(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationSessionConfiguration": map[string]any{
			"SessionDuration": dur,
		},
	})
}

func (h *Handler) handleListApplicationAssignmentsForPrincipal(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn   string `json:"InstanceArn"`
		PrincipalID   string `json:"PrincipalId"`
		PrincipalType string `json:"PrincipalType"`
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
		views = append(views, appAssignmentView{
			ApplicationArn: a.ApplicationArn,
			PrincipalID:    a.PrincipalID,
			PrincipalType:  a.PrincipalType,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationAssignments": views,
		keyNextToken:             nil,
	})
}
