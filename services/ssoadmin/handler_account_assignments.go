package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type assignmentView struct {
	AccountID        string `json:"AccountId"`
	PermissionSetArn string `json:"PermissionSetArn"`
	PrincipalID      string `json:"PrincipalId"`
	PrincipalType    string `json:"PrincipalType"`
}

func (h *Handler) handleCreateAccountAssignment(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		TargetID         string `json:"TargetId"`
		TargetType       string `json:"TargetType"`
		PrincipalID      string `json:"PrincipalId"`
		PrincipalType    string `json:"PrincipalType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.PermissionSetArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "PermissionSetArn is required")
	}
	if req.TargetID == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "TargetId is required")
	}
	if req.TargetType != "" && req.TargetType != targetTypeAWSAccount {
		return writeError(c, http.StatusBadRequest, "ValidationException", "TargetType must be AWS_ACCOUNT")
	}
	if req.PrincipalID == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "PrincipalId is required")
	}
	if req.PrincipalType == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "PrincipalType is required")
	}
	if req.PrincipalType != principalTypeUser && req.PrincipalType != principalTypeGroup {
		return writeError(c, http.StatusBadRequest, "ValidationException", "PrincipalType must be USER or GROUP")
	}

	requestID, err := h.Backend.CreateAccountAssignment(
		req.InstanceArn,
		req.PermissionSetArn,
		req.TargetID,
		req.PrincipalID,
		req.PrincipalType,
	)
	if err != nil {
		return handleBackendError(c, err, "failed to create account assignment")
	}

	status, _ := h.Backend.DescribeAccountAssignmentCreationStatus(req.InstanceArn, requestID)

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentCreationStatus": toProvisioningView(status),
	})
}

func (h *Handler) handleDescribeAccountAssignmentCreationStatus(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                        string `json:"InstanceArn"`
		AccountAssignmentCreationRequestID string `json:"AccountAssignmentCreationRequestId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	status, err := h.Backend.DescribeAccountAssignmentCreationStatus(
		req.InstanceArn,
		req.AccountAssignmentCreationRequestID,
	)
	if err != nil {
		return handleBackendError(c, err, "request not found: "+req.AccountAssignmentCreationRequestID)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentCreationStatus": toProvisioningView(status),
	})
}

func (h *Handler) handleDeleteAccountAssignment(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		TargetID         string `json:"TargetId"`
		TargetType       string `json:"TargetType"`
		PrincipalID      string `json:"PrincipalId"`
		PrincipalType    string `json:"PrincipalType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.PrincipalType != "" && req.PrincipalType != principalTypeUser && req.PrincipalType != principalTypeGroup {
		return writeError(c, http.StatusBadRequest, "ValidationException", "PrincipalType must be USER or GROUP")
	}
	if req.TargetType != "" && req.TargetType != targetTypeAWSAccount {
		return writeError(c, http.StatusBadRequest, "ValidationException", "TargetType must be AWS_ACCOUNT")
	}

	requestID, err := h.Backend.DeleteAccountAssignment(
		req.InstanceArn,
		req.PermissionSetArn,
		req.TargetID,
		req.PrincipalID,
		req.PrincipalType,
	)
	if err != nil {
		return handleBackendError(c, err, "failed to delete account assignment")
	}

	status, _ := h.Backend.DescribeAccountAssignmentDeletionStatus(req.InstanceArn, requestID)

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentDeletionStatus": toProvisioningView(status),
	})
}

func (h *Handler) handleDescribeAccountAssignmentDeletionStatus(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                        string `json:"InstanceArn"`
		AccountAssignmentDeletionRequestID string `json:"AccountAssignmentDeletionRequestId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	status, err := h.Backend.DescribeAccountAssignmentDeletionStatus(
		req.InstanceArn,
		req.AccountAssignmentDeletionRequestID,
	)
	if err != nil {
		return handleBackendError(c, err, "request not found: "+req.AccountAssignmentDeletionRequestID)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentDeletionStatus": toProvisioningView(status),
	})
}

func (h *Handler) handleListAccountAssignments(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		AccountID        string `json:"AccountId"`
		NextToken        string `json:"NextToken"`
		MaxResults       int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	list := h.Backend.ListAccountAssignments(req.InstanceArn, req.PermissionSetArn, req.AccountID)

	views := make([]assignmentView, 0, len(list))
	for _, a := range list {
		views = append(views, assignmentView{
			AccountID:        a.AccountID,
			PermissionSetArn: a.PermissionSetArn,
			PrincipalID:      a.PrincipalID,
			PrincipalType:    a.PrincipalType,
		})
	}

	page, next := paginateBy(views, req.MaxResults, req.NextToken, func(v assignmentView) string {
		return v.AccountID + "|" + v.PermissionSetArn + "|" + v.PrincipalType + "|" + v.PrincipalID
	})

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignments": page,
		keyNextToken:         next,
	})
}

func (h *Handler) handleListAccountAssignmentCreationStatus(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		Filter      struct {
			Status string `json:"Status"`
		} `json:"Filter"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	statuses := h.Backend.ListAccountAssignmentCreationStatus(req.InstanceArn, req.Filter.Status)

	out := make([]provisioningStatusView, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, toProvisioningView(status))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentsCreationStatus": out,
		keyNextToken:                       nil,
	})
}

func (h *Handler) handleListAccountAssignmentDeletionStatus(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		Filter      struct {
			Status string `json:"Status"`
		} `json:"Filter"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	statuses := h.Backend.ListAccountAssignmentDeletionStatus(req.InstanceArn, req.Filter.Status)

	out := make([]provisioningStatusView, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, toProvisioningView(status))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentsDeletionStatus": out,
		keyNextToken:                       nil,
	})
}

func (h *Handler) handleListAccountAssignmentsForPrincipal(c *echo.Context, body []byte) error {
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

	assignments := h.Backend.ListAccountAssignmentsForPrincipal(req.InstanceArn, req.PrincipalID, req.PrincipalType)

	views := make([]assignmentView, 0, len(assignments))
	for _, a := range assignments {
		views = append(views, assignmentView{
			AccountID:        a.AccountID,
			PermissionSetArn: a.PermissionSetArn,
			PrincipalID:      a.PrincipalID,
			PrincipalType:    a.PrincipalType,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignments": views,
		keyNextToken:         nil,
	})
}
