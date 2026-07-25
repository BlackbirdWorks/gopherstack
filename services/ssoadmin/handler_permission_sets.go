package ssoadmin

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

type permissionSetView struct {
	PermissionSetArn string  `json:"PermissionSetArn"`
	Name             string  `json:"Name"`
	Description      string  `json:"Description,omitempty"`
	SessionDuration  string  `json:"SessionDuration,omitempty"`
	RelayState       string  `json:"RelayState,omitempty"`
	CreatedDate      float64 `json:"CreatedDate,omitempty"`
}

func (h *Handler) handleCreatePermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn     string    `json:"InstanceArn"`
		Name            string    `json:"Name"`
		Description     string    `json:"Description"`
		SessionDuration string    `json:"SessionDuration"`
		RelayState      string    `json:"RelayState"`
		Tags            []tagView `json:"Tags"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.Name == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "Name is required")
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	ps, err := h.Backend.CreatePermissionSet(
		req.InstanceArn,
		req.Name,
		req.Description,
		req.SessionDuration,
		req.RelayState,
		tags,
	)
	if err != nil {
		if errors.Is(err, ErrPermissionSetAlreadyExists) {
			return writeError(c, http.StatusBadRequest, "ConflictException", "permission set already exists: "+req.Name)
		}

		return handleBackendError(c, err, "failed to create permission set: "+req.Name)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSet": permissionSetView{
			PermissionSetArn: ps.PermissionSetArn,
			Name:             ps.Name,
			Description:      ps.Description,
			SessionDuration:  ps.SessionDuration,
			RelayState:       ps.RelayState,
			CreatedDate:      float64(ps.CreatedDate.Unix()),
		},
	})
}

func (h *Handler) handleDescribePermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	ps, err := h.Backend.DescribePermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSet": permissionSetView{
			PermissionSetArn: ps.PermissionSetArn,
			Name:             ps.Name,
			Description:      ps.Description,
			SessionDuration:  ps.SessionDuration,
			RelayState:       ps.RelayState,
			CreatedDate:      float64(ps.CreatedDate.Unix()),
		},
	})
}

func (h *Handler) handleListPermissionSets(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		NextToken   string `json:"NextToken"`
		MaxResults  int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	list := h.Backend.ListPermissionSets(req.InstanceArn)
	sort.Slice(list, func(i, j int) bool {
		return list[i].PermissionSetArn < list[j].PermissionSetArn
	})

	arns := make([]string, 0, len(list))
	for _, ps := range list {
		arns = append(arns, ps.PermissionSetArn)
	}

	page, next := paginateStrings(arns, req.MaxResults, req.NextToken)

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSets": page,
		keyNextToken:     next,
	})
}

func (h *Handler) handleDeletePermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeletePermissionSet(req.InstanceArn, req.PermissionSetArn); err != nil {
		if errors.Is(err, ErrPermissionSetHasAssignments) {
			return writeError(c, http.StatusBadRequest, "ConflictException",
				"permission set is still associated with one or more accounts: "+req.PermissionSetArn)
		}

		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdatePermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		Description      string `json:"Description"`
		SessionDuration  string `json:"SessionDuration"`
		RelayState       string `json:"RelayState"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.UpdatePermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.Description,
		req.SessionDuration,
		req.RelayState,
	); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleProvisionPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		TargetType       string `json:"TargetType"`
		TargetID         string `json:"TargetId"`
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
	if req.TargetType == "" {
		req.TargetType = targetTypeAllProvisionedAccounts
	}
	if req.TargetType != targetTypeAWSAccount && req.TargetType != targetTypeAllProvisionedAccounts {
		return writeError(c, http.StatusBadRequest, "ValidationException",
			"TargetType must be AWS_ACCOUNT or ALL_PROVISIONED_ACCOUNTS")
	}
	if req.TargetType == targetTypeAWSAccount && req.TargetID == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException",
			"TargetId is required when TargetType is AWS_ACCOUNT")
	}

	requestID, err := h.Backend.ProvisionPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.TargetType,
		req.TargetID,
	)
	if err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	status, _ := h.Backend.DescribePermissionSetProvisioningStatus(req.InstanceArn, requestID)

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSetProvisioningStatus": toPermissionSetProvisioningStatusView(status),
	})
}

func (h *Handler) handleDescribePermissionSetProvisioningStatus(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                     string `json:"InstanceArn"`
		ProvisionPermissionSetRequestID string `json:"ProvisionPermissionSetRequestId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	status, err := h.Backend.DescribePermissionSetProvisioningStatus(
		req.InstanceArn,
		req.ProvisionPermissionSetRequestID,
	)
	if err != nil {
		return handleBackendError(c, err, "request not found: "+req.ProvisionPermissionSetRequestID)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSetProvisioningStatus": toPermissionSetProvisioningStatusView(status),
	})
}

func (h *Handler) handleListPermissionSetProvisioningStatus(c *echo.Context, body []byte) error {
	return listProvisioningStatusMetadata(
		c, body,
		h.Backend.ListPermissionSetProvisioningStatus,
		toPermissionSetProvisioningStatusMetadataView,
		func(v permissionSetProvisioningStatusMetadataView) string { return v.RequestID },
		"PermissionSetsProvisioningStatus",
	)
}

func (h *Handler) handleListPermissionSetsProvisionedToAccount(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		AccountID   string `json:"AccountId"`
		NextToken   string `json:"NextToken"`
		MaxResults  int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.AccountID == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "AccountId is required")
	}

	arns := h.Backend.ListPermissionSetsProvisionedToAccount(req.InstanceArn, req.AccountID)

	page, next := paginateStrings(arns, req.MaxResults, req.NextToken)

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSets": page,
		keyNextToken:     next,
	})
}

func (h *Handler) handleListAccountsForProvisionedPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		NextToken        string `json:"NextToken"`
		MaxResults       int    `json:"MaxResults"`
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

	accounts, err := h.Backend.ListAccountsForProvisionedPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	page, next := paginateStrings(accounts, req.MaxResults, req.NextToken)

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountIds": page,
		keyNextToken: next,
	})
}
