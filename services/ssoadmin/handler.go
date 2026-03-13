package ssoadmin

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	targetPrefix    = "SWBExternalService."
	ssoAdminService = "sso"
)

// Handler is the Echo HTTP handler for the SSO Admin service.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new SSO Admin handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the handler name.
func (h *Handler) Name() string { return "SsoAdmin" }

// GetSupportedOperations returns all supported SSO Admin operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"ListInstances",
		"CreateInstance",
		"DescribeInstance",
		"DeleteInstance",
		"CreatePermissionSet",
		"DescribePermissionSet",
		"ListPermissionSets",
		"DeletePermissionSet",
		"UpdatePermissionSet",
		"CreateAccountAssignment",
		"DescribeAccountAssignmentCreationStatus",
		"DeleteAccountAssignment",
		"DescribeAccountAssignmentDeletionStatus",
		"ListAccountAssignments",
		"AttachManagedPolicyToPermissionSet",
		"DetachManagedPolicyFromPermissionSet",
		"ListManagedPoliciesInPermissionSet",
		"PutInlinePolicyToPermissionSet",
		"GetInlinePolicyForPermissionSet",
		"DeleteInlinePolicyFromPermissionSet",
		"ProvisionPermissionSet",
		"DescribePermissionSetProvisioningStatus",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the service name for chaos injection.
func (h *Handler) ChaosServiceName() string { return ssoAdminService }

// ChaosOperations returns the operations subject to chaos injection.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions for chaos injection.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a matcher that identifies SSO Admin requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the matching priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
}

// ExtractResource returns the instance ARN from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.InstanceArn
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		op := h.ExtractOperation(c)
		log := logger.Load(c.Request().Context())

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return writeError(c, http.StatusBadRequest, "ResourceNotFoundException", "failed to read request body")
		}

		log.Debug("ssoadmin request", "operation", op)

		return h.dispatch(c, op, body)
	}
}

func (h *Handler) dispatch(c *echo.Context, op string, body []byte) error {
	switch op {
	case "ListInstances":
		return h.handleListInstances(c, body)
	case "CreateInstance":
		return h.handleCreateInstance(c, body)
	case "DescribeInstance":
		return h.handleDescribeInstance(c, body)
	case "DeleteInstance":
		return h.handleDeleteInstance(c, body)
	case "CreatePermissionSet":
		return h.handleCreatePermissionSet(c, body)
	case "DescribePermissionSet":
		return h.handleDescribePermissionSet(c, body)
	case "ListPermissionSets":
		return h.handleListPermissionSets(c, body)
	case "DeletePermissionSet":
		return h.handleDeletePermissionSet(c, body)
	case "UpdatePermissionSet":
		return h.handleUpdatePermissionSet(c, body)
	default:
		return h.dispatchAssignmentAndPolicy(c, op, body)
	}
}

//nolint:cyclop // intentional large switch for assignment and policy operations
func (h *Handler) dispatchAssignmentAndPolicy(c *echo.Context, op string, body []byte) error {
	switch op {
	case "CreateAccountAssignment":
		return h.handleCreateAccountAssignment(c, body)
	case "DescribeAccountAssignmentCreationStatus":
		return h.handleDescribeAccountAssignmentCreationStatus(c, body)
	case "DeleteAccountAssignment":
		return h.handleDeleteAccountAssignment(c, body)
	case "DescribeAccountAssignmentDeletionStatus":
		return h.handleDescribeAccountAssignmentDeletionStatus(c, body)
	case "ListAccountAssignments":
		return h.handleListAccountAssignments(c, body)
	case "AttachManagedPolicyToPermissionSet":
		return h.handleAttachManagedPolicyToPermissionSet(c, body)
	case "DetachManagedPolicyFromPermissionSet":
		return h.handleDetachManagedPolicyFromPermissionSet(c, body)
	case "ListManagedPoliciesInPermissionSet":
		return h.handleListManagedPoliciesInPermissionSet(c, body)
	case "PutInlinePolicyToPermissionSet":
		return h.handlePutInlinePolicyToPermissionSet(c, body)
	case "GetInlinePolicyForPermissionSet":
		return h.handleGetInlinePolicyForPermissionSet(c, body)
	case "DeleteInlinePolicyFromPermissionSet":
		return h.handleDeleteInlinePolicyFromPermissionSet(c, body)
	case "ProvisionPermissionSet":
		return h.handleProvisionPermissionSet(c, body)
	case "DescribePermissionSetProvisioningStatus":
		return h.handleDescribePermissionSetProvisioningStatus(c, body)
	case "TagResource":
		return h.handleTagResource(c, body)
	case "UntagResource":
		return h.handleUntagResource(c, body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, body)
	default:
		return writeError(c, http.StatusBadRequest, "UnknownOperationException", "unknown operation: "+op)
	}
}

// --- request/response types ---

type instanceView struct {
	InstanceArn     string `json:"InstanceArn"`
	OwnerAccountID  string `json:"OwnerAccountId"`
	IdentityStoreID string `json:"IdentityStoreId"`
	Name            string `json:"Name"`
	Status          string `json:"Status"`
}

type permissionSetView struct {
	PermissionSetArn string `json:"PermissionSetArn"`
	Name             string `json:"Name"`
	Description      string `json:"Description,omitempty"`
	SessionDuration  string `json:"SessionDuration,omitempty"`
	RelayState       string `json:"RelayState,omitempty"`
	CreatedDate      string `json:"CreatedDate,omitempty"`
}

type provisioningStatusView struct {
	RequestID   string `json:"RequestId"`
	Status      string `json:"Status"`
	CreatedDate string `json:"CreatedDate,omitempty"`
}

type assignmentView struct {
	AccountID        string `json:"AccountId"`
	PermissionSetArn string `json:"PermissionSetArn"`
	PrincipalID      string `json:"PrincipalId"`
	PrincipalType    string `json:"PrincipalType"`
}

type managedPolicyView struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

type tagView struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// --- handlers ---

func (h *Handler) handleListInstances(c *echo.Context, _ []byte) error {
	instances := h.Backend.ListInstances()
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].InstanceArn < instances[j].InstanceArn
	})

	views := make([]instanceView, 0, len(instances))
	for _, inst := range instances {
		views = append(views, instanceView{
			InstanceArn:     inst.InstanceArn,
			OwnerAccountID:  inst.OwnerAccountID,
			IdentityStoreID: inst.IdentityStoreID,
			Name:            inst.Name,
			Status:          inst.Status,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Instances": views,
		"NextToken": nil,
	})
}

func (h *Handler) handleCreateInstance(c *echo.Context, body []byte) error {
	var req struct {
		Name            string `json:"Name"`
		OwnerAccountID  string `json:"OwnerAccountId"`
		IdentityStoreID string `json:"IdentityStoreId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	inst, err := h.Backend.CreateInstance(req.Name, req.OwnerAccountID, req.IdentityStoreID)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"InstanceArn": inst.InstanceArn,
	})
}

func (h *Handler) handleDescribeInstance(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}

	inst, err := h.Backend.DescribeInstance(req.InstanceArn)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"InstanceArn":     inst.InstanceArn,
		"OwnerAccountId":  inst.OwnerAccountID,
		"IdentityStoreId": inst.IdentityStoreID,
		"Name":            inst.Name,
		"Status":          inst.Status,
	})
}

func (h *Handler) handleDeleteInstance(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}

	if err := h.Backend.DeleteInstance(req.InstanceArn); err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
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
			return writeError(c, http.StatusConflict, "ConflictException", "permission set already exists: "+req.Name)
		}

		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSet": permissionSetView{
			PermissionSetArn: ps.PermissionSetArn,
			Name:             ps.Name,
			Description:      ps.Description,
			SessionDuration:  ps.SessionDuration,
			RelayState:       ps.RelayState,
			CreatedDate:      ps.CreatedDate.Format("2006-01-02T15:04:05Z"),
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
		return writeError(c, http.StatusBadRequest, err.Error(), "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSet": permissionSetView{
			PermissionSetArn: ps.PermissionSetArn,
			Name:             ps.Name,
			Description:      ps.Description,
			SessionDuration:  ps.SessionDuration,
			RelayState:       ps.RelayState,
			CreatedDate:      ps.CreatedDate.Format("2006-01-02T15:04:05Z"),
		},
	})
}

func (h *Handler) handleListPermissionSets(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
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

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSets": arns,
		"NextToken":      nil,
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
		return writeError(c, http.StatusBadRequest, err.Error(), "permission set not found: "+req.PermissionSetArn)
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
		return writeError(c, http.StatusBadRequest, err.Error(), "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

//nolint:dupl // create and delete assignment handlers have similar structure but distinct semantics
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

	requestID, err := h.Backend.CreateAccountAssignment(
		req.InstanceArn,
		req.PermissionSetArn,
		req.TargetID,
		req.PrincipalID,
		req.PrincipalType,
	)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	status, _ := h.Backend.DescribeAccountAssignmentCreationStatus(req.InstanceArn, requestID)

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentCreationStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: status.CreatedDate.Format("2006-01-02T15:04:05Z"),
		},
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
		return writeError(
			c,
			http.StatusBadRequest,
			err.Error(),
			"request not found: "+req.AccountAssignmentCreationRequestID,
		)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentCreationStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: status.CreatedDate.Format("2006-01-02T15:04:05Z"),
		},
	})
}

//nolint:dupl // create and delete assignment handlers have similar structure but distinct semantics
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

	requestID, err := h.Backend.DeleteAccountAssignment(
		req.InstanceArn,
		req.PermissionSetArn,
		req.TargetID,
		req.PrincipalID,
		req.PrincipalType,
	)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	status, _ := h.Backend.DescribeAccountAssignmentDeletionStatus(req.InstanceArn, requestID)

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentDeletionStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: status.CreatedDate.Format("2006-01-02T15:04:05Z"),
		},
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
		return writeError(
			c,
			http.StatusBadRequest,
			err.Error(),
			"request not found: "+req.AccountAssignmentDeletionRequestID,
		)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentDeletionStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: status.CreatedDate.Format("2006-01-02T15:04:05Z"),
		},
	})
}

func (h *Handler) handleListAccountAssignments(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		AccountID        string `json:"AccountId"`
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

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignments": views,
		"NextToken":          nil,
	})
}

func (h *Handler) handleAttachManagedPolicyToPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		ManagedPolicyArn string `json:"ManagedPolicyArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	name := req.ManagedPolicyArn
	parts := strings.Split(req.ManagedPolicyArn, "/")
	if len(parts) > 0 {
		name = parts[len(parts)-1]
	}

	if err := h.Backend.AttachManagedPolicyToPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.ManagedPolicyArn,
		name,
	); err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDetachManagedPolicyFromPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		ManagedPolicyArn string `json:"ManagedPolicyArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DetachManagedPolicyFromPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.ManagedPolicyArn,
	); err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListManagedPoliciesInPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	policies, err := h.Backend.ListManagedPoliciesInPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	views := make([]managedPolicyView, 0, len(policies))
	for _, mp := range policies {
		views = append(views, managedPolicyView(mp))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AttachedManagedPolicies": views,
		"NextToken":               nil,
	})
}

func (h *Handler) handlePutInlinePolicyToPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		InlinePolicy     string `json:"InlinePolicy"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.PutInlinePolicyToPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.InlinePolicy,
	); err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetInlinePolicyForPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	policy, err := h.Backend.GetInlinePolicyForPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"InlinePolicy": policy,
	})
}

func (h *Handler) handleDeleteInlinePolicyFromPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteInlinePolicyFromPermissionSet(req.InstanceArn, req.PermissionSetArn); err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
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

	requestID, err := h.Backend.ProvisionPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	status, _ := h.Backend.DescribePermissionSetProvisioningStatus(req.InstanceArn, requestID)

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSetProvisioningStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: status.CreatedDate.Format("2006-01-02T15:04:05Z"),
		},
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
		return writeError(
			c,
			http.StatusBadRequest,
			err.Error(),
			"request not found: "+req.ProvisionPermissionSetRequestID,
		)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSetProvisioningStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: status.CreatedDate.Format("2006-01-02T15:04:05Z"),
		},
	})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string    `json:"InstanceArn"`
		ResourceArn string    `json:"ResourceArn"`
		Tags        []tagView `json:"Tags"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(req.InstanceArn, req.ResourceArn, tags); err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string   `json:"InstanceArn"`
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.UntagResource(req.InstanceArn, req.ResourceArn, req.TagKeys); err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	tags, err := h.Backend.ListTagsForResource(req.InstanceArn, req.ResourceArn)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error(), err.Error())
	}

	tagList := make([]tagView, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, tagView{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return writeJSON(c, http.StatusOK, map[string]any{
		"Tags":      tagList,
		"NextToken": nil,
	})
}

// --- helpers ---

func writeJSON(c *echo.Context, status int, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")
	c.Response().WriteHeader(status)
	_, _ = c.Response().Write(data)

	return nil
}

func writeError(c *echo.Context, status int, errType, message string) error {
	return writeJSON(c, status, map[string]string{
		"__type":  errType,
		"message": message,
	})
}
