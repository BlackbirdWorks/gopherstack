package ssoadmin

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyNextToken              = "NextToken"
	keyInstanceArn            = "InstanceArn"
	keyName                   = "Name"
	keyStatus                 = "Status"
	keyTags                   = "Tags"
	keyApplicationArn         = "ApplicationArn"
	keyApplication            = "Application"
	keyApplicationProviderArn = "ApplicationProviderArn"
)

const (
	targetPrefix    = "SWBExternalService."
	ssoAdminService = "sso"
)

// Handler is the Echo HTTP handler for the SSO Admin service.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new SSO Admin handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the handler name.
func (h *Handler) Name() string { return "SsoAdmin" }

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// GetSupportedOperations returns all supported SSO Admin operations (sorted).
// When adding a new operation, register it in this slice AND in the dispatch function below.
func (h *Handler) GetSupportedOperations() []string {
	ops := []string{
		"AddRegion",
		"AttachCustomerManagedPolicyReferenceToPermissionSet",
		"AttachManagedPolicyToPermissionSet",
		"CreateAccountAssignment",
		"CreateApplication",
		"CreateApplicationAssignment",
		"CreateInstance",
		"CreateInstanceAccessControlAttributeConfiguration",
		"CreatePermissionSet",
		"CreateTrustedTokenIssuer",
		"DeleteAccountAssignment",
		"DeleteApplication",
		"DeleteApplicationAccessScope",
		"DeleteApplicationAssignment",
		"DeleteApplicationAuthenticationMethod",
		"DeleteApplicationGrant",
		"DeleteInlinePolicyFromPermissionSet",
		"DeleteInstance",
		"DeleteInstanceAccessControlAttributeConfiguration",
		"DeletePermissionSet",
		"DeletePermissionsBoundaryFromPermissionSet",
		"DeleteTrustedTokenIssuer",
		"DescribeApplication",
		"DescribeAccountAssignmentCreationStatus",
		"DescribeRegion",
		"DescribeAccountAssignmentDeletionStatus",
		"DescribeApplicationAssignment",
		"DescribeApplicationProvider",
		"DescribeInstance",
		"DescribeInstanceAccessControlAttributeConfiguration",
		"DescribePermissionSet",
		"DescribePermissionSetProvisioningStatus",
		"DescribeTrustedTokenIssuer",
		"DetachCustomerManagedPolicyReferenceFromPermissionSet",
		"DetachManagedPolicyFromPermissionSet",
		"GetApplicationAssignmentConfiguration",
		"GetApplicationAccessScope",
		"GetApplicationAuthenticationMethod",
		"GetApplicationGrant",
		"GetApplicationSessionConfiguration",
		"GetPermissionsBoundaryForPermissionSet",
		"GetInlinePolicyForPermissionSet",
		"ListAccountAssignmentCreationStatus",
		"ListAccountAssignmentDeletionStatus",
		"ListAccountAssignments",
		"ListAccountAssignmentsForPrincipal",
		"ListAccountsForProvisionedPermissionSet",
		"ListApplicationAccessScopes",
		"ListApplicationAssignments",
		"ListApplicationAssignmentsForPrincipal",
		"ListApplicationAuthenticationMethods",
		"ListApplicationGrants",
		"ListApplicationProviders",
		"ListApplications",
		"ListCustomerManagedPolicyReferencesInPermissionSet",
		"ListInstances",
		"ListManagedPoliciesInPermissionSet",
		"ListPermissionSetProvisioningStatus",
		"ListPermissionSets",
		"ListPermissionSetsProvisionedToAccount",
		"ListRegions",
		"ListTagsForResource",
		"ListTrustedTokenIssuers",
		"ProvisionPermissionSet",
		"PutApplicationAccessScope",
		"PutApplicationAssignmentConfiguration",
		"PutApplicationAuthenticationMethod",
		"PutApplicationGrant",
		"PutApplicationSessionConfiguration",
		"PutInlinePolicyToPermissionSet",
		"PutPermissionsBoundaryToPermissionSet",
		"RemoveRegion",
		"TagResource",
		"UntagResource",
		"UpdateApplication",
		"UpdateInstance",
		"UpdateInstanceAccessControlAttributeConfiguration",
		"UpdatePermissionSet",
		"UpdateTrustedTokenIssuer",
	}
	sort.Strings(ops)

	return ops
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
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		target := c.Request().Header.Get("X-Amz-Target")
		op := strings.TrimPrefix(target, targetPrefix)
		if op == "" || op == target {
			return writeError(c, http.StatusBadRequest,
				"UnrecognizedClientException", "missing or invalid X-Amz-Target header")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "ssoadmin: failed to read request body", "error", err)

			return writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "ssoadmin request", "operation", op)

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
		return h.dispatchNewOps(c, op, body)
	}
}

func (h *Handler) dispatchNewOps(c *echo.Context, op string, body []byte) error {
	switch op {
	case "AddRegion":
		return h.handleAddRegion(c, body)
	case "AttachCustomerManagedPolicyReferenceToPermissionSet":
		return h.handleAttachCustomerManagedPolicyReferenceToPermissionSet(c, body)
	case "CreateApplication":
		return h.handleCreateApplication(c, body)
	case "CreateApplicationAssignment":
		return h.handleCreateApplicationAssignment(c, body)
	case "CreateInstanceAccessControlAttributeConfiguration":
		return h.handleCreateInstanceAccessControlAttributeConfiguration(c, body)
	case "CreateTrustedTokenIssuer":
		return h.handleCreateTrustedTokenIssuer(c, body)
	case "DeleteApplication":
		return h.handleDeleteApplication(c, body)
	case "DeleteApplicationAccessScope":
		return h.handleDeleteApplicationAccessScope(c, body)
	case "DeleteApplicationAssignment":
		return h.handleDeleteApplicationAssignment(c, body)
	case "DeleteApplicationAuthenticationMethod":
		return h.handleDeleteApplicationAuthenticationMethod(c, body)
	case "DetachCustomerManagedPolicyReferenceFromPermissionSet":
		return h.handleDetachCustomerManagedPolicyReferenceFromPermissionSet(c, body)
	default:
		return h.dispatchNewestOps(c, op, body)
	}
}

func (h *Handler) dispatchNewestOps(c *echo.Context, op string, body []byte) error {
	switch op {
	case "DeleteApplicationGrant":
		return h.handleDeleteApplicationGrant(c, body)
	case "DeleteInstanceAccessControlAttributeConfiguration":
		return h.handleDeleteInstanceAccessControlAttributeConfiguration(c, body)
	case "DeleteTrustedTokenIssuer":
		return h.handleDeleteTrustedTokenIssuer(c, body)
	case "DescribeApplication":
		return h.handleDescribeApplication(c, body)
	case "DescribeApplicationAssignment":
		return h.handleDescribeApplicationAssignment(c, body)
	case "DescribeApplicationProvider":
		return h.handleDescribeApplicationProvider(c, body)
	case "DescribeInstanceAccessControlAttributeConfiguration":
		return h.handleDescribeInstanceAccessControlAttributeConfiguration(c, body)
	case "DescribeTrustedTokenIssuer":
		return h.handleDescribeTrustedTokenIssuer(c, body)
	case "GetPermissionsBoundaryForPermissionSet":
		return h.handleGetPermissionsBoundaryForPermissionSet(c, body)
	case "ListAccountAssignmentCreationStatus":
		return h.handleListAccountAssignmentCreationStatus(c, body)
	case "ListAccountAssignmentDeletionStatus":
		return h.handleListAccountAssignmentDeletionStatus(c, body)
	case "ListAccountAssignmentsForPrincipal":
		return h.handleListAccountAssignmentsForPrincipal(c, body)
	case "ListPermissionSetsProvisionedToAccount":
		return h.handleListPermissionSetsProvisionedToAccount(c, body)
	default:
		return h.dispatchNewestAppOps(c, op, body)
	}
}

func (h *Handler) dispatchNewestAppOps(c *echo.Context, op string, body []byte) error {
	switch op {
	case "ListApplicationAccessScopes":
		return h.handleListApplicationAccessScopes(c, body)
	case "ListApplicationAssignments":
		return h.handleListApplicationAssignments(c, body)
	case "ListApplicationAuthenticationMethods":
		return h.handleListApplicationAuthenticationMethods(c, body)
	case "ListApplicationGrants":
		return h.handleListApplicationGrants(c, body)
	case "ListApplicationProviders":
		return h.handleListApplicationProviders(c, body)
	case "ListApplications":
		return h.handleListApplications(c, body)
	default:
		return h.dispatchNewestConfigOps(c, op, body)
	}
}

func (h *Handler) dispatchNewestConfigOps(c *echo.Context, op string, body []byte) error {
	switch op {
	case "DeletePermissionsBoundaryFromPermissionSet":
		return h.handleDeletePermissionsBoundaryFromPermissionSet(c, body)
	case "GetApplicationAssignmentConfiguration":
		return h.handleGetApplicationAssignmentConfiguration(c, body)
	case "GetApplicationSessionConfiguration":
		return h.handleGetApplicationSessionConfiguration(c, body)
	case "ListCustomerManagedPolicyReferencesInPermissionSet":
		return h.handleListCustomerManagedPolicyReferencesInPermissionSet(c, body)
	case "ListPermissionSetProvisioningStatus":
		return h.handleListPermissionSetProvisioningStatus(c, body)
	case "ListRegions":
		return h.handleListRegions(c, body)
	case "ListTrustedTokenIssuers":
		return h.handleListTrustedTokenIssuers(c, body)
	case "PutApplicationAccessScope":
		return h.handlePutApplicationAccessScope(c, body)
	case "PutApplicationAssignmentConfiguration":
		return h.handlePutApplicationAssignmentConfiguration(c, body)
	default:
		return h.dispatchUpdateOps(c, op, body)
	}
}

func (h *Handler) dispatchUpdateOps(c *echo.Context, op string, body []byte) error {
	switch op {
	case "PutApplicationAuthenticationMethod":
		return h.handlePutApplicationAuthenticationMethod(c, body)
	case "PutApplicationGrant":
		return h.handlePutApplicationGrant(c, body)
	case "PutApplicationSessionConfiguration":
		return h.handlePutApplicationSessionConfiguration(c, body)
	case "PutPermissionsBoundaryToPermissionSet":
		return h.handlePutPermissionsBoundaryToPermissionSet(c, body)
	case "RemoveRegion":
		return h.handleRemoveRegion(c, body)
	case "UpdateApplication":
		return h.handleUpdateApplication(c, body)
	case "UpdateInstance":
		return h.handleUpdateInstance(c, body)
	case "UpdateInstanceAccessControlAttributeConfiguration":
		return h.handleUpdateInstanceAccessControlAttributeConfiguration(c, body)
	case "UpdateTrustedTokenIssuer":
		return h.handleUpdateTrustedTokenIssuer(c, body)
	default:
		return h.dispatchRefinement3Ops(c, op, body)
	}
}

func (h *Handler) dispatchRefinement3Ops(c *echo.Context, op string, body []byte) error {
	switch op {
	case "GetApplicationAccessScope":
		return h.handleGetApplicationAccessScope(c, body)
	case "GetApplicationAuthenticationMethod":
		return h.handleGetApplicationAuthenticationMethod(c, body)
	case "GetApplicationGrant":
		return h.handleGetApplicationGrant(c, body)
	case "ListAccountsForProvisionedPermissionSet":
		return h.handleListAccountsForProvisionedPermissionSet(c, body)
	case "ListApplicationAssignmentsForPrincipal":
		return h.handleListApplicationAssignmentsForPrincipal(c, body)
	case "DescribeRegion":
		return h.handleDescribeRegion(c, body)
	default:
		return writeError(c, http.StatusBadRequest, "UnknownOperationException", "unknown operation: "+op)
	}
}

// --- request/response types ---

type instanceView struct {
	InstanceArn     string  `json:"InstanceArn"`
	OwnerAccountID  string  `json:"OwnerAccountId"`
	IdentityStoreID string  `json:"IdentityStoreId"`
	Name            string  `json:"Name"`
	Status          string  `json:"Status"`
	CreatedDate     float64 `json:"CreatedDate,omitempty"`
}

type permissionSetView struct {
	PermissionSetArn string  `json:"PermissionSetArn"`
	Name             string  `json:"Name"`
	Description      string  `json:"Description,omitempty"`
	SessionDuration  string  `json:"SessionDuration,omitempty"`
	RelayState       string  `json:"RelayState,omitempty"`
	CreatedDate      float64 `json:"CreatedDate,omitempty"`
}

type provisioningStatusView struct {
	RequestID   string  `json:"RequestId"`
	Status      string  `json:"Status"`
	CreatedDate float64 `json:"CreatedDate,omitempty"`
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
			CreatedDate:     float64(inst.CreatedDate.Unix()),
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Instances":  views,
		keyNextToken: nil,
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
		return handleBackendError(c, err, "failed to create instance")
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyInstanceArn: inst.InstanceArn,
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
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	tagList := make([]tagView, 0, len(inst.Tags))
	for k, v := range inst.Tags {
		tagList = append(tagList, tagView{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return writeJSON(c, http.StatusOK, map[string]any{
		keyInstanceArn:    inst.InstanceArn,
		"OwnerAccountId":  inst.OwnerAccountID,
		"IdentityStoreId": inst.IdentityStoreID,
		keyName:           inst.Name,
		keyStatus:         inst.Status,
		"CreatedDate":     float64(inst.CreatedDate.Unix()),
		keyTags:           tagList,
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
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
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
		keyNextToken:     nil,
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
			return writeError(c, http.StatusConflict, "ConflictException",
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
		"AccountAssignmentCreationStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
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
		return handleBackendError(c, err, "request not found: "+req.AccountAssignmentCreationRequestID)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentCreationStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
		},
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
		"AccountAssignmentDeletionStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
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
		return handleBackendError(c, err, "request not found: "+req.AccountAssignmentDeletionRequestID)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentDeletionStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
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
		keyNextToken:         nil,
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
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
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
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
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
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	views := make([]managedPolicyView, 0, len(policies))
	for _, mp := range policies {
		views = append(views, managedPolicyView(mp))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AttachedManagedPolicies": views,
		keyNextToken:              nil,
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
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
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
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
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

	requestID, err := h.Backend.ProvisionPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	status, _ := h.Backend.DescribePermissionSetProvisioningStatus(req.InstanceArn, requestID)

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSetProvisioningStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
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
		return handleBackendError(c, err, "request not found: "+req.ProvisionPermissionSetRequestID)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSetProvisioningStatus": provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
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
		if errors.Is(err, ErrTooManyTagsException) {
			return writeError(c, http.StatusBadRequest, "TooManyTagsException",
				"you have exceeded the maximum number of tags (50) for this resource")
		}

		return handleBackendError(c, err, "resource not found: "+req.ResourceArn)
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
		return handleBackendError(c, err, "resource not found: "+req.ResourceArn)
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
		return handleBackendError(c, err, "resource not found: "+req.ResourceArn)
	}

	tagList := make([]tagView, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, tagView{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTags:      tagList,
		keyNextToken: nil,
	})
}

// --- helpers ---

// handleBackendError maps a backend error to the appropriate HTTP response.
func handleBackendError(c *echo.Context, err error, notFoundMsg string) error {
	if errors.Is(err, awserr.ErrInvalidParameter) {
		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	switch err.Error() {
	case "ResourceNotFoundException":
		return writeError(c, http.StatusNotFound, "ResourceNotFoundException", notFoundMsg)
	case "ConflictException":
		return writeError(c, http.StatusConflict, "ConflictException", notFoundMsg)
	default:
		return writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}
}

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

// --- new operation view types ---

type applicationView struct {
	ApplicationArn         string  `json:"ApplicationArn"`
	ApplicationProviderArn string  `json:"ApplicationProviderArn"`
	Name                   string  `json:"Name"`
	Description            string  `json:"Description,omitempty"`
	InstanceArn            string  `json:"InstanceArn"`
	Status                 string  `json:"Status"`
	CreatedDate            float64 `json:"CreatedDate,omitempty"`
}

type trustedTokenIssuerView struct {
	TrustedTokenIssuerArn  string `json:"TrustedTokenIssuerArn"`
	Name                   string `json:"Name"`
	InstanceArn            string `json:"InstanceArn"`
	TrustedTokenIssuerType string `json:"TrustedTokenIssuerType,omitempty"`
}

type customerManagedPolicyReferenceView struct {
	Name string `json:"Name"`
	Path string `json:"Path,omitempty"`
}

// --- new operation handlers ---

func (h *Handler) handleAddRegion(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		RegionName  string `json:"RegionName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.AddRegion(req.InstanceArn, req.RegionName); err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleAttachCustomerManagedPolicyReferenceToPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                    string                             `json:"InstanceArn"`
		PermissionSetArn               string                             `json:"PermissionSetArn"`
		CustomerManagedPolicyReference customerManagedPolicyReferenceView `json:"CustomerManagedPolicyReference"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.AttachCustomerManagedPolicyReferenceToPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.CustomerManagedPolicyReference.Name,
		req.CustomerManagedPolicyReference.Path,
	); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateApplication(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn            string    `json:"InstanceArn"`
		ApplicationProviderArn string    `json:"ApplicationProviderArn"`
		Name                   string    `json:"Name"`
		Description            string    `json:"Description"`
		Tags                   []tagView `json:"Tags"`
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

	app, err := h.Backend.CreateApplication(
		req.InstanceArn,
		req.ApplicationProviderArn,
		req.Name,
		req.Description,
		tags,
	)
	if err != nil {
		if errors.Is(err, ErrApplicationAlreadyExists) {
			return writeError(c, http.StatusConflict, "ConflictException", "application already exists: "+req.Name)
		}

		return handleBackendError(c, err, "failed to create application: "+req.Name)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplicationArn: app.ApplicationArn,
		keyApplication: applicationView{
			ApplicationArn:         app.ApplicationArn,
			ApplicationProviderArn: app.ApplicationProviderArn,
			Name:                   app.Name,
			Description:            app.Description,
			InstanceArn:            app.InstanceArn,
			Status:                 app.Status,
			CreatedDate:            float64(app.CreatedDate.Unix()),
		},
	})
}

func (h *Handler) handleDeleteApplication(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}

	if err := h.Backend.DeleteApplication(req.ApplicationArn); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

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

func (h *Handler) handleDeleteApplicationAccessScope(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		Scope          string `json:"Scope"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteApplicationAccessScope(req.ApplicationArn, req.Scope); err != nil {
		return handleBackendError(c, err, "scope not found: "+req.Scope)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteApplicationAuthenticationMethod(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		AuthMethodType string `json:"AuthenticationMethodType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteApplicationAuthenticationMethod(req.ApplicationArn, req.AuthMethodType); err != nil {
		return handleBackendError(c, err, "authentication method not found: "+req.AuthMethodType)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateInstanceAccessControlAttributeConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                                 string `json:"InstanceArn"`
		InstanceAccessControlAttributeConfiguration struct {
			AccessControlAttributes []struct {
				Key   string `json:"Key"`
				Value struct {
					Source []string `json:"Source"`
				} `json:"Value"`
			} `json:"AccessControlAttributes"`
		} `json:"InstanceAccessControlAttributeConfiguration"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	attrs := make(
		[]AccessControlAttribute,
		0,
		len(req.InstanceAccessControlAttributeConfiguration.AccessControlAttributes),
	)
	for _, a := range req.InstanceAccessControlAttributeConfiguration.AccessControlAttributes {
		attrs = append(attrs, AccessControlAttribute{
			Key:   a.Key,
			Value: AccessControlAttributeValue{Source: a.Value.Source},
		})
	}

	if err := h.Backend.CreateInstanceAccessControlAttributeConfiguration(req.InstanceArn, attrs); err != nil {
		if errors.Is(err, ErrACAAlreadyExists) {
			return writeError(c, http.StatusConflict, "ConflictException",
				"instance access control attribute configuration already exists for: "+req.InstanceArn)
		}

		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerConfiguration *struct {
			OidcJwtConfiguration *struct {
				IssuerURL                  string `json:"IssuerUrl"`
				ClaimAttributePath         string `json:"ClaimAttributePath"`
				IdentityStoreAttributePath string `json:"IdentityStoreAttributePath"`
				JwksRetrievalOption        string `json:"JwksRetrievalOption"`
			} `json:"OidcJwtConfiguration,omitempty"`
		} `json:"TrustedTokenIssuerConfiguration,omitempty"`
		InstanceArn            string    `json:"InstanceArn"`
		Name                   string    `json:"Name"`
		TrustedTokenIssuerType string    `json:"TrustedTokenIssuerType"`
		Tags                   []tagView `json:"Tags"`
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
	if req.TrustedTokenIssuerType == "" {
		req.TrustedTokenIssuerType = ttiDefaultType
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	var cfg *TrustedTokenIssuerConfiguration
	if req.TrustedTokenIssuerConfiguration != nil {
		cfg = &TrustedTokenIssuerConfiguration{}
		if req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidcSrc := req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfg.OidcJwtConfiguration = &OidcJwtConfiguration{
				IssuerURL:                  oidcSrc.IssuerURL,
				ClaimAttributePath:         oidcSrc.ClaimAttributePath,
				IdentityStoreAttributePath: oidcSrc.IdentityStoreAttributePath,
				JwksRetrievalOption:        oidcSrc.JwksRetrievalOption,
			}
		}
	}

	ti, err := h.Backend.CreateTrustedTokenIssuer(req.InstanceArn, req.Name, req.TrustedTokenIssuerType, tags, cfg)
	if err != nil {
		if errors.Is(err, ErrTrustedTokenIssuerAlreadyExists) {
			return writeError(
				c,
				http.StatusConflict,
				"ConflictException",
				"trusted token issuer already exists: "+req.Name,
			)
		}

		return handleBackendError(c, err, "failed to create trusted token issuer: "+req.Name)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuerArn": ti.TrustedTokenIssuerArn,
	})
}

func (h *Handler) handleDeleteApplicationGrant(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		GrantType      string `json:"GrantType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.DeleteApplicationGrant(req.ApplicationArn, req.GrantType); err != nil {
		return handleBackendError(c, err, "application grant not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeApplication(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	app, err := h.Backend.DescribeApplication(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	tagList := make([]tagView, 0, len(app.Tags))
	for k, v := range app.Tags {
		tagList = append(tagList, tagView{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplication: map[string]any{
			keyApplicationArn:         app.ApplicationArn,
			keyApplicationProviderArn: app.ApplicationProviderArn,
			keyName:                   app.Name,
			"Description":             app.Description,
			keyInstanceArn:            app.InstanceArn,
			keyStatus:                 app.Status,
			"CreatedDate":             float64(app.CreatedDate.Unix()),
			keyTags:                   tagList,
		},
	})
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

func (h *Handler) handleDescribeApplicationProvider(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationProviderArn string `json:"ApplicationProviderArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	provider, err := h.Backend.DescribeApplicationProvider(req.ApplicationProviderArn)
	if err != nil {
		return handleBackendError(c, err, "application provider not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplicationProviderArn: provider.ApplicationProviderArn,
		"DisplayData":             provider.DisplayData,
	})
}

func (h *Handler) handleListApplicationAccessScopes(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	scopes, err := h.Backend.ListApplicationAccessScopes(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Scopes":     scopes,
		keyNextToken: nil,
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

func (h *Handler) handleListApplicationAuthenticationMethods(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	methods, err := h.Backend.ListApplicationAuthenticationMethods(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	out := make([]map[string]any, 0, len(methods))
	for _, method := range methods {
		out = append(out, map[string]any{"AuthenticationMethodType": method})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AuthenticationMethods": out,
		keyNextToken:            nil,
	})
}

func (h *Handler) handleListApplicationGrants(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	grants, err := h.Backend.ListApplicationGrants(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	out := make([]map[string]any, 0, len(grants))
	for _, grant := range grants {
		out = append(out, map[string]any{"GrantType": grant})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Grants":     out,
		keyNextToken: nil,
	})
}

func (h *Handler) handleListApplicationProviders(c *echo.Context, _ []byte) error {
	providers := h.Backend.ListApplicationProviders()
	out := make([]map[string]any, 0, len(providers))
	for _, provider := range providers {
		out = append(out, map[string]any{
			keyApplicationProviderArn: provider.ApplicationProviderArn,
			"DisplayData":             provider.DisplayData,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"ApplicationProviders": out,
		keyNextToken:           nil,
	})
}

func (h *Handler) handleListApplications(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	apps := h.Backend.ListApplications(req.InstanceArn)
	sort.Slice(apps, func(i, j int) bool { return apps[i].ApplicationArn < apps[j].ApplicationArn })
	out := make([]applicationView, 0, len(apps))
	for _, app := range apps {
		out = append(out, applicationView{
			ApplicationArn:         app.ApplicationArn,
			ApplicationProviderArn: app.ApplicationProviderArn,
			Name:                   app.Name,
			Description:            app.Description,
			InstanceArn:            app.InstanceArn,
			Status:                 app.Status,
			CreatedDate:            float64(app.CreatedDate.Unix()),
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Applications": out,
		keyNextToken:   nil,
	})
}

func (h *Handler) handlePutApplicationAccessScope(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		Scope          string `json:"Scope"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.PutApplicationAccessScope(req.ApplicationArn, req.Scope); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
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

func (h *Handler) handlePutApplicationAuthenticationMethod(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn     string `json:"ApplicationArn"`
		AuthMethodType     string `json:"AuthenticationMethodType"`
		AuthenticationType string `json:"AuthenticationType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	methodType := req.AuthMethodType
	if methodType == "" {
		methodType = req.AuthenticationType
	}
	if err := h.Backend.PutApplicationAuthenticationMethod(req.ApplicationArn, methodType); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handlePutApplicationGrant(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		GrantType      string `json:"GrantType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.PutApplicationGrant(req.ApplicationArn, req.GrantType); err != nil {
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

func (h *Handler) handleUpdateApplication(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		Name           string `json:"Name"`
		Description    string `json:"Description"`
		Status         string `json:"Status"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	app, err := h.Backend.UpdateApplication(req.ApplicationArn, req.Name, req.Description, req.Status)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplication: applicationView{
			ApplicationArn:         app.ApplicationArn,
			ApplicationProviderArn: app.ApplicationProviderArn,
			Name:                   app.Name,
			Description:            app.Description,
			InstanceArn:            app.InstanceArn,
			Status:                 app.Status,
			CreatedDate:            float64(app.CreatedDate.Unix()),
		},
	})
}

func (h *Handler) handleDeleteTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerArn string `json:"TrustedTokenIssuerArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.TrustedTokenIssuerArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "TrustedTokenIssuerArn is required")
	}
	if err := h.Backend.DeleteTrustedTokenIssuer(req.TrustedTokenIssuerArn); err != nil {
		return handleBackendError(c, err, "trusted token issuer not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerArn string `json:"TrustedTokenIssuerArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	issuer, err := h.Backend.DescribeTrustedTokenIssuer(req.TrustedTokenIssuerArn)
	if err != nil {
		return handleBackendError(c, err, "trusted token issuer not found")
	}

	tagList := make([]tagView, 0, len(issuer.Tags))
	for k, v := range issuer.Tags {
		tagList = append(tagList, tagView{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	ttiMap := map[string]any{
		"TrustedTokenIssuerArn":  issuer.TrustedTokenIssuerArn,
		keyName:                  issuer.Name,
		keyInstanceArn:           issuer.InstanceArn,
		"TrustedTokenIssuerType": issuer.TrustedTokenIssuerType,
		keyTags:                  tagList,
	}
	if issuer.TrustedTokenIssuerConfiguration != nil {
		cfgMap := map[string]any{}
		if issuer.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidc := issuer.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfgMap["OidcJwtConfiguration"] = map[string]any{
				"IssuerUrl":                  oidc.IssuerURL,
				"ClaimAttributePath":         oidc.ClaimAttributePath,
				"IdentityStoreAttributePath": oidc.IdentityStoreAttributePath,
				"JwksRetrievalOption":        oidc.JwksRetrievalOption,
			}
		}
		ttiMap["TrustedTokenIssuerConfiguration"] = cfgMap
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuer": ttiMap,
	})
}

func (h *Handler) handleListTrustedTokenIssuers(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	issuers := h.Backend.ListTrustedTokenIssuers(req.InstanceArn)
	sort.Slice(
		issuers,
		func(i, j int) bool { return issuers[i].TrustedTokenIssuerArn < issuers[j].TrustedTokenIssuerArn },
	)
	out := make([]trustedTokenIssuerView, 0, len(issuers))
	for _, issuer := range issuers {
		out = append(out, trustedTokenIssuerView{
			TrustedTokenIssuerArn:  issuer.TrustedTokenIssuerArn,
			Name:                   issuer.Name,
			InstanceArn:            issuer.InstanceArn,
			TrustedTokenIssuerType: issuer.TrustedTokenIssuerType,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuers": out,
		keyNextToken:          nil,
	})
}

func (h *Handler) handleUpdateTrustedTokenIssuer(c *echo.Context, body []byte) error {
	var req struct {
		TrustedTokenIssuerConfiguration *struct {
			OidcJwtConfiguration *struct {
				IssuerURL                  string `json:"IssuerUrl"`
				ClaimAttributePath         string `json:"ClaimAttributePath"`
				IdentityStoreAttributePath string `json:"IdentityStoreAttributePath"`
				JwksRetrievalOption        string `json:"JwksRetrievalOption"`
			} `json:"OidcJwtConfiguration"`
		} `json:"TrustedTokenIssuerConfiguration"`
		TrustedTokenIssuerArn  string `json:"TrustedTokenIssuerArn"`
		Name                   string `json:"Name"`
		TrustedTokenIssuerType string `json:"TrustedTokenIssuerType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	var cfg *TrustedTokenIssuerConfiguration
	if req.TrustedTokenIssuerConfiguration != nil {
		cfg = &TrustedTokenIssuerConfiguration{}
		if req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration != nil {
			oidc := req.TrustedTokenIssuerConfiguration.OidcJwtConfiguration
			cfg.OidcJwtConfiguration = &OidcJwtConfiguration{
				IssuerURL:                  oidc.IssuerURL,
				ClaimAttributePath:         oidc.ClaimAttributePath,
				IdentityStoreAttributePath: oidc.IdentityStoreAttributePath,
				JwksRetrievalOption:        oidc.JwksRetrievalOption,
			}
		}
	}

	issuer, err := h.Backend.UpdateTrustedTokenIssuer(
		req.TrustedTokenIssuerArn,
		req.Name,
		req.TrustedTokenIssuerType,
		cfg,
	)
	if err != nil {
		return handleBackendError(c, err, "trusted token issuer not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TrustedTokenIssuer": trustedTokenIssuerView{
			TrustedTokenIssuerArn:  issuer.TrustedTokenIssuerArn,
			Name:                   issuer.Name,
			InstanceArn:            issuer.InstanceArn,
			TrustedTokenIssuerType: issuer.TrustedTokenIssuerType,
		},
	})
}

func (h *Handler) handleDeleteInstanceAccessControlAttributeConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.DeleteInstanceAccessControlAttributeConfiguration(req.InstanceArn); err != nil {
		return handleBackendError(c, err, "instance access control attribute configuration not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeInstanceAccessControlAttributeConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	cfg, err := h.Backend.DescribeInstanceAccessControlAttributeConfiguration(req.InstanceArn)
	if err != nil {
		return handleBackendError(c, err, "instance access control attribute configuration not found")
	}

	attrs := make([]map[string]any, 0, len(cfg.AccessControlAttributes))
	for _, attr := range cfg.AccessControlAttributes {
		attrs = append(attrs, map[string]any{
			"Key": attr.Key,
			"Value": map[string]any{
				"Source": attr.Value.Source,
			},
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"InstanceAccessControlAttributeConfiguration": map[string]any{
			"AccessControlAttributes": attrs,
		},
		keyStatus: "ENABLED",
	})
}

func (h *Handler) handlePutPermissionsBoundaryToPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn         string `json:"InstanceArn"`
		PermissionSetArn    string `json:"PermissionSetArn"`
		PermissionsBoundary struct {
			ManagedPolicyArn string `json:"ManagedPolicyArn"`
		} `json:"PermissionsBoundary"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.PutPermissionsBoundaryToPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.PermissionsBoundary.ManagedPolicyArn,
	); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetPermissionsBoundaryForPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	boundaryArn, err := h.Backend.GetPermissionsBoundaryForPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return handleBackendError(c, err, "permissions boundary not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionsBoundary": map[string]any{
			"ManagedPolicyArn": boundaryArn,
		},
	})
}

func (h *Handler) handleListAccountAssignmentCreationStatus(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	statuses := h.Backend.ListAccountAssignmentCreationStatus(req.InstanceArn)

	out := make([]provisioningStatusView, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentsCreationStatus": out,
		keyNextToken:                       nil,
	})
}

func (h *Handler) handleListAccountAssignmentDeletionStatus(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	statuses := h.Backend.ListAccountAssignmentDeletionStatus(req.InstanceArn)

	out := make([]provisioningStatusView, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountAssignmentsDeletionStatus": out,
		keyNextToken:                       nil,
	})
}

func (h *Handler) handleListPermissionSetProvisioningStatus(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	statuses := h.Backend.ListPermissionSetProvisioningStatus(req.InstanceArn)

	out := make([]provisioningStatusView, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, provisioningStatusView{
			RequestID:   status.RequestID,
			Status:      status.Status,
			CreatedDate: float64(status.CreatedDate.Unix()),
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSetsProvisioningStatus": out,
		keyNextToken:                       nil,
	})
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

func (h *Handler) handleDeletePermissionsBoundaryFromPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
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
	if err := h.Backend.DeletePermissionsBoundaryFromPermissionSet(req.InstanceArn, req.PermissionSetArn); err != nil {
		return handleBackendError(c, err, "permissions boundary not found for: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListCustomerManagedPolicyReferencesInPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	refs, err := h.Backend.ListCustomerManagedPolicyReferencesInPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	out := make([]customerManagedPolicyReferenceView, 0, len(refs))
	for _, ref := range refs {
		out = append(out, customerManagedPolicyReferenceView(ref))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"CustomerManagedPolicyReferences": out,
		keyNextToken:                      nil,
	})
}

func (h *Handler) handleRemoveRegion(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		RegionName  string `json:"RegionName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.RegionName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "RegionName is required")
	}
	if err := h.Backend.RemoveRegion(req.InstanceArn, req.RegionName); err != nil {
		return handleBackendError(c, err, "region not found: "+req.RegionName)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListRegions(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	regions, err := h.Backend.ListRegions(req.InstanceArn)
	if err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	out := make([]map[string]any, 0, len(regions))
	for _, r := range regions {
		out = append(out, map[string]any{
			"Region":          r.Region,
			"RegionScopeType": r.RegionScopeType,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Regions":    out,
		keyNextToken: nil,
	})
}

func (h *Handler) handleDescribeRegion(c *echo.Context, _ []byte) error {
	// DescribeRegion returns metadata about a region for an SSO instance.
	// In-process simulation returns a minimal stub response.
	return writeJSON(c, http.StatusOK, map[string]any{
		"Region": map[string]any{},
	})
}

func (h *Handler) handleUpdateInstance(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		Name        string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if err := h.Backend.UpdateInstance(req.InstanceArn, req.Name); err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdateInstanceAccessControlAttributeConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                                 string `json:"InstanceArn"`
		InstanceAccessControlAttributeConfiguration struct {
			AccessControlAttributes []struct {
				Key   string `json:"Key"`
				Value struct {
					Source []string `json:"Source"`
				} `json:"Value"`
			} `json:"AccessControlAttributes"`
		} `json:"InstanceAccessControlAttributeConfiguration"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}

	attrs := make(
		[]AccessControlAttribute,
		0,
		len(req.InstanceAccessControlAttributeConfiguration.AccessControlAttributes),
	)
	for _, a := range req.InstanceAccessControlAttributeConfiguration.AccessControlAttributes {
		attrs = append(attrs, AccessControlAttribute{
			Key:   a.Key,
			Value: AccessControlAttributeValue{Source: a.Value.Source},
		})
	}

	if err := h.Backend.UpdateInstanceAccessControlAttributeConfiguration(req.InstanceArn, attrs); err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDetachCustomerManagedPolicyReferenceFromPermissionSet(
	c *echo.Context,
	body []byte,
) error {
	var req struct {
		InstanceArn                    string                             `json:"InstanceArn"`
		PermissionSetArn               string                             `json:"PermissionSetArn"`
		CustomerManagedPolicyReference customerManagedPolicyReferenceView `json:"CustomerManagedPolicyReference"`
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
	if err := h.Backend.DetachCustomerManagedPolicyReferenceFromPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.CustomerManagedPolicyReference.Name,
		req.CustomerManagedPolicyReference.Path,
	); err != nil {
		return handleBackendError(c, err, "customer managed policy reference not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListPermissionSetsProvisionedToAccount(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		AccountID   string `json:"AccountId"`
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

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionSets": arns,
		keyNextToken:     nil,
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

func (h *Handler) handleGetApplicationAccessScope(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		Scope          string `json:"Scope"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}
	if req.Scope == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "Scope is required")
	}

	scope, err := h.Backend.GetApplicationAccessScope(req.ApplicationArn, req.Scope)
	if err != nil {
		return handleBackendError(c, err, "scope not found: "+req.Scope)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Scope":             scope,
		"AuthorizedTargets": []string{},
	})
}

func (h *Handler) handleGetApplicationAuthenticationMethod(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn           string `json:"ApplicationArn"`
		AuthenticationMethodType string `json:"AuthenticationMethodType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}
	if req.AuthenticationMethodType == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "AuthenticationMethodType is required")
	}

	authMethod, err := h.Backend.GetApplicationAuthenticationMethod(req.ApplicationArn, req.AuthenticationMethodType)
	if err != nil {
		return handleBackendError(c, err, "authentication method not found: "+req.AuthenticationMethodType)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AuthenticationMethod": map[string]any{
			"AuthenticationMethodType": authMethod,
		},
	})
}

func (h *Handler) handleGetApplicationGrant(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		GrantType      string `json:"GrantType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}
	if req.GrantType == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "GrantType is required")
	}

	grantType, err := h.Backend.GetApplicationGrant(req.ApplicationArn, req.GrantType)
	if err != nil {
		return handleBackendError(c, err, "grant not found: "+req.GrantType)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Grant": map[string]any{
			"GrantType": grantType,
		},
	})
}

func (h *Handler) handleListAccountsForProvisionedPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
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

	return writeJSON(c, http.StatusOK, map[string]any{
		"AccountIds": accounts,
		keyNextToken: nil,
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
