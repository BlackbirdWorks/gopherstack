package organizations

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	orgService      = "organizations"
	orgTargetPrefix = "AWSOrganizationsV20161128."
)

// Handler is the HTTP handler for the AWS Organizations JSON 1.1 API.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Organizations handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Organizations" }

// GetSupportedOperations returns the list of supported Organizations operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateOrganization",
		"DescribeOrganization",
		"DeleteOrganization",
		"ListAccounts",
		"CreateAccount",
		"DescribeCreateAccountStatus",
		"DescribeAccount",
		"RemoveAccountFromOrganization",
		"MoveAccount",
		"ListRoots",
		"CreateOrganizationalUnit",
		"DescribeOrganizationalUnit",
		"DeleteOrganizationalUnit",
		"UpdateOrganizationalUnit",
		"ListOrganizationalUnitsForParent",
		"ListAccountsForParent",
		"ListParents",
		"ListChildren",
		"CreatePolicy",
		"DescribePolicy",
		"UpdatePolicy",
		"DeletePolicy",
		"ListPolicies",
		"AttachPolicy",
		"DetachPolicy",
		"ListPoliciesForTarget",
		"ListTargetsForPolicy",
		"EnablePolicyType",
		"DisablePolicyType",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"EnableAWSServiceAccess",
		"DisableAWSServiceAccess",
		"ListAWSServiceAccessForOrganization",
		"RegisterDelegatedAdministrator",
		"DeregisterDelegatedAdministrator",
		"ListDelegatedAdministrators",
		"EnableAllFeatures",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return orgService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.region} }

// RouteMatcher returns a function that matches Organizations JSON 1.1 API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), orgTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), orgTargetPrefix)
}

// ExtractResource extracts the primary resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any

	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"AccountId", "OrganizationalUnitId", "PolicyId", "ResourceId"} {
		if v, ok := data[key]; ok {
			if s, isStr := v.(string); isStr {
				return s
			}
		}
	}

	return ""
}

// Handler returns the Echo handler function for Organizations requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		target := c.Request().Header.Get("X-Amz-Target")

		if !strings.HasPrefix(target, orgTargetPrefix) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterException",
				"missing or invalid X-Amz-Target header")
		}

		op := strings.TrimPrefix(target, orgTargetPrefix)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "organizations: failed to read request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "organizations request", "op", op)

		return h.dispatch(c, op, body)
	}
}

// dispatch routes to the appropriate handler based on the operation name.
func (h *Handler) dispatch(c *echo.Context, op string, body []byte) error {
	if ok, result := h.dispatchOrg(c, op, body); ok {
		return result
	}

	if ok, result := h.dispatchAccount(c, op, body); ok {
		return result
	}

	if ok, result := h.dispatchOU(c, op, body); ok {
		return result
	}

	if ok, result := h.dispatchPolicy(c, op, body); ok {
		return result
	}

	if ok, result := h.dispatchMisc(c, op, body); ok {
		return result
	}

	return h.writeError(c, http.StatusBadRequest, "UnknownOperationException", "unknown operation: "+op)
}

// dispatchOrg handles organization-level operations.
func (h *Handler) dispatchOrg(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "CreateOrganization":
		return true, h.handleCreateOrganization(c, body)
	case "DescribeOrganization":
		return true, h.handleDescribeOrganization(c, body)
	case "DeleteOrganization":
		return true, h.handleDeleteOrganization(c, body)
	case "EnableAllFeatures":
		return true, h.handleEnableAllFeatures(c, body)
	case "ListRoots":
		return true, h.handleListRoots(c, body)
	}

	return false, nil
}

// dispatchAccount handles account operations.
func (h *Handler) dispatchAccount(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "ListAccounts":
		return true, h.handleListAccounts(c, body)
	case "CreateAccount":
		return true, h.handleCreateAccount(c, body)
	case "DescribeCreateAccountStatus":
		return true, h.handleDescribeCreateAccountStatus(c, body)
	case "DescribeAccount":
		return true, h.handleDescribeAccount(c, body)
	case "RemoveAccountFromOrganization":
		return true, h.handleRemoveAccountFromOrganization(c, body)
	case "MoveAccount":
		return true, h.handleMoveAccount(c, body)
	}

	return false, nil
}

// dispatchOU handles OU and hierarchy operations.
func (h *Handler) dispatchOU(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "CreateOrganizationalUnit":
		return true, h.handleCreateOrganizationalUnit(c, body)
	case "DescribeOrganizationalUnit":
		return true, h.handleDescribeOrganizationalUnit(c, body)
	case "DeleteOrganizationalUnit":
		return true, h.handleDeleteOrganizationalUnit(c, body)
	case "UpdateOrganizationalUnit":
		return true, h.handleUpdateOrganizationalUnit(c, body)
	case "ListOrganizationalUnitsForParent":
		return true, h.handleListOrganizationalUnitsForParent(c, body)
	case "ListAccountsForParent":
		return true, h.handleListAccountsForParent(c, body)
	case "ListParents":
		return true, h.handleListParents(c, body)
	case "ListChildren":
		return true, h.handleListChildren(c, body)
	}

	return false, nil
}

// dispatchPolicy handles policy operations.
func (h *Handler) dispatchPolicy(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "CreatePolicy":
		return true, h.handleCreatePolicy(c, body)
	case "DescribePolicy":
		return true, h.handleDescribePolicy(c, body)
	case "UpdatePolicy":
		return true, h.handleUpdatePolicy(c, body)
	case "DeletePolicy":
		return true, h.handleDeletePolicy(c, body)
	case "ListPolicies":
		return true, h.handleListPolicies(c, body)
	case "AttachPolicy":
		return true, h.handleAttachPolicy(c, body)
	case "DetachPolicy":
		return true, h.handleDetachPolicy(c, body)
	case "ListPoliciesForTarget":
		return true, h.handleListPoliciesForTarget(c, body)
	case "ListTargetsForPolicy":
		return true, h.handleListTargetsForPolicy(c, body)
	case "EnablePolicyType":
		return true, h.handleEnablePolicyType(c, body)
	case "DisablePolicyType":
		return true, h.handleDisablePolicyType(c, body)
	}

	return false, nil
}

// dispatchMisc handles tag, service access, and delegated admin operations.
func (h *Handler) dispatchMisc(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "TagResource":
		return true, h.handleTagResource(c, body)
	case "UntagResource":
		return true, h.handleUntagResource(c, body)
	case "ListTagsForResource":
		return true, h.handleListTagsForResource(c, body)
	case "EnableAWSServiceAccess":
		return true, h.handleEnableAWSServiceAccess(c, body)
	case "DisableAWSServiceAccess":
		return true, h.handleDisableAWSServiceAccess(c, body)
	case "ListAWSServiceAccessForOrganization":
		return true, h.handleListAWSServiceAccessForOrganization(c, body)
	case "RegisterDelegatedAdministrator":
		return true, h.handleRegisterDelegatedAdministrator(c, body)
	case "DeregisterDelegatedAdministrator":
		return true, h.handleDeregisterDelegatedAdministrator(c, body)
	case "ListDelegatedAdministrators":
		return true, h.handleListDelegatedAdministrators(c, body)
	}

	return false, nil
}

// ----------------------------------------
// Organization handlers
// ----------------------------------------

func (h *Handler) handleCreateOrganization(c *echo.Context, body []byte) error {
	var req createOrganizationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	org, _, err := h.Backend.CreateOrganization(req.FeatureSet)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createOrganizationResponse{
		Organization: toOrganizationObject(org),
	})
}

func (h *Handler) handleDescribeOrganization(c *echo.Context, _ []byte) error {
	org, err := h.Backend.DescribeOrganization()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeOrganizationResponse{
		Organization: toOrganizationObject(org),
	})
}

func (h *Handler) handleDeleteOrganization(c *echo.Context, _ []byte) error {
	if err := h.Backend.DeleteOrganization(); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleEnableAllFeatures(c *echo.Context, _ []byte) error {
	if err := h.Backend.EnsureOrgExists(); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// ----------------------------------------
// Account handlers
// ----------------------------------------

func (h *Handler) handleListAccounts(c *echo.Context, _ []byte) error {
	accounts, err := h.Backend.ListAccounts()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]accountObject, 0, len(accounts))
	for _, a := range accounts {
		objs = append(objs, toAccountObject(a))
	}

	return c.JSON(http.StatusOK, listAccountsResponse{Accounts: objs})
}

func (h *Handler) handleCreateAccount(c *echo.Context, body []byte) error {
	var req createAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.AccountName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "AccountName is required")
	}

	if req.Email == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Email is required")
	}

	status, err := h.Backend.CreateAccount(req.AccountName, req.Email, req.Tags)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createAccountResponse{CreateAccountStatus: *status})
}

func (h *Handler) handleDescribeCreateAccountStatus(c *echo.Context, body []byte) error {
	var req describeCreateAccountStatusRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	status, err := h.Backend.DescribeCreateAccountStatus(req.CreateAccountRequestID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeCreateAccountStatusResponse{CreateAccountStatus: *status})
}

func (h *Handler) handleDescribeAccount(c *echo.Context, body []byte) error {
	var req describeAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	acct, err := h.Backend.DescribeAccount(req.AccountID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeAccountResponse{Account: toAccountObject(acct)})
}

func (h *Handler) handleRemoveAccountFromOrganization(c *echo.Context, body []byte) error {
	var req removeAccountFromOrganizationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.RemoveAccountFromOrganization(req.AccountID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleMoveAccount(c *echo.Context, body []byte) error {
	var req moveAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.MoveAccount(req.AccountID, req.SourceParentID, req.DestinationParentID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// ----------------------------------------
// Root handlers
// ----------------------------------------

func (h *Handler) handleListRoots(c *echo.Context, _ []byte) error {
	roots, err := h.Backend.ListRoots()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]rootObject, 0, len(roots))
	for _, r := range roots {
		objs = append(objs, toRootObject(r))
	}

	return c.JSON(http.StatusOK, listRootsResponse{Roots: objs})
}

// ----------------------------------------
// OU handlers
// ----------------------------------------

func (h *Handler) handleCreateOrganizationalUnit(c *echo.Context, body []byte) error {
	var req createOrganizationalUnitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	ou, err := h.Backend.CreateOrganizationalUnit(req.ParentID, req.Name, req.Tags)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createOrganizationalUnitResponse{OrganizationalUnit: toOUObject(ou)})
}

func (h *Handler) handleDescribeOrganizationalUnit(c *echo.Context, body []byte) error {
	var req describeOrganizationalUnitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	ou, err := h.Backend.DescribeOrganizationalUnit(req.OrganizationalUnitID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeOrganizationalUnitResponse{OrganizationalUnit: toOUObject(ou)})
}

func (h *Handler) handleDeleteOrganizationalUnit(c *echo.Context, body []byte) error {
	var req deleteOrganizationalUnitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DeleteOrganizationalUnit(req.OrganizationalUnitID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUpdateOrganizationalUnit(c *echo.Context, body []byte) error {
	var req updateOrganizationalUnitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	ou, err := h.Backend.UpdateOrganizationalUnit(req.OrganizationalUnitID, req.Name)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateOrganizationalUnitResponse{OrganizationalUnit: toOUObject(ou)})
}

func (h *Handler) handleListOrganizationalUnitsForParent(c *echo.Context, body []byte) error {
	var req listOrganizationalUnitsForParentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	ous, err := h.Backend.ListOrganizationalUnitsForParent(req.ParentID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]ouObject, 0, len(ous))
	for _, ou := range ous {
		objs = append(objs, toOUObject(ou))
	}

	return c.JSON(http.StatusOK, listOrganizationalUnitsForParentResponse{OrganizationalUnits: objs})
}

func (h *Handler) handleListAccountsForParent(c *echo.Context, body []byte) error {
	var req listAccountsForParentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	accounts, err := h.Backend.ListAccountsForParent(req.ParentID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]accountObject, 0, len(accounts))
	for _, a := range accounts {
		objs = append(objs, toAccountObject(a))
	}

	return c.JSON(http.StatusOK, listAccountsForParentResponse{Accounts: objs})
}

func (h *Handler) handleListParents(c *echo.Context, body []byte) error {
	var req listParentsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	parents, err := h.Backend.ListParents(req.ChildID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listParentsResponse{Parents: parents})
}

func (h *Handler) handleListChildren(c *echo.Context, body []byte) error {
	var req listChildrenRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	children, err := h.Backend.ListChildren(req.ParentID, req.ChildType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listChildrenResponse{Children: children})
}

// ----------------------------------------
// Policy handlers
// ----------------------------------------

func (h *Handler) handleCreatePolicy(c *echo.Context, body []byte) error {
	var req createPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	p, err := h.Backend.CreatePolicy(req.Name, req.Description, req.Content, req.Type, req.Tags)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createPolicyResponse{Policy: toPolicyObject(p)})
}

func (h *Handler) handleDescribePolicy(c *echo.Context, body []byte) error {
	var req describePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	p, err := h.Backend.DescribePolicy(req.PolicyID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describePolicyResponse{Policy: toPolicyObject(p)})
}

func (h *Handler) handleUpdatePolicy(c *echo.Context, body []byte) error {
	var req updatePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	p, err := h.Backend.UpdatePolicy(req.PolicyID, req.Name, req.Description, req.Content)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updatePolicyResponse{Policy: toPolicyObject(p)})
}

func (h *Handler) handleDeletePolicy(c *echo.Context, body []byte) error {
	var req deletePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DeletePolicy(req.PolicyID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListPolicies(c *echo.Context, body []byte) error {
	var req listPoliciesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	policies, err := h.Backend.ListPolicies(req.Filter)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]policySummaryObject, 0, len(policies))
	for _, p := range policies {
		objs = append(objs, toPolicySummaryObject(p))
	}

	return c.JSON(http.StatusOK, listPoliciesResponse{Policies: objs})
}

func (h *Handler) handleAttachPolicy(c *echo.Context, body []byte) error {
	var req attachPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.AttachPolicy(req.PolicyID, req.TargetID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDetachPolicy(c *echo.Context, body []byte) error {
	var req detachPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DetachPolicy(req.PolicyID, req.TargetID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListPoliciesForTarget(c *echo.Context, body []byte) error {
	var req listPoliciesForTargetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	policies, err := h.Backend.ListPoliciesForTarget(req.TargetID, req.Filter)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]policySummaryObject, 0, len(policies))
	for _, p := range policies {
		objs = append(objs, toPolicySummaryObject(p))
	}

	return c.JSON(http.StatusOK, listPoliciesForTargetResponse{Policies: objs})
}

func (h *Handler) handleListTargetsForPolicy(c *echo.Context, body []byte) error {
	var req listTargetsForPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	targets, err := h.Backend.ListTargetsForPolicy(req.PolicyID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]policyTargetObject, 0, len(targets))
	for _, t := range targets {
		objs = append(objs, policyTargetObject(t))
	}

	return c.JSON(http.StatusOK, listTargetsForPolicyResponse{Targets: objs})
}

func (h *Handler) handleEnablePolicyType(c *echo.Context, body []byte) error {
	var req enablePolicyTypeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	root, err := h.Backend.EnablePolicyType(req.RootID, req.PolicyType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, enablePolicyTypeResponse{Root: toRootObject(root)})
}

func (h *Handler) handleDisablePolicyType(c *echo.Context, body []byte) error {
	var req disablePolicyTypeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	root, err := h.Backend.DisablePolicyType(req.RootID, req.PolicyType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, disablePolicyTypeResponse{Root: toRootObject(root)})
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.TagResource(req.ResourceID, req.Tags); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.UntagResource(req.ResourceID, req.TagKeys); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsForResourceResponse{Tags: tags})
}

// ----------------------------------------
// Service access handlers
// ----------------------------------------

func (h *Handler) handleEnableAWSServiceAccess(c *echo.Context, body []byte) error {
	var req enableAWSServiceAccessRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.EnableAWSServiceAccess(req.ServicePrincipal); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDisableAWSServiceAccess(c *echo.Context, body []byte) error {
	var req disableAWSServiceAccessRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DisableAWSServiceAccess(req.ServicePrincipal); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListAWSServiceAccessForOrganization(c *echo.Context, _ []byte) error {
	sps, err := h.Backend.ListAWSServiceAccessForOrganization()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]enabledServicePrincipalObject, 0, len(sps))
	for _, sp := range sps {
		objs = append(objs, enabledServicePrincipalObject(sp))
	}

	return c.JSON(http.StatusOK, listAWSServiceAccessResponse{EnabledServicePrincipals: objs})
}

// ----------------------------------------
// Delegated admin handlers
// ----------------------------------------

func (h *Handler) handleRegisterDelegatedAdministrator(c *echo.Context, body []byte) error {
	var req registerDelegatedAdministratorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.RegisterDelegatedAdministrator(req.AccountID, req.ServicePrincipal); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDeregisterDelegatedAdministrator(c *echo.Context, body []byte) error {
	var req deregisterDelegatedAdministratorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DeregisterDelegatedAdministrator(req.AccountID, req.ServicePrincipal); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListDelegatedAdministrators(c *echo.Context, body []byte) error {
	var req listDelegatedAdministratorsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	admins, err := h.Backend.ListDelegatedAdministrators(req.ServicePrincipal)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]delegatedAdminObject, 0, len(admins))
	for _, da := range admins {
		objs = append(objs, delegatedAdminObject{
			ID:             da.AccountID,
			ARN:            da.ARN,
			Name:           da.Name,
			Email:          da.Email,
			Status:         da.Status,
			JoinedMethod:   da.JoinedMethod,
			JoinedAt:       da.JoinedAt,
			DelegationTime: da.DelegationTime,
		})
	}

	return c.JSON(http.StatusOK, listDelegatedAdministratorsResponse{DelegatedAdministrators: objs})
}

// ----------------------------------------
// Error handling
// ----------------------------------------

func (h *Handler) writeError(c *echo.Context, statusCode int, errType, message string) error {
	return c.JSON(statusCode, map[string]string{
		"__type":  errType,
		"message": message,
	})
}

func (h *Handler) handleBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusBadRequest, extractErrorType(err), err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusBadRequest, extractErrorType(err), err.Error())
	case errors.Is(err, awserr.ErrConflict):
		return h.writeError(c, http.StatusBadRequest, extractErrorType(err), err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, extractErrorType(err), err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}
}

// extractErrorType extracts the AWS error type from an error message.
// Error messages are formatted as "TypeName: message".
func extractErrorType(err error) string {
	msg := err.Error()

	if idx := strings.Index(msg, ":"); idx > 0 {
		return msg[:idx]
	}

	return "ServiceException"
}

// ----------------------------------------
// Conversion helpers
// ----------------------------------------

func toOrganizationObject(org *Organization) organizationObject {
	return organizationObject{
		ID:                 org.ID,
		ARN:                org.ARN,
		FeatureSet:         org.FeatureSet,
		MasterAccountID:    org.MasterAccountID,
		MasterAccountARN:   org.MasterAccountARN,
		MasterAccountEmail: org.MasterAccountEmail,
	}
}

func toAccountObject(a *Account) accountObject {
	return accountObject{
		ID:           a.ID,
		ARN:          a.ARN,
		Name:         a.Name,
		Email:        a.Email,
		Status:       a.Status,
		JoinedMethod: a.JoinedMethod,
		JoinedAt:     a.JoinedAt,
	}
}

func toRootObject(r *Root) rootObject {
	pts := make([]policyTypeObject, 0, len(r.PolicyTypes))
	for _, pt := range r.PolicyTypes {
		pts = append(pts, policyTypeObject(pt))
	}

	return rootObject{
		ID:          r.ID,
		ARN:         r.ARN,
		Name:        r.Name,
		PolicyTypes: pts,
	}
}

func toOUObject(ou *OrganizationalUnit) ouObject {
	return ouObject{
		ID:   ou.ID,
		ARN:  ou.ARN,
		Name: ou.Name,
	}
}

func toPolicyObject(p *Policy) policyObject {
	return policyObject{
		PolicySummary: toPolicySummaryObject(p),
		Content:       p.Content,
	}
}

func toPolicySummaryObject(p *Policy) policySummaryObject {
	return policySummaryObject{
		ID:          p.PolicySummary.ID,
		ARN:         p.PolicySummary.ARN,
		Name:        p.PolicySummary.Name,
		Description: p.PolicySummary.Description,
		Type:        p.PolicySummary.Type,
		AwsManaged:  p.PolicySummary.AwsManaged,
	}
}
