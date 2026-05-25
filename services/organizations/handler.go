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
	Backend StorageBackend
}

// NewHandler creates a new Organizations handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Organizations" }

// GetSupportedOperations returns the list of supported Organizations operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcceptHandshake",
		"AttachPolicy",
		"CancelHandshake",
		"CloseAccount",
		"CreateAccount",
		"CreateGovCloudAccount",
		"CreateOrganization",
		"CreateOrganizationalUnit",
		"CreatePolicy",
		"DeclineHandshake",
		"DeleteOrganization",
		"DeleteOrganizationalUnit",
		"DeletePolicy",
		"DeleteResourcePolicy",
		"DeregisterDelegatedAdministrator",
		"DescribeAccount",
		"DescribeCreateAccountStatus",
		"DescribeEffectivePolicy",
		"DescribeHandshake",
		"DescribeOrganization",
		"DescribeOrganizationalUnit",
		"DescribePolicy",
		"DescribeResourcePolicy",
		"DescribeResponsibilityTransfer",
		"DetachPolicy",
		"DisableAWSServiceAccess",
		"DisablePolicyType",
		"EnableAWSServiceAccess",
		"EnableAllFeatures",
		"EnablePolicyType",
		"InviteAccountToOrganization",
		"InviteOrganizationToTransferResponsibility",
		"LeaveOrganization",
		"ListAccounts",
		"ListAccountsForParent",
		"ListAccountsWithInvalidEffectivePolicy",
		"ListAWSServiceAccessForOrganization",
		"ListChildren",
		"ListCreateAccountStatus",
		"ListDelegatedAdministrators",
		"ListDelegatedServicesForAccount",
		"ListEffectivePolicyValidationErrors",
		"ListHandshakesForAccount",
		"ListHandshakesForOrganization",
		"ListInboundResponsibilityTransfers",
		"ListOrganizationalUnitsForParent",
		"ListOutboundResponsibilityTransfers",
		"ListParents",
		"ListPolicies",
		"ListPoliciesForTarget",
		"ListRoots",
		"ListTagsForResource",
		"ListTargetsForPolicy",
		"MoveAccount",
		"PutResourcePolicy",
		"RegisterDelegatedAdministrator",
		"RemoveAccountFromOrganization",
		"TagResource",
		"TerminateResponsibilityTransfer",
		"UntagResource",
		"UpdateOrganizationalUnit",
		"UpdatePolicy",
		"UpdateResponsibilityTransfer",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return orgService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// Reset clears all backend state (used in tests).
func (h *Handler) Reset() { h.Backend.Reset() }

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

	for _, key := range []string{"AccountId", "HandshakeId", "OrganizationalUnitId", "PolicyId", "ResourceId"} {
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

	if ok, result := h.dispatchNewOps(c, op, body); ok {
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
	case "CloseAccount":
		return true, h.handleCloseAccount(c, body)
	case "CreateGovCloudAccount":
		return true, h.handleCreateGovCloudAccount(c, body)
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
	hs, err := h.Backend.EnableAllFeatures()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, enableAllFeaturesResponse{Handshake: toHandshakeObject(hs)})
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

	// Default RoleName.
	roleName := req.RoleName
	if roleName == "" {
		roleName = "OrganizationAccountAccessRole"
	}

	// Validate IamUserAccessToBilling.
	iamAccess := req.IamUserAccessToBilling
	if iamAccess == "" {
		iamAccess = "ALLOW"
	}
	if iamAccess != "ALLOW" && iamAccess != "DENY" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "IamUserAccessToBilling must be ALLOW or DENY")
	}

	status, err := h.Backend.CreateAccount(req.AccountName, req.Email, roleName, iamAccess, req.Tags)
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
		objs = append(objs, enabledServicePrincipalObject{
			DateEnabled:      epochSeconds(sp.DateEnabled),
			ServicePrincipal: sp.ServicePrincipal,
		})
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
			JoinedAt:       epochSeconds(da.JoinedAt),
			DelegationTime: epochSeconds(da.DelegationTime),
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
	var pts []policyTypeObject
	if len(org.AvailablePolicyTypes) > 0 {
		pts = make([]policyTypeObject, 0, len(org.AvailablePolicyTypes))
		for _, pt := range org.AvailablePolicyTypes {
			pts = append(pts, policyTypeObject(pt))
		}
	}

	return organizationObject{
		AvailablePolicyTypes: pts,
		ID:                   org.ID,
		ARN:                  org.ARN,
		FeatureSet:           org.FeatureSet,
		MasterAccountID:      org.MasterAccountID,
		MasterAccountARN:     org.MasterAccountARN,
		MasterAccountEmail:   org.MasterAccountEmail,
	}
}

func toAccountObject(a *Account) accountObject {
	return accountObject{
		ID:                     a.ID,
		ARN:                    a.ARN,
		Name:                   a.Name,
		Email:                  a.Email,
		Status:                 a.Status,
		JoinedMethod:           a.JoinedMethod,
		JoinedAt:               epochSeconds(a.JoinedAt),
		RoleName:               a.RoleName,
		IamUserAccessToBilling: a.IamUserAccessToBilling,
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

// dispatchNewOps handles handshake, resource-policy, and effective-policy operations.
func (h *Handler) dispatchNewOps(c *echo.Context, op string, body []byte) (bool, error) {
	if ok, result := h.dispatchHandshakeOps(c, op, body); ok {
		return true, result
	}

	if ok, result := h.dispatchTransferOps(c, op, body); ok {
		return true, result
	}

	switch op {
	case "DeleteResourcePolicy":
		return true, h.handleDeleteResourcePolicy(c, body)
	case "DescribeResourcePolicy":
		return true, h.handleDescribeResourcePolicy(c, body)
	case "PutResourcePolicy":
		return true, h.handlePutResourcePolicy(c, body)
	case "DescribeEffectivePolicy":
		return true, h.handleDescribeEffectivePolicy(c, body)
	case "ListCreateAccountStatus":
		return true, h.handleListCreateAccountStatus(c, body)
	case "ListAccountsWithInvalidEffectivePolicy":
		return true, h.handleListAccountsWithInvalidEffectivePolicy(c, body)
	case "ListDelegatedServicesForAccount":
		return true, h.handleListDelegatedServicesForAccount(c, body)
	case "ListEffectivePolicyValidationErrors":
		return true, h.handleListEffectivePolicyValidationErrors(c, body)
	}

	return false, nil
}

// dispatchHandshakeOps handles invitation and handshake listing operations.
func (h *Handler) dispatchHandshakeOps(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "AcceptHandshake":
		return true, h.handleAcceptHandshake(c, body)
	case "CancelHandshake":
		return true, h.handleCancelHandshake(c, body)
	case "DeclineHandshake":
		return true, h.handleDeclineHandshake(c, body)
	case "DescribeHandshake":
		return true, h.handleDescribeHandshake(c, body)
	case "InviteAccountToOrganization":
		return true, h.handleInviteAccountToOrganization(c, body)
	case "LeaveOrganization":
		return true, h.handleLeaveOrganization(c, body)
	case "ListHandshakesForAccount":
		return true, h.handleListHandshakesForAccount(c, body)
	case "ListHandshakesForOrganization":
		return true, h.handleListHandshakesForOrganization(c, body)
	}

	return false, nil
}

// dispatchTransferOps handles responsibility-transfer operations.
func (h *Handler) dispatchTransferOps(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "DescribeResponsibilityTransfer":
		return true, h.handleDescribeResponsibilityTransfer(c, body)
	case "InviteOrganizationToTransferResponsibility":
		return true, h.handleInviteOrganizationToTransferResponsibility(c, body)
	case "ListInboundResponsibilityTransfers":
		return true, h.handleListInboundResponsibilityTransfers(c, body)
	case "ListOutboundResponsibilityTransfers":
		return true, h.handleListOutboundResponsibilityTransfers(c, body)
	case "TerminateResponsibilityTransfer":
		return true, h.handleTerminateResponsibilityTransfer(c, body)
	case "UpdateResponsibilityTransfer":
		return true, h.handleUpdateResponsibilityTransfer(c, body)
	}

	return false, nil
}

// ----------------------------------------
// CloseAccount / CreateGovCloudAccount handlers
// ----------------------------------------

func (h *Handler) handleCloseAccount(c *echo.Context, body []byte) error {
	var req closeAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.AccountID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "AccountId is required")
	}

	if err := h.Backend.CloseAccount(req.AccountID); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleCreateGovCloudAccount(c *echo.Context, body []byte) error {
	var req createGovCloudAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.AccountName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "AccountName is required")
	}

	if req.Email == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Email is required")
	}

	// Default RoleName.
	roleName := req.RoleName
	if roleName == "" {
		roleName = "OrganizationAccountAccessRole"
	}

	// Validate IamUserAccessToBilling.
	iamAccess := req.IamUserAccessToBilling
	if iamAccess == "" {
		iamAccess = "ALLOW"
	}
	if iamAccess != "ALLOW" && iamAccess != "DENY" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "IamUserAccessToBilling must be ALLOW or DENY")
	}

	status, err := h.Backend.CreateGovCloudAccount(req.AccountName, req.Email, roleName, iamAccess, req.Tags)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createGovCloudAccountResponse{CreateAccountStatus: *status})
}

// ----------------------------------------
// Handshake handlers
// ----------------------------------------

func (h *Handler) handleAcceptHandshake(c *echo.Context, body []byte) error {
	var req acceptHandshakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.AcceptHandshake(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, acceptHandshakeResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleCancelHandshake(c *echo.Context, body []byte) error {
	var req cancelHandshakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.CancelHandshake(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, cancelHandshakeResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleDeclineHandshake(c *echo.Context, body []byte) error {
	var req declineHandshakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.DeclineHandshake(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, declineHandshakeResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleDescribeHandshake(c *echo.Context, body []byte) error {
	var req describeHandshakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.DescribeHandshake(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeHandshakeResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleDescribeResponsibilityTransfer(c *echo.Context, body []byte) error {
	var req describeResponsibilityTransferRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.DescribeResponsibilityTransfer(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeResponsibilityTransferResponse{HandshakeDetails: toHandshakeObject(hs)})
}

// ----------------------------------------
// New handshake / invitation / transfer handlers
// ----------------------------------------

func (h *Handler) handleInviteAccountToOrganization(c *echo.Context, body []byte) error {
	var req inviteAccountToOrganizationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Target.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Target.Id is required")
	}

	hs, err := h.Backend.InviteAccountToOrganization(req.Target, req.Notes)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, inviteAccountToOrganizationResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleInviteOrganizationToTransferResponsibility(c *echo.Context, body []byte) error {
	var req inviteOrganizationToTransferResponsibilityRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Target.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Target.Id is required")
	}

	hs, err := h.Backend.InviteOrganizationToTransferResponsibility(req.Target, req.Notes)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, inviteOrganizationToTransferResponsibilityResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleLeaveOrganization(c *echo.Context, _ []byte) error {
	if err := h.Backend.LeaveOrganization(); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListHandshakesForAccount(c *echo.Context, body []byte) error {
	var req listHandshakesFilterRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
		}
	}

	handshakes, err := h.Backend.ListHandshakesForAccount(req.Filter.ActionType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]handshakeObject, 0, len(handshakes))
	for _, hs := range handshakes {
		objs = append(objs, toHandshakeObject(hs))
	}

	return c.JSON(http.StatusOK, listHandshakesForAccountResponse{Handshakes: objs})
}

func (h *Handler) handleListHandshakesForOrganization(c *echo.Context, body []byte) error {
	var req listHandshakesFilterRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
		}
	}

	handshakes, err := h.Backend.ListHandshakesForOrganization(req.Filter.ActionType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]handshakeObject, 0, len(handshakes))
	for _, hs := range handshakes {
		objs = append(objs, toHandshakeObject(hs))
	}

	return c.JSON(http.StatusOK, listHandshakesForOrganizationResponse{Handshakes: objs})
}

func (h *Handler) handleListInboundResponsibilityTransfers(c *echo.Context, _ []byte) error {
	handshakes, err := h.Backend.ListInboundResponsibilityTransfers()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]handshakeObject, 0, len(handshakes))
	for _, hs := range handshakes {
		objs = append(objs, toHandshakeObject(hs))
	}

	return c.JSON(http.StatusOK, listInboundResponsibilityTransfersResponse{ResponsibilityTransfers: objs})
}

func (h *Handler) handleListOutboundResponsibilityTransfers(c *echo.Context, _ []byte) error {
	handshakes, err := h.Backend.ListOutboundResponsibilityTransfers()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]handshakeObject, 0, len(handshakes))
	for _, hs := range handshakes {
		objs = append(objs, toHandshakeObject(hs))
	}

	return c.JSON(http.StatusOK, listOutboundResponsibilityTransfersResponse{ResponsibilityTransfers: objs})
}

func (h *Handler) handleTerminateResponsibilityTransfer(c *echo.Context, body []byte) error {
	var req terminateResponsibilityTransferRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.TerminateResponsibilityTransfer(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, terminateResponsibilityTransferResponse{HandshakeDetails: toHandshakeObject(hs)})
}

func (h *Handler) handleUpdateResponsibilityTransfer(c *echo.Context, body []byte) error {
	var req updateResponsibilityTransferRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	if req.Action == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Action is required")
	}

	hs, err := h.Backend.UpdateResponsibilityTransfer(req.HandshakeID, req.Action)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateResponsibilityTransferResponse{HandshakeDetails: toHandshakeObject(hs)})
}

// ----------------------------------------
// New account-status / delegated-service / effective-policy handlers
// ----------------------------------------

func (h *Handler) handleListCreateAccountStatus(c *echo.Context, body []byte) error {
	var req listCreateAccountStatusRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	statuses, err := h.Backend.ListCreateAccountStatus(req.States)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]CreateAccountStatus, 0, len(statuses))
	for _, s := range statuses {
		objs = append(objs, *s)
	}

	return c.JSON(http.StatusOK, listCreateAccountStatusResponse{CreateAccountStatuses: objs})
}

func (h *Handler) handleListAccountsWithInvalidEffectivePolicy(c *echo.Context, body []byte) error {
	var req listAccountsWithInvalidEffectivePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.PolicyType == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "PolicyType is required")
	}

	accounts, err := h.Backend.ListAccountsWithInvalidEffectivePolicy(req.PolicyType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]accountObject, 0, len(accounts))
	for _, a := range accounts {
		objs = append(objs, toAccountObject(a))
	}

	return c.JSON(http.StatusOK, listAccountsWithInvalidEffectivePolicyResponse{Accounts: objs})
}

func (h *Handler) handleListDelegatedServicesForAccount(c *echo.Context, body []byte) error {
	var req listDelegatedServicesForAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.AccountID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "AccountId is required")
	}

	services, err := h.Backend.ListDelegatedServicesForAccount(req.AccountID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]delegatedServiceObject, 0, len(services))
	for _, svc := range services {
		objs = append(objs, delegatedServiceObject{
			ServicePrincipal:      svc.ServicePrincipal,
			DelegationEnabledDate: epochSeconds(svc.DelegationEnabledDate),
		})
	}

	return c.JSON(http.StatusOK, listDelegatedServicesForAccountResponse{DelegatedServices: objs})
}

func (h *Handler) handleListEffectivePolicyValidationErrors(c *echo.Context, body []byte) error {
	var req listEffectivePolicyValidationErrorsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.PolicyType == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "PolicyType is required")
	}

	errs, err := h.Backend.ListEffectivePolicyValidationErrors(req.PolicyType, req.TargetID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listEffectivePolicyValidationErrorsResponse{ValidationErrors: errs})
}

// ----------------------------------------
// ResourcePolicy handlers
// ----------------------------------------

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context, _ []byte) error {
	if err := h.Backend.DeleteResourcePolicy(); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDescribeResourcePolicy(c *echo.Context, _ []byte) error {
	rp, err := h.Backend.DescribeResourcePolicy()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeResourcePolicyResponse{
		ResourcePolicy: resourcePolicyObject{
			Content: rp.Content,
			ResourcePolicySummary: resourcePolicySummaryObject{
				ARN: rp.ARN,
				ID:  rp.ID,
			},
		},
	})
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context, body []byte) error {
	var req struct {
		Content string `json:"Content"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Content == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Content is required")
	}

	rp, err := h.Backend.PutResourcePolicy(req.Content)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeResourcePolicyResponse{
		ResourcePolicy: resourcePolicyObject{
			Content: rp.Content,
			ResourcePolicySummary: resourcePolicySummaryObject{
				ARN: rp.ARN,
				ID:  rp.ID,
			},
		},
	})
}

// ----------------------------------------
// EffectivePolicy handlers
// ----------------------------------------

func (h *Handler) handleDescribeEffectivePolicy(c *echo.Context, body []byte) error {
	var req describeEffectivePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.PolicyType == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "PolicyType is required")
	}

	ep, err := h.Backend.DescribeEffectivePolicy(req.PolicyType, req.TargetID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeEffectivePolicyResponse{
		EffectivePolicy: effectivePolicyObject{
			LastUpdatedTimestamp: epochSeconds(ep.LastUpdatedTimestamp),
			PolicyContent:        ep.PolicyContent,
			PolicyID:             ep.PolicyID,
			PolicyType:           ep.PolicyType,
			TargetID:             ep.TargetID,
		},
	})
}

// ----------------------------------------
// Handshake conversion helpers
// ----------------------------------------

func toHandshakeObject(h *Handshake) handshakeObject {
	parties := make([]handshakePartyObject, 0, len(h.Parties))
	for _, p := range h.Parties {
		parties = append(parties, handshakePartyObject(p))
	}

	resources := toHandshakeResourceObjects(h.Resources)

	return handshakeObject{
		ID:                  h.ID,
		ARN:                 h.ARN,
		Action:              h.Action,
		State:               h.State,
		RequestedTimestamp:  epochSeconds(h.RequestedTimestamp),
		ExpirationTimestamp: epochSeconds(h.ExpirationTimestamp),
		Parties:             parties,
		Resources:           resources,
	}
}

func toHandshakeResourceObjects(rs []HandshakeResource) []handshakeResourceObject {
	out := make([]handshakeResourceObject, 0, len(rs))

	for _, r := range rs {
		obj := handshakeResourceObject{
			Type:  r.Type,
			Value: r.Value,
		}

		if len(r.Resources) > 0 {
			obj.Resources = toHandshakeResourceObjects(r.Resources)
		}

		out = append(out, obj)
	}

	return out
}
