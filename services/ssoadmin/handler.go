package ssoadmin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
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
	keyAuthenticationMethod   = "AuthenticationMethod"
	keyGrant                  = "Grant"
)

const (
	targetPrefix    = "SWBExternalService."
	ssoAdminService = "sso"

	// maxPageSize is the upper bound AWS SSO Admin list ops apply to MaxResults.
	maxPageSize = 100
)

// paginateStrings applies MaxResults + NextToken pagination to an
// already-sorted string slice. NextToken is base64-encoded to be opaque to
// callers. The token is nil (untyped) on the last page so the JSON response
// omits/zeroes it as AWS does.
func paginateStrings(items []string, maxResults int, nextToken string) ([]string, any) {
	start := 0

	if nextToken != "" {
		cursor := nextToken
		if dec, err := base64.StdEncoding.DecodeString(nextToken); err == nil {
			cursor = string(dec)
		}

		start = len(items)

		for i, v := range items {
			if v >= cursor {
				start = i

				break
			}
		}
	}

	if start > len(items) {
		start = len(items)
	}

	limit := maxResults
	if limit <= 0 || limit > maxPageSize {
		limit = maxPageSize
	}

	end := min(start+limit, len(items))

	page := items[start:end]

	var next any
	if end < len(items) {
		next = base64.StdEncoding.EncodeToString([]byte(items[end]))
	}

	return page, next
}

// paginateBy sorts items by keyFn, then applies MaxResults + NextToken
// pagination using the key as the cursor. NextToken is base64-encoded to be
// opaque to callers. Returns the page plus the NextToken (nil on the last page).
func paginateBy[T any](items []T, maxResults int, nextToken string, keyFn func(T) string) ([]T, any) {
	slices.SortFunc(items, func(a, b T) int {
		return strings.Compare(keyFn(a), keyFn(b))
	})

	start := 0

	if nextToken != "" {
		cursor := nextToken
		if dec, err := base64.StdEncoding.DecodeString(nextToken); err == nil {
			cursor = string(dec)
		}

		start = len(items)

		for i := range items {
			if keyFn(items[i]) >= cursor {
				start = i

				break
			}
		}
	}

	if start > len(items) {
		start = len(items)
	}

	limit := maxResults
	if limit <= 0 || limit > maxPageSize {
		limit = maxPageSize
	}

	end := min(start+limit, len(items))

	page := items[start:end]

	var next any
	if end < len(items) {
		next = base64.StdEncoding.EncodeToString([]byte(keyFn(items[end])))
	}

	return page, next
}

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

// ssoAdminOps maps all SSO Admin operation names to their handler functions.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var ssoAdminOps = map[string]func(*Handler, *echo.Context, []byte) error{
	// Instance operations
	"ListInstances":    (*Handler).handleListInstances,
	"CreateInstance":   (*Handler).handleCreateInstance,
	"DescribeInstance": (*Handler).handleDescribeInstance,
	"DeleteInstance":   (*Handler).handleDeleteInstance,
	// Permission Set operations
	"CreatePermissionSet":   (*Handler).handleCreatePermissionSet,
	"DescribePermissionSet": (*Handler).handleDescribePermissionSet,
	"ListPermissionSets":    (*Handler).handleListPermissionSets,
	"DeletePermissionSet":   (*Handler).handleDeletePermissionSet,
	"UpdatePermissionSet":   (*Handler).handleUpdatePermissionSet,
	// Account Assignment operations
	"CreateAccountAssignment":                 (*Handler).handleCreateAccountAssignment,
	"DescribeAccountAssignmentCreationStatus": (*Handler).handleDescribeAccountAssignmentCreationStatus,
	"DeleteAccountAssignment":                 (*Handler).handleDeleteAccountAssignment,
	"DescribeAccountAssignmentDeletionStatus": (*Handler).handleDescribeAccountAssignmentDeletionStatus,
	"ListAccountAssignments":                  (*Handler).handleListAccountAssignments,
	// Policy operations
	"AttachManagedPolicyToPermissionSet":      (*Handler).handleAttachManagedPolicyToPermissionSet,
	"DetachManagedPolicyFromPermissionSet":    (*Handler).handleDetachManagedPolicyFromPermissionSet,
	"ListManagedPoliciesInPermissionSet":      (*Handler).handleListManagedPoliciesInPermissionSet,
	"PutInlinePolicyToPermissionSet":          (*Handler).handlePutInlinePolicyToPermissionSet,
	"GetInlinePolicyForPermissionSet":         (*Handler).handleGetInlinePolicyForPermissionSet,
	"DeleteInlinePolicyFromPermissionSet":     (*Handler).handleDeleteInlinePolicyFromPermissionSet,
	"ProvisionPermissionSet":                  (*Handler).handleProvisionPermissionSet,
	"DescribePermissionSetProvisioningStatus": (*Handler).handleDescribePermissionSetProvisioningStatus,
	// Tag operations
	"TagResource":         (*Handler).handleTagResource,
	"UntagResource":       (*Handler).handleUntagResource,
	"ListTagsForResource": (*Handler).handleListTagsForResource,
	// Region operations
	"AddRegion":      (*Handler).handleAddRegion,
	"RemoveRegion":   (*Handler).handleRemoveRegion,
	"ListRegions":    (*Handler).handleListRegions,
	"DescribeRegion": (*Handler).handleDescribeRegion,
	// Customer managed policy operations
	"AttachCustomerManagedPolicyReferenceToPermissionSet":   (*Handler).handleAttachCustomerManagedPolicyReferenceToPermissionSet,   //nolint:lll // key+value identifier is too long to shorten further
	"DetachCustomerManagedPolicyReferenceFromPermissionSet": (*Handler).handleDetachCustomerManagedPolicyReferenceFromPermissionSet, //nolint:lll // key+value identifier is too long to shorten further
	"ListCustomerManagedPolicyReferencesInPermissionSet":    (*Handler).handleListCustomerManagedPolicyReferencesInPermissionSet,    //nolint:lll // key+value identifier is too long to shorten further
	// Application operations
	"CreateApplication":           (*Handler).handleCreateApplication,
	"DeleteApplication":           (*Handler).handleDeleteApplication,
	"DescribeApplication":         (*Handler).handleDescribeApplication,
	"UpdateApplication":           (*Handler).handleUpdateApplication,
	"ListApplications":            (*Handler).handleListApplications,
	"ListApplicationProviders":    (*Handler).handleListApplicationProviders,
	"DescribeApplicationProvider": (*Handler).handleDescribeApplicationProvider,
	// Application assignment operations
	"CreateApplicationAssignment":            (*Handler).handleCreateApplicationAssignment,
	"DeleteApplicationAssignment":            (*Handler).handleDeleteApplicationAssignment,
	"DescribeApplicationAssignment":          (*Handler).handleDescribeApplicationAssignment,
	"ListApplicationAssignments":             (*Handler).handleListApplicationAssignments,
	"ListApplicationAssignmentsForPrincipal": (*Handler).handleListApplicationAssignmentsForPrincipal,
	// Application access scope operations
	"PutApplicationAccessScope":    (*Handler).handlePutApplicationAccessScope,
	"DeleteApplicationAccessScope": (*Handler).handleDeleteApplicationAccessScope,
	"ListApplicationAccessScopes":  (*Handler).handleListApplicationAccessScopes,
	"GetApplicationAccessScope":    (*Handler).handleGetApplicationAccessScope,
	// Application authentication method operations
	"PutApplicationAuthenticationMethod":    (*Handler).handlePutApplicationAuthenticationMethod,
	"DeleteApplicationAuthenticationMethod": (*Handler).handleDeleteApplicationAuthenticationMethod,
	"ListApplicationAuthenticationMethods":  (*Handler).handleListApplicationAuthenticationMethods,
	"GetApplicationAuthenticationMethod":    (*Handler).handleGetApplicationAuthenticationMethod,
	// Application grant operations
	"PutApplicationGrant":    (*Handler).handlePutApplicationGrant,
	"DeleteApplicationGrant": (*Handler).handleDeleteApplicationGrant,
	"ListApplicationGrants":  (*Handler).handleListApplicationGrants,
	"GetApplicationGrant":    (*Handler).handleGetApplicationGrant,
	// Application session/assignment configuration operations
	"PutApplicationSessionConfiguration":    (*Handler).handlePutApplicationSessionConfiguration,
	"GetApplicationSessionConfiguration":    (*Handler).handleGetApplicationSessionConfiguration,
	"PutApplicationAssignmentConfiguration": (*Handler).handlePutApplicationAssignmentConfiguration,
	"GetApplicationAssignmentConfiguration": (*Handler).handleGetApplicationAssignmentConfiguration,
	// IACAC operations
	"CreateInstanceAccessControlAttributeConfiguration":   (*Handler).handleCreateInstanceAccessControlAttributeConfiguration,   //nolint:lll // key+value identifier is too long to shorten further
	"DeleteInstanceAccessControlAttributeConfiguration":   (*Handler).handleDeleteInstanceAccessControlAttributeConfiguration,   //nolint:lll // key+value identifier is too long to shorten further
	"DescribeInstanceAccessControlAttributeConfiguration": (*Handler).handleDescribeInstanceAccessControlAttributeConfiguration, //nolint:lll // key+value identifier is too long to shorten further
	"UpdateInstanceAccessControlAttributeConfiguration":   (*Handler).handleUpdateInstanceAccessControlAttributeConfiguration,   //nolint:lll // key+value identifier is too long to shorten further
	// Trusted Token Issuer operations
	"CreateTrustedTokenIssuer":   (*Handler).handleCreateTrustedTokenIssuer,
	"DeleteTrustedTokenIssuer":   (*Handler).handleDeleteTrustedTokenIssuer,
	"DescribeTrustedTokenIssuer": (*Handler).handleDescribeTrustedTokenIssuer,
	"UpdateTrustedTokenIssuer":   (*Handler).handleUpdateTrustedTokenIssuer,
	"ListTrustedTokenIssuers":    (*Handler).handleListTrustedTokenIssuers,
	// Permissions boundary operations
	"PutPermissionsBoundaryToPermissionSet":      (*Handler).handlePutPermissionsBoundaryToPermissionSet,
	"GetPermissionsBoundaryForPermissionSet":     (*Handler).handleGetPermissionsBoundaryForPermissionSet,
	"DeletePermissionsBoundaryFromPermissionSet": (*Handler).handleDeletePermissionsBoundaryFromPermissionSet,
	// Other account/permission set operations
	"ListAccountAssignmentCreationStatus":     (*Handler).handleListAccountAssignmentCreationStatus,
	"ListAccountAssignmentDeletionStatus":     (*Handler).handleListAccountAssignmentDeletionStatus,
	"ListAccountAssignmentsForPrincipal":      (*Handler).handleListAccountAssignmentsForPrincipal,
	"ListPermissionSetsProvisionedToAccount":  (*Handler).handleListPermissionSetsProvisionedToAccount,
	"ListAccountsForProvisionedPermissionSet": (*Handler).handleListAccountsForProvisionedPermissionSet,
	"ListPermissionSetProvisioningStatus":     (*Handler).handleListPermissionSetProvisioningStatus,
	"UpdateInstance":                          (*Handler).handleUpdateInstance,
}

func (h *Handler) dispatch(c *echo.Context, op string, body []byte) error {
	if fn, ok := ssoAdminOps[op]; ok {
		return fn(h, c, body)
	}

	return writeError(c, http.StatusBadRequest, "UnknownOperationException", "unknown operation: "+op)
}

type provisioningStatusView struct {
	RequestID        string  `json:"RequestId"`
	Status           string  `json:"Status"`
	FailureReason    string  `json:"FailureReason,omitempty"`
	AccountID        string  `json:"AccountId,omitempty"`
	PermissionSetArn string  `json:"PermissionSetArn,omitempty"`
	TargetType       string  `json:"TargetType,omitempty"`
	PrincipalID      string  `json:"PrincipalId,omitempty"`
	PrincipalType    string  `json:"PrincipalType,omitempty"`
	CreatedDate      float64 `json:"CreatedDate,omitempty"`
}

type tagView struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

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

// toProvisioningView converts a backend ProvisioningStatus to its JSON view.
func toProvisioningView(s *ProvisioningStatus) provisioningStatusView {
	return provisioningStatusView{
		RequestID:        s.RequestID,
		Status:           s.Status,
		CreatedDate:      float64(s.CreatedDate.Unix()),
		FailureReason:    s.FailureReason,
		AccountID:        s.AccountID,
		PermissionSetArn: s.PermissionSetArn,
		TargetType:       s.TargetType,
		PrincipalID:      s.PrincipalID,
		PrincipalType:    s.PrincipalType,
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
