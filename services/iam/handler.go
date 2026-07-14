package iam

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// iamAPIVersion is the IAM query protocol version used to identify IAM requests.
const (
	iamAPIVersion = "Version=2010-05-08"
	unknownOp     = "Unknown"

	opListUsers                   = "ListUsers"
	opListRoles                   = "ListRoles"
	opListPolicies                = "ListPolicies"
	opListGroups                  = "ListGroups"
	opListInstanceProfiles        = "ListInstanceProfiles"
	opListInstanceProfilesForRole = "ListInstanceProfilesForRole"
	xmlElemPolicy                 = "Policy"
	notApplicable                 = "N/A"

	minMaxSessionDuration = 3600
	maxMaxSessionDuration = 43200

	// SSH public key operation names.
	opUploadSSHPublicKey = "UploadSSHPublicKey"
	opGetSSHPublicKey    = "GetSSHPublicKey"
	opListSSHPublicKeys  = "ListSSHPublicKeys"
	opUpdateSSHPublicKey = "UpdateSSHPublicKey"
	opDeleteSSHPublicKey = "DeleteSSHPublicKey"

	// MFA device operation names.
	opEnableMFADevice        = "EnableMFADevice"
	opDeactivateMFADevice    = "DeactivateMFADevice"
	opListMFADevices         = "ListMFADevices"
	opCreateVirtualMFADevice = "CreateVirtualMFADevice"

	// Access advisor / service-specific-credential operation names.
	opGenerateServiceLastAccessed    = "GenerateServiceLastAccessedDetails"
	opGetServiceLastAccessed         = "GetServiceLastAccessedDetails"
	opResetServiceSpecificCredential = "ResetServiceSpecificCredential"
)

// Handler is the Echo HTTP handler for IAM operations.
type Handler struct {
	Backend StorageBackend `json:"backend"`
	actions map[string]iamActionFn
	tags    map[string]*svcTags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new IAM handler with the given storage backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("iam.tags"),
	}
	h.actions = h.buildDispatchTable()

	return h
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("iam." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// tagsSnapshot returns a deep copy of every Handler-level resource tag map, for
// persistence. Role/User/Group/Policy tags live on the entity itself and are
// captured by the backend's own Snapshot; this covers the remaining taggable
// resource kinds whose tags are tracked only on the Handler (instance
// profiles, MFA devices, SAML/OIDC providers, server certificates) — see
// setTags/getTags/removeTags above. Without this, those tags were silently
// dropped on every persistence restore.
func (h *Handler) tagsSnapshot() map[string]map[string]string {
	h.tagsMu.RLock("tagsSnapshot")
	defer h.tagsMu.RUnlock()

	if len(h.tags) == 0 {
		return nil
	}

	out := make(map[string]map[string]string, len(h.tags))

	for resourceID, t := range h.tags {
		if t == nil {
			continue
		}

		out[resourceID] = t.Clone()
	}

	return out
}

// restoreTags rebuilds the Handler-level tag map from a persisted snapshot.
func (h *Handler) restoreTags(snapshot map[string]map[string]string) {
	h.tagsMu.Lock("restoreTags")
	defer h.tagsMu.Unlock()

	h.tags = make(map[string]*svcTags.Tags, len(snapshot))

	for resourceID, kv := range snapshot {
		t := svcTags.New("iam." + resourceID + ".tags")
		t.Merge(kv)
		h.tags[resourceID] = t
	}
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "IAM"
}

// coreIAMOperations returns the operation names for the original core IAM
// resource CRUD surface (users, roles, policies, groups, access keys,
// instance profiles, tags, providers, login profiles, account, reporting).
func coreIAMOperations() []string {
	return []string{
		"CreateUser", "DeleteUser", opListUsers, "GetUser", "UpdateUser",
		"CreateRole", "DeleteRole", opListRoles, "GetRole", "UpdateRole", "UpdateRoleDescription",
		"CreatePolicy", "DeletePolicy", opListPolicies,
		"GetPolicy", "GetPolicyVersion",
		"AttachUserPolicy", "DetachUserPolicy", "AttachRolePolicy",
		"DetachRolePolicy",
		"ListAttachedUserPolicies", "ListAttachedRolePolicies",
		"PutUserPolicy", "GetUserPolicy", "DeleteUserPolicy", "ListUserPolicies",
		"PutRolePolicy", "GetRolePolicy", "DeleteRolePolicy", "ListRolePolicies",
		"PutGroupPolicy", "GetGroupPolicy", "DeleteGroupPolicy", "ListGroupPolicies",
		"PutUserPermissionsBoundary", "DeleteUserPermissionsBoundary",
		"PutRolePermissionsBoundary", "DeleteRolePermissionsBoundary",
		"UpdateAssumeRolePolicy",
		"GetAccountAuthorizationDetails",
		"SimulatePrincipalPolicy",
		"GenerateCredentialReport", "GetCredentialReport",
		"CreateGroup", "DeleteGroup", "AddUserToGroup", opListGroups, "UpdateGroup",
		"RemoveUserFromGroup", "GetGroup",
		"AttachGroupPolicy", "DetachGroupPolicy", "ListAttachedGroupPolicies",
		"CreateAccessKey", "DeleteAccessKey", "ListAccessKeys",
		"UpdateAccessKey", "GetAccessKeyLastUsed",
		"CreateInstanceProfile", "DeleteInstanceProfile", opListInstanceProfiles, "GetInstanceProfile",
		"AddRoleToInstanceProfile", "RemoveRoleFromInstanceProfile", opListInstanceProfilesForRole,
		"ListRoleTags", "TagRole", "UntagRole",
		"ListPolicyTags", "TagPolicy", "UntagPolicy",
		"ListUserTags", "TagUser", "UntagUser",
		"ListGroupTags", "TagGroup", "UntagGroup",
		// SAML Providers
		"CreateSAMLProvider", "UpdateSAMLProvider", "DeleteSAMLProvider",
		"GetSAMLProvider", "ListSAMLProviders",
		// OIDC Providers
		"CreateOpenIDConnectProvider", "UpdateOpenIDConnectProviderThumbprint",
		"DeleteOpenIDConnectProvider", "GetOpenIDConnectProvider", "ListOpenIDConnectProviders",
		"RemoveClientIDFromOpenIDConnectProvider",
		// Login Profiles
		"CreateLoginProfile", "UpdateLoginProfile", "DeleteLoginProfile", "GetLoginProfile",
		// Account Aliases
		"ListAccountAliases", "DeleteAccountAlias",
		// Account Password Policy
		"GetAccountPasswordPolicy", "UpdateAccountPasswordPolicy", "DeleteAccountPasswordPolicy",
		// Policy Versions
		"ListPolicyVersions", "SetDefaultPolicyVersion", "DeletePolicyVersion",
		// Virtual MFA Devices
		"ListVirtualMFADevices", "DeleteVirtualMFADevice",
		// Groups
		"ListGroupsForUser",
		// Policy entity queries
		"ListEntitiesForPolicy",
		// Service-Specific Credentials
		"ListServiceSpecificCredentials", "DeleteServiceSpecificCredential",
		// Simulation
		"SimulateCustomPolicy",
		// Service Linked Role status
		"GetServiceLinkedRoleDeletionStatus",
		// Miscellaneous
		"GetServiceLastAccessedDetails", "SetSecurityTokenServicePreferences",
		"GetAccountSummary",
	}
}

// extendedIAMOperations returns the operation names added in the first and
// second "new operations" passes (delegation, OIDC client IDs, password
// change, policy versions, service-linked roles, service-specific
// credentials, virtual MFA, permissions boundaries, context keys).
func extendedIAMOperations() []string {
	return []string{
		// New operations (first pass)
		"AcceptDelegationRequest",
		"AddClientIDToOpenIDConnectProvider",
		"AssociateDelegationRequest",
		"ChangePassword",
		"CreateAccountAlias",
		"CreateDelegationRequest",
		"CreatePolicyVersion",
		"CreateServiceLinkedRole",
		"CreateServiceSpecificCredential",
		"CreateVirtualMFADevice",
		// New operations (second pass)
		"UpdateServiceSpecificCredential",
		"GetUserPermissionsBoundary",
		"GetRolePermissionsBoundary",
		"GetContextKeysForCustomPolicy",
		"GetContextKeysForPrincipalPolicy",
		"GetMFADevice",
	}
}

// completenessIAMOperations returns the operation names added in the
// completeness pass that replaced previously notImplemented operations
// (SSH keys, server/signing certificates, MFA device tags, organizations,
// delegation requests).
func completenessIAMOperations() []string {
	return []string{
		// Completeness pass — previously notImplemented
		"DeactivateMFADevice",
		"DeleteSSHPublicKey",
		"DeleteServerCertificate",
		"DeleteServiceLinkedRole",
		"DeleteSigningCertificate",
		"DisableOrganizationsRootCredentialsManagement",
		"DisableOrganizationsRootSessions",
		"DisableOutboundWebIdentityFederation",
		"EnableMFADevice",
		"EnableOrganizationsRootCredentialsManagement",
		"EnableOrganizationsRootSessions",
		"EnableOutboundWebIdentityFederation",
		"GenerateOrganizationsAccessReport",
		"GenerateServiceLastAccessedDetails",
		"GetDelegationRequest",
		"GetHumanReadableSummary",
		"GetOrganizationsAccessReport",
		"GetOutboundWebIdentityFederationInfo",
		"GetSSHPublicKey",
		"GetServerCertificate",
		"GetServiceLastAccessedDetailsWithEntities",
		"ListDelegationRequests",
		"ListInstanceProfileTags",
		"ListMFADeviceTags",
		"ListMFADevices",
		"ListOpenIDConnectProviderTags",
		"ListOrganizationsFeatures",
		"ListPoliciesGrantingServiceAccess",
		"ListSAMLProviderTags",
		"ListSSHPublicKeys",
		"ListServerCertificateTags",
		"ListServerCertificates",
		"ListSigningCertificates",
		"RejectDelegationRequest",
		"ResetServiceSpecificCredential",
		"ResyncMFADevice",
		"SendDelegationToken",
		"TagInstanceProfile",
		"TagMFADevice",
		"TagOpenIDConnectProvider",
		"TagSAMLProvider",
		"TagServerCertificate",
		"UntagInstanceProfile",
		"UntagMFADevice",
		"UntagOpenIDConnectProvider",
		"UntagSAMLProvider",
		"UntagServerCertificate",
		"UpdateDelegationRequest",
		"UpdateSSHPublicKey",
		"UpdateServerCertificate",
		"UpdateSigningCertificate",
		"UploadSSHPublicKey",
		"UploadServerCertificate",
		"UploadSigningCertificate",
	}
}

// GetSupportedOperations returns the list of supported IAM operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := coreIAMOperations()
	ops = append(ops, extendedIAMOperations()...)
	ops = append(ops, completenessIAMOperations()...)

	return ops
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "iam" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IAM instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches IAM requests.
// IAM requests are form-encoded POSTs containing the IAM API version.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}

		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		return strings.Contains(string(body), iamAPIVersion)
	}
}

// MatchPriority returns the routing priority for the IAM handler.
// Higher than Dashboard (50) but lower than DynamoDB/SSM (100).
func (h *Handler) MatchPriority() int {
	return service.PriorityFormStandard
}

// ExtractOperation extracts the IAM action from the request body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return unknownOp
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownOp
	}

	action := vals.Get("Action")
	if action == "" {
		return unknownOp
	}

	return action
}

// ExtractResource extracts the primary resource name from the IAM request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	for _, key := range []string{"UserName", "RoleName", "PolicyName", "GroupName", "InstanceProfileName"} {
		if v := vals.Get(key); v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for IAM requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if c.Request().Method == http.MethodGet && c.Request().URL.Path == "/" {
			return c.JSON(http.StatusOK, h.GetSupportedOperations())
		}

		if c.Request().Method != http.MethodPost {
			return c.String(http.StatusMethodNotAllowed, "Method not allowed")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read IAM request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "ServiceFailure", "failed to read request body")
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValue", "invalid request body")
		}

		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		log.DebugContext(ctx, "IAM request", "action", action)

		response, reqErr := h.dispatch(ctx, action, vals)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "text/xml")

		xmlBytes, marshalErr := marshalXML(response)
		if marshalErr != nil {
			log.ErrorContext(ctx, "failed to marshal IAM response", "action", action, "error", marshalErr)

			return h.writeError(c, http.StatusInternalServerError, "ServiceFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

type iamActionFn func(vals url.Values, reqID string) (any, error)

// iamListTagsResult is the inner result element for ListRoleTags, ListPolicyTags, and ListUserTags.
// The XMLName field is set dynamically per action to produce the correct element name.
type iamListTagsResult struct {
	XMLName     xml.Name     `xml:""`
	Tags        []svcTags.KV `xml:"Tags>member"`
	IsTruncated bool         `xml:"IsTruncated"`
}

// iamListTagsResponse is the XML envelope for ListRoleTags, ListPolicyTags, and ListUserTags.
// The XMLName field is set dynamically per action to produce the correct element name.
type iamListTagsResponse struct {
	XMLName          xml.Name         `xml:""`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	Result           iamListTagsResult
}

// iamSimpleTagResponse is the XML response for TagRole, UntagRole, TagPolicy, UntagPolicy,
// TagUser, and UntagUser. The XMLName field is set dynamically per action.
type iamSimpleTagResponse struct {
	XMLName          xml.Name         `xml:""`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// buildDispatchTable merges all IAM sub-tables into a single map, called once at construction.
func (h *Handler) buildDispatchTable() map[string]iamActionFn {
	subtables := []map[string]iamActionFn{
		h.iamUserDispatchTable(),
		h.iamRoleDispatchTable(),
		h.iamPolicyBasicDispatchTable(),
		h.iamPolicyAttachDispatchTable(),
		h.iamGroupAttachedPolicyDispatchTable(),
		h.iamInlinePolicyDispatchTable(),
		h.iamPermissionBoundaryDispatchTable(),
		h.iamOtherOperationsDispatchTable(),
		h.iamReportingDispatchTable(),
		h.iamGroupDispatchTable(),
		h.iamAccessKeyDispatchTable(),
		h.iamInstanceProfileDispatchTable(),
		h.iamTagDispatchTable(),
		h.iamSAMLProviderDispatchTable(),
		h.iamOIDCProviderDispatchTable(),
		h.iamLoginProfileDispatchTable(),
		h.iamMiscDispatchTable(),
		h.iamNewOpsDispatchTable(),
		h.iamRefinementDispatchTable(),
		h.iamRefinement2DispatchTable(),   // overrides with PathPrefix filtering + new ops
		h.iamCompletenessDispatchTable(),  // previously notImplemented operations
		h.iamComprehensiveDispatchTable(), // SSH keys, MFA linking, access advisor, real SSC reset
	}

	combined := make(map[string]iamActionFn)
	for _, t := range subtables {
		maps.Copy(combined, t)
	}

	return combined
}

// dispatch routes the IAM action to the appropriate handler.
func (h *Handler) dispatch(
	_ context.Context,
	action string,
	vals url.Values,
) (any, error) {
	reqID := newRequestID()

	fn, ok := h.actions[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s is not a valid IAM action", ErrInvalidAction, action)
	}

	return fn(vals, reqID)
}

// handleError writes a standardized IAM XML error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)

	statusCode := http.StatusBadRequest

	var code string

	switch {
	case errors.Is(reqErr, ErrUserNotFound),
		errors.Is(reqErr, ErrRoleNotFound),
		errors.Is(reqErr, ErrPolicyNotFound),
		errors.Is(reqErr, ErrGroupNotFound),
		errors.Is(reqErr, ErrAccessKeyNotFound),
		errors.Is(reqErr, ErrInstanceProfileNotFound),
		errors.Is(reqErr, ErrInlinePolicyNotFound),
		errors.Is(reqErr, ErrSAMLProviderNotFound),
		errors.Is(reqErr, ErrOIDCProviderNotFound),
		errors.Is(reqErr, ErrLoginProfileNotFound):
		code = "NoSuchEntity"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrUserAlreadyExists),
		errors.Is(reqErr, ErrRoleAlreadyExists),
		errors.Is(reqErr, ErrPolicyAlreadyExists),
		errors.Is(reqErr, ErrGroupAlreadyExists),
		errors.Is(reqErr, ErrInstanceProfileAlreadyExists),
		errors.Is(reqErr, ErrSAMLProviderAlreadyExists),
		errors.Is(reqErr, ErrOIDCProviderAlreadyExists),
		errors.Is(reqErr, ErrLoginProfileAlreadyExists):
		code = "EntityAlreadyExists"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrDeleteConflict):
		code = "DeleteConflict"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrLimitExceeded):
		code = "LimitExceeded"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrMalformedPolicyDocument):
		code = "MalformedPolicyDocument"
	case errors.Is(reqErr, ErrInvalidAction):
		code = "InvalidAction"
	case errors.Is(reqErr, ErrInvalidOIDCProviderURL):
		code = "InvalidInput"
	case errors.Is(reqErr, ErrInvalidPassword):
		code = "InvalidInput"
	case errors.Is(reqErr, ErrValidationError):
		code = "ValidationError"
	case errors.Is(reqErr, ErrInvalidAuthenticationCode):
		code = "InvalidAuthenticationCode"
		statusCode = http.StatusForbidden
	default:
		// Real AWS IAM (query protocol) returns "ServiceFailure" for unhandled
		// server errors, not the JSON-protocol-style "InternalFailure".
		code = "ServiceFailure"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "IAM internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "IAM request error", "error", reqErr, "action", action)
	}

	return h.writeError(c, statusCode, code, reqErr.Error())
}

// writeError writes an IAM XML error response.
func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &ErrorResponse{
		Xmlns:     iamXMLNS,
		Error:     IAMError{Code: code, Message: message, Type: "Sender"},
		RequestID: newRequestID(),
	}

	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

// marshalXML encodes the payload with the XML declaration header.
func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// newRequestID generates a simple unique request ID.
func newRequestID() string {
	return "gopherstack-" + newID("req")
}

// encodePolicyDocument percent-encodes a policy document for wire output.
//
// Real AWS IAM returns policy documents URL-encoded (RFC 3986) on the following
// operations: GetRole, GetRolePolicy, GetUserPolicy, GetGroupPolicy,
// GetPolicyVersion, and GetAccountAuthorizationDetails. Callers are expected to
// URL-decode the result; some SDKs do this automatically. See e.g.
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetPolicyVersion.html
//
// The backend always stores/validates/evaluates the plain-JSON document; this
// encoding is applied only at the XML marshal boundary, never persisted.
func encodePolicyDocument(doc string) string {
	if doc == "" {
		return ""
	}

	return url.QueryEscape(doc)
}

// maxItemsUpperBound is the AWS upper bound on the MaxItems pagination
// parameter for IAM list operations. Values above this are clamped down.
const maxItemsUpperBound = 1000

// parseMaxItems converts a query-string MaxItems value to an int.
// Returns 0 for empty, non-numeric, or non-positive values; returning 0 signals
// the backend to apply its own default page size. AWS accepts MaxItems in the
// range 1–1000 and clamps larger values down to 1000.
func parseMaxItems(s string) int {
	if s == "" {
		return 0
	}

	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0
	}

	if n > maxItemsUpperBound {
		n = maxItemsUpperBound
	}

	return n
}

// parseConditionContext parses ContextEntries.member.N.{ContextKeyName,ContextKeyType,
// ContextKeyValues.member.M} from IAM SimulatePolicy form values into a ConditionContext.
// All values are stored in Extra keyed by lower-cased ContextKeyName.
func parseConditionContext(vals url.Values) ConditionContext {
	extra := make(map[string]string)

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("ContextEntries.member.%d.", i)
		keyName := vals.Get(prefix + "ContextKeyName")

		if keyName == "" {
			break
		}

		// Values are a member list; join multiple values with a comma.
		var values []string

		for j := 1; ; j++ {
			v := vals.Get(fmt.Sprintf("%sContextKeyValues.member.%d", prefix, j))
			if v == "" {
				break
			}

			values = append(values, v)
		}

		lower := strings.ToLower(keyName)
		// Map well-known keys to ConditionContext fields via Extra.
		switch lower {
		case ctxKeySourceIP:
			if len(values) > 0 {
				extra[lower] = values[0]
			}
		default:
			if len(values) > 0 {
				extra[lower] = strings.Join(values, ",")
			}
		}
	}

	if len(extra) == 0 {
		return ConditionContext{}
	}

	return ConditionContext{Extra: extra}
}

// simResultsToXML converts SimulationResult slice to the XML representation.
func simResultsToXML(results []SimulationResult) []SimulationEvalResultXML {
	xmlResults := make([]SimulationEvalResultXML, 0, len(results))

	for _, r := range results {
		entry := SimulationEvalResultXML{
			EvalActionName:   r.ActionName,
			EvalResourceName: r.ResourceName,
			EvalDecision:     r.Decision,
		}

		for policyID, decision := range r.EvalDecisionDetails {
			entry.EvalDecisionDetails = append(entry.EvalDecisionDetails,
				EvalDecisionDetailEntry{Key: policyID, Value: decision})
		}

		if r.AllowedByPermissionsBoundary != nil {
			entry.PermissionsBoundaryDecisionDetail = &PermBoundaryDecisionXML{
				AllowedByPermissionsBoundary: *r.AllowedByPermissionsBoundary,
			}
		}

		xmlResults = append(xmlResults, entry)
	}

	return xmlResults
}

// parseIndexedValues parses form values with a given prefix followed by an integer index.
// Example: prefix "ActionNames.member." extracts "ActionNames.member.1", "ActionNames.member.2", etc.
func parseIndexedValues(vals url.Values, prefix string) []string {
	var result []string

	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s%d", prefix, i))
		if v == "" {
			return result
		}

		result = append(result, v)
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}

// Purge removes all resources older than the given cutoff time.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	h.Backend.Purge(ctx, cutoff)
}

// iamNewOpsDispatchTable returns the dispatch entries for the 10 new IAM operations.
func (h *Handler) iamNewOpsDispatchTable() map[string]iamActionFn {
	table := make(map[string]iamActionFn)
	maps.Copy(table, h.iamNewOpsAccountActions())
	maps.Copy(table, h.iamNewOpsPolicyActions())
	maps.Copy(table, h.iamNewOpsRoleAndCredentialActions())
	maps.Copy(table, h.iamNewOpsDelegationAndOIDCActions())

	return table
}

// iamRefinementDispatchTable returns the dispatch entries for all refinement-pass operations.
func (h *Handler) iamRefinementDispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)

	tables := []map[string]iamActionFn{
		h.iamAccessKeyRefinementDispatch(),
		h.iamAccountAliasRefinementDispatch(),
		h.iamGroupRefinementDispatch(),
		h.iamVirtualMFADispatch(),
		h.iamPolicyVersionMgmtDispatch(),
		h.iamPasswordPolicyDispatch(),
		h.iamEntitiesForPolicyDispatch(),
		h.iamServiceSpecificCredDispatch(),
		h.iamEntityUpdateDispatch(),
		h.iamOIDCRefinementDispatch(),
		h.iamInstanceProfileRefinementDispatch(),
		h.iamSimulateCustomPolicyDispatch(),
		h.iamServiceLinkedRoleStatusDispatch(),
		h.iamGroupTagsDispatch(),
	}

	for _, t := range tables {
		maps.Copy(combined, t)
	}

	return combined
}

// iamRefinement2DispatchTable merges PathPrefix-filtered list overrides,
// permissions-boundary getters, context-key stubs, and credential management
// operations.  Entries here override earlier dispatch tables.
func (h *Handler) iamRefinement2DispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)

	maps.Copy(combined, h.iamRefinement2ListTable())
	maps.Copy(combined, h.iamRefinement2ListTable2())
	maps.Copy(combined, h.iamRefinement2PermsBoundaryTable())
	maps.Copy(combined, h.iamRefinement2CredTable())

	return combined
}

// iamCompletenessDispatchTable returns the dispatch table for all previously
// notImplemented IAM operations.  Trivial stubs return empty-but-valid
// responses; key operations have real logic.
func (h *Handler) iamCompletenessDispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)
	maps.Copy(combined, h.iamInstanceProfileTagDispatch())
	maps.Copy(combined, h.iamMFADeviceDispatch())
	maps.Copy(combined, h.iamOIDCTagDispatch())
	maps.Copy(combined, h.iamSAMLTagDispatch())
	maps.Copy(combined, h.iamServerCertReadDispatch())
	maps.Copy(combined, h.iamServerCertWriteDispatch())
	maps.Copy(combined, h.iamServerCertTagsDispatch())
	maps.Copy(combined, h.iamSSHKeyCompletenessDispatch())
	maps.Copy(combined, h.iamSigningCertificateDispatch())
	maps.Copy(combined, h.iamResetServiceSpecificCredentialCompletenessDispatch())
	maps.Copy(combined, h.iamDeleteServiceLinkedRoleDispatch())
	maps.Copy(combined, h.iamOrgsDispatch())
	maps.Copy(combined, h.iamDelegationDispatch())
	maps.Copy(combined, h.iamOrgsReportDispatch())

	return combined
}

// xmlLocalName is a helper to construct an xml.Name with only the Local field set.
func xmlLocalName(name string) xml.Name {
	return xml.Name{Local: name}
}

// iamComprehensiveDispatchTable returns dispatch entries for comprehensive IAM operations:
// SSH keys, MFA device linking, real access advisor, reset service-specific credential.
// These entries override earlier stub implementations.
func (h *Handler) iamComprehensiveDispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)
	maps.Copy(combined, h.iamSSHKeyUploadGetDispatch())
	maps.Copy(combined, h.iamSSHKeyListDeleteDispatch())
	maps.Copy(combined, h.iamMFALinkDispatch())
	maps.Copy(combined, h.iamAccessAdvisorDispatch())
	maps.Copy(combined, h.iamSSCResetDispatch())
	maps.Copy(combined, h.iamVirtualMFAFullDispatch())

	return combined
}
