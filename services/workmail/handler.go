package workmail

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	workmailServiceName  = "WorkMail"
	workmailSigningName  = "workmail"
	workmailTargetPrefix = "WorkMailService."
	workmailContentType  = "application/x-amz-json-1.1"
)

// Handler serves Amazon WorkMail JSON operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
	region  string
}

// NewHandler creates a WorkMail handler backed by the provided storage backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend, region: backend.Region()}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return workmailServiceName }

// Reset clears backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// MatchPriority returns routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// RouteMatcher matches WorkMail X-Amz-Target headers.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), workmailTargetPrefix)
	}
}

// ExtractOperation returns the operation name from X-Amz-Target.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	if !strings.HasPrefix(target, workmailTargetPrefix) {
		return "Unknown"
	}

	return strings.TrimPrefix(target, workmailTargetPrefix)
}

// ExtractResource returns the primary resource identifier from the request body.
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// GetSupportedOperations returns the list of implemented operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for name := range h.ops {
		ops = append(ops, name)
	}

	return ops
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()), h.Name(), workmailContentType,
			h.GetSupportedOperations(), h.dispatch, h.handleError,
		)
	}
}

// Register registers routes (WorkMail uses no static routes).
func (h *Handler) Register(_ context.Context, _ *echo.Echo) error { return nil }

// ChaosServiceName returns the signing service name.
func (h *Handler) ChaosServiceName() string { return workmailSigningName }

// ChaosOperations returns operations eligible for fault injection.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns configured service regions.
func (h *Handler) ChaosRegions() []string { return []string{h.region} }

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, newUnknownOpError(action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	code := "InternalServiceError"
	status := http.StatusInternalServerError

	switch {
	case errors.Is(err, ErrNotFound):
		code, status = "EntityNotFoundException", http.StatusBadRequest
	case errors.Is(err, ErrConflict):
		code, status = "EntityAlreadyExistsException", http.StatusBadRequest
	case errors.Is(err, ErrValidation):
		code, status = "InvalidParameterException", http.StatusBadRequest
	case errors.Is(err, ErrLimitExceeded):
		code, status = "LimitExceededException", http.StatusBadRequest
	case errors.Is(err, ErrMailDomainState):
		code, status = "MailDomainStateException", http.StatusBadRequest
	case errors.Is(err, ErrEntityState):
		code, status = "EntityStateException", http.StatusBadRequest
	case isUnknownOp(err):
		code, status = "InvalidParameterException", http.StatusBadRequest
	}

	payload, marshalErr := json.Marshal(service.JSONErrorResponse{Type: code, Message: err.Error()})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", workmailContentType)

	return c.JSONBlob(status, payload)
}

// unknownOpError creates an error for unrecognized operations.
type unknownOpError struct{ op string }

func (e *unknownOpError) Error() string { return "unknown operation: " + e.op }

func newUnknownOpError(op string) error { return &unknownOpError{op: op} }

func isUnknownOp(err error) bool {
	var e *unknownOpError

	return errors.As(err, &e)
}

// buildOps constructs the operation dispatch table.
func (h *Handler) buildOps() map[string]service.JSONOpFunc { //nolint:funlen // existing issue.
	return map[string]service.JSONOpFunc{
		// Organizations
		"CreateOrganization":   service.WrapOp(h.handleCreateOrganization),
		"DescribeOrganization": service.WrapOp(h.handleDescribeOrganization),
		"DeleteOrganization":   service.WrapOp(h.handleDeleteOrganization),
		"ListOrganizations":    service.WrapOp(h.handleListOrganizations),

		// Users
		"CreateUser":                service.WrapOp(h.handleCreateUser),
		"DescribeUser":              service.WrapOp(h.handleDescribeUser),
		"UpdateUser":                service.WrapOp(h.handleUpdateUser),
		"DeleteUser":                service.WrapOp(h.handleDeleteUser),
		"ListUsers":                 service.WrapOp(h.handleListUsers),
		"RegisterToWorkMail":        service.WrapOp(h.handleRegisterToWorkMail),
		"DeregisterFromWorkMail":    service.WrapOp(h.handleDeregisterFromWorkMail),
		"ResetPassword":             service.WrapOp(h.handleResetPassword),
		"GetMailboxDetails":         service.WrapOp(h.handleGetMailboxDetails),
		"UpdateMailboxQuota":        service.WrapOp(h.handleUpdateMailboxQuota),
		"UpdatePrimaryEmailAddress": service.WrapOp(h.handleUpdatePrimaryEmailAddress),

		// Groups
		"CreateGroup":                 service.WrapOp(h.handleCreateGroup),
		"DescribeGroup":               service.WrapOp(h.handleDescribeGroup),
		"UpdateGroup":                 service.WrapOp(h.handleUpdateGroup),
		"DeleteGroup":                 service.WrapOp(h.handleDeleteGroup),
		"ListGroups":                  service.WrapOp(h.handleListGroups),
		"AssociateMemberToGroup":      service.WrapOp(h.handleAssociateMemberToGroup),
		"DisassociateMemberFromGroup": service.WrapOp(h.handleDisassociateMemberFromGroup),
		"ListGroupMembers":            service.WrapOp(h.handleListGroupMembers),
		"ListGroupsForEntity":         service.WrapOp(h.handleListGroupsForEntity),

		// Resources
		"CreateResource":                   service.WrapOp(h.handleCreateResource),
		"DescribeResource":                 service.WrapOp(h.handleDescribeResource),
		"UpdateResource":                   service.WrapOp(h.handleUpdateResource),
		"DeleteResource":                   service.WrapOp(h.handleDeleteResource),
		"ListResources":                    service.WrapOp(h.handleListResources),
		"AssociateDelegateToResource":      service.WrapOp(h.handleAssociateDelegateToResource),
		"DisassociateDelegateFromResource": service.WrapOp(h.handleDisassociateDelegateFromResource),
		"ListResourceDelegates":            service.WrapOp(h.handleListResourceDelegates),

		// Aliases
		"CreateAlias": service.WrapOp(h.handleCreateAlias),
		"DeleteAlias": service.WrapOp(h.handleDeleteAlias),
		"ListAliases": service.WrapOp(h.handleListAliases),

		// Mailbox permissions
		"PutMailboxPermissions":    service.WrapOp(h.handlePutMailboxPermissions),
		"DeleteMailboxPermissions": service.WrapOp(h.handleDeleteMailboxPermissions),
		"ListMailboxPermissions":   service.WrapOp(h.handleListMailboxPermissions),

		// Mail domains
		"RegisterMailDomain":      service.WrapOp(h.handleRegisterMailDomain),
		"DeregisterMailDomain":    service.WrapOp(h.handleDeregisterMailDomain),
		"GetMailDomain":           service.WrapOp(h.handleGetMailDomain),
		"ListMailDomains":         service.WrapOp(h.handleListMailDomains),
		"UpdateDefaultMailDomain": service.WrapOp(h.handleUpdateDefaultMailDomain),

		// Access control rules
		"PutAccessControlRule":    service.WrapOp(h.handlePutAccessControlRule),
		"DeleteAccessControlRule": service.WrapOp(h.handleDeleteAccessControlRule),
		"GetAccessControlEffect":  service.WrapOp(h.handleGetAccessControlEffect),
		"ListAccessControlRules":  service.WrapOp(h.handleListAccessControlRules),

		// Impersonation roles
		"CreateImpersonationRole": service.WrapOp(h.handleCreateImpersonationRole),
		"GetImpersonationRole":    service.WrapOp(h.handleGetImpersonationRole),
		"UpdateImpersonationRole": service.WrapOp(h.handleUpdateImpersonationRole),
		"DeleteImpersonationRole": service.WrapOp(h.handleDeleteImpersonationRole),
		"ListImpersonationRoles":  service.WrapOp(h.handleListImpersonationRoles),

		// Tags
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),

		// Describe entity
		"DescribeEntity": service.WrapOp(h.handleDescribeEntity),

		// Availability configurations
		"CreateAvailabilityConfiguration": service.WrapOp(h.handleCreateAvailabilityConfiguration),
		"DeleteAvailabilityConfiguration": service.WrapOp(h.handleDeleteAvailabilityConfiguration),
		"UpdateAvailabilityConfiguration": service.WrapOp(h.handleUpdateAvailabilityConfiguration),
		"ListAvailabilityConfigurations":  service.WrapOp(h.handleListAvailabilityConfigurations),
		"TestAvailabilityConfiguration":   service.WrapOp(h.handleTestAvailabilityConfiguration),

		// Mobile device access rules
		"CreateMobileDeviceAccessRule": service.WrapOp(h.handleCreateMobileDeviceAccessRule),
		"DeleteMobileDeviceAccessRule": service.WrapOp(h.handleDeleteMobileDeviceAccessRule),
		"UpdateMobileDeviceAccessRule": service.WrapOp(h.handleUpdateMobileDeviceAccessRule),
		"ListMobileDeviceAccessRules":  service.WrapOp(h.handleListMobileDeviceAccessRules),
		"GetMobileDeviceAccessEffect":  service.WrapOp(h.handleGetMobileDeviceAccessEffect),

		// Mobile device access overrides
		"PutMobileDeviceAccessOverride":    service.WrapOp(h.handlePutMobileDeviceAccessOverride),
		"DeleteMobileDeviceAccessOverride": service.WrapOp(h.handleDeleteMobileDeviceAccessOverride),
		"GetMobileDeviceAccessOverride":    service.WrapOp(h.handleGetMobileDeviceAccessOverride),
		"ListMobileDeviceAccessOverrides":  service.WrapOp(h.handleListMobileDeviceAccessOverrides),

		// Email monitoring configuration
		"PutEmailMonitoringConfiguration":      service.WrapOp(h.handlePutEmailMonitoringConfiguration),
		"DeleteEmailMonitoringConfiguration":   service.WrapOp(h.handleDeleteEmailMonitoringConfiguration),
		"DescribeEmailMonitoringConfiguration": service.WrapOp(h.handleDescribeEmailMonitoringConfiguration),

		// Inbound DMARC settings
		"PutInboundDmarcSettings":      service.WrapOp(h.handlePutInboundDmarcSettings),
		"DescribeInboundDmarcSettings": service.WrapOp(h.handleDescribeInboundDmarcSettings),

		// Retention policies
		"PutRetentionPolicy":        service.WrapOp(h.handlePutRetentionPolicy),
		"DeleteRetentionPolicy":     service.WrapOp(h.handleDeleteRetentionPolicy),
		"GetDefaultRetentionPolicy": service.WrapOp(h.handleGetDefaultRetentionPolicy),

		// Mailbox export jobs
		"StartMailboxExportJob":    service.WrapOp(h.handleStartMailboxExportJob),
		"CancelMailboxExportJob":   service.WrapOp(h.handleCancelMailboxExportJob),
		"DescribeMailboxExportJob": service.WrapOp(h.handleDescribeMailboxExportJob),
		"ListMailboxExportJobs":    service.WrapOp(h.handleListMailboxExportJobs),

		// Identity center applications
		"CreateIdentityCenterApplication": service.WrapOp(h.handleCreateIdentityCenterApplication),
		"DeleteIdentityCenterApplication": service.WrapOp(h.handleDeleteIdentityCenterApplication),

		// Identity provider configuration
		"PutIdentityProviderConfiguration":      service.WrapOp(h.handlePutIdentityProviderConfiguration),
		"DeleteIdentityProviderConfiguration":   service.WrapOp(h.handleDeleteIdentityProviderConfiguration),
		"DescribeIdentityProviderConfiguration": service.WrapOp(h.handleDescribeIdentityProviderConfiguration),

		// Personal access tokens
		"DeletePersonalAccessToken":      service.WrapOp(h.handleDeletePersonalAccessToken),
		"GetPersonalAccessTokenMetadata": service.WrapOp(h.handleGetPersonalAccessTokenMetadata),
		"ListPersonalAccessTokens":       service.WrapOp(h.handleListPersonalAccessTokens),

		// Impersonation role effect
		"GetImpersonationRoleEffect": service.WrapOp(h.handleGetImpersonationRoleEffect),

		// Assume impersonation role
		"AssumeImpersonationRole": service.WrapOp(h.handleAssumeImpersonationRole),
	}
}
