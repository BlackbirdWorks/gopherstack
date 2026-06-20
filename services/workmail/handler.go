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

// ---- request/response types ----

type createOrgReq struct {
	Alias   string   `json:"Alias"`
	Domains []string `json:"Domains"`
}

type createOrgResp struct {
	OrganizationID string `json:"OrganizationId"`
}

func (h *Handler) handleCreateOrganization(_ context.Context, req *createOrgReq) (*createOrgResp, error) {
	org, err := h.Backend.CreateOrganization(req.Alias, req.Domains)
	if err != nil {
		return nil, err
	}

	return &createOrgResp{OrganizationID: org.OrgID}, nil
}

type describeOrgReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type describeOrgResp struct {
	OrganizationID          string `json:"OrganizationId"`
	Alias                   string `json:"Alias"`
	State                   string `json:"State"`
	ARN                     string `json:"ARN"`
	DirectoryID             string `json:"DirectoryId"`
	DirectoryType           string `json:"DirectoryType"`
	DefaultMailDomain       string `json:"DefaultMailDomain"`
	CompletedDate           int64  `json:"CompletedDate"`
	InteroperabilityEnabled bool   `json:"InteroperabilityEnabled"`
}

func (h *Handler) handleDescribeOrganization(_ context.Context, req *describeOrgReq) (*describeOrgResp, error) {
	org, err := h.Backend.DescribeOrganization(req.OrganizationID)
	if err != nil {
		return nil, err
	}

	return &describeOrgResp{
		OrganizationID:    org.OrgID,
		Alias:             org.Alias,
		State:             org.State,
		ARN:               org.ARN,
		DirectoryID:       org.DirectoryID,
		DirectoryType:     org.DirectoryType,
		DefaultMailDomain: org.DefaultMailDomain,
		CompletedDate:     org.CompletedDate.Unix(),
	}, nil
}

type deleteOrgReq struct {
	OrganizationID  string `json:"OrganizationId"`
	DeleteDirectory bool   `json:"DeleteDirectory"`
}

type deleteOrgResp struct {
	OrganizationID string `json:"OrganizationId"`
	State          string `json:"State"`
}

func (h *Handler) handleDeleteOrganization(_ context.Context, req *deleteOrgReq) (*deleteOrgResp, error) {
	if err := h.Backend.DeleteOrganization(req.OrganizationID, req.DeleteDirectory); err != nil {
		return nil, err
	}

	return &deleteOrgResp{OrganizationID: req.OrganizationID, State: "DELETED"}, nil
}

type listOrgsReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type orgSummaryResp struct {
	OrganizationID    string `json:"OrganizationId"`
	Alias             string `json:"Alias"`
	DefaultMailDomain string `json:"DefaultMailDomain,omitempty"`
	State             string `json:"State"`
	ErrorMessage      string `json:"ErrorMessage,omitempty"`
}

type listOrgsResp struct {
	NextToken             string           `json:"NextToken,omitempty"`
	OrganizationSummaries []orgSummaryResp `json:"OrganizationSummaries"`
}

func (h *Handler) handleListOrganizations(_ context.Context, req *listOrgsReq) (*listOrgsResp, error) {
	orgs, next, err := h.Backend.ListOrganizations(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]orgSummaryResp, 0, len(orgs))
	for _, o := range orgs {
		summaries = append(summaries, orgSummaryResp{
			OrganizationID:    o.OrgID,
			Alias:             o.Alias,
			DefaultMailDomain: o.DefaultMailDomain,
			State:             o.State,
			ErrorMessage:      o.ErrorMessage,
		})
	}

	return &listOrgsResp{OrganizationSummaries: summaries, NextToken: next}, nil
}

// ---- Users ----

type createUserReq struct {
	OrganizationID string `json:"OrganizationId"`
	Name           string `json:"Name"`
	DisplayName    string `json:"DisplayName"`
	Password       string `json:"Password"`
	Role           string `json:"Role"`
}

type createUserResp struct {
	UserID string `json:"UserId"`
}

func (h *Handler) handleCreateUser(_ context.Context, req *createUserReq) (*createUserResp, error) {
	role := req.Role
	if role == "" {
		role = "USER"
	}
	u, err := h.Backend.CreateUser(req.OrganizationID, req.Name, req.DisplayName, req.Password, role)
	if err != nil {
		return nil, err
	}

	return &createUserResp{UserID: u.UserID}, nil
}

type describeUserReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserID         string `json:"UserId"`
}

type describeUserResp struct {
	UserID       string `json:"UserId"`
	Name         string `json:"Name"`
	Email        string `json:"Email,omitempty"`
	DisplayName  string `json:"DisplayName,omitempty"`
	FirstName    string `json:"FirstName,omitempty"`
	LastName     string `json:"LastName,omitempty"`
	State        string `json:"State"`
	UserRole     string `json:"UserRole"`
	EnabledDate  int64  `json:"EnabledDate,omitempty"`
	DisabledDate int64  `json:"DisabledDate,omitempty"`
}

func (h *Handler) handleDescribeUser(_ context.Context, req *describeUserReq) (*describeUserResp, error) {
	u, err := h.Backend.DescribeUser(req.OrganizationID, req.UserID)
	if err != nil {
		return nil, err
	}

	resp := &describeUserResp{
		UserID:      u.UserID,
		Name:        u.Name,
		Email:       u.Email,
		DisplayName: u.DisplayName,
		FirstName:   u.FirstName,
		LastName:    u.LastName,
		State:       u.State,
		UserRole:    u.Role,
	}
	if !u.EnabledDate.IsZero() {
		resp.EnabledDate = u.EnabledDate.Unix()
	}
	if !u.DisabledDate.IsZero() {
		resp.DisabledDate = u.DisabledDate.Unix()
	}

	return resp, nil
}

type updateUserReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserID         string `json:"UserId"`
	DisplayName    string `json:"DisplayName"`
	FirstName      string `json:"FirstName"`
	LastName       string `json:"LastName"`
}

type emptyResp struct{}

func (h *Handler) handleUpdateUser(_ context.Context, req *updateUserReq) (*emptyResp, error) {
	if err := h.Backend.UpdateUser(
		req.OrganizationID, req.UserID, req.DisplayName, req.FirstName, req.LastName,
	); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteUserReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserID         string `json:"UserId"`
}

func (h *Handler) handleDeleteUser(_ context.Context, req *deleteUserReq) (*emptyResp, error) {
	if err := h.Backend.DeleteUser(req.OrganizationID, req.UserID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listUsersReq struct {
	OrganizationID string `json:"OrganizationId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type userSummaryResp struct {
	ID          string `json:"Id"`
	Name        string `json:"Name"`
	Email       string `json:"Email,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	State       string `json:"State"`
}

type listUsersResp struct {
	NextToken string            `json:"NextToken,omitempty"`
	Users     []userSummaryResp `json:"Users"`
}

func (h *Handler) handleListUsers(_ context.Context, req *listUsersReq) (*listUsersResp, error) {
	users, next, err := h.Backend.ListUsers(req.OrganizationID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]userSummaryResp, 0, len(users))
	for _, u := range users {
		summaries = append(summaries, userSummaryResp{
			ID:          u.UserID,
			Name:        u.Name,
			Email:       u.Email,
			DisplayName: u.DisplayName,
			State:       u.State,
		})
	}

	return &listUsersResp{Users: summaries, NextToken: next}, nil
}

type registerWorkMailReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	Email          string `json:"Email"`
}

func (h *Handler) handleRegisterToWorkMail(_ context.Context, req *registerWorkMailReq) (*emptyResp, error) {
	if err := h.Backend.RegisterToWorkMail(req.OrganizationID, req.EntityID, req.Email); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deregisterWorkMailReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
}

func (h *Handler) handleDeregisterFromWorkMail(_ context.Context, req *deregisterWorkMailReq) (*emptyResp, error) {
	if err := h.Backend.DeregisterFromWorkMail(req.OrganizationID, req.EntityID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type resetPasswordReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserID         string `json:"UserId"`
	Password       string `json:"Password"`
}

func (h *Handler) handleResetPassword(_ context.Context, req *resetPasswordReq) (*emptyResp, error) {
	if err := h.Backend.ResetPassword(req.OrganizationID, req.UserID, req.Password); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type getMailboxDetailsReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserID         string `json:"UserId"`
}

type getMailboxDetailsResp struct {
	MailboxQuota int32   `json:"MailboxQuota"`
	MailboxSize  float64 `json:"MailboxSize"`
}

func (h *Handler) handleGetMailboxDetails(
	_ context.Context,
	req *getMailboxDetailsReq,
) (*getMailboxDetailsResp, error) {
	details, err := h.Backend.GetMailboxDetails(req.OrganizationID, req.UserID)
	if err != nil {
		return nil, err
	}

	return &getMailboxDetailsResp{MailboxQuota: details.MailboxQuota, MailboxSize: details.MailboxSize}, nil
}

type updateMailboxQuotaReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserID         string `json:"UserId"`
	MailboxQuota   int32  `json:"MailboxQuota"`
}

func (h *Handler) handleUpdateMailboxQuota(_ context.Context, req *updateMailboxQuotaReq) (*emptyResp, error) {
	if err := h.Backend.UpdateMailboxQuota(req.OrganizationID, req.UserID, req.MailboxQuota); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type updatePrimaryEmailReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	Email          string `json:"Email"`
}

func (h *Handler) handleUpdatePrimaryEmailAddress(_ context.Context, req *updatePrimaryEmailReq) (*emptyResp, error) {
	if err := h.Backend.UpdatePrimaryEmailAddress(req.OrganizationID, req.EntityID, req.Email); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

// ---- Groups ----

type createGroupReq struct {
	OrganizationID              string `json:"OrganizationId"`
	Name                        string `json:"Name"`
	HiddenFromGlobalAddressList bool   `json:"HiddenFromGlobalAddressList"`
}

type createGroupResp struct {
	GroupID string `json:"GroupId"`
}

func (h *Handler) handleCreateGroup(_ context.Context, req *createGroupReq) (*createGroupResp, error) {
	g, err := h.Backend.CreateGroup(req.OrganizationID, req.Name, req.HiddenFromGlobalAddressList)
	if err != nil {
		return nil, err
	}

	return &createGroupResp{GroupID: g.GroupID}, nil
}

type describeGroupReq struct {
	OrganizationID string `json:"OrganizationId"`
	GroupID        string `json:"GroupId"`
}

type describeGroupResp struct {
	GroupID                     string `json:"GroupId"`
	Name                        string `json:"Name"`
	Email                       string `json:"Email,omitempty"`
	State                       string `json:"State"`
	EnabledDate                 int64  `json:"EnabledDate,omitempty"`
	DisabledDate                int64  `json:"DisabledDate,omitempty"`
	HiddenFromGlobalAddressList bool   `json:"HiddenFromGlobalAddressList"`
}

func (h *Handler) handleDescribeGroup(_ context.Context, req *describeGroupReq) (*describeGroupResp, error) {
	g, err := h.Backend.DescribeGroup(req.OrganizationID, req.GroupID)
	if err != nil {
		return nil, err
	}

	resp := &describeGroupResp{
		GroupID: g.GroupID,
		Name:    g.Name,
		Email:   g.Email,
		State:   g.State,
	}
	if !g.EnabledDate.IsZero() {
		resp.EnabledDate = g.EnabledDate.Unix()
	}
	if !g.DisabledDate.IsZero() {
		resp.DisabledDate = g.DisabledDate.Unix()
	}

	return resp, nil
}

type updateGroupReq struct {
	OrganizationID              string `json:"OrganizationId"`
	GroupID                     string `json:"GroupId"`
	HiddenFromGlobalAddressList bool   `json:"HiddenFromGlobalAddressList"`
}

func (h *Handler) handleUpdateGroup(_ context.Context, req *updateGroupReq) (*emptyResp, error) {
	if err := h.Backend.UpdateGroup(req.OrganizationID, req.GroupID, req.HiddenFromGlobalAddressList); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteGroupReq struct {
	OrganizationID string `json:"OrganizationId"`
	GroupID        string `json:"GroupId"`
}

func (h *Handler) handleDeleteGroup(_ context.Context, req *deleteGroupReq) (*emptyResp, error) {
	if err := h.Backend.DeleteGroup(req.OrganizationID, req.GroupID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listGroupsReq struct {
	OrganizationID string `json:"OrganizationId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type groupSummaryResp struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Email string `json:"Email,omitempty"`
	State string `json:"State"`
}

type listGroupsResp struct {
	NextToken string             `json:"NextToken,omitempty"`
	Groups    []groupSummaryResp `json:"Groups"`
}

func (h *Handler) handleListGroups(_ context.Context, req *listGroupsReq) (*listGroupsResp, error) {
	groups, next, err := h.Backend.ListGroups(req.OrganizationID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]groupSummaryResp, 0, len(groups))
	for _, g := range groups {
		summaries = append(summaries, groupSummaryResp{ID: g.GroupID, Name: g.Name, Email: g.Email, State: g.State})
	}

	return &listGroupsResp{Groups: summaries, NextToken: next}, nil
}

type associateMemberReq struct {
	OrganizationID string `json:"OrganizationId"`
	GroupID        string `json:"GroupId"`
	MemberID       string `json:"MemberId"`
}

func (h *Handler) handleAssociateMemberToGroup(_ context.Context, req *associateMemberReq) (*emptyResp, error) {
	if err := h.Backend.AssociateMemberToGroup(req.OrganizationID, req.GroupID, req.MemberID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type disassociateMemberReq struct {
	OrganizationID string `json:"OrganizationId"`
	GroupID        string `json:"GroupId"`
	MemberID       string `json:"MemberId"`
}

func (h *Handler) handleDisassociateMemberFromGroup(_ context.Context, req *disassociateMemberReq) (*emptyResp, error) {
	if err := h.Backend.DisassociateMemberFromGroup(req.OrganizationID, req.GroupID, req.MemberID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listGroupMembersReq struct {
	OrganizationID string `json:"OrganizationId"`
	GroupID        string `json:"GroupId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type memberResp struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Type  string `json:"Type"`
	State string `json:"State"`
}

type listGroupMembersResp struct {
	NextToken string       `json:"NextToken,omitempty"`
	Members   []memberResp `json:"Members"`
}

func (h *Handler) handleListGroupMembers(_ context.Context, req *listGroupMembersReq) (*listGroupMembersResp, error) {
	members, next, err := h.Backend.ListGroupMembers(req.OrganizationID, req.GroupID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	mresps := make([]memberResp, 0, len(members))
	for _, m := range members {
		mresps = append(mresps, memberResp{ID: m.MemberID, Name: m.Name, Type: m.MemberType, State: m.State})
	}

	return &listGroupMembersResp{Members: mresps, NextToken: next}, nil
}

type listGroupsForEntityReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

func (h *Handler) handleListGroupsForEntity(_ context.Context, req *listGroupsForEntityReq) (*listGroupsResp, error) {
	groups, next, err := h.Backend.ListGroupsForEntity(req.OrganizationID, req.EntityID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]groupSummaryResp, 0, len(groups))
	for _, g := range groups {
		summaries = append(summaries, groupSummaryResp{ID: g.GroupID, Name: g.Name, Email: g.Email, State: g.State})
	}

	return &listGroupsResp{Groups: summaries, NextToken: next}, nil
}

// ---- Resources ----

type createResourceReq struct {
	OrganizationID string `json:"OrganizationId"`
	Name           string `json:"Name"`
	Type           string `json:"Type"`
	Description    string `json:"Description"`
}

type createResourceResp struct {
	ResourceID string `json:"ResourceId"`
}

func (h *Handler) handleCreateResource(_ context.Context, req *createResourceReq) (*createResourceResp, error) {
	r, err := h.Backend.CreateResource(req.OrganizationID, req.Name, req.Type, req.Description)
	if err != nil {
		return nil, err
	}

	return &createResourceResp{ResourceID: r.ResourceID}, nil
}

type describeResourceReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
}

type describeResourceResp struct {
	ResourceID   string `json:"ResourceId"`
	Name         string `json:"Name"`
	Email        string `json:"Email,omitempty"`
	Type         string `json:"Type"`
	Description  string `json:"Description,omitempty"`
	State        string `json:"State"`
	EnabledDate  int64  `json:"EnabledDate,omitempty"`
	DisabledDate int64  `json:"DisabledDate,omitempty"`
}

func (h *Handler) handleDescribeResource(_ context.Context, req *describeResourceReq) (*describeResourceResp, error) {
	r, err := h.Backend.DescribeResource(req.OrganizationID, req.ResourceID)
	if err != nil {
		return nil, err
	}

	resp := &describeResourceResp{
		ResourceID:  r.ResourceID,
		Name:        r.Name,
		Email:       r.Email,
		Type:        r.ResourceType,
		Description: r.Description,
		State:       r.State,
	}
	if !r.EnabledDate.IsZero() {
		resp.EnabledDate = r.EnabledDate.Unix()
	}
	if !r.DisabledDate.IsZero() {
		resp.DisabledDate = r.DisabledDate.Unix()
	}

	return resp, nil
}

type updateResourceReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
	Name           string `json:"Name"`
	Description    string `json:"Description"`
}

func (h *Handler) handleUpdateResource(_ context.Context, req *updateResourceReq) (*emptyResp, error) {
	if err := h.Backend.UpdateResource(req.OrganizationID, req.ResourceID, req.Name, req.Description); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteResourceReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
}

func (h *Handler) handleDeleteResource(_ context.Context, req *deleteResourceReq) (*emptyResp, error) {
	if err := h.Backend.DeleteResource(req.OrganizationID, req.ResourceID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listResourcesReq struct {
	OrganizationID string `json:"OrganizationId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type resourceSummaryResp struct {
	ID          string `json:"Id"`
	Name        string `json:"Name"`
	Email       string `json:"Email,omitempty"`
	Type        string `json:"Type"`
	State       string `json:"State"`
	Description string `json:"Description,omitempty"`
}

type listResourcesResp struct {
	NextToken string                `json:"NextToken,omitempty"`
	Resources []resourceSummaryResp `json:"Resources"`
}

func (h *Handler) handleListResources(_ context.Context, req *listResourcesReq) (*listResourcesResp, error) {
	resources, next, err := h.Backend.ListResources(req.OrganizationID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]resourceSummaryResp, 0, len(resources))
	for _, r := range resources {
		summaries = append(summaries, resourceSummaryResp{
			ID:          r.ResourceID,
			Name:        r.Name,
			Email:       r.Email,
			Type:        r.ResourceType,
			State:       r.State,
			Description: r.Description,
		})
	}

	return &listResourcesResp{Resources: summaries, NextToken: next}, nil
}

type associateDelegateReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
	EntityID       string `json:"EntityId"`
}

func (h *Handler) handleAssociateDelegateToResource(_ context.Context, req *associateDelegateReq) (*emptyResp, error) {
	if err := h.Backend.AssociateDelegateToResource(req.OrganizationID, req.ResourceID, req.EntityID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type disassociateDelegateReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
	EntityID       string `json:"EntityId"`
}

func (h *Handler) handleDisassociateDelegateFromResource(
	_ context.Context,
	req *disassociateDelegateReq,
) (*emptyResp, error) {
	if err := h.Backend.DisassociateDelegateFromResource(req.OrganizationID, req.ResourceID, req.EntityID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listDelegatesReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type delegateResp struct {
	ID   string `json:"Id"`
	Type string `json:"Type"`
}

type listDelegatesResp struct {
	NextToken string         `json:"NextToken,omitempty"`
	Delegates []delegateResp `json:"Delegates"`
}

func (h *Handler) handleListResourceDelegates(_ context.Context, req *listDelegatesReq) (*listDelegatesResp, error) {
	delegates, next, err := h.Backend.ListResourceDelegates(
		req.OrganizationID,
		req.ResourceID,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	dresps := make([]delegateResp, 0, len(delegates))
	for _, d := range delegates {
		dresps = append(dresps, delegateResp{ID: d.DelegateID, Type: d.DelegateType})
	}

	return &listDelegatesResp{Delegates: dresps, NextToken: next}, nil
}

// ---- Aliases ----

type createAliasReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	Alias          string `json:"Alias"`
}

func (h *Handler) handleCreateAlias(_ context.Context, req *createAliasReq) (*emptyResp, error) {
	if err := h.Backend.CreateAlias(req.OrganizationID, req.EntityID, req.Alias); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteAliasReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	Alias          string `json:"Alias"`
}

func (h *Handler) handleDeleteAlias(_ context.Context, req *deleteAliasReq) (*emptyResp, error) {
	if err := h.Backend.DeleteAlias(req.OrganizationID, req.EntityID, req.Alias); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listAliasesReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type listAliasesResp struct {
	NextToken string   `json:"NextToken,omitempty"`
	Aliases   []string `json:"Aliases"`
}

func (h *Handler) handleListAliases(_ context.Context, req *listAliasesReq) (*listAliasesResp, error) {
	aliases, next, err := h.Backend.ListAliases(req.OrganizationID, req.EntityID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	return &listAliasesResp{Aliases: aliases, NextToken: next}, nil
}

// ---- Mailbox Permissions ----

type putMailboxPermsReq struct {
	OrganizationID   string   `json:"OrganizationId"`
	EntityID         string   `json:"EntityId"`
	GranteeID        string   `json:"GranteeId"`
	PermissionValues []string `json:"PermissionValues"`
}

func (h *Handler) handlePutMailboxPermissions(_ context.Context, req *putMailboxPermsReq) (*emptyResp, error) {
	if err := h.Backend.PutMailboxPermissions(
		req.OrganizationID, req.EntityID, req.GranteeID, req.PermissionValues,
	); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteMailboxPermsReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	GranteeID      string `json:"GranteeId"`
}

func (h *Handler) handleDeleteMailboxPermissions(_ context.Context, req *deleteMailboxPermsReq) (*emptyResp, error) {
	if err := h.Backend.DeleteMailboxPermissions(req.OrganizationID, req.EntityID, req.GranteeID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listMailboxPermsReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type permissionResp struct {
	GranteeID        string   `json:"GranteeId"`
	GranteeType      string   `json:"GranteeType"`
	PermissionValues []string `json:"PermissionValues"`
}

type listMailboxPermsResp struct {
	NextToken   string           `json:"NextToken,omitempty"`
	Permissions []permissionResp `json:"Permissions"`
}

func (h *Handler) handleListMailboxPermissions(
	_ context.Context,
	req *listMailboxPermsReq,
) (*listMailboxPermsResp, error) {
	perms, next, err := h.Backend.ListMailboxPermissions(
		req.OrganizationID,
		req.EntityID,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	presps := make([]permissionResp, 0, len(perms))
	for _, p := range perms {
		presps = append(presps, permissionResp{
			GranteeID:        p.GranteeID,
			GranteeType:      p.GranteeType,
			PermissionValues: p.Permissions,
		})
	}

	return &listMailboxPermsResp{Permissions: presps, NextToken: next}, nil
}

// ---- Mail Domains ----

type registerMailDomainReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

func (h *Handler) handleRegisterMailDomain(_ context.Context, req *registerMailDomainReq) (*emptyResp, error) {
	if err := h.Backend.RegisterMailDomain(req.OrganizationID, req.DomainName); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deregisterMailDomainReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

func (h *Handler) handleDeregisterMailDomain(_ context.Context, req *deregisterMailDomainReq) (*emptyResp, error) {
	if err := h.Backend.DeregisterMailDomain(req.OrganizationID, req.DomainName); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type getMailDomainReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

type getMailDomainResp struct {
	DomainName                  string `json:"DomainName,omitempty"`
	OwnershipVerificationStatus string `json:"OwnershipVerificationStatus,omitempty"`
	IsDefault                   bool   `json:"IsDefault"`
	IsTestDomain                bool   `json:"IsTestDomain"`
}

func (h *Handler) handleGetMailDomain(_ context.Context, req *getMailDomainReq) (*getMailDomainResp, error) {
	d, err := h.Backend.GetMailDomain(req.OrganizationID, req.DomainName)
	if err != nil {
		return nil, err
	}

	return &getMailDomainResp{
		DomainName:                  d.DomainName,
		IsDefault:                   d.IsDefault,
		IsTestDomain:                d.IsTestDomain,
		OwnershipVerificationStatus: d.OwnershipVerificationStatus,
	}, nil
}

type listMailDomainsReq struct {
	OrganizationID string `json:"OrganizationId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type mailDomainSummaryResp struct {
	DomainName   string `json:"DomainName"`
	IsDefault    bool   `json:"IsDefault"`
	IsTestDomain bool   `json:"IsTestDomain"`
}

type listMailDomainsResp struct {
	NextToken   string                  `json:"NextToken,omitempty"`
	MailDomains []mailDomainSummaryResp `json:"MailDomains"`
}

func (h *Handler) handleListMailDomains(_ context.Context, req *listMailDomainsReq) (*listMailDomainsResp, error) {
	domains, next, err := h.Backend.ListMailDomains(req.OrganizationID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	dresps := make([]mailDomainSummaryResp, 0, len(domains))
	for _, d := range domains {
		dresps = append(dresps, mailDomainSummaryResp{
			DomainName:   d.DomainName,
			IsDefault:    d.IsDefault,
			IsTestDomain: d.IsTestDomain,
		})
	}

	return &listMailDomainsResp{MailDomains: dresps, NextToken: next}, nil
}

type updateDefaultMailDomainReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

func (h *Handler) handleUpdateDefaultMailDomain(
	_ context.Context,
	req *updateDefaultMailDomainReq,
) (*emptyResp, error) {
	if err := h.Backend.UpdateDefaultMailDomain(req.OrganizationID, req.DomainName); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

// ---- Access Control Rules ----

type putACRReq struct {
	OrganizationID string   `json:"OrganizationId"`
	Name           string   `json:"Name"`
	Effect         string   `json:"Effect"`
	Description    string   `json:"Description"`
	IPRanges       []string `json:"IpRanges"`
	NotIPRanges    []string `json:"NotIpRanges"`
	Actions        []string `json:"Actions"`
	NotActions     []string `json:"NotActions"`
	UserIDs        []string `json:"UserIds"`
	NotUserIDs     []string `json:"NotUserIds"`
}

func (h *Handler) handlePutAccessControlRule(_ context.Context, req *putACRReq) (*emptyResp, error) {
	_, err := h.Backend.PutAccessControlRule(
		req.OrganizationID, req.Name, req.Effect, req.Description,
		req.IPRanges, req.NotIPRanges,
		req.Actions, req.NotActions,
		req.UserIDs, req.NotUserIDs,
	)
	if err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteACRReq struct {
	OrganizationID string `json:"OrganizationId"`
	Name           string `json:"Name"`
}

func (h *Handler) handleDeleteAccessControlRule(_ context.Context, req *deleteACRReq) (*emptyResp, error) {
	if err := h.Backend.DeleteAccessControlRule(req.OrganizationID, req.Name); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type getACEReq struct {
	OrganizationID string `json:"OrganizationId"`
	IPAddress      string `json:"IpAddress"`
	Action         string `json:"Action"`
	UserID         string `json:"UserId"`
}

type getACEResp struct {
	Effect       string   `json:"Effect"`
	MatchedRules []string `json:"MatchedRules"`
}

func (h *Handler) handleGetAccessControlEffect(_ context.Context, req *getACEReq) (*getACEResp, error) {
	effect, matched, err := h.Backend.GetAccessControlEffect(req.OrganizationID, req.IPAddress, req.Action, req.UserID)
	if err != nil {
		return nil, err
	}

	return &getACEResp{Effect: effect, MatchedRules: matched}, nil
}

type listACRReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type acrResp struct {
	Name        string   `json:"Name"`
	Effect      string   `json:"Effect"`
	Description string   `json:"Description,omitempty"`
	IPRanges    []string `json:"IPRanges,omitempty"`
	NotIPRanges []string `json:"NotIPRanges,omitempty"`
	Actions     []string `json:"Actions,omitempty"`
	NotActions  []string `json:"NotActions,omitempty"`
	UserIDs     []string `json:"UserIds,omitempty"`
	NotUserIDs  []string `json:"NotUserIds,omitempty"`
}

type listACRResp struct {
	Rules []acrResp `json:"Rules"`
}

func (h *Handler) handleListAccessControlRules(_ context.Context, req *listACRReq) (*listACRResp, error) {
	rules, err := h.Backend.ListAccessControlRules(req.OrganizationID)
	if err != nil {
		return nil, err
	}

	rresps := make([]acrResp, 0, len(rules))
	for _, r := range rules {
		rresps = append(rresps, acrResp{
			Name:        r.Name,
			Effect:      r.Effect,
			Description: r.Description,
			IPRanges:    r.IPRanges,
			NotIPRanges: r.NotIPRanges,
			Actions:     r.Actions,
			NotActions:  r.NotActions,
			UserIDs:     r.UserIDs,
			NotUserIDs:  r.NotUserIDs,
		})
	}

	return &listACRResp{Rules: rresps}, nil
}

// ---- Impersonation Roles ----

type impersonationRuleReq struct {
	ImpersonationRuleID string   `json:"ImpersonationRuleId"`
	Name                string   `json:"Name"`
	Description         string   `json:"Description"`
	Effect              string   `json:"Effect"`
	TargetUsers         []string `json:"TargetUsers"`
	NotTargetUsers      []string `json:"NotTargetUsers"`
}

type createImpersonationRoleReq struct {
	OrganizationID string                 `json:"OrganizationId"`
	Name           string                 `json:"Name"`
	Type           string                 `json:"Type"`
	Description    string                 `json:"Description"`
	Rules          []impersonationRuleReq `json:"Rules"`
}

type createImpersonationRoleResp struct {
	ImpersonationRoleID string `json:"ImpersonationRoleId"`
}

func (h *Handler) handleCreateImpersonationRole(
	_ context.Context,
	req *createImpersonationRoleReq,
) (*createImpersonationRoleResp, error) {
	rules := make([]ImpersonationRule, 0, len(req.Rules))
	for _, r := range req.Rules {
		rules = append(rules, ImpersonationRule{
			RuleID:         r.ImpersonationRuleID,
			Name:           r.Name,
			Description:    r.Description,
			Effect:         r.Effect,
			TargetUsers:    r.TargetUsers,
			NotTargetUsers: r.NotTargetUsers,
		})
	}

	role, err := h.Backend.CreateImpersonationRole(req.OrganizationID, req.Name, req.Type, req.Description, rules)
	if err != nil {
		return nil, err
	}

	return &createImpersonationRoleResp{ImpersonationRoleID: role.RoleID}, nil
}

type getImpersonationRoleReq struct {
	OrganizationID      string `json:"OrganizationId"`
	ImpersonationRoleID string `json:"ImpersonationRoleId"`
}

type impersonationRuleResp struct {
	ImpersonationRuleID string   `json:"ImpersonationRuleId"`
	Name                string   `json:"Name,omitempty"`
	Description         string   `json:"Description,omitempty"`
	Effect              string   `json:"Effect"`
	TargetUsers         []string `json:"TargetUsers,omitempty"`
	NotTargetUsers      []string `json:"NotTargetUsers,omitempty"`
}

type getImpersonationRoleResp struct {
	ImpersonationRoleID string                  `json:"ImpersonationRoleId"`
	Name                string                  `json:"Name"`
	Type                string                  `json:"Type"`
	Description         string                  `json:"Description,omitempty"`
	Rules               []impersonationRuleResp `json:"Rules,omitempty"`
	DateCreated         int64                   `json:"DateCreated"`
	DateModified        int64                   `json:"DateModified"`
}

func (h *Handler) handleGetImpersonationRole(
	_ context.Context,
	req *getImpersonationRoleReq,
) (*getImpersonationRoleResp, error) {
	role, err := h.Backend.GetImpersonationRole(req.OrganizationID, req.ImpersonationRoleID)
	if err != nil {
		return nil, err
	}

	rresps := make([]impersonationRuleResp, 0, len(role.Rules))
	for _, r := range role.Rules {
		rresps = append(rresps, impersonationRuleResp{
			ImpersonationRuleID: r.RuleID,
			Name:                r.Name,
			Description:         r.Description,
			Effect:              r.Effect,
			TargetUsers:         r.TargetUsers,
			NotTargetUsers:      r.NotTargetUsers,
		})
	}

	return &getImpersonationRoleResp{
		ImpersonationRoleID: role.RoleID,
		Name:                role.Name,
		Type:                role.RoleType,
		Description:         role.Description,
		Rules:               rresps,
		DateCreated:         role.DateCreated.Unix(),
		DateModified:        role.DateModified.Unix(),
	}, nil
}

type updateImpersonationRoleReq struct {
	OrganizationID      string                 `json:"OrganizationId"`
	ImpersonationRoleID string                 `json:"ImpersonationRoleId"`
	Name                string                 `json:"Name"`
	Type                string                 `json:"Type"`
	Description         string                 `json:"Description"`
	Rules               []impersonationRuleReq `json:"Rules"`
}

func (h *Handler) handleUpdateImpersonationRole(
	_ context.Context,
	req *updateImpersonationRoleReq,
) (*emptyResp, error) {
	rules := make([]ImpersonationRule, 0, len(req.Rules))
	for _, r := range req.Rules {
		rules = append(rules, ImpersonationRule{
			RuleID:         r.ImpersonationRuleID,
			Name:           r.Name,
			Description:    r.Description,
			Effect:         r.Effect,
			TargetUsers:    r.TargetUsers,
			NotTargetUsers: r.NotTargetUsers,
		})
	}

	if err := h.Backend.UpdateImpersonationRole(
		req.OrganizationID, req.ImpersonationRoleID, req.Name, req.Type, req.Description, rules,
	); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteImpersonationRoleReq struct {
	OrganizationID      string `json:"OrganizationId"`
	ImpersonationRoleID string `json:"ImpersonationRoleId"`
}

func (h *Handler) handleDeleteImpersonationRole(
	_ context.Context,
	req *deleteImpersonationRoleReq,
) (*emptyResp, error) {
	if err := h.Backend.DeleteImpersonationRole(req.OrganizationID, req.ImpersonationRoleID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listImpersonationRolesReq struct {
	OrganizationID string `json:"OrganizationId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type impersonationRoleSummaryResp struct {
	ImpersonationRoleID string `json:"ImpersonationRoleId"`
	Name                string `json:"Name"`
	Type                string `json:"Type"`
	DateCreated         int64  `json:"DateCreated"`
	DateModified        int64  `json:"DateModified"`
}

type listImpersonationRolesResp struct {
	NextToken string                         `json:"NextToken,omitempty"`
	Items     []impersonationRoleSummaryResp `json:"Items"`
}

func (h *Handler) handleListImpersonationRoles(
	_ context.Context,
	req *listImpersonationRolesReq,
) (*listImpersonationRolesResp, error) {
	roles, next, err := h.Backend.ListImpersonationRoles(req.OrganizationID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]impersonationRoleSummaryResp, 0, len(roles))
	for _, r := range roles {
		summaries = append(summaries, impersonationRoleSummaryResp{
			ImpersonationRoleID: r.RoleID,
			Name:                r.Name,
			Type:                r.RoleType,
			DateCreated:         r.DateCreated.Unix(),
			DateModified:        r.DateModified.Unix(),
		})
	}

	return &listImpersonationRolesResp{Items: summaries, NextToken: next}, nil
}

// ---- Tags ----

type tagReq struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, req *tagReq) (*emptyResp, error) {
	tags := make([]Tag, 0, len(req.Tags))
	for _, t := range req.Tags {
		tags = append(tags, Tag{Key: t.Key, Value: t.Value})
	}
	if err := h.Backend.TagResource(req.ResourceARN, tags); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type untagReq struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, req *untagReq) (*emptyResp, error) {
	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listTagsReq struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsResp struct {
	Tags []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, req *listTagsReq) (*listTagsResp, error) {
	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	tresp := make([]struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	}, 0, len(tags))
	for _, t := range tags {
		tresp = append(tresp, struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		}{Key: t.Key, Value: t.Value})
	}

	return &listTagsResp{Tags: tresp}, nil
}

// ---- DescribeEntity ----

type describeEntityReq struct {
	OrganizationID string `json:"OrganizationId"`
	Email          string `json:"Email"`
}

type describeEntityResp struct {
	EntityID string `json:"EntityId"`
	Name     string `json:"Name"`
	Type     string `json:"Type"`
}

func (h *Handler) handleDescribeEntity(_ context.Context, req *describeEntityReq) (*describeEntityResp, error) {
	entity, err := h.Backend.DescribeEntity(req.OrganizationID, req.Email)
	if err != nil {
		return nil, err
	}

	return &describeEntityResp{EntityID: entity.EntityID, Name: entity.Name, Type: entity.Type}, nil
}

// ---- Availability Configurations ----

type ewsProviderJSON struct {
	EwsEndpoint string `json:"EwsEndpoint"`
	EwsUsername string `json:"EwsUsername"`
	EwsPassword string `json:"EwsPassword"`
}

type lambdaProviderJSON struct {
	LambdaArn string `json:"LambdaArn"`
}

type createAvailabilityConfigReq struct {
	EwsProvider    *ewsProviderJSON    `json:"EwsProvider"`
	LambdaProvider *lambdaProviderJSON `json:"LambdaProvider"`
	OrganizationID string              `json:"OrganizationId"`
	DomainName     string              `json:"DomainName"`
}

func (h *Handler) handleCreateAvailabilityConfiguration(
	_ context.Context, req *createAvailabilityConfigReq,
) (*struct{}, error) {
	var ewsProv *AvailabilityEwsProvider
	var lambdaARN string
	if req.EwsProvider != nil {
		ewsProv = &AvailabilityEwsProvider{
			EwsEndpoint: req.EwsProvider.EwsEndpoint,
			EwsUsername: req.EwsProvider.EwsUsername,
			EwsPassword: req.EwsProvider.EwsPassword,
		}
	} else if req.LambdaProvider != nil {
		lambdaARN = req.LambdaProvider.LambdaArn
	}
	_, err := h.Backend.CreateAvailabilityConfiguration(req.OrganizationID, req.DomainName, ewsProv, lambdaARN)
	if err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type deleteAvailabilityConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
	DomainName     string `json:"DomainName"`
}

func (h *Handler) handleDeleteAvailabilityConfiguration(
	_ context.Context, req *deleteAvailabilityConfigReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteAvailabilityConfiguration(req.OrganizationID, req.DomainName)
}

type updateAvailabilityConfigReq struct {
	EwsProvider    *ewsProviderJSON    `json:"EwsProvider"`
	LambdaProvider *lambdaProviderJSON `json:"LambdaProvider"`
	OrganizationID string              `json:"OrganizationId"`
	DomainName     string              `json:"DomainName"`
}

func (h *Handler) handleUpdateAvailabilityConfiguration(
	_ context.Context, req *updateAvailabilityConfigReq,
) (*struct{}, error) {
	var ewsProv *AvailabilityEwsProvider
	var lambdaARN string
	if req.EwsProvider != nil {
		ewsProv = &AvailabilityEwsProvider{
			EwsEndpoint: req.EwsProvider.EwsEndpoint,
			EwsUsername: req.EwsProvider.EwsUsername,
			EwsPassword: req.EwsProvider.EwsPassword,
		}
	} else if req.LambdaProvider != nil {
		lambdaARN = req.LambdaProvider.LambdaArn
	}

	return &struct{}{}, h.Backend.UpdateAvailabilityConfiguration(
		req.OrganizationID,
		req.DomainName,
		ewsProv,
		lambdaARN,
	)
}

type listAvailabilityConfigsReq struct {
	OrganizationID string `json:"OrganizationId"`
	MaxResults     *int32 `json:"MaxResults"`
	NextToken      string `json:"NextToken"`
}

type availabilityConfigJSON struct {
	EwsProvider    *json.RawMessage `json:"EwsProvider,omitempty"`
	LambdaProvider *json.RawMessage `json:"LambdaProvider,omitempty"`
	DomainName     string           `json:"DomainName"`
	ProviderType   string           `json:"ProviderType"`
	DateCreated    int64            `json:"DateCreated"`
	DateModified   int64            `json:"DateModified"`
}

type listAvailabilityConfigsResp struct {
	NextToken                  string                   `json:"NextToken,omitempty"`
	AvailabilityConfigurations []availabilityConfigJSON `json:"AvailabilityConfigurations"`
}

func (h *Handler) handleListAvailabilityConfigurations(
	_ context.Context, req *listAvailabilityConfigsReq,
) (*listAvailabilityConfigsResp, error) {
	maxResults := int32(0)
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	cfgs, next, err := h.Backend.ListAvailabilityConfigurations(req.OrganizationID, maxResults, req.NextToken)
	if err != nil {
		return nil, err
	}
	result := make([]availabilityConfigJSON, 0, len(cfgs))
	for _, c := range cfgs {
		item := availabilityConfigJSON{
			DomainName:   c.DomainName,
			ProviderType: c.ProviderType,
			DateCreated:  c.DateCreated.Unix(),
			DateModified: c.DateModified.Unix(),
		}
		if c.ProviderType == "EWS" { //nolint:goconst // existing issue.
			raw, _ := json.Marshal(map[string]string{
				"EwsEndpoint": c.EwsEndpoint,
				"EwsUsername": c.EwsUsername,
			})
			rm := json.RawMessage(raw)
			item.EwsProvider = &rm
		} else {
			raw, _ := json.Marshal(map[string]string{"LambdaArn": c.LambdaARN})
			rm := json.RawMessage(raw)
			item.LambdaProvider = &rm
		}
		result = append(result, item)
	}

	return &listAvailabilityConfigsResp{AvailabilityConfigurations: result, NextToken: next}, nil
}

type testAvailabilityConfigReq struct {
	EwsProvider    *ewsProviderJSON    `json:"EwsProvider"`
	LambdaProvider *lambdaProviderJSON `json:"LambdaProvider"`
	OrganizationID string              `json:"OrganizationId"`
	DomainName     string              `json:"DomainName"`
}

type testAvailabilityConfigResp struct {
	FailureReason string `json:"FailureReason,omitempty"`
	TestPassed    bool   `json:"TestPassed"`
}

func (h *Handler) handleTestAvailabilityConfiguration(
	_ context.Context, req *testAvailabilityConfigReq,
) (*testAvailabilityConfigResp, error) {
	passed, reason, err := h.Backend.TestAvailabilityConfiguration(req.OrganizationID, req.DomainName)
	if err != nil {
		return nil, err
	}

	return &testAvailabilityConfigResp{TestPassed: passed, FailureReason: reason}, nil
}

// ---- Mobile Device Access Rules ----

type createMobileDeviceAccessRuleReq struct {
	OrganizationID            string   `json:"OrganizationId"`
	Name                      string   `json:"Name"`
	Effect                    string   `json:"Effect"`
	Description               string   `json:"Description"`
	DeviceModels              []string `json:"DeviceModels"`
	NotDeviceModels           []string `json:"NotDeviceModels"`
	DeviceTypes               []string `json:"DeviceTypes"`
	NotDeviceTypes            []string `json:"NotDeviceTypes"`
	DeviceOperatingSystems    []string `json:"DeviceOperatingSystems"`
	NotDeviceOperatingSystems []string `json:"NotDeviceOperatingSystems"`
	DeviceUserAgents          []string `json:"DeviceUserAgents"`
	NotDeviceUserAgents       []string `json:"NotDeviceUserAgents"`
}

type createMobileDeviceAccessRuleResp struct {
	MobileDeviceAccessRuleId string `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateMobileDeviceAccessRule(
	_ context.Context, req *createMobileDeviceAccessRuleReq,
) (*createMobileDeviceAccessRuleResp, error) {
	rule, err := h.Backend.CreateMobileDeviceAccessRule(
		req.OrganizationID, req.Name, req.Effect, req.Description,
		req.DeviceModels, req.NotDeviceModels, req.DeviceTypes, req.NotDeviceTypes,
		req.DeviceOperatingSystems, req.NotDeviceOperatingSystems, req.DeviceUserAgents, req.NotDeviceUserAgents,
	)
	if err != nil {
		return nil, err
	}

	return &createMobileDeviceAccessRuleResp{MobileDeviceAccessRuleId: rule.RuleID}, nil
}

type deleteMobileDeviceAccessRuleReq struct {
	OrganizationID           string `json:"OrganizationId"`
	MobileDeviceAccessRuleId string `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteMobileDeviceAccessRule(
	_ context.Context, req *deleteMobileDeviceAccessRuleReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteMobileDeviceAccessRule(req.OrganizationID, req.MobileDeviceAccessRuleId)
}

type updateMobileDeviceAccessRuleReq struct {
	OrganizationID            string   `json:"OrganizationId"`
	MobileDeviceAccessRuleId  string   `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
	Name                      string   `json:"Name"`
	Effect                    string   `json:"Effect"`
	Description               string   `json:"Description"`
	DeviceModels              []string `json:"DeviceModels"`
	NotDeviceModels           []string `json:"NotDeviceModels"`
	DeviceTypes               []string `json:"DeviceTypes"`
	NotDeviceTypes            []string `json:"NotDeviceTypes"`
	DeviceOperatingSystems    []string `json:"DeviceOperatingSystems"`
	NotDeviceOperatingSystems []string `json:"NotDeviceOperatingSystems"`
	DeviceUserAgents          []string `json:"DeviceUserAgents"`
	NotDeviceUserAgents       []string `json:"NotDeviceUserAgents"`
}

func (h *Handler) handleUpdateMobileDeviceAccessRule(
	_ context.Context, req *updateMobileDeviceAccessRuleReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.UpdateMobileDeviceAccessRule(
		req.OrganizationID, req.MobileDeviceAccessRuleId, req.Name, req.Effect, req.Description,
		req.DeviceModels, req.NotDeviceModels, req.DeviceTypes, req.NotDeviceTypes,
		req.DeviceOperatingSystems, req.NotDeviceOperatingSystems, req.DeviceUserAgents, req.NotDeviceUserAgents,
	)
}

type listMobileDeviceAccessRulesReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type mobileDeviceAccessRuleJSON struct {
	MobileDeviceAccessRuleId  string   `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
	Name                      string   `json:"Name"`
	Effect                    string   `json:"Effect"`
	Description               string   `json:"Description,omitempty"`
	DeviceModels              []string `json:"DeviceModels,omitempty"`
	NotDeviceModels           []string `json:"NotDeviceModels,omitempty"`
	DeviceTypes               []string `json:"DeviceTypes,omitempty"`
	NotDeviceTypes            []string `json:"NotDeviceTypes,omitempty"`
	DeviceOperatingSystems    []string `json:"DeviceOperatingSystems,omitempty"`
	NotDeviceOperatingSystems []string `json:"NotDeviceOperatingSystems,omitempty"`
	DeviceUserAgents          []string `json:"DeviceUserAgents,omitempty"`
	NotDeviceUserAgents       []string `json:"NotDeviceUserAgents,omitempty"`
	DateCreated               int64    `json:"DateCreated"`
	DateModified              int64    `json:"DateModified"`
}

type listMobileDeviceAccessRulesResp struct {
	Rules []mobileDeviceAccessRuleJSON `json:"Rules"`
}

func mobileRuleToJSON(r *MobileDeviceAccessRule) mobileDeviceAccessRuleJSON {
	return mobileDeviceAccessRuleJSON{
		MobileDeviceAccessRuleId:  r.RuleID,
		Name:                      r.Name,
		Effect:                    r.Effect,
		Description:               r.Description,
		DeviceModels:              r.DeviceModels,
		NotDeviceModels:           r.NotDeviceModels,
		DeviceTypes:               r.DeviceTypes,
		NotDeviceTypes:            r.NotDeviceTypes,
		DeviceOperatingSystems:    r.DeviceOperatingSystems,
		NotDeviceOperatingSystems: r.NotDeviceOperatingSystems,
		DeviceUserAgents:          r.DeviceUserAgents,
		NotDeviceUserAgents:       r.NotDeviceUserAgents,
		DateCreated:               r.DateCreated.Unix(),
		DateModified:              r.DateModified.Unix(),
	}
}

func (h *Handler) handleListMobileDeviceAccessRules(
	_ context.Context, req *listMobileDeviceAccessRulesReq,
) (*listMobileDeviceAccessRulesResp, error) {
	rules, err := h.Backend.ListMobileDeviceAccessRules(req.OrganizationID)
	if err != nil {
		return nil, err
	}
	result := make([]mobileDeviceAccessRuleJSON, 0, len(rules))
	for _, r := range rules {
		result = append(result, mobileRuleToJSON(r))
	}

	return &listMobileDeviceAccessRulesResp{Rules: result}, nil
}

type getMobileDeviceAccessEffectReq struct {
	OrganizationID        string `json:"OrganizationId"`
	DeviceType            string `json:"DeviceType"`
	DeviceModel           string `json:"DeviceModel"`
	DeviceOperatingSystem string `json:"DeviceOperatingSystem"`
	DeviceUserAgent       string `json:"DeviceUserAgent"`
}

type mobileDeviceMatchedRuleJSON struct {
	MobileDeviceAccessRuleId string `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
	Name                     string `json:"Name"`
}

type getMobileDeviceAccessEffectResp struct {
	Effect       string                        `json:"Effect"`
	MatchedRules []mobileDeviceMatchedRuleJSON `json:"MatchedRules"`
}

func (h *Handler) handleGetMobileDeviceAccessEffect(
	_ context.Context, req *getMobileDeviceAccessEffectReq,
) (*getMobileDeviceAccessEffectResp, error) {
	effect, matched, err := h.Backend.GetMobileDeviceAccessEffect(
		req.OrganizationID, req.DeviceType, req.DeviceModel, req.DeviceOperatingSystem, req.DeviceUserAgent,
	)
	if err != nil {
		return nil, err
	}
	matchedJSON := make([]mobileDeviceMatchedRuleJSON, 0, len(matched))
	for _, m := range matched {
		matchedJSON = append(matchedJSON, mobileDeviceMatchedRuleJSON{
			MobileDeviceAccessRuleId: m.RuleID,
			Name:                     m.Name,
		})
	}

	return &getMobileDeviceAccessEffectResp{Effect: effect, MatchedRules: matchedJSON}, nil
}

// ---- Mobile Device Access Overrides ----

type putMobileDeviceAccessOverrideReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId       string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
	Effect         string `json:"Effect"`
	Description    string `json:"Description"`
}

func (h *Handler) handlePutMobileDeviceAccessOverride(
	_ context.Context, req *putMobileDeviceAccessOverrideReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.PutMobileDeviceAccessOverride(
		req.OrganizationID, req.UserId, req.DeviceId, req.Effect, req.Description,
	)
}

type deleteMobileDeviceAccessOverrideReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId       string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteMobileDeviceAccessOverride(
	_ context.Context, req *deleteMobileDeviceAccessOverrideReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteMobileDeviceAccessOverride(
		req.OrganizationID, req.UserId, req.DeviceId,
	)
}

type getMobileDeviceAccessOverrideReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId       string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
}

type mobileDeviceAccessOverrideJSON struct {
	UserId       string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId     string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
	Effect       string `json:"Effect"`
	Description  string `json:"Description,omitempty"`
	DateCreated  int64  `json:"DateCreated"`
	DateModified int64  `json:"DateModified"`
}

func (h *Handler) handleGetMobileDeviceAccessOverride(
	_ context.Context, req *getMobileDeviceAccessOverrideReq,
) (*mobileDeviceAccessOverrideJSON, error) {
	ov, err := h.Backend.GetMobileDeviceAccessOverride(req.OrganizationID, req.UserId, req.DeviceId)
	if err != nil {
		return nil, err
	}

	return &mobileDeviceAccessOverrideJSON{
		UserId:       ov.UserID,
		DeviceId:     ov.DeviceID,
		Effect:       ov.Effect,
		Description:  ov.Description,
		DateCreated:  ov.DateCreated.Unix(),
		DateModified: ov.DateModified.Unix(),
	}, nil
}

type listMobileDeviceAccessOverridesReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId       string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
	MaxResults     *int32 `json:"MaxResults"`
	NextToken      string `json:"NextToken"`
}

type listMobileDeviceAccessOverridesResp struct {
	NextToken string                           `json:"NextToken,omitempty"`
	Overrides []mobileDeviceAccessOverrideJSON `json:"Overrides"`
}

func (h *Handler) handleListMobileDeviceAccessOverrides(
	_ context.Context, req *listMobileDeviceAccessOverridesReq,
) (*listMobileDeviceAccessOverridesResp, error) {
	maxResults := int32(0)
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	overrides, next, err := h.Backend.ListMobileDeviceAccessOverrides(
		req.OrganizationID, req.UserId, req.DeviceId, maxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}
	result := make([]mobileDeviceAccessOverrideJSON, 0, len(overrides))
	for _, ov := range overrides {
		result = append(result, mobileDeviceAccessOverrideJSON{
			UserId:       ov.UserID,
			DeviceId:     ov.DeviceID,
			Effect:       ov.Effect,
			Description:  ov.Description,
			DateCreated:  ov.DateCreated.Unix(),
			DateModified: ov.DateModified.Unix(),
		})
	}

	return &listMobileDeviceAccessOverridesResp{Overrides: result, NextToken: next}, nil
}

// ---- Email Monitoring Configuration ----

type putEmailMonitoringConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
	RoleArn        string `json:"RoleArn"`
	LogGroupArn    string `json:"LogGroupArn"`
}

func (h *Handler) handlePutEmailMonitoringConfiguration(
	_ context.Context, req *putEmailMonitoringConfigReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.PutEmailMonitoringConfiguration(req.OrganizationID, req.RoleArn, req.LogGroupArn)
}

type deleteEmailMonitoringConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
}

func (h *Handler) handleDeleteEmailMonitoringConfiguration(
	_ context.Context, req *deleteEmailMonitoringConfigReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteEmailMonitoringConfiguration(req.OrganizationID)
}

type describeEmailMonitoringConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type describeEmailMonitoringConfigResp struct {
	RoleArn     string `json:"RoleArn,omitempty"`
	LogGroupArn string `json:"LogGroupArn,omitempty"`
}

func (h *Handler) handleDescribeEmailMonitoringConfiguration(
	_ context.Context, req *describeEmailMonitoringConfigReq,
) (*describeEmailMonitoringConfigResp, error) {
	cfg, err := h.Backend.DescribeEmailMonitoringConfiguration(req.OrganizationID)
	if err != nil {
		return nil, err
	}

	return &describeEmailMonitoringConfigResp{RoleArn: cfg.RoleARN, LogGroupArn: cfg.LogGroupARN}, nil
}

// ---- Inbound DMARC Settings ----

type putInboundDmarcSettingsReq struct {
	OrganizationID string `json:"OrganizationId"`
	Enforced       bool   `json:"Enforced"`
}

func (h *Handler) handlePutInboundDmarcSettings(
	_ context.Context, req *putInboundDmarcSettingsReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.PutInboundDmarcSettings(req.OrganizationID, req.Enforced)
}

type describeInboundDmarcSettingsReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type describeInboundDmarcSettingsResp struct {
	Enforced bool `json:"Enforced"`
}

func (h *Handler) handleDescribeInboundDmarcSettings(
	_ context.Context, req *describeInboundDmarcSettingsReq,
) (*describeInboundDmarcSettingsResp, error) {
	enforced, err := h.Backend.DescribeInboundDmarcSettings(req.OrganizationID)
	if err != nil {
		return nil, err
	}

	return &describeInboundDmarcSettingsResp{Enforced: enforced}, nil
}

// ---- Retention Policies ----

type folderConfigJSON struct {
	Period *int32 `json:"Period,omitempty"`
	Name   string `json:"Name"`
	Action string `json:"Action"`
}

type putRetentionPolicyReq struct {
	OrganizationID       string             `json:"OrganizationId"`
	ID                   string             `json:"Id"`
	Name                 string             `json:"Name"`
	Description          string             `json:"Description"`
	FolderConfigurations []folderConfigJSON `json:"FolderConfigurations"`
}

func (h *Handler) handlePutRetentionPolicy(
	_ context.Context, req *putRetentionPolicyReq,
) (*struct{}, error) {
	folderCfgs := make([]*FolderConfiguration, 0, len(req.FolderConfigurations))
	for _, fc := range req.FolderConfigurations {
		folderCfgs = append(folderCfgs, &FolderConfiguration{
			Name:   fc.Name,
			Action: fc.Action,
			Period: fc.Period,
		})
	}

	return &struct{}{}, h.Backend.PutRetentionPolicy(
		req.OrganizationID, req.ID, req.Name, req.Description, folderCfgs,
	)
}

type deleteRetentionPolicyReq struct {
	OrganizationID string `json:"OrganizationId"`
	Id             string `json:"Id"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteRetentionPolicy(
	_ context.Context, req *deleteRetentionPolicyReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteRetentionPolicy(req.OrganizationID, req.Id)
}

type getDefaultRetentionPolicyReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type getDefaultRetentionPolicyResp struct {
	Id                   string             `json:"Id,omitempty"` //nolint:revive,staticcheck // existing issue.
	Name                 string             `json:"Name,omitempty"`
	Description          string             `json:"Description,omitempty"`
	FolderConfigurations []folderConfigJSON `json:"FolderConfigurations"`
}

func (h *Handler) handleGetDefaultRetentionPolicy(
	_ context.Context, req *getDefaultRetentionPolicyReq,
) (*getDefaultRetentionPolicyResp, error) {
	pol, err := h.Backend.GetDefaultRetentionPolicy(req.OrganizationID)
	if err != nil {
		return nil, err
	}
	folderCfgs := make([]folderConfigJSON, 0, len(pol.FolderConfigurations))
	for _, fc := range pol.FolderConfigurations {
		folderCfgs = append(folderCfgs, folderConfigJSON{
			Name:   fc.Name,
			Action: fc.Action,
			Period: fc.Period,
		})
	}

	return &getDefaultRetentionPolicyResp{
		Id:                   pol.ID,
		Name:                 pol.Name,
		Description:          pol.Description,
		FolderConfigurations: folderCfgs,
	}, nil
}

// ---- Mailbox Export Jobs ----

type startMailboxExportJobReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityId       string `json:"EntityId"` //nolint:revive,staticcheck // existing issue.
	Description    string `json:"Description"`
	RoleArn        string `json:"RoleArn"`
	KmsKeyArn      string `json:"KmsKeyArn"`
	S3BucketName   string `json:"S3BucketName"`
	S3Prefix       string `json:"S3Prefix"`
}

type startMailboxExportJobResp struct {
	JobId string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleStartMailboxExportJob(
	_ context.Context, req *startMailboxExportJobReq,
) (*startMailboxExportJobResp, error) {
	job, err := h.Backend.StartMailboxExportJob(
		req.OrganizationID, req.EntityId, req.Description, req.RoleArn, req.KmsKeyArn,
		req.S3BucketName, req.S3Prefix,
	)
	if err != nil {
		return nil, err
	}

	return &startMailboxExportJobResp{JobId: job.JobID}, nil
}

type cancelMailboxExportJobReq struct {
	OrganizationID string `json:"OrganizationId"`
	JobId          string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCancelMailboxExportJob(
	_ context.Context, req *cancelMailboxExportJobReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.CancelMailboxExportJob(req.OrganizationID, req.JobId)
}

type describeMailboxExportJobReq struct {
	OrganizationID string `json:"OrganizationId"`
	JobId          string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

type describeMailboxExportJobResp struct {
	S3Prefix          string `json:"S3Prefix,omitempty"`
	EntityId          string `json:"EntityId,omitempty"` //nolint:revive,staticcheck // existing issue.
	Description       string `json:"Description,omitempty"`
	RoleArn           string `json:"RoleArn,omitempty"`
	KmsKeyArn         string `json:"KmsKeyArn,omitempty"`
	S3BucketName      string `json:"S3BucketName,omitempty"`
	JobId             string `json:"JobId,omitempty"` //nolint:revive,staticcheck // existing issue.
	S3Path            string `json:"S3Path,omitempty"`
	State             string `json:"State,omitempty"`
	ErrorInfo         string `json:"ErrorInfo,omitempty"`
	StartTime         int64  `json:"StartTime,omitempty"`
	EndTime           int64  `json:"EndTime,omitempty"`
	EstimatedProgress int32  `json:"EstimatedProgress"`
}

func (h *Handler) handleDescribeMailboxExportJob(
	_ context.Context, req *describeMailboxExportJobReq,
) (*describeMailboxExportJobResp, error) {
	job, err := h.Backend.DescribeMailboxExportJob(req.OrganizationID, req.JobId)
	if err != nil {
		return nil, err
	}
	resp := &describeMailboxExportJobResp{
		JobId:             job.JobID,
		EntityId:          job.EntityID,
		Description:       job.Description,
		RoleArn:           job.RoleARN,
		KmsKeyArn:         job.KmsKeyARN,
		S3BucketName:      job.S3BucketName,
		S3Prefix:          job.S3Prefix,
		S3Path:            job.S3Path,
		EstimatedProgress: job.EstimatedProgress,
		State:             job.State,
		ErrorInfo:         job.ErrorInfo,
		StartTime:         job.StartTime.Unix(),
	}
	if !job.EndTime.IsZero() {
		resp.EndTime = job.EndTime.Unix()
	}

	return resp, nil
}

type listMailboxExportJobsReq struct {
	OrganizationID string `json:"OrganizationId"`
	MaxResults     *int32 `json:"MaxResults"`
	NextToken      string `json:"NextToken"`
}

type mailboxExportJobSummaryJSON struct {
	JobId        string `json:"JobId"`    //nolint:revive,staticcheck // existing issue.
	EntityId     string `json:"EntityId"` //nolint:revive,staticcheck // existing issue.
	Description  string `json:"Description,omitempty"`
	S3BucketName string `json:"S3BucketName,omitempty"`
	State        string `json:"State"`
	StartTime    int64  `json:"StartTime,omitempty"`
	EndTime      int64  `json:"EndTime,omitempty"`
}

type listMailboxExportJobsResp struct {
	NextToken string                        `json:"NextToken,omitempty"`
	Jobs      []mailboxExportJobSummaryJSON `json:"Jobs"`
}

func (h *Handler) handleListMailboxExportJobs(
	_ context.Context, req *listMailboxExportJobsReq,
) (*listMailboxExportJobsResp, error) {
	maxResults := int32(0)
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	jobs, next, err := h.Backend.ListMailboxExportJobs(req.OrganizationID, maxResults, req.NextToken)
	if err != nil {
		return nil, err
	}
	result := make([]mailboxExportJobSummaryJSON, 0, len(jobs))
	for _, j := range jobs {
		item := mailboxExportJobSummaryJSON{
			JobId:        j.JobID,
			EntityId:     j.EntityID,
			Description:  j.Description,
			S3BucketName: j.S3BucketName,
			State:        j.State,
			StartTime:    j.StartTime.Unix(),
		}
		if !j.EndTime.IsZero() {
			item.EndTime = j.EndTime.Unix()
		}
		result = append(result, item)
	}

	return &listMailboxExportJobsResp{Jobs: result, NextToken: next}, nil
}

// ---- Identity Center Applications ----

type createIdentityCenterApplicationReq struct {
	InstanceArn string `json:"InstanceArn"`
	Name        string `json:"Name"`
}

type createIdentityCenterApplicationResp struct {
	ApplicationArn string `json:"ApplicationArn"`
}

func (h *Handler) handleCreateIdentityCenterApplication(
	_ context.Context, req *createIdentityCenterApplicationReq,
) (*createIdentityCenterApplicationResp, error) {
	appARN, err := h.Backend.CreateIdentityCenterApplication(req.InstanceArn, req.Name)
	if err != nil {
		return nil, err
	}

	return &createIdentityCenterApplicationResp{ApplicationArn: appARN}, nil
}

type deleteIdentityCenterApplicationReq struct {
	ApplicationArn string `json:"ApplicationArn"`
}

func (h *Handler) handleDeleteIdentityCenterApplication(
	_ context.Context, req *deleteIdentityCenterApplicationReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteIdentityCenterApplication(req.ApplicationArn)
}

// ---- Identity Provider Configuration ----

type identityCenterConfigJSON struct {
	ApplicationArn string `json:"ApplicationArn"`
	InstanceArn    string `json:"InstanceArn"`
}

type personalAccessTokenConfigJSON struct {
	LifetimeInDays *int32 `json:"LifetimeInDays,omitempty"`
	Status         string `json:"Status"`
}

type putIdentityProviderConfigReq struct {
	IdentityCenterConfiguration      *identityCenterConfigJSON      `json:"IdentityCenterConfiguration"`
	PersonalAccessTokenConfiguration *personalAccessTokenConfigJSON `json:"PersonalAccessTokenConfiguration"`
	OrganizationID                   string                         `json:"OrganizationId"`
	AuthenticationMode               string                         `json:"AuthenticationMode"`
}

func (h *Handler) handlePutIdentityProviderConfiguration(
	_ context.Context, req *putIdentityProviderConfigReq,
) (*struct{}, error) {
	appARN, instanceARN, patStatus := "", "", ""
	var patLifetime int32
	if req.IdentityCenterConfiguration != nil {
		appARN = req.IdentityCenterConfiguration.ApplicationArn
		instanceARN = req.IdentityCenterConfiguration.InstanceArn
	}
	if req.PersonalAccessTokenConfiguration != nil {
		patStatus = req.PersonalAccessTokenConfiguration.Status
		if req.PersonalAccessTokenConfiguration.LifetimeInDays != nil {
			patLifetime = *req.PersonalAccessTokenConfiguration.LifetimeInDays
		}
	}

	return &struct{}{}, h.Backend.PutIdentityProviderConfiguration(
		req.OrganizationID, req.AuthenticationMode, appARN, instanceARN, patStatus, patLifetime,
	)
}

type deleteIdentityProviderConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
}

func (h *Handler) handleDeleteIdentityProviderConfiguration(
	_ context.Context, req *deleteIdentityProviderConfigReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteIdentityProviderConfiguration(req.OrganizationID)
}

type describeIdentityProviderConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type describeIdentityProviderConfigResp struct {
	IdentityCenterConfiguration      *identityCenterConfigJSON      `json:"IdentityCenterConfiguration,omitempty"`
	PersonalAccessTokenConfiguration *personalAccessTokenConfigJSON `json:"PersonalAccessTokenConfiguration,omitempty"`
	AuthenticationMode               string                         `json:"AuthenticationMode,omitempty"`
}

func (h *Handler) handleDescribeIdentityProviderConfiguration(
	_ context.Context, req *describeIdentityProviderConfigReq,
) (*describeIdentityProviderConfigResp, error) {
	cfg, err := h.Backend.DescribeIdentityProviderConfiguration(req.OrganizationID)
	if err != nil {
		return nil, err
	}
	resp := &describeIdentityProviderConfigResp{AuthenticationMode: cfg.AuthMode}
	if cfg.IdentityCenterAppARN != "" || cfg.IdentityCenterInstanceARN != "" {
		resp.IdentityCenterConfiguration = &identityCenterConfigJSON{
			ApplicationArn: cfg.IdentityCenterAppARN,
			InstanceArn:    cfg.IdentityCenterInstanceARN,
		}
	}
	if cfg.PATStatus != "" {
		resp.PersonalAccessTokenConfiguration = &personalAccessTokenConfigJSON{
			Status:         cfg.PATStatus,
			LifetimeInDays: cfg.PATLifetimeDays,
		}
	}

	return resp, nil
}

// ---- Personal Access Tokens ----

type deletePersonalAccessTokenReq struct {
	OrganizationID        string `json:"OrganizationId"`
	PersonalAccessTokenId string `json:"PersonalAccessTokenId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeletePersonalAccessToken(
	_ context.Context, req *deletePersonalAccessTokenReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeletePersonalAccessToken(req.OrganizationID, req.PersonalAccessTokenId)
}

type getPersonalAccessTokenMetadataReq struct {
	OrganizationID        string `json:"OrganizationId"`
	PersonalAccessTokenId string `json:"PersonalAccessTokenId"` //nolint:revive,staticcheck // existing issue.
}

type personalAccessTokenMetadataResp struct {
	PersonalAccessTokenId string   `json:"PersonalAccessTokenId,omitempty"` //nolint:revive,staticcheck // existing issue.
	UserId                string   `json:"UserId,omitempty"`                //nolint:revive // existing issue.
	Name                  string   `json:"Name,omitempty"`
	Scopes                []string `json:"Scopes,omitempty"`
	DateCreated           int64    `json:"DateCreated,omitempty"`
	DateLastUsed          int64    `json:"DateLastUsed,omitempty"`
	ExpiresTime           int64    `json:"ExpiresTime,omitempty"`
}

func (h *Handler) handleGetPersonalAccessTokenMetadata(
	_ context.Context, req *getPersonalAccessTokenMetadataReq,
) (*personalAccessTokenMetadataResp, error) {
	tok, err := h.Backend.GetPersonalAccessTokenMetadata(req.OrganizationID, req.PersonalAccessTokenId)
	if err != nil {
		return nil, err
	}

	return &personalAccessTokenMetadataResp{
		PersonalAccessTokenId: tok.TokenID,
		UserId:                tok.UserID,
		Name:                  tok.Name,
		DateCreated:           tok.DateCreated.Unix(),
		DateLastUsed:          tok.DateLastUsed.Unix(),
		ExpiresTime:           tok.ExpiresTime.Unix(),
		Scopes:                tok.Scopes,
	}, nil
}

type listPersonalAccessTokensReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"` //nolint:revive,staticcheck // existing issue.
	MaxResults     *int32 `json:"MaxResults"`
	NextToken      string `json:"NextToken"`
}

type personalAccessTokenSummaryJSON struct {
	PersonalAccessTokenId string   `json:"PersonalAccessTokenId"` //nolint:revive,staticcheck // existing issue.
	UserId                string   `json:"UserId,omitempty"`      //nolint:revive // existing issue.
	Name                  string   `json:"Name,omitempty"`
	Scopes                []string `json:"Scopes,omitempty"`
	DateCreated           int64    `json:"DateCreated,omitempty"`
	DateLastUsed          int64    `json:"DateLastUsed,omitempty"`
	ExpiresTime           int64    `json:"ExpiresTime,omitempty"`
}

type listPersonalAccessTokensResp struct {
	NextToken                    string                           `json:"NextToken,omitempty"`
	PersonalAccessTokenSummaries []personalAccessTokenSummaryJSON `json:"PersonalAccessTokenSummaries"`
}

func (h *Handler) handleListPersonalAccessTokens(
	_ context.Context, req *listPersonalAccessTokensReq,
) (*listPersonalAccessTokensResp, error) {
	maxResults := int32(0)
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	tokens, next, err := h.Backend.ListPersonalAccessTokens(req.OrganizationID, req.UserId, maxResults, req.NextToken)
	if err != nil {
		return nil, err
	}
	result := make([]personalAccessTokenSummaryJSON, 0, len(tokens))
	for _, tok := range tokens {
		result = append(result, personalAccessTokenSummaryJSON{
			PersonalAccessTokenId: tok.TokenID,
			UserId:                tok.UserID,
			Name:                  tok.Name,
			DateCreated:           tok.DateCreated.Unix(),
			DateLastUsed:          tok.DateLastUsed.Unix(),
			ExpiresTime:           tok.ExpiresTime.Unix(),
			Scopes:                tok.Scopes,
		})
	}

	return &listPersonalAccessTokensResp{PersonalAccessTokenSummaries: result, NextToken: next}, nil
}

// ---- Impersonation Role Effect ----

type getImpersonationRoleEffectReq struct {
	OrganizationID      string `json:"OrganizationId"`
	ImpersonationRoleId string `json:"ImpersonationRoleId"` //nolint:revive,staticcheck // existing issue.
	TargetUser          string `json:"TargetUser"`
}

type impersonationMatchedRuleJSON struct {
	ImpersonationRuleId string `json:"ImpersonationRuleId"` //nolint:revive,staticcheck // existing issue.
	Name                string `json:"Name,omitempty"`
}

type getImpersonationRoleEffectResp struct {
	Effect       string                         `json:"Effect,omitempty"`
	Type         string                         `json:"Type,omitempty"`
	MatchedRules []impersonationMatchedRuleJSON `json:"MatchedRules,omitempty"`
}

func (h *Handler) handleGetImpersonationRoleEffect(
	_ context.Context, req *getImpersonationRoleEffectReq,
) (*getImpersonationRoleEffectResp, error) {
	effect, roleType, matched, err := h.Backend.GetImpersonationRoleEffect(
		req.OrganizationID, req.ImpersonationRoleId, req.TargetUser,
	)
	if err != nil {
		return nil, err
	}
	matchedJSON := make([]impersonationMatchedRuleJSON, 0, len(matched))
	for _, m := range matched {
		matchedJSON = append(matchedJSON, impersonationMatchedRuleJSON{
			ImpersonationRuleId: m.RuleID,
			Name:                m.Name,
		})
	}

	return &getImpersonationRoleEffectResp{Effect: effect, Type: roleType, MatchedRules: matchedJSON}, nil
}

// ---- Assume Impersonation Role ----

type assumeImpersonationRoleReq struct {
	OrganizationID      string `json:"OrganizationId"`
	ImpersonationRoleId string `json:"ImpersonationRoleId"` //nolint:revive,staticcheck // existing issue.
}

type assumeImpersonationRoleResp struct {
	Token     string `json:"Token,omitempty"`
	ExpiresIn int64  `json:"ExpiresIn,omitempty"`
}

func (h *Handler) handleAssumeImpersonationRole(
	_ context.Context, req *assumeImpersonationRoleReq,
) (*assumeImpersonationRoleResp, error) {
	token, expiresIn, err := h.Backend.AssumeImpersonationRole(req.OrganizationID, req.ImpersonationRoleId)
	if err != nil {
		return nil, err
	}

	return &assumeImpersonationRoleResp{Token: token, ExpiresIn: expiresIn}, nil
}
