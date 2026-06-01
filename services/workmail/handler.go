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
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
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
	GroupID      string `json:"GroupId"`
	Name         string `json:"Name"`
	Email        string `json:"Email,omitempty"`
	State        string `json:"State"`
	EnabledDate  int64  `json:"EnabledDate,omitempty"`
	DisabledDate int64  `json:"DisabledDate,omitempty"`
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
