package workmail

import (
	"context"
)

// ---- Users ----

type createUserReq struct {
	OrganizationID              string `json:"OrganizationId"`
	Name                        string `json:"Name"`
	DisplayName                 string `json:"DisplayName"`
	Password                    string `json:"Password"`
	Role                        string `json:"Role"`
	FirstName                   string `json:"FirstName"`
	LastName                    string `json:"LastName"`
	IdentityProviderUserID      string `json:"IdentityProviderUserId"`
	HiddenFromGlobalAddressList bool   `json:"HiddenFromGlobalAddressList"`
}

type createUserResp struct {
	UserID string `json:"UserId"`
}

func (h *Handler) handleCreateUser(_ context.Context, req *createUserReq) (*createUserResp, error) {
	role := req.Role
	if role == "" {
		role = roleUser
	}
	u, err := h.Backend.CreateUser(req.OrganizationID, req.Name, CreateUserParams{
		DisplayName:                 req.DisplayName,
		Password:                    req.Password,
		Role:                        role,
		FirstName:                   req.FirstName,
		LastName:                    req.LastName,
		IdentityProviderUserID:      req.IdentityProviderUserID,
		HiddenFromGlobalAddressList: req.HiddenFromGlobalAddressList,
	})
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
	UserID                          string `json:"UserId"`
	Name                            string `json:"Name"`
	Email                           string `json:"Email,omitempty"`
	DisplayName                     string `json:"DisplayName,omitempty"`
	FirstName                       string `json:"FirstName,omitempty"`
	LastName                        string `json:"LastName,omitempty"`
	State                           string `json:"State"`
	UserRole                        string `json:"UserRole"`
	City                            string `json:"City,omitempty"`
	Company                         string `json:"Company,omitempty"`
	Country                         string `json:"Country,omitempty"`
	Department                      string `json:"Department,omitempty"`
	Initials                        string `json:"Initials,omitempty"`
	JobTitle                        string `json:"JobTitle,omitempty"`
	Office                          string `json:"Office,omitempty"`
	Street                          string `json:"Street,omitempty"`
	Telephone                       string `json:"Telephone,omitempty"`
	ZipCode                         string `json:"ZipCode,omitempty"`
	IdentityProviderIdentityStoreID string `json:"IdentityProviderIdentityStoreId,omitempty"`
	IdentityProviderUserID          string `json:"IdentityProviderUserId,omitempty"`
	EnabledDate                     int64  `json:"EnabledDate,omitempty"`
	DisabledDate                    int64  `json:"DisabledDate,omitempty"`
	MailboxProvisionedDate          int64  `json:"MailboxProvisionedDate,omitempty"`
	MailboxDeprovisionedDate        int64  `json:"MailboxDeprovisionedDate,omitempty"`
	HiddenFromGlobalAddressList     bool   `json:"HiddenFromGlobalAddressList"`
}

func (h *Handler) handleDescribeUser(_ context.Context, req *describeUserReq) (*describeUserResp, error) {
	u, err := h.Backend.DescribeUser(req.OrganizationID, req.UserID)
	if err != nil {
		return nil, err
	}

	resp := &describeUserResp{
		UserID:                          u.UserID,
		Name:                            u.Name,
		Email:                           u.Email,
		DisplayName:                     u.DisplayName,
		FirstName:                       u.FirstName,
		LastName:                        u.LastName,
		State:                           u.State,
		UserRole:                        u.Role,
		City:                            u.City,
		Company:                         u.Company,
		Country:                         u.Country,
		Department:                      u.Department,
		Initials:                        u.Initials,
		JobTitle:                        u.JobTitle,
		Office:                          u.Office,
		Street:                          u.Street,
		Telephone:                       u.Telephone,
		ZipCode:                         u.ZipCode,
		IdentityProviderIdentityStoreID: u.IdentityProviderIdentityStoreID,
		IdentityProviderUserID:          u.IdentityProviderUserID,
		HiddenFromGlobalAddressList:     u.HiddenFromGlobalAddressList,
	}
	if !u.EnabledDate.IsZero() {
		resp.EnabledDate = u.EnabledDate.Unix()
	}
	if !u.DisabledDate.IsZero() {
		resp.DisabledDate = u.DisabledDate.Unix()
	}
	if !u.MailboxProvisionedDate.IsZero() {
		resp.MailboxProvisionedDate = u.MailboxProvisionedDate.Unix()
	}
	if !u.MailboxDeprovisionedDate.IsZero() {
		resp.MailboxDeprovisionedDate = u.MailboxDeprovisionedDate.Unix()
	}

	return resp, nil
}

type updateUserReq struct {
	HiddenFromGlobalAddressList *bool  `json:"HiddenFromGlobalAddressList"`
	Department                  string `json:"Department"`
	JobTitle                    string `json:"JobTitle"`
	FirstName                   string `json:"FirstName"`
	LastName                    string `json:"LastName"`
	City                        string `json:"City"`
	Company                     string `json:"Company"`
	Country                     string `json:"Country"`
	OrganizationID              string `json:"OrganizationId"`
	DisplayName                 string `json:"DisplayName"`
	Office                      string `json:"Office"`
	Initials                    string `json:"Initials"`
	Street                      string `json:"Street"`
	Telephone                   string `json:"Telephone"`
	ZipCode                     string `json:"ZipCode"`
	IdentityProviderUserID      string `json:"IdentityProviderUserId"`
	Role                        string `json:"Role"`
	UserID                      string `json:"UserId"`
}

type emptyResp struct{}

func (h *Handler) handleUpdateUser(_ context.Context, req *updateUserReq) (*emptyResp, error) {
	if err := h.Backend.UpdateUser(req.OrganizationID, req.UserID, UpdateUserParams{
		DisplayName:                 req.DisplayName,
		FirstName:                   req.FirstName,
		LastName:                    req.LastName,
		City:                        req.City,
		Company:                     req.Company,
		Country:                     req.Country,
		Department:                  req.Department,
		Initials:                    req.Initials,
		JobTitle:                    req.JobTitle,
		Office:                      req.Office,
		Street:                      req.Street,
		Telephone:                   req.Telephone,
		ZipCode:                     req.ZipCode,
		IdentityProviderUserID:      req.IdentityProviderUserID,
		Role:                        req.Role,
		HiddenFromGlobalAddressList: req.HiddenFromGlobalAddressList,
	}); err != nil {
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

// listUsersFiltersReq mirrors aws-sdk-go-v2/service/workmail/types.
// ListUsersFilters, the ListUsersInput.Filters wire shape.
type listUsersFiltersReq struct {
	DisplayNamePrefix            string `json:"DisplayNamePrefix"`
	PrimaryEmailPrefix           string `json:"PrimaryEmailPrefix"`
	State                        string `json:"State"`
	UsernamePrefix               string `json:"UsernamePrefix"`
	IdentityProviderUserIDPrefix string `json:"IdentityProviderUserIdPrefix"`
}

type listUsersReq struct {
	Filters        *listUsersFiltersReq `json:"Filters"`
	OrganizationID string               `json:"OrganizationId"`
	NextToken      string               `json:"NextToken"`
	MaxResults     int32                `json:"MaxResults"`
}

type userSummaryResp struct {
	ID           string `json:"Id"`
	Name         string `json:"Name"`
	Email        string `json:"Email,omitempty"`
	DisplayName  string `json:"DisplayName,omitempty"`
	State        string `json:"State"`
	UserRole     string `json:"UserRole,omitempty"`
	EnabledDate  int64  `json:"EnabledDate,omitempty"`
	DisabledDate int64  `json:"DisabledDate,omitempty"`
}

type listUsersResp struct {
	NextToken string            `json:"NextToken,omitempty"`
	Users     []userSummaryResp `json:"Users"`
}

func (h *Handler) handleListUsers(_ context.Context, req *listUsersReq) (*listUsersResp, error) {
	var filter *UserFilter
	if req.Filters != nil {
		filter = &UserFilter{
			DisplayNamePrefix:            req.Filters.DisplayNamePrefix,
			PrimaryEmailPrefix:           req.Filters.PrimaryEmailPrefix,
			State:                        req.Filters.State,
			UsernamePrefix:               req.Filters.UsernamePrefix,
			IdentityProviderUserIDPrefix: req.Filters.IdentityProviderUserIDPrefix,
		}
	}

	users, next, err := h.Backend.ListUsers(req.OrganizationID, filter, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]userSummaryResp, 0, len(users))
	for _, u := range users {
		s := userSummaryResp{
			ID:          u.UserID,
			Name:        u.Name,
			Email:       u.Email,
			DisplayName: u.DisplayName,
			State:       u.State,
			UserRole:    u.Role,
		}
		if !u.EnabledDate.IsZero() {
			s.EnabledDate = u.EnabledDate.Unix()
		}
		if !u.DisabledDate.IsZero() {
			s.DisabledDate = u.DisabledDate.Unix()
		}
		summaries = append(summaries, s)
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
