package workmail

import (
	"context"
)

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
		role = roleUser
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
	UserRole    string `json:"UserRole,omitempty"`
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
			UserRole:    u.Role,
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
