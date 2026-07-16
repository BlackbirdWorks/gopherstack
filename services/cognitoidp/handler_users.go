package cognitoidp

import (
	"context"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleAdminCreateUser(_ context.Context, in *adminCreateUserInput) (*adminCreateUserOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.Backend.AdminCreateUserWithPolicy(in.UserPoolID, in.Username, in.TemporaryPassword, attrs)
	if err != nil {
		return nil, err
	}

	return &adminCreateUserOutput{
		User: adminUserType{
			Username:       user.Username,
			UserStatus:     user.Status,
			UserCreateDate: float64(user.CreatedAt.Unix()),
			Attributes:     sortedAttributeList(user.Attributes),
			Enabled:        user.Enabled,
		},
	}, nil
}

func (h *Handler) handleAdminSetUserPassword(
	_ context.Context,
	in *adminSetUserPasswordInput,
) (*adminSetUserPasswordOutput, error) {
	if err := h.Backend.AdminSetUserPassword(in.UserPoolID, in.Username, in.Password, in.Permanent); err != nil {
		return nil, err
	}

	return &adminSetUserPasswordOutput{}, nil
}

func (h *Handler) handleAdminGetUser(_ context.Context, in *adminGetUserInput) (*adminGetUserOutput, error) {
	user, err := h.Backend.AdminGetUser(in.UserPoolID, in.Username)
	if err != nil {
		return nil, err
	}

	attrs := userAttrsWithSub(user)

	updatedAt := user.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = user.CreatedAt
	}

	return &adminGetUserOutput{
		Username:             user.Username,
		UserStatus:           user.Status,
		UserCreateDate:       float64(user.CreatedAt.Unix()),
		UserLastModifiedDate: float64(updatedAt.Unix()),
		UserAttributes:       sortedAttributeList(attrs),
		Enabled:              user.Enabled,
		PreferredMfaSetting:  user.PreferredMfaSetting,
		UserMFASettingList:   user.UserMFASettingList,
	}, nil
}

// userAttrsWithSub returns a copy of the user's attribute map with the 'sub' attribute injected.
func userAttrsWithSub(u *User) map[string]string {
	attrs := maps.Clone(u.Attributes)
	if attrs == nil {
		attrs = make(map[string]string)
	}

	attrs["sub"] = u.Sub

	return attrs
}

func (h *Handler) handleAdminDeleteUser(
	_ context.Context,
	in *adminDeleteUserInput,
) (*adminDeleteUserOutput, error) {
	if err := h.Backend.AdminDeleteUser(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminDeleteUserOutput{}, nil
}

// toUserSummary converts a backend User to a userSummary for API responses.
func toUserSummary(u *User) *userSummary {
	updatedAt := u.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = u.CreatedAt
	}

	return &userSummary{
		Username:         u.Username,
		UserStatus:       u.Status,
		UserCreateDate:   float64(u.CreatedAt.Unix()),
		UserLastModified: float64(updatedAt.Unix()),
		Attributes:       sortedAttributeList(userAttrsWithSub(u)),
		Enabled:          u.Enabled,
	}
}

func (h *Handler) handleListUsers(
	_ context.Context,
	in *listUsersInput,
) (*listUsersOutput, error) {
	limit, err := validateCognitoMaxResults(in.Limit)
	if err != nil {
		return nil, err
	}

	// ListUsersFiltered already returns users sorted by username, giving a
	// stable ordering for pagination tokens.
	users, err := h.Backend.ListUsersFiltered(in.UserPoolID, in.Filter)
	if err != nil {
		return nil, err
	}

	start := 0
	if in.PaginationToken != "" {
		for i, u := range users {
			if u.Username == in.PaginationToken {
				start = i

				break
			}
		}
	}

	users = users[start:]

	nextToken := ""
	if len(users) > limit {
		nextToken = users[limit].Username
		users = users[:limit]
	}

	summaries := make([]*userSummary, 0, len(users))
	for _, u := range users {
		summaries = append(summaries, toUserSummary(u))
	}

	return &listUsersOutput{Users: summaries, PaginationToken: nextToken}, nil
}

func (h *Handler) handleGetUser(
	_ context.Context,
	in *getUserInput,
) (*getUserOutput, error) {
	user, err := h.Backend.GetUser(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &getUserOutput{
		Username:       user.Username,
		UserAttributes: sortedAttributeList(userAttrsWithSub(user)),
	}, nil
}

func (h *Handler) handleAdminDisableUser(
	_ context.Context,
	in *adminDisableUserInput,
) (*adminDisableUserOutput, error) {
	if err := h.Backend.AdminDisableUser(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminDisableUserOutput{}, nil
}

func (h *Handler) handleAdminEnableUser(
	_ context.Context,
	in *adminEnableUserInput,
) (*adminEnableUserOutput, error) {
	if err := h.Backend.AdminEnableUser(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminEnableUserOutput{}, nil
}

func (h *Handler) handleDeleteUser(_ context.Context, in *deleteUserInput) (*deleteUserOutput, error) {
	if err := h.Backend.DeleteUser(in.AccessToken); err != nil {
		return nil, err
	}

	return &deleteUserOutput{}, nil
}

func (h *Handler) handleGetUserAccurate(
	_ context.Context,
	in *getUserAccurateInput,
) (*getUserWithMFAOutput, error) {
	user, err := h.Backend.GetUser(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &getUserWithMFAOutput{
		Username:            user.Username,
		UserAttributes:      sortedAttributeList(userAttrsWithSub(user)),
		UserMFASettingList:  user.UserMFASettingList,
		PreferredMfaSetting: user.PreferredMfaSetting,
	}, nil
}

func (h *Handler) handleAdminCreateUserFull(
	_ context.Context,
	in *adminCreateUserFullInput,
) (*adminCreateUserFullOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.Backend.AdminCreateUserFull(
		in.UserPoolID,
		in.Username,
		in.TemporaryPassword,
		attrs,
		in.MessageAction,
		in.DesiredDeliveryMediums,
		in.ForceAliasCreation,
	)
	if err != nil {
		return nil, err
	}

	return &adminCreateUserFullOutput{User: toAdminUserJSON(user)}, nil
}

func toAdminUserJSON(u *User) *adminUserJSON {
	return &adminUserJSON{
		Username:             u.Username,
		UserStatus:           u.Status,
		UserAttributes:       sortedAttributeList(userAttrsWithSub(u)),
		UserCreateDate:       float64(u.CreatedAt.Unix()),
		UserLastModifiedDate: float64(u.UpdatedAt.Unix()),
		Enabled:              u.Enabled,
	}
}

func (h *Handler) handleAdminSetUserPasswordFull(
	_ context.Context,
	in *adminSetUserPasswordFullInput,
) (*adminSetUserPasswordFullOutput, error) {
	if err := h.Backend.AdminSetUserPasswordFull(in.UserPoolID, in.Username, in.Password, in.Permanent); err != nil {
		return nil, err
	}

	return &adminSetUserPasswordFullOutput{}, nil
}

func (h *Handler) handleGetUserAuthFactors(
	_ context.Context,
	in *getUserAuthFactorsInput,
) (*getUserAuthFactorsOutput, error) {
	user, factors, err := h.Backend.GetUserAuthFactors(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &getUserAuthFactorsOutput{Username: user.Username, ConfiguredUserAuthFactors: factors}, nil
}

func (h *Handler) usersOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminCreateUser":      service.WrapOp(h.handleAdminCreateUser),
		"AdminSetUserPassword": service.WrapOp(h.handleAdminSetUserPassword),
		"AdminGetUser":         service.WrapOp(h.handleAdminGetUser),
		"AdminDeleteUser":      service.WrapOp(h.handleAdminDeleteUser),
		"ListUsers":            service.WrapOp(h.handleListUsers),
		"GetUser":              service.WrapOp(h.handleGetUser),
		"DeleteUser":           service.WrapOp(h.handleDeleteUser),
		"AdminDisableUser":     service.WrapOp(h.handleAdminDisableUser),
		"AdminEnableUser":      service.WrapOp(h.handleAdminEnableUser),
	}
}

func (h *Handler) usersOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetUserAuthFactors": service.WrapOp(h.handleGetUserAuthFactors),
	}
}

func (h *Handler) usersOpsC() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opGetUser: wrapAccuracy(h.handleGetUserAccurate),
	}
}

func (h *Handler) usersOpsD() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opAdminCreateUser:      wrapAccuracy(h.handleAdminCreateUserFull),
		opAdminSetUserPassword: wrapAccuracy(h.handleAdminSetUserPasswordFull),
	}
}
