package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// adminGroupsForUserPageSize is this backend's default page size for
// AdminListGroupsForUser; real AWS doesn't document an exact default, so
// this is chosen generously (larger than any realistic per-user group
// membership count) so pagination only activates when a caller explicitly
// requests a smaller Limit.
const adminGroupsForUserPageSize = 100

func toGroupSummary(g *Group) *groupSummary {
	return &groupSummary{
		GroupName:    g.GroupName,
		UserPoolID:   g.UserPoolID,
		Description:  g.Description,
		Precedence:   g.Precedence,
		CreationDate: float64(g.CreatedAt.Unix()),
	}
}

func (h *Handler) handleCreateGroup(
	_ context.Context,
	in *createGroupInput,
) (*createGroupOutput, error) {
	g, err := h.Backend.CreateGroup(in.UserPoolID, in.GroupName, in.Description, in.Precedence)
	if err != nil {
		return nil, err
	}

	return &createGroupOutput{Group: toGroupSummary(g)}, nil
}

func (h *Handler) handleDeleteGroup(_ context.Context, in *deleteGroupInput) (*deleteGroupOutput, error) {
	if err := h.Backend.DeleteGroup(in.UserPoolID, in.GroupName); err != nil {
		return nil, err
	}

	return &deleteGroupOutput{}, nil
}

func (h *Handler) handleListGroups(_ context.Context, in *listGroupsInput) (*listGroupsOutput, error) {
	groups, err := h.Backend.ListGroups(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]*groupSummary, 0, len(groups))
	for _, g := range groups {
		out = append(out, toGroupSummary(g))
	}

	return &listGroupsOutput{Groups: out}, nil
}

func (h *Handler) handleAdminAddUserToGroup(
	_ context.Context,
	in *adminAddUserToGroupInput,
) (*adminAddUserToGroupOutput, error) {
	if err := h.Backend.AdminAddUserToGroup(in.UserPoolID, in.Username, in.GroupName); err != nil {
		return nil, err
	}

	return &adminAddUserToGroupOutput{}, nil
}

func (h *Handler) handleAdminRemoveUserFromGroup(
	_ context.Context,
	in *adminRemoveUserFromGroupInput,
) (*adminRemoveUserFromGroupOutput, error) {
	if err := h.Backend.AdminRemoveUserFromGroup(in.UserPoolID, in.Username, in.GroupName); err != nil {
		return nil, err
	}

	return &adminRemoveUserFromGroupOutput{}, nil
}

func (h *Handler) handleAdminListGroupsForUser(
	_ context.Context,
	in *adminListGroupsForUserInput,
) (*adminListGroupsForUserOutput, error) {
	groups, err := h.Backend.AdminListGroupsForUser(in.UserPoolID, in.Username)
	if err != nil {
		return nil, err
	}

	pg := page.New(groups, in.NextToken, in.Limit, adminGroupsForUserPageSize)

	out := make([]*groupSummary, 0, len(pg.Data))
	for _, g := range pg.Data {
		out = append(out, toGroupSummary(g))
	}

	return &adminListGroupsForUserOutput{Groups: out, NextToken: pg.Next}, nil
}

func (h *Handler) handleListUsersInGroup(
	_ context.Context,
	in *listUsersInGroupInput,
) (*listUsersInGroupOutput, error) {
	users, err := h.Backend.ListUsersInGroup(in.UserPoolID, in.GroupName)
	if err != nil {
		return nil, err
	}

	summaries := make([]*userSummary, 0, len(users))
	for _, u := range users {
		summaries = append(summaries, toUserSummary(u))
	}

	return &listUsersInGroupOutput{Users: summaries}, nil
}

func (h *Handler) handleUpdateGroup(_ context.Context, in *updateGroupInput) (*updateGroupOutput, error) {
	g, err := h.Backend.UpdateGroup(in.UserPoolID, in.GroupName, in.Description, in.Precedence)
	if err != nil {
		return nil, err
	}

	return &updateGroupOutput{Group: toGroupSummary(g)}, nil
}

func (h *Handler) handleGetGroup(_ context.Context, in *getGroupInput) (*getGroupOutput, error) {
	g, err := h.Backend.GetGroup(in.UserPoolID, in.GroupName)
	if err != nil {
		return nil, err
	}

	return &getGroupOutput{Group: toGroupSummary(g)}, nil
}

func (h *Handler) handleCreateGroupFull(
	_ context.Context,
	in *createGroupFullInput,
) (*createGroupFullOutput, error) {
	g, err := h.Backend.CreateGroupFull(in.UserPoolID, in.GroupName, in.Description, in.RoleArn, in.Precedence)
	if err != nil {
		return nil, err
	}

	return &createGroupFullOutput{Group: toGroupFullSummary(g)}, nil
}

func (h *Handler) handleUpdateGroupFull(
	_ context.Context,
	in *updateGroupFullInput,
) (*updateGroupFullOutput, error) {
	g, err := h.Backend.UpdateGroupFull(in.UserPoolID, in.GroupName, in.Description, in.RoleArn, in.Precedence)
	if err != nil {
		return nil, err
	}

	return &updateGroupFullOutput{Group: toGroupFullSummary(g)}, nil
}

func (h *Handler) handleGetGroupFull(
	_ context.Context,
	in *getGroupFullInput,
) (*getGroupFullOutput, error) {
	g, err := h.Backend.GetGroup(in.UserPoolID, in.GroupName)
	if err != nil {
		return nil, err
	}

	return &getGroupFullOutput{Group: toGroupFullSummary(g)}, nil
}

func (h *Handler) handleListGroupsFull(
	_ context.Context,
	in *listGroupsFullInput,
) (*listGroupsFullOutput, error) {
	groups, nextToken, err := h.Backend.ListGroupsPage(in.UserPoolID, in.Limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]*groupFullSummary, 0, len(groups))

	for _, g := range groups {
		out = append(out, toGroupFullSummary(g))
	}

	return &listGroupsFullOutput{Groups: out, NextToken: nextToken}, nil
}

func (h *Handler) handleListUsersInGroupFull(
	_ context.Context,
	in *listUsersInGroupFullInput,
) (*listUsersInGroupFullOutput, error) {
	users, nextToken, err := h.Backend.ListUsersInGroupPage(in.UserPoolID, in.GroupName, in.Limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]adminUserJSON, 0, len(users))

	for _, u := range users {
		out = append(out, *toAdminUserJSON(u))
	}

	return &listUsersInGroupFullOutput{Users: out, NextToken: nextToken}, nil
}

func toGroupFullSummary(g *Group) *groupFullSummary {
	out := &groupFullSummary{
		GroupName:   g.GroupName,
		UserPoolID:  g.UserPoolID,
		Description: g.Description,
		RoleArn:     g.RoleArn,
		Precedence:  g.Precedence,
	}

	if !g.CreatedAt.IsZero() {
		out.CreationDate = float64(g.CreatedAt.Unix())
	}

	if !g.LastModifiedAt.IsZero() {
		out.LastModifiedDate = float64(g.LastModifiedAt.Unix())
	}

	return out
}

func (h *Handler) groupsOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateGroup":              service.WrapOp(h.handleCreateGroup),
		"DeleteGroup":              service.WrapOp(h.handleDeleteGroup),
		"GetGroup":                 service.WrapOp(h.handleGetGroup),
		"ListGroups":               service.WrapOp(h.handleListGroups),
		"AdminAddUserToGroup":      service.WrapOp(h.handleAdminAddUserToGroup),
		"AdminRemoveUserFromGroup": service.WrapOp(h.handleAdminRemoveUserFromGroup),
		"AdminListGroupsForUser":   service.WrapOp(h.handleAdminListGroupsForUser),
		"ListUsersInGroup":         service.WrapOp(h.handleListUsersInGroup),
		"UpdateGroup":              service.WrapOp(h.handleUpdateGroup),
	}
}

func (h *Handler) groupsOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateGroup:      wrapAccuracy(h.handleCreateGroupFull),
		opUpdateGroup:      wrapAccuracy(h.handleUpdateGroupFull),
		opGetGroup:         wrapAccuracy(h.handleGetGroupFull),
		opListGroups:       wrapAccuracy(h.handleListGroupsFull),
		opListUsersInGroup: wrapAccuracy(h.handleListUsersInGroupFull),
	}
}
