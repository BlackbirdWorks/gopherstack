package workmail

import (
	"context"
)

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
		GroupID:                     g.GroupID,
		Name:                        g.Name,
		Email:                       g.Email,
		State:                       g.State,
		HiddenFromGlobalAddressList: g.Hidden,
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

// listGroupsFiltersReq mirrors aws-sdk-go-v2/service/workmail/types.
// ListGroupsFilters, the ListGroupsInput.Filters wire shape.
type listGroupsFiltersReq struct {
	NamePrefix         string `json:"NamePrefix"`
	PrimaryEmailPrefix string `json:"PrimaryEmailPrefix"`
	State              string `json:"State"`
}

type listGroupsReq struct {
	Filters        *listGroupsFiltersReq `json:"Filters"`
	OrganizationID string                `json:"OrganizationId"`
	NextToken      string                `json:"NextToken"`
	MaxResults     int32                 `json:"MaxResults"`
}

type groupSummaryResp struct {
	ID           string `json:"Id"`
	Name         string `json:"Name"`
	Email        string `json:"Email,omitempty"`
	State        string `json:"State"`
	EnabledDate  int64  `json:"EnabledDate,omitempty"`
	DisabledDate int64  `json:"DisabledDate,omitempty"`
}

type listGroupsResp struct {
	NextToken string             `json:"NextToken,omitempty"`
	Groups    []groupSummaryResp `json:"Groups"`
}

func (h *Handler) handleListGroups(_ context.Context, req *listGroupsReq) (*listGroupsResp, error) {
	var filter *GroupFilter
	if req.Filters != nil {
		filter = &GroupFilter{
			NamePrefix:         req.Filters.NamePrefix,
			PrimaryEmailPrefix: req.Filters.PrimaryEmailPrefix,
			State:              req.Filters.State,
		}
	}

	groups, next, err := h.Backend.ListGroups(req.OrganizationID, filter, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]groupSummaryResp, 0, len(groups))
	for _, g := range groups {
		s := groupSummaryResp{ID: g.GroupID, Name: g.Name, Email: g.Email, State: g.State}
		if !g.EnabledDate.IsZero() {
			s.EnabledDate = g.EnabledDate.Unix()
		}
		if !g.DisabledDate.IsZero() {
			s.DisabledDate = g.DisabledDate.Unix()
		}
		summaries = append(summaries, s)
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

// listGroupsForEntityFiltersReq mirrors aws-sdk-go-v2/service/workmail/
// types.ListGroupsForEntityFilters, the ListGroupsForEntityInput.Filters
// wire shape (a single dimension: GroupNamePrefix).
type listGroupsForEntityFiltersReq struct {
	GroupNamePrefix string `json:"GroupNamePrefix"`
}

type listGroupsForEntityReq struct {
	Filters        *listGroupsForEntityFiltersReq `json:"Filters"`
	OrganizationID string                         `json:"OrganizationId"`
	EntityID       string                         `json:"EntityId"`
	NextToken      string                         `json:"NextToken"`
	MaxResults     int32                          `json:"MaxResults"`
}

// groupIdentifierResp mirrors aws-sdk-go-v2/service/workmail/types.
// GroupIdentifier, the ListGroupsForEntity item shape. It is a narrower type
// than types.Group (used by ListGroups): only GroupId/GroupName, no Email or
// State.
type groupIdentifierResp struct {
	GroupID   string `json:"GroupId"`
	GroupName string `json:"GroupName"`
}

type listGroupsForEntityResp struct {
	NextToken string                `json:"NextToken,omitempty"`
	Groups    []groupIdentifierResp `json:"Groups"`
}

func (h *Handler) handleListGroupsForEntity(
	_ context.Context,
	req *listGroupsForEntityReq,
) (*listGroupsForEntityResp, error) {
	var groupNamePrefix string
	if req.Filters != nil {
		groupNamePrefix = req.Filters.GroupNamePrefix
	}

	groups, next, err := h.Backend.ListGroupsForEntity(
		req.OrganizationID, req.EntityID, groupNamePrefix, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	summaries := make([]groupIdentifierResp, 0, len(groups))
	for _, g := range groups {
		summaries = append(summaries, groupIdentifierResp{GroupID: g.GroupID, GroupName: g.Name})
	}

	return &listGroupsForEntityResp{Groups: summaries, NextToken: next}, nil
}
