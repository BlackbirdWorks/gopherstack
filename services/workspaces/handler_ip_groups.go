package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildIPGroupsOps returns the map of IP access control group operations.
func (h *Handler) buildIPGroupsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateIpGroup":        service.WrapOp(h.handleCreateIpGroup),
		"DescribeIpGroups":     service.WrapOp(h.handleDescribeIpGroups),
		"DeleteIpGroup":        service.WrapOp(h.handleDeleteIpGroup),
		"AuthorizeIpRules":     service.WrapOp(h.handleAuthorizeIpRules),
		"RevokeIpRules":        service.WrapOp(h.handleRevokeIpRules),
		"UpdateRulesOfIpGroup": service.WrapOp(h.handleUpdateRulesOfIpGroup),
		"AssociateIpGroups":    service.WrapOp(h.handleAssociateIpGroups),
		"DisassociateIpGroups": service.WrapOp(h.handleDisassociateIpGroups),
	}
}

type createIpGroupInput struct { //nolint:revive,staticcheck // existing issue.
	GroupName string       `json:"GroupName"`
	GroupDesc string       `json:"GroupDesc"`
	Tags      []tagItem    `json:"Tags"`
	UserRules []ipRuleItem `json:"UserRules"`
}

type createIpGroupOutput struct { //nolint:revive,staticcheck // existing issue.
	GroupId string `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateIpGroup( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *createIpGroupInput,
) (*createIpGroupOutput, error) {
	tags := tagsToMap(req.Tags)

	id, err := h.Backend.CreateIpGroup(req.GroupName, req.GroupDesc, req.UserRules, tags)
	if err != nil {
		return nil, err
	}

	return &createIpGroupOutput{GroupId: id}, nil
}

type describeIpGroupsInput struct { //nolint:revive,staticcheck // existing issue.
	NextToken  string   `json:"NextToken"`
	GroupIds   []string `json:"GroupIds"` //nolint:revive // existing issue.
	MaxResults int32    `json:"MaxResults"`
}

type workspacesIpGroupResp struct { //nolint:revive,staticcheck // existing issue.
	GroupId   string       `json:"groupId"` //nolint:revive,staticcheck // existing issue.
	GroupName string       `json:"groupName"`
	GroupDesc string       `json:"groupDesc"`
	UserRules []ipRuleItem `json:"userRules"`
}

type describeIpGroupsOutput struct { //nolint:revive,staticcheck // existing issue.
	NextToken string                  `json:"NextToken,omitempty"`
	Result    []workspacesIpGroupResp `json:"Result"`
}

func (h *Handler) handleDescribeIpGroups( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *describeIpGroupsInput,
) (*describeIpGroupsOutput, error) {
	groups, nextToken, err := h.Backend.DescribeIpGroups(
		req.GroupIds,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspacesIpGroupResp, 0, len(groups))
	for _, g := range groups {
		items = append(items, workspacesIpGroupResp{
			GroupId:   g.GroupID,
			GroupName: g.GroupName,
			GroupDesc: g.GroupDesc,
			UserRules: g.UserRules,
		})
	}

	return &describeIpGroupsOutput{Result: items, NextToken: nextToken}, nil
}

type deleteIpGroupInput struct { //nolint:revive,staticcheck // existing issue.
	GroupId string `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteIpGroup( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *deleteIpGroupInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteIPGroup(req.GroupId)
}

type authorizeIpRulesInput struct { //nolint:revive,staticcheck // existing issue.
	GroupId   string       `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
	UserRules []ipRuleItem `json:"UserRules"`
}

func (h *Handler) handleAuthorizeIpRules( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *authorizeIpRulesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.AuthorizeIpRules(req.GroupId, req.UserRules)
}

type revokeIpRulesInput struct { //nolint:revive,staticcheck // existing issue.
	GroupId   string   `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
	UserRules []string `json:"UserRules"`
}

func (h *Handler) handleRevokeIpRules( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *revokeIpRulesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.RevokeIpRules(req.GroupId, req.UserRules)
}

type updateRulesOfIpGroupInput struct { //nolint:revive,staticcheck // existing issue.
	GroupId   string       `json:"GroupId"` //nolint:revive,staticcheck // existing issue.
	UserRules []ipRuleItem `json:"UserRules"`
}

func (h *Handler) handleUpdateRulesOfIpGroup( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *updateRulesOfIpGroupInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateRulesOfIpGroup(req.GroupId, req.UserRules)
}

type associateIpGroupsInput struct { //nolint:revive,staticcheck // existing issue.
	DirectoryId string   `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	GroupIds    []string `json:"GroupIds"`    //nolint:revive // existing issue.
}

func (h *Handler) handleAssociateIpGroups( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *associateIpGroupsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.AssociateIpGroups(req.DirectoryId, req.GroupIds)
}

type disassociateIpGroupsInput struct { //nolint:revive,staticcheck // existing issue.
	DirectoryId string   `json:"DirectoryId"` //nolint:revive,staticcheck // existing issue.
	GroupIds    []string `json:"GroupIds"`    //nolint:revive // existing issue.
}

func (h *Handler) handleDisassociateIpGroups( //nolint:revive,staticcheck // existing issue.
	_ context.Context,
	req *disassociateIpGroupsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DisassociateIpGroups(req.DirectoryId, req.GroupIds)
}
