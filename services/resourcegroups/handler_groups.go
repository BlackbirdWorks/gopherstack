package resourcegroups

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type handleCreateGroupInput struct {
	Name          string                   `json:"Name"`
	Description   string                   `json:"Description"`
	Tags          *tags.Tags               `json:"Tags"`
	ResourceQuery *ResourceQuery           `json:"ResourceQuery"`
	Configuration []GroupConfigurationItem `json:"Configuration"`
}

type groupConfigurationBody struct {
	Status        string                   `json:"Status,omitempty"`
	Configuration []GroupConfigurationItem `json:"Configuration,omitempty"`
}

// createGroupOutput mirrors CreateGroupOutput: Tags is a top-level sibling of
// Group (types.Group itself carries no Tags member), matching the real API.
type createGroupOutput struct {
	Group              *getGroupBody           `json:"Group"`
	ResourceQuery      *ResourceQuery          `json:"ResourceQuery,omitempty"`
	GroupConfiguration *groupConfigurationBody `json:"GroupConfiguration,omitempty"`
	Tags               map[string]string       `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateGroup(ctx context.Context, in *handleCreateGroupInput) (*createGroupOutput, error) {
	g, err := h.Backend.CreateGroup(ctx, in.Name, in.Description, in.ResourceQuery, in.Tags, in.Configuration)
	if err != nil {
		return nil, err
	}

	out := &createGroupOutput{
		Group:         groupBodyFromGroup(g),
		ResourceQuery: g.ResourceQuery,
	}

	if g.Tags != nil {
		if tagMap := g.Tags.Clone(); len(tagMap) > 0 {
			out.Tags = tagMap
		}
	}

	if len(in.Configuration) > 0 {
		out.GroupConfiguration = &groupConfigurationBody{
			Configuration: in.Configuration,
			Status:        "UPDATE_COMPLETE",
		}
	}

	return out, nil
}

// deleteGroupOutput mirrors DeleteGroupOutput: AWS echoes back the deleted
// group's description in the response.
type deleteGroupOutput struct {
	Group *getGroupBody `json:"Group"`
}

func (h *Handler) handleDeleteGroup(ctx context.Context, in *groupNameInput) (*deleteGroupOutput, error) {
	g, err := h.Backend.DeleteGroup(ctx, in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &deleteGroupOutput{Group: groupBodyFromGroup(g)}, nil
}

type listGroupsInput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Filters    []ListGroupsFilter `json:"Filters"`
	NextToken  string             `json:"NextToken"`
	MaxResults int                `json:"MaxResults"`
}

type listGroupIdentifierOutput struct {
	GroupName   string `json:"GroupName"`
	GroupArn    string `json:"GroupArn"`
	Description string `json:"Description,omitempty"`
	Owner       string `json:"Owner,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	Criticality int    `json:"Criticality,omitempty"`
}

type listGroupsGroupOutput struct {
	GroupArn    string `json:"GroupArn"`
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
	Owner       string `json:"Owner,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	Criticality int    `json:"Criticality,omitempty"`
}

type listGroupsOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Groups           []listGroupsGroupOutput     `json:"Groups"`
	GroupIdentifiers []listGroupIdentifierOutput `json:"GroupIdentifiers"`
	NextToken        string                      `json:"NextToken,omitempty"`
}

func (h *Handler) handleListGroups(ctx context.Context, in *listGroupsInput) (*listGroupsOutput, error) {
	groups, nextToken := h.Backend.ListGroups(ctx, in.Filters, in.NextToken, in.MaxResults)
	identifiers := make([]listGroupIdentifierOutput, 0, len(groups))
	groupsList := make([]listGroupsGroupOutput, 0, len(groups))

	for _, group := range groups {
		identifiers = append(identifiers, listGroupIdentifierOutput{
			GroupName:   group.Name,
			GroupArn:    group.ARN,
			Description: group.Description,
			Owner:       group.Owner,
			DisplayName: group.DisplayName,
			Criticality: group.Criticality,
		})
		groupsList = append(groupsList, listGroupsGroupOutput{
			GroupArn:    group.ARN,
			Name:        group.Name,
			Description: group.Description,
			Owner:       group.Owner,
			DisplayName: group.DisplayName,
			Criticality: group.Criticality,
		})
	}

	return &listGroupsOutput{Groups: groupsList, GroupIdentifiers: identifiers, NextToken: nextToken}, nil
}

// getGroupBody is the AWS-accurate wire shape of types.Group: it deliberately
// excludes Tags and ResourceQuery, which travel as separate top-level
// response fields (see createGroupOutput and getGroupQueryOutput) and are not
// members of the Group shape itself.
type getGroupBody struct {
	ApplicationTag map[string]string `json:"ApplicationTag,omitempty"`
	GroupArn       string            `json:"GroupArn"`
	Name           string            `json:"Name"`
	Description    string            `json:"Description,omitempty"`
	Owner          string            `json:"Owner,omitempty"`
	DisplayName    string            `json:"DisplayName,omitempty"`
	Criticality    int               `json:"Criticality,omitempty"`
}

// groupBodyFromGroup builds the AWS wire-shaped Group body from the backend's
// internal representation.
func groupBodyFromGroup(g *Group) *getGroupBody {
	return &getGroupBody{
		GroupArn:       g.ARN,
		Name:           g.Name,
		Description:    g.Description,
		Owner:          g.Owner,
		DisplayName:    g.DisplayName,
		Criticality:    g.Criticality,
		ApplicationTag: g.ApplicationTag,
	}
}

type getGroupOutput struct {
	Group *getGroupBody `json:"Group"`
}

func (h *Handler) handleGetGroup(ctx context.Context, in *groupNameInput) (*getGroupOutput, error) {
	g, err := h.Backend.GetGroup(ctx, in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &getGroupOutput{Group: groupBodyFromGroup(g)}, nil
}

type getGroupQueryOutput struct {
	GroupQuery *groupQueryOutput `json:"GroupQuery"`
}

type groupQueryOutput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	GroupName     string         `json:"GroupName"`
}

func (h *Handler) handleGetGroupQuery(ctx context.Context, in *groupNameInput) (*getGroupQueryOutput, error) {
	g, err := h.Backend.GetGroup(ctx, in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &getGroupQueryOutput{GroupQuery: &groupQueryOutput{
		GroupName:     g.Name,
		ResourceQuery: g.ResourceQuery,
	}}, nil
}

// getGroupConfigurationOutput mirrors GetGroupConfigurationOutput. The real
// GroupConfiguration shape has no GroupName member (unlike GroupQuery); it
// only carries Configuration, FailureReason, ProposedConfiguration, and
// Status, so it reuses groupConfigurationBody rather than a bespoke type.
type getGroupConfigurationOutput struct {
	GroupConfiguration *groupConfigurationBody `json:"GroupConfiguration"`
}

func (h *Handler) handleGetGroupConfiguration(
	ctx context.Context,
	in *groupNameInput,
) (*getGroupConfigurationOutput, error) {
	g, err := h.Backend.GetGroup(ctx, in.resolvedName())
	if err != nil {
		return nil, err
	}

	items, err := h.Backend.GetGroupConfigurationItems(ctx, g.Name)
	if err != nil {
		return nil, err
	}

	body := &groupConfigurationBody{Configuration: items}
	if len(items) > 0 {
		body.Status = "UPDATE_COMPLETE"
	}

	return &getGroupConfigurationOutput{GroupConfiguration: body}, nil
}

type updateGroupInput struct {
	Group       string `json:"Group"`
	GroupName   string `json:"GroupName"`
	Description string `json:"Description"`
	DisplayName string `json:"DisplayName"`
	Criticality int    `json:"Criticality"`
}

func (g *updateGroupInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

type updateGroupOutput struct {
	Group *getGroupBody `json:"Group"`
}

func (h *Handler) handleUpdateGroup(ctx context.Context, in *updateGroupInput) (*updateGroupOutput, error) {
	name := in.resolvedName()
	if name == "" {
		return nil, fmt.Errorf("%w: Group or GroupName is required", ErrValidation)
	}

	g, err := h.Backend.UpdateGroup(ctx, name, in.Description, in.DisplayName, in.Criticality)
	if err != nil {
		return nil, err
	}

	return &updateGroupOutput{Group: groupBodyFromGroup(g)}, nil
}

type updateGroupQueryInput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	Group         string         `json:"Group"`
	GroupName     string         `json:"GroupName"`
}

func (g *updateGroupQueryInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

type updateGroupQueryOutput struct {
	GroupQuery *groupQueryOutput `json:"GroupQuery"`
}

func (h *Handler) handleUpdateGroupQuery(
	ctx context.Context,
	in *updateGroupQueryInput,
) (*updateGroupQueryOutput, error) {
	name := in.resolvedName()
	if name == "" {
		return nil, fmt.Errorf("%w: Group or GroupName is required", ErrValidation)
	}

	g, err := h.Backend.UpdateGroupQuery(ctx, name, in.ResourceQuery)
	if err != nil {
		return nil, err
	}

	return &updateGroupQueryOutput{GroupQuery: &groupQueryOutput{
		GroupName:     g.Name,
		ResourceQuery: g.ResourceQuery,
	}}, nil
}

// handlePutGroupConfiguration stores a configuration for a group.
type putGroupConfigurationInput struct {
	Group         string                   `json:"Group"`
	GroupName     string                   `json:"GroupName"`
	Configuration []GroupConfigurationItem `json:"Configuration"`
}

func (g *putGroupConfigurationInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

type putGroupConfigurationOutput struct{}

func (h *Handler) handlePutGroupConfiguration(
	ctx context.Context,
	in *putGroupConfigurationInput,
) (*putGroupConfigurationOutput, error) {
	if err := h.Backend.PutGroupConfiguration(ctx, in.resolvedName(), in.Configuration); err != nil {
		return nil, err
	}

	return &putGroupConfigurationOutput{}, nil
}
