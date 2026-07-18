package emr

import (
	"context"
)

// --- CreateStudio ---

type createStudioInput struct {
	Name                     string   `json:"Name"`
	AuthMode                 string   `json:"AuthMode"`
	DefaultS3Location        string   `json:"DefaultS3Location"`
	EngineSecurityGroupID    string   `json:"EngineSecurityGroupId"`
	ServiceRole              string   `json:"ServiceRole"`
	VpcID                    string   `json:"VpcId"`
	WorkspaceSecurityGroupID string   `json:"WorkspaceSecurityGroupId"`
	SubnetIDs                []string `json:"SubnetIds"`
	Tags                     []Tag    `json:"Tags"`
}

type createStudioOutput struct {
	StudioID string `json:"StudioId"`
	URL      string `json:"Url"`
}

func (h *Handler) handleCreateStudio(
	ctx context.Context,
	in *createStudioInput,
) (*createStudioOutput, error) {
	studio, err := h.Backend.CreateStudio(ctx, in.Name,
		in.AuthMode,
		in.DefaultS3Location,
		in.EngineSecurityGroupID,
		in.ServiceRole,
		in.VpcID,
		in.WorkspaceSecurityGroupID,
		in.SubnetIDs,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createStudioOutput{
		StudioID: studio.StudioID,
		URL:      studio.URL,
	}, nil
}

// --- DeleteStudio ---

type deleteStudioInput struct {
	StudioID string `json:"StudioId"`
}

func (h *Handler) handleDeleteStudio(
	ctx context.Context,
	in *deleteStudioInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteStudio(ctx, in.StudioID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- CreateStudioSessionMapping ---

type createStudioSessionMappingInput struct {
	StudioID         string `json:"StudioId"`
	IdentityType     string `json:"IdentityType"`
	IdentityID       string `json:"IdentityId,omitempty"`
	IdentityName     string `json:"IdentityName,omitempty"`
	SessionPolicyArn string `json:"SessionPolicyArn"`
}

func (h *Handler) handleCreateStudioSessionMapping(
	ctx context.Context,
	in *createStudioSessionMappingInput,
) (*emptyOutput, error) {
	if err := h.Backend.CreateStudioSessionMapping(ctx, in.StudioID,
		in.IdentityType,
		in.IdentityID,
		in.IdentityName,
		in.SessionPolicyArn,
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- DeleteStudioSessionMapping ---

type deleteStudioSessionMappingInput struct {
	StudioID     string `json:"StudioId"`
	IdentityType string `json:"IdentityType"`
	IdentityID   string `json:"IdentityId,omitempty"`
	IdentityName string `json:"IdentityName,omitempty"`
}

func (h *Handler) handleDeleteStudioSessionMapping(
	ctx context.Context,
	in *deleteStudioSessionMappingInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteStudioSessionMapping(ctx, in.StudioID,
		in.IdentityType,
		in.IdentityID,
		in.IdentityName,
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- DescribeStudio ---

type describeStudioInput struct {
	StudioID string `json:"StudioId"`
}

type describeStudioOutput struct {
	Studio *Studio `json:"Studio"`
}

func (h *Handler) handleDescribeStudio(ctx context.Context, in *describeStudioInput) (*describeStudioOutput, error) {
	studio, err := h.Backend.DescribeStudio(ctx, in.StudioID)
	if err != nil {
		return nil, err
	}

	return &describeStudioOutput{Studio: studio}, nil
}

// --- UpdateStudio ---

type updateStudioInput struct {
	StudioID          string   `json:"StudioId"`
	Name              string   `json:"Name"`
	Description       string   `json:"Description"`
	DefaultS3Location string   `json:"DefaultS3Location"`
	SubnetIDs         []string `json:"SubnetIds"`
}

type updateStudioOutput struct{}

func (h *Handler) handleUpdateStudio(ctx context.Context, in *updateStudioInput) (*updateStudioOutput, error) {
	if err := h.Backend.UpdateStudio(ctx, in.StudioID, in.Name, in.Description, in.DefaultS3Location, ""); err != nil {
		return nil, err
	}

	return &updateStudioOutput{}, nil
}

// --- ListStudios ---

type listStudiosInput struct {
	Marker string `json:"Marker"`
}

type listStudiosOutput struct {
	Marker  string          `json:"Marker,omitempty"`
	Studios []StudioSummary `json:"Studios"`
}

func (h *Handler) handleListStudios(ctx context.Context, in *listStudiosInput) (*listStudiosOutput, error) {
	studios, nextMarker := h.Backend.ListStudios(ctx, in.Marker)

	return &listStudiosOutput{Studios: studios, Marker: nextMarker}, nil
}

// --- GetStudioSessionMapping ---

type getStudioSessionMappingInput struct {
	StudioID     string `json:"StudioId"`
	IdentityType string `json:"IdentityType"`
	IdentityID   string `json:"IdentityId,omitempty"`
	IdentityName string `json:"IdentityName,omitempty"`
}

type getStudioSessionMappingOutput struct {
	SessionMapping *StudioSessionMapping `json:"SessionMapping"`
}

func (h *Handler) handleGetStudioSessionMapping(
	ctx context.Context,
	in *getStudioSessionMappingInput,
) (*getStudioSessionMappingOutput, error) {
	mapping, err := h.Backend.GetStudioSessionMapping(ctx, in.StudioID, in.IdentityType, in.IdentityID, in.IdentityName)
	if err != nil {
		return nil, err
	}

	return &getStudioSessionMappingOutput{SessionMapping: mapping}, nil
}

// --- ListStudioSessionMappings ---

type listStudioSessionMappingsInput struct {
	StudioID     string `json:"StudioId"`
	IdentityType string `json:"IdentityType"`
}

type listStudioSessionMappingsOutput struct {
	SessionMappings []StudioSessionMapping `json:"SessionMappings"`
}

func (h *Handler) handleListStudioSessionMappings(
	ctx context.Context,
	in *listStudioSessionMappingsInput,
) (*listStudioSessionMappingsOutput, error) {
	mappings := h.Backend.ListStudioSessionMappings(ctx, in.StudioID, in.IdentityType)

	return &listStudioSessionMappingsOutput{SessionMappings: mappings}, nil
}

// --- UpdateStudioSessionMapping ---

type updateStudioSessionMappingInput struct {
	StudioID         string `json:"StudioId"`
	IdentityType     string `json:"IdentityType"`
	IdentityID       string `json:"IdentityId,omitempty"`
	IdentityName     string `json:"IdentityName,omitempty"`
	SessionPolicyArn string `json:"SessionPolicyArn"`
}

type updateStudioSessionMappingOutput struct{}

func (h *Handler) handleUpdateStudioSessionMapping(
	ctx context.Context,
	in *updateStudioSessionMappingInput,
) (*updateStudioSessionMappingOutput, error) {
	if err := h.Backend.UpdateStudioSessionMapping(ctx, in.StudioID,
		in.IdentityType,
		in.IdentityID,
		in.IdentityName,
		in.SessionPolicyArn,
	); err != nil {
		return nil, err
	}

	return &updateStudioSessionMappingOutput{}, nil
}
