package codepipeline

import (
	"context"
	"fmt"
)

// maxResultsCapActionTypes is the per-operation pagination cap for ListActionTypes.
const maxResultsCapActionTypes int32 = 25

// validActionCategory returns true if cat is a valid AWS ActionCategory value.
func validActionCategory(cat string) bool {
	switch cat {
	case "Source", "Build", "Deploy", "Test", "Invoke", "Approval", "Compute":
		return true
	default:
		return false
	}
}

type createCustomActionTypeInput struct {
	Settings                *ActionTypeSettings           `json:"settings,omitempty"`
	Category                string                        `json:"category"`
	Provider                string                        `json:"provider"`
	Version                 string                        `json:"version"`
	ConfigurationProperties []ActionConfigurationProperty `json:"configurationProperties,omitempty"`
	Tags                    []Tag                         `json:"tags,omitempty"`
	InputArtifactDetails    ArtifactDetails               `json:"inputArtifactDetails"`
	OutputArtifactDetails   ArtifactDetails               `json:"outputArtifactDetails"`
}

type customActionTypeResponse struct {
	Settings                      *ActionTypeSettings           `json:"settings,omitempty"`
	ID                            ActionTypeID                  `json:"id"`
	ActionConfigurationProperties []ActionConfigurationProperty `json:"actionConfigurationProperties"`
	InputArtifactDetails          ArtifactDetails               `json:"inputArtifactDetails"`
	OutputArtifactDetails         ArtifactDetails               `json:"outputArtifactDetails"`
}

type createCustomActionTypeOutput struct {
	Tags       []Tag                    `json:"tags,omitempty"`
	ActionType customActionTypeResponse `json:"actionType"`
}

func (h *Handler) handleCreateCustomActionType(
	ctx context.Context,
	in *createCustomActionTypeInput,
) (*createCustomActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
	}

	if !validActionCategory(in.Category) {
		return nil, fmt.Errorf("%w: invalid category %q", ErrValidation, in.Category)
	}

	if in.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", errInvalidRequest)
	}

	if in.Version == "" {
		return nil, fmt.Errorf("%w: version is required", errInvalidRequest)
	}

	cat := &CustomActionType{
		Category:                in.Category,
		Provider:                in.Provider,
		Version:                 in.Version,
		InputArtifactDetails:    in.InputArtifactDetails,
		OutputArtifactDetails:   in.OutputArtifactDetails,
		Settings:                in.Settings,
		ConfigurationProperties: in.ConfigurationProperties,
		Tags:                    tagsToMap(in.Tags),
	}

	created, err := h.Backend.CreateCustomActionType(ctx, cat)
	if err != nil {
		return nil, err
	}

	configProps := created.ConfigurationProperties
	if configProps == nil {
		configProps = []ActionConfigurationProperty{}
	}

	return &createCustomActionTypeOutput{
		ActionType: customActionTypeResponse{
			ID: ActionTypeID{
				Category: created.Category,
				Owner:    keyOwnerCustom,
				Provider: created.Provider,
				Version:  created.Version,
			},
			InputArtifactDetails:          created.InputArtifactDetails,
			OutputArtifactDetails:         created.OutputArtifactDetails,
			Settings:                      created.Settings,
			ActionConfigurationProperties: configProps,
		},
		Tags: in.Tags,
	}, nil
}

type deleteCustomActionTypeInput struct {
	Category string `json:"category"`
	Provider string `json:"provider"`
	Version  string `json:"version"`
}

type deleteCustomActionTypeOutput struct{}

func (h *Handler) handleDeleteCustomActionType(
	ctx context.Context,
	in *deleteCustomActionTypeInput,
) (*deleteCustomActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
	}

	if !validActionCategory(in.Category) {
		return nil, fmt.Errorf("%w: invalid category %q", ErrValidation, in.Category)
	}

	if in.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", errInvalidRequest)
	}

	if in.Version == "" {
		return nil, fmt.Errorf("%w: version is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteCustomActionType(ctx, in.Category, in.Provider, in.Version); err != nil {
		return nil, err
	}

	return &deleteCustomActionTypeOutput{}, nil
}

type getActionTypeInput struct {
	Category string `json:"category"`
	Owner    string `json:"owner"`
	Provider string `json:"provider"`
	Version  string `json:"version"`
}

type getActionTypeOutput struct {
	ActionType customActionTypeResponse `json:"actionType"`
}

func (h *Handler) handleGetActionType(
	ctx context.Context,
	in *getActionTypeInput,
) (*getActionTypeOutput, error) {
	if in.Category == "" {
		return nil, fmt.Errorf("%w: category is required", errInvalidRequest)
	}

	if !validActionCategory(in.Category) {
		return nil, fmt.Errorf("%w: invalid category %q", ErrValidation, in.Category)
	}

	if in.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", errInvalidRequest)
	}

	if in.Version == "" {
		return nil, fmt.Errorf("%w: version is required", errInvalidRequest)
	}

	cat, err := h.Backend.GetActionType(ctx, in.Category, in.Owner, in.Provider, in.Version)
	if err != nil {
		return nil, err
	}

	catConfigProps := cat.ConfigurationProperties
	if catConfigProps == nil {
		catConfigProps = []ActionConfigurationProperty{}
	}

	return &getActionTypeOutput{
		ActionType: customActionTypeResponse{
			ID: ActionTypeID{
				Category: cat.Category,
				Owner:    keyOwnerCustom,
				Provider: cat.Provider,
				Version:  cat.Version,
			},
			InputArtifactDetails:          cat.InputArtifactDetails,
			OutputArtifactDetails:         cat.OutputArtifactDetails,
			Settings:                      cat.Settings,
			ActionConfigurationProperties: catConfigProps,
		},
	}, nil
}

type updateActionTypeInputBody struct {
	Settings                *ActionTypeSettings           `json:"settings,omitempty"`
	ID                      ActionTypeID                  `json:"id"`
	ConfigurationProperties []ActionConfigurationProperty `json:"actionConfigurationProperties,omitempty"`
	InputArtifactDetails    ArtifactDetails               `json:"inputArtifactDetails"`
	OutputArtifactDetails   ArtifactDetails               `json:"outputArtifactDetails"`
}

type updateActionTypeInput struct {
	ActionType updateActionTypeInputBody `json:"actionType"`
}

func (h *Handler) handleUpdateActionType(
	ctx context.Context,
	in *updateActionTypeInput,
) (*emptyOut, error) {
	id := in.ActionType.ID

	if id.Category == "" {
		return nil, fmt.Errorf("%w: actionType.id.category is required", errInvalidRequest)
	}

	if id.Provider == "" {
		return nil, fmt.Errorf("%w: actionType.id.provider is required", errInvalidRequest)
	}

	if id.Version == "" {
		return nil, fmt.Errorf("%w: actionType.id.version is required", errInvalidRequest)
	}

	owner := id.Owner
	if owner == "" {
		owner = keyOwnerCustom
	}

	cat := &CustomActionType{
		Category:                id.Category,
		Owner:                   owner,
		Provider:                id.Provider,
		Version:                 id.Version,
		Settings:                in.ActionType.Settings,
		ConfigurationProperties: in.ActionType.ConfigurationProperties,
		InputArtifactDetails:    in.ActionType.InputArtifactDetails,
		OutputArtifactDetails:   in.ActionType.OutputArtifactDetails,
	}

	if err := h.Backend.UpdateActionType(ctx, cat); err != nil {
		return nil, err
	}

	return &emptyOut{}, nil
}

type listActionTypesInput struct {
	ActionOwnerFilter string `json:"actionOwnerFilter"`
	RegionFilter      string `json:"regionFilter"`
	NextToken         string `json:"nextToken"`
}

type listActionTypesOutput struct {
	NextToken   string           `json:"nextToken,omitempty"`
	ActionTypes []map[string]any `json:"actionTypes"`
}

func (h *Handler) handleListActionTypes(
	ctx context.Context,
	in *listActionTypesInput,
) (*listActionTypesOutput, error) {
	types := h.Backend.ListActionTypes(ctx)
	items := make([]map[string]any, 0, len(types))

	for _, at := range types {
		owner := at.Owner
		if owner == "" {
			owner = keyOwnerCustom
		}

		if in.ActionOwnerFilter != "" && owner != in.ActionOwnerFilter {
			continue
		}

		item := map[string]any{
			"id": map[string]any{
				"category": at.Category,
				"owner":    owner,
				"provider": at.Provider,
				"version":  at.Version,
			},
			"inputArtifactDetails": map[string]any{
				"minimumCount": at.InputArtifactDetails.MinimumCount,
				"maximumCount": at.InputArtifactDetails.MaximumCount,
			},
			"outputArtifactDetails": map[string]any{
				"minimumCount": at.OutputArtifactDetails.MinimumCount,
				"maximumCount": at.OutputArtifactDetails.MaximumCount,
			},
		}

		if at.Settings != nil {
			item["settings"] = at.Settings
		}

		if len(at.ConfigurationProperties) > 0 {
			item["actionConfigurationProperties"] = at.ConfigurationProperties
		}

		items = append(items, item)
	}

	page, nextToken, err := cpPaginate(items, in.NextToken, 0, maxResultsCapActionTypes)
	if err != nil {
		return nil, err
	}

	return &listActionTypesOutput{NextToken: nextToken, ActionTypes: page}, nil
}
