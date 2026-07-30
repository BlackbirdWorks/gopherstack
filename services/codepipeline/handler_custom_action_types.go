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

// actionTypeDeclarationResponse is the real GetActionType/UpdateActionType wire
// shape (types.ActionTypeDeclaration) -- structurally unrelated to
// customActionTypeResponse (types.ActionType, the legacy CreateCustomActionType/
// ListActionTypes shape): Executor/Permissions/Properties/Urls/Description here
// have no legacy equivalent, and this shape has no Settings/
// actionConfigurationProperties at all.
type actionTypeDeclarationResponse struct {
	Executor              *ActionTypeExecutor    `json:"executor,omitempty"`
	Permissions           *ActionTypePermissions `json:"permissions,omitempty"`
	Urls                  *ActionTypeUrls        `json:"urls,omitempty"`
	Description           string                 `json:"description,omitempty"`
	ID                    ActionTypeID           `json:"id"`
	Properties            []ActionTypeProperty   `json:"properties,omitempty"`
	InputArtifactDetails  ArtifactDetails        `json:"inputArtifactDetails"`
	OutputArtifactDetails ArtifactDetails        `json:"outputArtifactDetails"`
}

type getActionTypeOutput struct {
	ActionType actionTypeDeclarationResponse `json:"actionType"`
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

	if in.Owner == "" {
		return nil, fmt.Errorf("%w: owner is required", errInvalidRequest)
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

	return &getActionTypeOutput{
		ActionType: actionTypeDeclarationResponse{
			ID: ActionTypeID{
				Category: cat.Category,
				Owner:    cat.Owner,
				Provider: cat.Provider,
				Version:  cat.Version,
			},
			InputArtifactDetails:  cat.InputArtifactDetails,
			OutputArtifactDetails: cat.OutputArtifactDetails,
			Description:           cat.Description,
			Executor:              cat.Executor,
			Permissions:           cat.Permissions,
			Properties:            cat.Properties,
			Urls:                  cat.Urls,
		},
	}, nil
}

// updateActionTypeDeclarationInput mirrors types.ActionTypeDeclaration, the real
// UpdateActionType request shape. Executor/ID/InputArtifactDetails/
// OutputArtifactDetails are pointers so a genuinely-missing required member can
// be told apart from a present-but-zero-valued one, matching how the real SDK's
// validateActionTypeDeclaration checks for nil, not zero values.
type updateActionTypeDeclarationInput struct {
	Executor              *ActionTypeExecutor    `json:"executor"`
	ID                    *ActionTypeID          `json:"id"`
	InputArtifactDetails  *ArtifactDetails       `json:"inputArtifactDetails"`
	OutputArtifactDetails *ArtifactDetails       `json:"outputArtifactDetails"`
	Permissions           *ActionTypePermissions `json:"permissions,omitempty"`
	Urls                  *ActionTypeUrls        `json:"urls,omitempty"`
	Description           string                 `json:"description,omitempty"`
	Properties            []ActionTypeProperty   `json:"properties,omitempty"`
}

type updateActionTypeInput struct {
	ActionType *updateActionTypeDeclarationInput `json:"actionType"`
}

// validActionTypeExecutorType reports whether t is a real types.ExecutorType enum value.
func validActionTypeExecutorType(t string) bool {
	return t == "Lambda" || t == "JobWorker"
}

// validateActionTypeExecutorInput mirrors the real SDK's validateActionTypeExecutor/
// validateExecutorConfiguration/validateLambdaExecutorConfiguration: Configuration
// and Type are required, and Configuration.LambdaExecutorConfiguration (when
// present) requires a non-empty LambdaFunctionArn.
func validateActionTypeExecutorInput(e *ActionTypeExecutor) error {
	if e.Configuration == nil {
		return fmt.Errorf("%w: actionType.executor.configuration is required", errInvalidRequest)
	}

	if e.Type == "" {
		return fmt.Errorf("%w: actionType.executor.type is required", errInvalidRequest)
	}

	if !validActionTypeExecutorType(e.Type) {
		return fmt.Errorf("%w: invalid executor type %q", ErrValidation, e.Type)
	}

	lc := e.Configuration.LambdaExecutorConfiguration
	if lc != nil && lc.LambdaFunctionArn == "" {
		return fmt.Errorf(
			"%w: actionType.executor.configuration.lambdaExecutorConfiguration.lambdaFunctionArn is required",
			errInvalidRequest,
		)
	}

	return nil
}

func (h *Handler) handleUpdateActionType(
	ctx context.Context,
	in *updateActionTypeInput,
) (*emptyOut, error) {
	if in.ActionType == nil {
		return nil, fmt.Errorf("%w: actionType is required", errInvalidRequest)
	}

	decl := in.ActionType

	if decl.ID == nil {
		return nil, fmt.Errorf("%w: actionType.id is required", errInvalidRequest)
	}

	id := *decl.ID

	if id.Category == "" {
		return nil, fmt.Errorf("%w: actionType.id.category is required", errInvalidRequest)
	}

	if !validActionCategory(id.Category) {
		return nil, fmt.Errorf("%w: invalid category %q", ErrValidation, id.Category)
	}

	if id.Owner == "" {
		return nil, fmt.Errorf("%w: actionType.id.owner is required", errInvalidRequest)
	}

	if id.Provider == "" {
		return nil, fmt.Errorf("%w: actionType.id.provider is required", errInvalidRequest)
	}

	if id.Version == "" {
		return nil, fmt.Errorf("%w: actionType.id.version is required", errInvalidRequest)
	}

	if decl.Executor == nil {
		return nil, fmt.Errorf("%w: actionType.executor is required", errInvalidRequest)
	}

	if err := validateActionTypeExecutorInput(decl.Executor); err != nil {
		return nil, err
	}

	if decl.InputArtifactDetails == nil {
		return nil, fmt.Errorf("%w: actionType.inputArtifactDetails is required", errInvalidRequest)
	}

	if decl.OutputArtifactDetails == nil {
		return nil, fmt.Errorf("%w: actionType.outputArtifactDetails is required", errInvalidRequest)
	}

	cat := &CustomActionType{
		Category:              id.Category,
		Owner:                 id.Owner,
		Provider:              id.Provider,
		Version:               id.Version,
		Description:           decl.Description,
		Executor:              decl.Executor,
		InputArtifactDetails:  *decl.InputArtifactDetails,
		OutputArtifactDetails: *decl.OutputArtifactDetails,
		Permissions:           decl.Permissions,
		Properties:            decl.Properties,
		Urls:                  decl.Urls,
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
