package verifiedpermissions

import (
	"context"
	"fmt"
)

type validationSettingsJSON struct {
	Mode string `json:"mode"`
}

type createPolicyStoreInput struct {
	Tags               map[string]string      `json:"tags"`
	Description        string                 `json:"description"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
	DeletionProtection string                 `json:"deletionProtection,omitempty"`
}

type createPolicyStoreOutput struct {
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	CreatedDate        string                 `json:"createdDate"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
}

func (h *Handler) handleCreatePolicyStore(
	_ context.Context,
	in *createPolicyStoreInput,
) (*createPolicyStoreOutput, error) {
	if in.ValidationSettings.Mode == "" {
		return nil, fmt.Errorf("%w: validationSettings.mode is required", errInvalidRequest)
	}

	if in.ValidationSettings.Mode != ValidationModeOff && in.ValidationSettings.Mode != ValidationModeStrict {
		return nil, fmt.Errorf(
			"%w: validationSettings.mode must be %q or %q",
			errInvalidRequest, ValidationModeOff, ValidationModeStrict,
		)
	}

	// AWS bounds PolicyStoreDescription at 150 characters.
	if len(in.Description) > maxPolicyStoreDescriptionLen {
		return nil, fmt.Errorf(
			"%w: description must be %d characters or fewer",
			errInvalidRequest, maxPolicyStoreDescriptionLen,
		)
	}

	ps, err := h.Backend.CreatePolicyStore(
		in.Description, in.Tags,
		in.ValidationSettings.Mode, in.DeletionProtection,
	)
	if err != nil {
		return nil, err
	}

	return &createPolicyStoreOutput{
		PolicyStoreID:      ps.PolicyStoreID,
		Arn:                ps.Arn,
		CreatedDate:        ps.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
		ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
	}, nil
}

type policyStoreIDInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

type policyStoreView struct {
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	Description        string                 `json:"description"`
	CreatedDate        string                 `json:"createdDate"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
	DeletionProtection string                 `json:"deletionProtection,omitempty"`
}

type getPolicyStoreOutput struct {
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	Description        string                 `json:"description"`
	CreatedDate        string                 `json:"createdDate"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
	DeletionProtection string                 `json:"deletionProtection,omitempty"`
}

func (h *Handler) handleGetPolicyStore(_ context.Context, in *policyStoreIDInput) (*getPolicyStoreOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	ps, err := h.Backend.GetPolicyStore(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	return &getPolicyStoreOutput{
		PolicyStoreID:      ps.PolicyStoreID,
		Arn:                ps.Arn,
		Description:        ps.Description,
		CreatedDate:        ps.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
		ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
		DeletionProtection: ps.DeletionProtection,
	}, nil
}

type listPolicyStoresInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listPolicyStoresOutput struct {
	NextToken    string            `json:"nextToken,omitempty"`
	PolicyStores []policyStoreView `json:"policyStores"`
}

func (h *Handler) handleListPolicyStores(
	_ context.Context,
	in *listPolicyStoresInput,
) (*listPolicyStoresOutput, error) {
	stores, nextToken := h.Backend.ListPolicyStores(in.NextToken, in.MaxResults)
	items := make([]policyStoreView, 0, len(stores))

	for i := range stores {
		ps := &stores[i]
		items = append(items, policyStoreView{
			PolicyStoreID:      ps.PolicyStoreID,
			Arn:                ps.Arn,
			Description:        ps.Description,
			CreatedDate:        ps.CreatedDate.UTC().Format(timeFormat),
			LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
			ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
			DeletionProtection: ps.DeletionProtection,
		})
	}

	return &listPolicyStoresOutput{PolicyStores: items, NextToken: nextToken}, nil
}

type updatePolicyStoreInput struct {
	PolicyStoreID      string                  `json:"policyStoreId"`
	Description        string                  `json:"description"`
	ValidationSettings *validationSettingsJSON `json:"validationSettings,omitempty"`
	DeletionProtection string                  `json:"deletionProtection,omitempty"`
}

type updatePolicyStoreOutput struct {
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
}

func (h *Handler) handleUpdatePolicyStore(
	_ context.Context,
	in *updatePolicyStoreInput,
) (*updatePolicyStoreOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	var validationMode string

	if in.ValidationSettings != nil {
		validationMode = in.ValidationSettings.Mode
	}

	ps, err := h.Backend.UpdatePolicyStore(in.PolicyStoreID, in.Description, validationMode, in.DeletionProtection)
	if err != nil {
		return nil, err
	}

	return &updatePolicyStoreOutput{
		PolicyStoreID:      ps.PolicyStoreID,
		Arn:                ps.Arn,
		LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
		ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
	}, nil
}

func (h *Handler) handleDeletePolicyStore(_ context.Context, in *policyStoreIDInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePolicyStore(in.PolicyStoreID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
