package glue

import (
	"context"
)

// createUsageProfileInput holds input for CreateUsageProfile.
type createUsageProfileInput struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Name        string            `json:"Name"`
	Description string            `json:"Description,omitempty"`
}

// createUsageProfileOutput holds the result for CreateUsageProfile.
type createUsageProfileOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateUsageProfile(
	_ context.Context,
	in *createUsageProfileInput,
) (*createUsageProfileOutput, error) {
	if _, err := h.Backend.CreateUsageProfile(in.Name, in.Description, in.Tags); err != nil {
		return nil, err
	}

	return &createUsageProfileOutput{Name: in.Name}, nil
}

// deleteUsageProfileInput holds input for DeleteUsageProfile.
type deleteUsageProfileInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteUsageProfile(
	_ context.Context,
	in *deleteUsageProfileInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteUsageProfile(in.Name)
}

// getUsageProfileInput holds input for GetUsageProfile.
type getUsageProfileInput struct {
	Name string `json:"Name"`
}

// getUsageProfileOutput holds the result for GetUsageProfile.
type getUsageProfileOutput struct {
	Tags           map[string]string `json:"Tags,omitempty"`
	Name           string            `json:"Name"`
	Description    string            `json:"Description,omitempty"`
	CreatedOn      float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn float64           `json:"LastModifiedOn,omitempty"`
}

func (h *Handler) handleGetUsageProfile(
	_ context.Context,
	in *getUsageProfileInput,
) (*getUsageProfileOutput, error) {
	if in.Name == "" {
		return &getUsageProfileOutput{}, nil
	}

	p, err := h.Backend.GetUsageProfile(in.Name)
	if err != nil {
		return nil, err
	}

	return &getUsageProfileOutput{
		Name:           p.Name,
		Description:    p.Description,
		CreatedOn:      float64(p.CreatedOn.Unix()),
		LastModifiedOn: float64(p.LastModifiedOn.Unix()),
		Tags:           p.Tags,
	}, nil
}

// listUsageProfilesInput holds input for ListUsageProfiles.
type listUsageProfilesInput struct{}

// listUsageProfilesOutput holds the result for ListUsageProfiles.
type listUsageProfilesOutput struct {
	Profiles []any `json:"Profiles"`
}

func (h *Handler) handleListUsageProfiles(
	_ context.Context,
	_ *listUsageProfilesInput,
) (*listUsageProfilesOutput, error) {
	profiles := h.Backend.ListUsageProfiles()
	result := make([]any, 0, len(profiles))
	for _, p := range profiles {
		result = append(result, p)
	}

	return &listUsageProfilesOutput{Profiles: result}, nil
}

// updateUsageProfileInput holds input for UpdateUsageProfile.
type updateUsageProfileInput struct {
	Name string `json:"Name"`
}

// updateUsageProfileOutput holds the result for UpdateUsageProfile.
type updateUsageProfileOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateUsageProfile(
	_ context.Context,
	in *updateUsageProfileInput,
) (*updateUsageProfileOutput, error) {
	if _, err := h.Backend.UpdateUsageProfile(in.Name, ""); err != nil {
		return nil, err
	}

	return &updateUsageProfileOutput{Name: in.Name}, nil
}
