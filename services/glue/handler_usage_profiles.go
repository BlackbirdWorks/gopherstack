package glue

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
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

// defaultListUsageProfilesLimit is used when ListUsageProfilesInput.MaxResults is unset.
const defaultListUsageProfilesLimit = 100

// listUsageProfilesInput holds input for ListUsageProfiles.
type listUsageProfilesInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// usageProfileDefinition mirrors
// aws-sdk-go-v2/service/glue/types.UsageProfileDefinition. CreatedOn/
// LastModifiedOn are epoch floats (via pkgs/awstime), not the raw
// UsageProfile.CreatedOn time.Time -- that field's plain json tag marshals to
// an RFC3339 string, which the real client rejects for this unixTimestamp
// wire shape (same class of bug pkgs/awstime exists to prevent).
type usageProfileDefinition struct {
	Name           string  `json:"Name"`
	Description    string  `json:"Description,omitempty"`
	CreatedOn      float64 `json:"CreatedOn,omitempty"`
	LastModifiedOn float64 `json:"LastModifiedOn,omitempty"`
}

// listUsageProfilesOutput holds the result for ListUsageProfiles.
type listUsageProfilesOutput struct {
	NextToken string                   `json:"NextToken,omitempty"`
	Profiles  []usageProfileDefinition `json:"Profiles"`
}

func (h *Handler) handleListUsageProfiles(
	_ context.Context,
	in *listUsageProfilesInput,
) (*listUsageProfilesOutput, error) {
	all := h.Backend.ListUsageProfiles()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListUsageProfilesLimit
	}

	page, next := paginateSlice(all, in.NextToken, limit)

	result := make([]usageProfileDefinition, 0, len(page))
	for _, p := range page {
		result = append(result, usageProfileDefinition{
			Name:           p.Name,
			Description:    p.Description,
			CreatedOn:      awstime.Epoch(p.CreatedOn),
			LastModifiedOn: awstime.Epoch(p.LastModifiedOn),
		})
	}

	return &listUsageProfilesOutput{Profiles: result, NextToken: next}, nil
}

// updateUsageProfileInput holds input for UpdateUsageProfile. Configuration
// (*types.ProfileConfiguration, required on the real op) has no backing
// model on UsageProfile -- the same deferred gap already documented for
// GetUsageProfile -- and is not accepted here.
type updateUsageProfileInput struct {
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
}

// updateUsageProfileOutput holds the result for UpdateUsageProfile.
type updateUsageProfileOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateUsageProfile(
	_ context.Context,
	in *updateUsageProfileInput,
) (*updateUsageProfileOutput, error) {
	if _, err := h.Backend.UpdateUsageProfile(in.Name, in.Description); err != nil {
		return nil, err
	}

	return &updateUsageProfileOutput{Name: in.Name}, nil
}
