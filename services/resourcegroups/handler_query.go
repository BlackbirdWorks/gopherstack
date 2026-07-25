package resourcegroups

import "context"

// handleSearchResources searches for resources matching a query.
type searchResourcesInput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	NextToken     string         `json:"NextToken"`
	MaxResults    int            `json:"MaxResults"`
}

type searchResourcesOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	ResourceIdentifiers []ResourceIdentifier `json:"ResourceIdentifiers"`
	QueryErrors         []queryErrorWire     `json:"QueryErrors,omitempty"`
	NextToken           string               `json:"NextToken,omitempty"`
}

func (h *Handler) handleSearchResources(ctx context.Context, in *searchResourcesInput) (*searchResourcesOutput, error) {
	identifiers, nextToken, err := h.Backend.SearchResources(ctx, in.ResourceQuery, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &searchResourcesOutput{ResourceIdentifiers: identifiers, NextToken: nextToken}, nil
}
