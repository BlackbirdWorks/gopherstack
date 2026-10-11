package resourcegroups

import "context"

// handleSearchResources searches for resources matching a query.
type searchResourcesInput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	NextToken     string         `json:"NextToken"`
	MaxResults    int            `json:"MaxResults"`
}

type searchResourcesOutput struct {
	NextToken           string               `json:"NextToken,omitempty"`
	ResourceIdentifiers []ResourceIdentifier `json:"ResourceIdentifiers"`
	QueryErrors         []queryErrorWire     `json:"QueryErrors,omitempty"`
}

func (h *Handler) handleSearchResources(ctx context.Context, in *searchResourcesInput) (*searchResourcesOutput, error) {
	cursor, err := parsePage(in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	p, err := h.Backend.SearchResourcesPage(ctx, in.ResourceQuery, cursor, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &searchResourcesOutput{
		ResourceIdentifiers: p.Identifiers,
		QueryErrors:         p.Errors,
		NextToken:           encodePageToken(p.NextToken),
	}, nil
}
