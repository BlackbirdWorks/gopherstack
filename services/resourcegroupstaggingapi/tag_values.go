package resourcegroupstaggingapi

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// GetTagValuesInput is the request payload for GetTagValues.
type GetTagValuesInput struct {
	// Key is the tag key whose values to enumerate.
	Key *string `json:"Key,omitempty"`
	// PaginationToken is the cursor from a previous call.
	PaginationToken *string `json:"PaginationToken,omitempty"`
}

// GetTagValuesOutput is the response payload for GetTagValues.
type GetTagValuesOutput struct {
	PaginationToken *string  `json:"PaginationToken,omitempty"`
	TagValues       []string `json:"TagValues"`
}

// GetTagValues returns all unique values for the given tag key.
// Values are returned in sorted order, with optional cursor-based pagination.
func (b *InMemoryBackend) GetTagValues(ctx context.Context, input *GetTagValuesInput) *GetTagValuesOutput {
	b.mu.Lock("GetTagValues")
	defer b.mu.Unlock()

	if input.Key == nil {
		return &GetTagValuesOutput{TagValues: []string{}}
	}

	all := b.getResources(ctx, nil, nil)
	valSet := make(map[string]struct{})
	key := *input.Key

	for _, r := range all {
		if v, ok := r.Tags[key]; ok {
			valSet[v] = struct{}{}
		}
	}

	values := collections.SortedKeys(valSet)

	page, nextToken := paginateStrings(values, ptrconv.String(input.PaginationToken), defaultResourcesPerPage)

	return &GetTagValuesOutput{TagValues: page, PaginationToken: nextToken}
}
