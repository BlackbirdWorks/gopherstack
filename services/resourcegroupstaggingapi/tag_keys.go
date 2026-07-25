package resourcegroupstaggingapi

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// GetTagKeysInput is the request payload for GetTagKeys.
type GetTagKeysInput struct {
	// PaginationToken is the cursor from a previous call.
	PaginationToken *string `json:"PaginationToken,omitempty"`
}

// GetTagKeysOutput is the response payload for GetTagKeys.
type GetTagKeysOutput struct {
	PaginationToken *string  `json:"PaginationToken,omitempty"`
	TagKeys         []string `json:"TagKeys"`
}

// GetTagKeys returns all unique tag keys across all registered resource providers.
// Keys are returned in sorted order, with optional cursor-based pagination. Returns
// [ErrPaginationTokenExpired] when PaginationToken does not resolve against the
// current key set.
func (b *InMemoryBackend) GetTagKeys(ctx context.Context, input *GetTagKeysInput) (*GetTagKeysOutput, error) {
	b.mu.Lock("GetTagKeys")
	defer b.mu.Unlock()

	all := b.getResources(ctx, nil, nil)
	keySet := make(map[string]struct{})

	for _, r := range all {
		for k := range r.Tags {
			keySet[k] = struct{}{}
		}
	}

	keys := collections.SortedKeys(keySet)

	page, nextToken, err := paginateStrings(keys, ptrconv.String(input.PaginationToken), defaultResourcesPerPage)
	if err != nil {
		return nil, err
	}

	return &GetTagKeysOutput{TagKeys: page, PaginationToken: nextToken}, nil
}
