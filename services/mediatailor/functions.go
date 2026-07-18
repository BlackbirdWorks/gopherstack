package mediatailor

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Function operations ---

// PutFunction creates or updates a function.
func (b *InMemoryBackend) PutFunction(
	functionID, functionType, description string,
	tags map[string]string,
) (*Function, error) {
	if functionType == "" {
		return nil, fmt.Errorf("%w: FunctionType is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutFunction")
	defer b.mu.Unlock()

	arnStr := arn.Build("mediatailor", b.region, b.accountID, fmt.Sprintf("function/%s", functionID))
	fn := &Function{
		Tags:         copyTags(tags),
		FunctionID:   functionID,
		FunctionType: functionType,
		ARN:          arnStr,
		Description:  description,
	}
	b.functions.Put(fn)

	return fn, nil
}

// GetFunction returns a function by ID.
func (b *InMemoryBackend) GetFunction(functionID string) (*Function, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	fn, ok := b.functions.Get(functionID)
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	return fn, nil
}

// DeleteFunction deletes a function.
func (b *InMemoryBackend) DeleteFunction(functionID string) error {
	b.mu.Lock("DeleteFunction")
	defer b.mu.Unlock()

	if !b.functions.Has(functionID) {
		return fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	b.functions.Delete(functionID)

	return nil
}

// ListFunctions returns a paginated list of functions.
func (b *InMemoryBackend) ListFunctions(maxResults int, nextToken string) ([]*FunctionSummary, string, error) {
	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	all := b.functions.All()

	sort.Slice(all, func(i, j int) bool { return all[i].FunctionID < all[j].FunctionID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	out := make([]*FunctionSummary, 0, len(pg.Data))
	for _, fn := range pg.Data {
		out = append(out, &FunctionSummary{
			FunctionID:   fn.FunctionID,
			FunctionType: fn.FunctionType,
			ARN:          fn.ARN,
			Tags:         copyTags(fn.Tags),
		})
	}

	return out, pg.Next, nil
}
