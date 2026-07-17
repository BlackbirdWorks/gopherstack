package lakeformation

import (
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// AddLFTagExpressionInternal seeds an LFTagExpression directly for testing.
func (b *InMemoryBackend) AddLFTagExpressionInternal(expr *LFTagExpression) {
	b.mu.Lock("AddLFTagExpressionInternal")
	defer b.mu.Unlock()

	cp := *expr

	if expr.Expression != nil {
		cp.Expression = make([]LFTag, len(expr.Expression))
		copy(cp.Expression, expr.Expression)
	}

	b.lfTagExpressions.Put(&cp)
}

// CreateLFTagExpression stores a new named LF-tag expression.
func (b *InMemoryBackend) CreateLFTagExpression(name, description, catalogID string, expression []LFTag) error {
	b.mu.Lock("CreateLFTagExpression")
	defer b.mu.Unlock()

	k := lfTagExpressionKeyStr(catalogID, name)

	// Validate each tag in the expression has a TagKey.
	for i, tag := range expression {
		if strings.TrimSpace(tag.TagKey) == "" {
			return fmt.Errorf("Expression[%d].TagKey is required: %w", i, ErrValidation)
		}
	}

	if b.lfTagExpressions.Has(k) {
		return awserr.New(
			"LF-tag expression already exists: "+name,
			awserr.ErrAlreadyExists,
		)
	}

	expr := make([]LFTag, len(expression))
	copy(expr, expression)

	b.lfTagExpressions.Put(&LFTagExpression{
		Name:        name,
		Description: description,
		CatalogID:   catalogID,
		Expression:  expr,
	})

	return nil
}

// DeleteLFTagExpression removes the named LF-tag expression.
func (b *InMemoryBackend) DeleteLFTagExpression(name, catalogID string) error {
	b.mu.Lock("DeleteLFTagExpression")
	defer b.mu.Unlock()

	k := lfTagExpressionKeyStr(catalogID, name)

	if !b.lfTagExpressions.Has(k) {
		return awserr.New(
			"LF-tag expression not found: "+name,
			awserr.ErrNotFound,
		)
	}

	b.lfTagExpressions.Delete(k)

	return nil
}

// ListLFTagExpressions returns a paginated list of LF-tag expressions for the given catalog.
func (b *InMemoryBackend) ListLFTagExpressions(
	catalogID string,
	maxResults int,
	nextToken string,
) ([]*LFTagExpression, string) {
	b.mu.RLock("ListLFTagExpressions")
	defer b.mu.RUnlock()

	all := make([]*LFTagExpression, 0, b.lfTagExpressions.Len())

	for _, v := range b.lfTagExpressions.All() {
		if catalogID != "" && v.CatalogID != catalogID {
			continue
		}

		expr := *v

		if v.Expression != nil {
			expr.Expression = make([]LFTag, len(v.Expression))
			copy(expr.Expression, v.Expression)
		}

		all = append(all, &expr)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].CatalogID != all[j].CatalogID {
			return all[i].CatalogID < all[j].CatalogID
		}

		return all[i].Name < all[j].Name
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// GetLFTagExpression returns the named LF-tag expression.
func (b *InMemoryBackend) GetLFTagExpression(name, catalogID string) (*LFTagExpression, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("Name is required: %w", ErrValidation)
	}
	b.mu.RLock("GetLFTagExpression")
	defer b.mu.RUnlock()
	k := lfTagExpressionKeyStr(catalogID, name)
	expr, ok := b.lfTagExpressions.Get(k)
	if !ok {
		return nil, awserr.New("LF-tag expression not found: "+name, awserr.ErrNotFound)
	}
	cp := *expr
	if expr.Expression != nil {
		cp.Expression = make([]LFTag, len(expr.Expression))
		copy(cp.Expression, expr.Expression)
	}

	return &cp, nil
}

// UpdateLFTagExpression updates the description and expression of an existing LF-tag expression.
func (b *InMemoryBackend) UpdateLFTagExpression(name, catalogID, description string, expression []LFTag) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("Name is required: %w", ErrValidation)
	}
	b.mu.Lock("UpdateLFTagExpression")
	defer b.mu.Unlock()
	k := lfTagExpressionKeyStr(catalogID, name)
	expr, ok := b.lfTagExpressions.Get(k)
	if !ok {
		return awserr.New("LF-tag expression not found: "+name, awserr.ErrNotFound)
	}
	expr.Description = description
	if expression != nil {
		if len(expression) == 0 {
			return fmt.Errorf("expression must not be empty: %w", ErrValidation)
		}
		cp := make([]LFTag, len(expression))
		copy(cp, expression)
		expr.Expression = cp
	}

	return nil
}
