package appsync

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// mergeTypeManual is the default merge type for source API associations.
const mergeTypeManual = "MANUAL_MERGE"

// AssociateMergedGraphqlAPI creates an association from a source API to a merged API.
func (b *InMemoryBackend) AssociateMergedGraphqlAPI(
	sourceAPIIdentifier, mergedAPIIdentifier, description, mergeType string,
) (*SourceAPIAssociation, error) {
	b.mu.Lock("AssociateMergedGraphqlAPI")
	defer b.mu.Unlock()

	if !b.apis.Has(sourceAPIIdentifier) {
		return nil, fmt.Errorf("%w: source api %s not found", ErrNotFound, sourceAPIIdentifier)
	}

	if !b.apis.Has(mergedAPIIdentifier) {
		return nil, fmt.Errorf("%w: merged api %s not found", ErrNotFound, mergedAPIIdentifier)
	}

	arnPath := fmt.Sprintf("sourceApis/%s/mergedApiAssociations", sourceAPIIdentifier)

	return b.buildSourceAssoc(sourceAPIIdentifier, mergedAPIIdentifier, description, mergeType, arnPath), nil
}

// AssociateSourceGraphqlAPI creates an association from a merged API to a source API.
func (b *InMemoryBackend) AssociateSourceGraphqlAPI(
	mergedAPIIdentifier, sourceAPIIdentifier, description, mergeType string,
) (*SourceAPIAssociation, error) {
	b.mu.Lock("AssociateSourceGraphqlAPI")
	defer b.mu.Unlock()

	if !b.apis.Has(mergedAPIIdentifier) {
		return nil, fmt.Errorf("%w: merged api %s not found", ErrNotFound, mergedAPIIdentifier)
	}

	if !b.apis.Has(sourceAPIIdentifier) {
		return nil, fmt.Errorf("%w: source api %s not found", ErrNotFound, sourceAPIIdentifier)
	}

	arnPath := fmt.Sprintf("mergedApis/%s/sourceApiAssociations", mergedAPIIdentifier)

	return b.buildSourceAssoc(sourceAPIIdentifier, mergedAPIIdentifier, description, mergeType, arnPath), nil
}

// buildSourceAssoc creates and stores a SourceAPIAssociation. Caller must hold the write lock.
func (b *InMemoryBackend) buildSourceAssoc(
	sourceAPIID, mergedAPIID, description, mergeType, arnPathPrefix string,
) *SourceAPIAssociation {
	if mergeType == "" {
		mergeType = mergeTypeManual
	}

	assocID := randomAPIID()
	assocARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("%s/%s", arnPathPrefix, assocID))

	assoc := &SourceAPIAssociation{
		AssociationID:              assocID,
		AssociationARN:             assocARN,
		SourceAPIID:                sourceAPIID,
		MergedAPIID:                mergedAPIID,
		Description:                description,
		AssociationStatus:          "MERGE_SCHEDULED",
		SourceAPIAssociationConfig: &SourceAPIAssociationConfig{MergeType: mergeType},
	}

	b.sourceAssocs.Put(assoc)

	cp := *assoc

	return &cp
}

// GetSourceAPIAssociation returns a source API association by merged API ID and association ID.
func (b *InMemoryBackend) GetSourceAPIAssociation(mergedAPIID, associationID string) (*SourceAPIAssociation, error) {
	b.mu.RLock("GetSourceApiAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.sourceAssocs.Get(associationID)
	if !ok || assoc.MergedAPIID != mergedAPIID {
		return nil, fmt.Errorf("%w: source api association %s not found", ErrNotFound, associationID)
	}

	cp := *assoc

	return &cp, nil
}

// ListSourceAPIAssociations returns all source API associations for a merged API.
func (b *InMemoryBackend) ListSourceAPIAssociations(mergedAPIID string) ([]*SourceAPIAssociation, error) {
	b.mu.RLock("ListSourceApiAssociations")
	defer b.mu.RUnlock()

	assocs := b.sourceAssocs.All()
	out := make([]*SourceAPIAssociation, 0, len(assocs))

	for _, assoc := range assocs {
		if assoc.MergedAPIID == mergedAPIID {
			cp := *assoc
			out = append(out, &cp)
		}
	}

	slices.SortFunc(out, func(a, b *SourceAPIAssociation) int {
		return strings.Compare(a.AssociationID, b.AssociationID)
	})

	return out, nil
}

// DisassociateMergedGraphqlAPI removes a merged API association from a source API.
func (b *InMemoryBackend) DisassociateMergedGraphqlAPI(sourceAPIID, associationID string) error {
	b.mu.Lock("DisassociateMergedGraphqlApi")
	defer b.mu.Unlock()

	assoc, ok := b.sourceAssocs.Get(associationID)
	if !ok || assoc.SourceAPIID != sourceAPIID {
		return fmt.Errorf("%w: merged api association %s not found", ErrNotFound, associationID)
	}

	b.sourceAssocs.Delete(associationID)

	return nil
}

// DisassociateSourceGraphqlAPI removes a source API association from a merged API.
func (b *InMemoryBackend) DisassociateSourceGraphqlAPI(mergedAPIID, associationID string) error {
	b.mu.Lock("DisassociateSourceGraphqlApi")
	defer b.mu.Unlock()

	assoc, ok := b.sourceAssocs.Get(associationID)
	if !ok || assoc.MergedAPIID != mergedAPIID {
		return fmt.Errorf("%w: source api association %s not found", ErrNotFound, associationID)
	}

	b.sourceAssocs.Delete(associationID)

	return nil
}

// UpdateSourceAPIAssociation updates the description of a source API association.
func (b *InMemoryBackend) UpdateSourceAPIAssociation(
	mergedAPIID, associationID, description string,
) (*SourceAPIAssociation, error) {
	b.mu.Lock("UpdateSourceApiAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.sourceAssocs.Get(associationID)
	if !ok || assoc.MergedAPIID != mergedAPIID {
		return nil, fmt.Errorf("%w: source api association %s not found", ErrNotFound, associationID)
	}

	cp := *assoc
	cp.Description = description

	b.sourceAssocs.Put(&cp)

	return &cp, nil
}
