package backup

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CancelLegalHold cancels (deletes) a legal hold by ID.
func (b *InMemoryBackend) CancelLegalHold(legalHoldID string) error {
	b.mu.Lock("CancelLegalHold")
	defer b.mu.Unlock()

	if !b.legalHolds.Has(legalHoldID) {
		return fmt.Errorf("%w: legal hold %s not found", ErrNotFound, legalHoldID)
	}

	b.legalHolds.Delete(legalHoldID)

	return nil
}

// CreateLegalHold creates a legal hold. sel (RecoveryPointSelection) is
// stored on the hold and is what ListRecoveryPointsByLegalHold filters
// against; a nil or all-empty selection covers every recovery point.
func (b *InMemoryBackend) CreateLegalHold(
	title, description string,
	sel *RecoveryPointSelection,
) (*LegalHold, error) {
	b.mu.Lock("CreateLegalHold")
	defer b.mu.Unlock()

	id := uuid.NewString()
	lhARN := arn.Build("backup", b.region, b.accountID, "legal-hold:"+id)
	lh := &LegalHold{
		LegalHoldID:            id,
		LegalHoldArn:           lhARN,
		Title:                  title,
		Description:            description,
		Status:                 statusActive,
		CreationDate:           time.Now().UTC(),
		RecoveryPointSelection: sel,
	}
	b.legalHolds.Put(lh)
	cp := *lh

	return &cp, nil
}

// GetLegalHold returns a legal hold by ID.
func (b *InMemoryBackend) GetLegalHold(legalHoldID string) (*LegalHold, error) {
	b.mu.RLock("GetLegalHold")
	defer b.mu.RUnlock()

	lh, ok := b.legalHolds.Get(legalHoldID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errLegalHoldNotFound, legalHoldID)
	}

	return lh, nil
}

// ListLegalHolds returns all legal holds.
func (b *InMemoryBackend) ListLegalHolds() []*LegalHold {
	b.mu.RLock("ListLegalHolds")
	defer b.mu.RUnlock()

	all := b.legalHolds.All()
	out := make([]*LegalHold, 0, len(all))
	for _, lh := range all {
		cp := *lh
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LegalHoldID < out[j].LegalHoldID })

	return out
}
