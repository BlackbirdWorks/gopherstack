package backup

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CancelLegalHold marks a legal hold CANCELED. A positive retainRecordInDays
// drops the record after that many days; otherwise it stays readable.
func (b *InMemoryBackend) CancelLegalHold(legalHoldID, description string, retainRecordInDays int64) error {
	b.mu.Lock("CancelLegalHold")
	defer b.mu.Unlock()

	b.purgeExpiredLegalHoldsLocked()

	lh, ok := b.legalHolds.Get(legalHoldID)
	if !ok {
		return fmt.Errorf("%w: legal hold %s not found", ErrNotFound, legalHoldID)
	}

	now := time.Now().UTC()
	lh.Status = statusCanceled
	lh.CancellationDate = &now
	lh.CancelDescription = description

	if retainRecordInDays > 0 {
		until := now.Add(time.Duration(retainRecordInDays) * 24 * time.Hour)
		lh.RetainRecordUntil = &until
	}

	return nil
}

// purgeExpiredLegalHoldsLocked deletes canceled holds past RetainRecordUntil.
func (b *InMemoryBackend) purgeExpiredLegalHoldsLocked() {
	now := time.Now()

	for _, lh := range b.legalHolds.All() {
		if lh.RetainRecordUntil != nil && lh.RetainRecordUntil.Before(now) {
			b.legalHolds.Delete(lh.LegalHoldID)
		}
	}
}

// CreateLegalHold creates a legal hold. sel (RecoveryPointSelection) is
// stored on the hold and is what ListRecoveryPointsByLegalHold filters
// against; a nil or all-empty selection covers every recovery point.
func (b *InMemoryBackend) CreateLegalHold(
	title, description string,
	sel *RecoveryPointSelection,
) (*LegalHold, error) {
	return b.CreateLegalHoldWithToken(title, description, sel, "")
}

// CreateLegalHoldWithToken is CreateLegalHold with an IdempotencyToken; a retry
// with the same token returns the hold it created.
func (b *InMemoryBackend) CreateLegalHoldWithToken(
	title, description string,
	sel *RecoveryPointSelection,
	token string,
) (*LegalHold, error) {
	b.mu.Lock("CreateLegalHold")
	defer b.mu.Unlock()

	if token != "" {
		for _, lh := range b.legalHolds.All() {
			if lh.IdempotencyToken == token {
				cp := *lh

				return &cp, nil
			}
		}
	}

	id := uuid.NewString()
	lhARN := arn.Build("backup", b.region, b.accountID, "legal-hold:"+id)
	lh := &LegalHold{
		LegalHoldID:            id,
		LegalHoldArn:           lhARN,
		IdempotencyToken:       token,
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
	b.mu.Lock("GetLegalHold")
	defer b.mu.Unlock()

	b.purgeExpiredLegalHoldsLocked()

	lh, ok := b.legalHolds.Get(legalHoldID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errLegalHoldNotFound, legalHoldID)
	}

	cp := *lh

	return &cp, nil
}

// ListLegalHolds returns legal holds, paginated by MaxResults/NextToken
// (real query params, ListLegalHolds serializers.go:6055-6061 -- lowercase
// "maxResults"/"nextToken" on the wire).
func (b *InMemoryBackend) ListLegalHolds(maxResults int, nextToken string) ([]*LegalHold, string) {
	b.mu.Lock("ListLegalHolds")
	defer b.mu.Unlock()

	b.purgeExpiredLegalHoldsLocked()

	all := b.legalHolds.All()
	out := make([]*LegalHold, 0, len(all))
	for _, lh := range all {
		cp := *lh
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LegalHoldID < out[j].LegalHoldID })

	return paginateByID(out, func(lh *LegalHold) string { return lh.LegalHoldID }, maxResults, nextToken)
}
