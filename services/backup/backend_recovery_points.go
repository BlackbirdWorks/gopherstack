package backup

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// ListRecoveryPointsByBackupVault returns all recovery points for a vault.
func (b *InMemoryBackend) ListRecoveryPointsByBackupVault(
	vaultName string,
) ([]*RecoveryPoint, error) {
	b.mu.RLock("ListRecoveryPointsByBackupVault")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	pts := b.recoveryPointsByVault.Get(vaultName)
	list := make([]*RecoveryPoint, 0, len(pts))
	for _, rp := range pts {
		cp := *rp
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *RecoveryPoint) int {
		if a.CreationDate.After(b.CreationDate) {
			return -1
		}
		if a.CreationDate.Before(b.CreationDate) {
			return 1
		}

		return 0
	})

	return list, nil
}

// DescribeRecoveryPoint returns a specific recovery point.
func (b *InMemoryBackend) DescribeRecoveryPoint(
	vaultName, recoveryPointArn string,
) (*RecoveryPoint, error) {
	b.mu.RLock("DescribeRecoveryPoint")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	rp, ok := b.recoveryPoints.Get(recoveryPointKey(vaultName, recoveryPointArn))
	if !ok {
		return nil, fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	cp := *rp

	return &cp, nil
}

// GetRecoveryPointRestoreMetadata returns restore metadata for a recovery point.
func (b *InMemoryBackend) GetRecoveryPointRestoreMetadata(
	vaultName, recoveryPointArn string,
) (map[string]string, error) {
	b.mu.RLock("GetRecoveryPointRestoreMetadata")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	if !b.recoveryPoints.Has(recoveryPointKey(vaultName, recoveryPointArn)) {
		return nil, fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	return map[string]string{}, nil
}

// DeleteRecoveryPoint deletes a recovery point from a vault.
func (b *InMemoryBackend) DeleteRecoveryPoint(vaultName, recoveryPointArn string) error {
	b.mu.Lock("DeleteRecoveryPoint")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	key := recoveryPointKey(vaultName, recoveryPointArn)
	if !b.recoveryPoints.Has(key) {
		return fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	b.recoveryPoints.Delete(key)
	if v, ok := b.vaults.Get(vaultName); ok {
		if v.NumberOfRecoveryPoints > 0 {
			v.NumberOfRecoveryPoints--
		}
	}

	return nil
}

// DisassociateRecoveryPoint disassociates a recovery point from a vault.
func (b *InMemoryBackend) DisassociateRecoveryPoint(vaultName, recoveryPointArn string) error {
	b.mu.Lock("DisassociateRecoveryPoint")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	key := recoveryPointKey(vaultName, recoveryPointArn)
	if !b.recoveryPoints.Has(key) {
		return fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	b.recoveryPoints.Delete(key)

	return nil
}

// DisassociateRecoveryPointFromParent disassociates a recovery point from its parent.
func (b *InMemoryBackend) DisassociateRecoveryPointFromParent(
	vaultName, recoveryPointArn string,
) error {
	b.mu.Lock("DisassociateRecoveryPointFromParent")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	if !b.recoveryPoints.Has(recoveryPointKey(vaultName, recoveryPointArn)) {
		return fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	return nil
}

// AddRecoveryPoint adds a recovery point to a vault (used internally and in tests).
func (b *InMemoryBackend) AddRecoveryPoint(vaultName string, rp *RecoveryPoint) error {
	b.mu.Lock("AddRecoveryPoint")
	defer b.mu.Unlock()

	vault, ok := b.vaults.Get(vaultName)
	if !ok {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	cp := *rp
	cp.BackupVaultName = vaultName
	b.recoveryPoints.Put(&cp)
	vault.NumberOfRecoveryPoints++

	return nil
}

// --- Vault compliance methods ---

// ListRecoveryPointsByLegalHold returns recovery points associated with a legal hold.
// In this implementation, returns all recovery points (legal hold → RP association not tracked).
func (b *InMemoryBackend) ListRecoveryPointsByLegalHold(legalHoldID string) []*RecoveryPoint {
	b.mu.RLock("ListRecoveryPointsByLegalHold")
	defer b.mu.RUnlock()

	_ = legalHoldID // association not tracked; return empty slice for accuracy

	return []*RecoveryPoint{}
}

// ListRecoveryPointsByResource returns recovery points for a given resource ARN across all vaults.
func (b *InMemoryBackend) ListRecoveryPointsByResource(resourceArn string) []*RecoveryPoint {
	b.mu.RLock("ListRecoveryPointsByResource")
	defer b.mu.RUnlock()

	var out []*RecoveryPoint
	for _, rp := range b.recoveryPoints.All() {
		if rp.ResourceArn == resourceArn {
			cp := *rp
			out = append(out, &cp)
		}
	}
	sort.Slice(
		out,
		func(i, j int) bool { return out[i].RecoveryPointArn < out[j].RecoveryPointArn },
	)

	return out
}

// ---- Recovery Point lifecycle ----

// UpdateRecoveryPointLifecycle updates the lifecycle of a recovery point,
// recomputing its CalculatedLifecycle transition timestamps from CreationDate.
func (b *InMemoryBackend) UpdateRecoveryPointLifecycle(
	vaultName, recoveryPointArn string,
	moveToColdStorageAfterDays, deleteAfterDays int64,
) error {
	b.mu.Lock("UpdateRecoveryPointLifecycle")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: %s", errVaultNotFoundB1, vaultName)
	}

	rp, ok := b.recoveryPoints.Get(recoveryPointKey(vaultName, recoveryPointArn))
	if !ok {
		return fmt.Errorf("%w: %s", errRecoveryPointNotFound, recoveryPointArn)
	}

	lc := &Lifecycle{
		MoveToColdStorageAfterDays: moveToColdStorageAfterDays,
		DeleteAfterDays:            deleteAfterDays,
	}
	rp.Lifecycle = lc
	rp.CalculatedLifecycle = calculateLifecycle(lc, rp.CreationDate)

	return nil
}

// calculateLifecycle derives CalculatedLifecycle transition timestamps for a
// recovery point's lifecycle policy, measured from the given reference time
// (normally the recovery point's CreationDate). Returns nil if lc is nil.
func calculateLifecycle(lc *Lifecycle, from time.Time) *CalculatedLifecycle {
	if lc == nil {
		return nil
	}

	cl := &CalculatedLifecycle{}
	if lc.MoveToColdStorageAfterDays > 0 {
		t := from.AddDate(0, 0, int(lc.MoveToColdStorageAfterDays))
		cl.MoveToColdStorageAt = &t
	}
	if lc.DeleteAfterDays > 0 {
		t := from.AddDate(0, 0, int(lc.DeleteAfterDays))
		cl.DeleteAt = &t
	}

	return cl
}

// GetRecoveryPointIndexDetails returns index details for a recovery point.
func (b *InMemoryBackend) GetRecoveryPointIndexDetails(
	vaultName, recoveryPointArn string,
) (string, error) {
	b.mu.RLock("GetRecoveryPointIndexDetails")
	defer b.mu.RUnlock()

	key := vaultName + ":" + recoveryPointArn
	status := b.recoveryPointIndexStatus[key]
	if status == "" {
		status = statusActive
	}

	return status, nil
}

// UpdateRecoveryPointIndexSettings updates indexing settings for a recovery point.
func (b *InMemoryBackend) UpdateRecoveryPointIndexSettings(
	vaultName, recoveryPointArn, indexStatus string,
) error {
	b.mu.Lock("UpdateRecoveryPointIndexSettings")
	defer b.mu.Unlock()

	key := vaultName + ":" + recoveryPointArn
	b.recoveryPointIndexStatus[key] = indexStatus

	return nil
}

// ListIndexedRecoveryPoints returns recovery points with an active index.
func (b *InMemoryBackend) ListIndexedRecoveryPoints() []*RecoveryPoint {
	b.mu.RLock("ListIndexedRecoveryPoints")
	defer b.mu.RUnlock()

	var out []*RecoveryPoint //nolint:prealloc // preserves nil (not empty-slice) return when no recovery points exist
	for _, rp := range b.recoveryPoints.All() {
		cp := *rp
		out = append(out, &cp)
	}
	sort.Slice(
		out,
		func(i, j int) bool { return out[i].RecoveryPointArn < out[j].RecoveryPointArn },
	)

	return out
}

// ---- Job operations ----

// ListRPFilter contains optional filter parameters for listing recovery points.
type ListRPFilter struct {
	CreatedAfter           *time.Time
	CreatedBefore          *time.Time
	ResourceArn            string
	ResourceType           string
	ParentRecoveryPointArn string
	NextToken              string
	MaxResults             int
}

// rpMatchesFilter reports whether rp satisfies all active fields in f.
func rpMatchesFilter(rp *RecoveryPoint, f ListRPFilter) bool {
	if f.ResourceArn != "" && rp.ResourceArn != f.ResourceArn {
		return false
	}
	if f.ResourceType != "" && rp.ResourceType != f.ResourceType {
		return false
	}
	if f.ParentRecoveryPointArn != "" && rp.ParentRecoveryPointArn != f.ParentRecoveryPointArn {
		return false
	}
	if f.CreatedAfter != nil && !rp.CreationDate.After(*f.CreatedAfter) {
		return false
	}
	if f.CreatedBefore != nil && !rp.CreationDate.Before(*f.CreatedBefore) {
		return false
	}

	return true
}

// ListRecoveryPointsFiltered returns recovery points for a vault with optional filters and pagination.
func (b *InMemoryBackend) ListRecoveryPointsFiltered(
	vaultName string,
	f ListRPFilter,
) ([]*RecoveryPoint, string, error) {
	b.mu.RLock("ListRecoveryPointsFiltered")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, "", fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	pts := b.recoveryPointsByVault.Get(vaultName)
	list := make([]*RecoveryPoint, 0, len(pts))
	for _, rp := range pts {
		if !rpMatchesFilter(rp, f) {
			continue
		}
		cp := *rp
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *RecoveryPoint) int {
		if d := b.CreationDate.Compare(a.CreationDate); d != 0 {
			return d
		}

		return strings.Compare(a.RecoveryPointArn, b.RecoveryPointArn)
	})

	page, token := paginateByID(
		list,
		func(rp *RecoveryPoint) string { return rp.RecoveryPointArn },
		f.MaxResults,
		f.NextToken,
	)

	return page, token, nil
}

// ---- ListCopyJobs filtering + pagination ----
