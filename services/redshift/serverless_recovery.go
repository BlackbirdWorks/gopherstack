package redshift

import (
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Serverless recovery points (Get/ListRecoveryPoint, RestoreFromRecoveryPoint)
// ---------------------------------------------------------------------------

// generateRecoveryPointLocked creates and stores one recovery point for the
// given namespace/workgroup pair. Callers must already hold b.mu for writing
// (same convention as DeleteNamespace inlining a final-snapshot write under
// its own lock rather than calling a separately-locking helper).
func (b *InMemoryBackend) generateRecoveryPointLocked(ns *Namespace, wg *Workgroup) *RecoveryPoint {
	rp := &RecoveryPoint{
		RecoveryPointID:         uuid.New().String(),
		NamespaceArn:            ns.NamespaceArn,
		NamespaceName:           ns.NamespaceName,
		WorkgroupName:           wg.WorkgroupName,
		RecoveryPointCreateTime: time.Now(),
	}
	b.slRecoveryPoints.Put(rp)
	b.slRecoveryPointIdx.insert(rp.RecoveryPointID)

	return rp
}

// AddRecoveryPointInternal seeds a recovery point directly. Test-only: real
// AWS never exposes a CreateRecoveryPoint operation (recovery points are
// created automatically), so this is not wired to any wire-reachable op --
// same convention as AddSnapshotInternal/AddReservedNodeInternal etc.
func (b *InMemoryBackend) AddRecoveryPointInternal(rp *RecoveryPoint) {
	b.mu.Lock("AddRecoveryPointInternal")
	defer b.mu.Unlock()

	b.slRecoveryPoints.Put(rp)
	b.slRecoveryPointIdx.insert(rp.RecoveryPointID)
}

// GetRecoveryPointSL returns a recovery point by ID.
func (b *InMemoryBackend) GetRecoveryPointSL(recoveryPointID string) (*RecoveryPoint, error) {
	b.mu.RLock("GetRecoveryPointSL")
	defer b.mu.RUnlock()

	rp, ok := b.slRecoveryPoints.Get(recoveryPointID)
	if !ok {
		return nil, fmt.Errorf("%w: recovery point %q not found", ErrRecoveryPointNotFound, recoveryPointID)
	}

	cp := *rp

	return &cp, nil
}

// ListRecoveryPointsParams holds ListRecoveryPointsInput's filters.
type ListRecoveryPointsParams struct {
	StartTime     time.Time
	EndTime       time.Time
	NamespaceName string
	NamespaceArn  string
	NextToken     string
	MaxResults    int
}

// matches reports whether rp satisfies every filter set on p (an unset
// filter matches everything). Split out of ListRecoveryPointsSL to keep that
// function's cognitive complexity flat as filters were added.
func (p ListRecoveryPointsParams) matches(rp *RecoveryPoint) bool {
	if p.NamespaceName != "" && rp.NamespaceName != p.NamespaceName {
		return false
	}

	if p.NamespaceArn != "" && rp.NamespaceArn != p.NamespaceArn {
		return false
	}

	if !p.StartTime.IsZero() && rp.RecoveryPointCreateTime.Before(p.StartTime) {
		return false
	}

	if !p.EndTime.IsZero() && rp.RecoveryPointCreateTime.After(p.EndTime) {
		return false
	}

	return true
}

// ListRecoveryPointsSL returns recovery points, optionally filtered by
// namespaceName, namespaceArn, and creation time range (all accepted per
// ListRecoveryPointsRequest in service-2.json).
func (b *InMemoryBackend) ListRecoveryPointsSL(p ListRecoveryPointsParams) ([]*RecoveryPoint, string) {
	b.mu.RLock("ListRecoveryPointsSL")
	defer b.mu.RUnlock()

	keys := b.slRecoveryPointIdx.ordered()
	list := make([]*RecoveryPoint, 0, len(keys))

	for _, id := range keys {
		rp, ok := b.slRecoveryPoints.Get(id)
		if !ok || !p.matches(rp) {
			continue
		}

		cp := *rp
		list = append(list, &cp)
	}

	maxResults := p.MaxResults
	nextToken := p.NextToken

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := decodeServerlessPageToken(nextToken)

	if startIdx >= len(list) {
		return []*RecoveryPoint{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// RestoreFromRecoveryPointSL restores namespaceName's data from recoveryPointID
// using workgroupName's compute. namespaceName/recoveryPointId/workgroupName
// are all required on the real RestoreFromRecoveryPointRequest (confirmed
// against service-2.json). Real AWS restores a namespace's storage layer in
// place; this backend does not simulate real data content, so once the FK
// checks pass, the existing Namespace is returned unchanged -- consistent
// with how this service's other "restore" ops (e.g.
// CreateSnapshotCopyConfiguration's cross-region copy) do not simulate real
// data movement, only the request/response contract.
func (b *InMemoryBackend) RestoreFromRecoveryPointSL(
	namespaceName, workgroupName, recoveryPointID string,
) (*Namespace, error) {
	b.mu.Lock("RestoreFromRecoveryPointSL")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces.Get(namespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	if wg.NamespaceName != namespaceName {
		return nil, fmt.Errorf(
			"%w: workgroup %q does not belong to namespace %q",
			ErrServerlessValidation, workgroupName, namespaceName,
		)
	}

	if _, exists := b.slRecoveryPoints.Get(recoveryPointID); !exists {
		return nil, fmt.Errorf("%w: recovery point %q not found", ErrRecoveryPointNotFound, recoveryPointID)
	}

	return cloneNamespace(ns), nil
}
