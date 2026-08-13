package redshift

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Serverless Snapshots
// ---------------------------------------------------------------------------

// CreateServerlessSnapshot creates a snapshot of a serverless namespace. tags
// holds CreateSnapshotInput's "tags" (confirmed present on
// CreateSnapshotRequest in service-2.json).
func (b *InMemoryBackend) CreateServerlessSnapshot(
	snapshotName, namespaceName string,
	retentionPeriod int,
	tags map[string]string,
) (*ServerlessSnapshot, error) {
	b.mu.Lock("CreateServerlessSnapshot")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces.Get(namespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	if _, exists := b.slSnapshots.Get(snapshotName); exists {
		return nil, fmt.Errorf(
			"%w: snapshot %q already exists",
			ErrServerlessConflict,
			snapshotName,
		)
	}

	snapArn := arn.Build("redshift-serverless", b.region, b.accountID, "snapshot/"+snapshotName)

	snap := &ServerlessSnapshot{
		SnapshotCreateTime:      time.Now(),
		SnapshotArn:             snapArn,
		SnapshotName:            snapshotName,
		NamespaceName:           namespaceName,
		NamespaceArn:            ns.NamespaceArn,
		Status:                  slStatusAvailable,
		AdminUsername:           ns.AdminUsername,
		SnapshotRetentionPeriod: retentionPeriod,
	}
	b.slSnapshots.Put(snap)
	b.slSnapshotIdx.insert(snapshotName)
	b.putServerlessTagsLocked(snapArn, tags)

	return cloneServerlessSnapshot(snap), nil
}

// GetServerlessSnapshot returns a serverless snapshot by name or ARN
// (GetSnapshotInput accepts either SnapshotName or SnapshotArn).
//
// ownerAccount is honestly single-account: this backend never simulates
// cross-account snapshot sharing for the serverless surface (AuthorizeSnapshotAccess
// is not part of this API; ServerlessSnapshot.AccountsWithRestoreAccess is
// declared for wire shape but never populated), so every snapshot's real
// owner is b.accountID. A non-empty ownerAccount that doesn't match
// b.accountID can therefore never resolve to a snapshot, same as real AWS
// would return for an inaccessible cross-account snapshot.
func (b *InMemoryBackend) GetServerlessSnapshot(nameOrArn, ownerAccount string) (*ServerlessSnapshot, error) {
	b.mu.RLock("GetServerlessSnapshot")
	defer b.mu.RUnlock()

	if ownerAccount != "" && ownerAccount != b.accountID {
		return nil, fmt.Errorf(
			"%w: snapshot %q not found",
			ErrServerlessSnapshotNotFound,
			nameOrArn,
		)
	}

	name := nameOrArn
	if idx := strings.LastIndex(nameOrArn, "/"); strings.Contains(nameOrArn, ":snapshot/") && idx >= 0 {
		name = nameOrArn[idx+1:]
	}

	snap, ok := b.slSnapshots.Get(name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: snapshot %q not found",
			ErrServerlessSnapshotNotFound,
			nameOrArn,
		)
	}

	return cloneServerlessSnapshot(snap), nil
}

// ListServerlessSnapshotsParams holds ListSnapshotsInput's filters.
type ListServerlessSnapshotsParams struct {
	StartTime     time.Time
	EndTime       time.Time
	NamespaceName string
	NamespaceArn  string
	OwnerAccount  string
	NextToken     string
	MaxResults    int
}

// matches reports whether snap satisfies every filter set on p (an unset
// filter matches everything). Split out of ListServerlessSnapshots to keep
// that function's cognitive complexity flat as filters were added.
func (p ListServerlessSnapshotsParams) matches(snap *ServerlessSnapshot) bool {
	if p.NamespaceName != "" && snap.NamespaceName != p.NamespaceName {
		return false
	}

	if p.NamespaceArn != "" && snap.NamespaceArn != p.NamespaceArn {
		return false
	}

	if !p.StartTime.IsZero() && snap.SnapshotCreateTime.Before(p.StartTime) {
		return false
	}

	if !p.EndTime.IsZero() && snap.SnapshotCreateTime.After(p.EndTime) {
		return false
	}

	return true
}

// ListServerlessSnapshots returns snapshots, optionally filtered by
// namespace name/ARN, creation time range, and owner account.
//
// p.OwnerAccount is honestly single-account -- see GetServerlessSnapshot's
// doc comment for why comparing against b.accountID is the correct filter
// here rather than a no-op.
func (b *InMemoryBackend) ListServerlessSnapshots(
	p ListServerlessSnapshotsParams,
) ([]*ServerlessSnapshot, string) {
	b.mu.RLock("ListServerlessSnapshots")
	defer b.mu.RUnlock()

	if p.OwnerAccount != "" && p.OwnerAccount != b.accountID {
		return []*ServerlessSnapshot{}, ""
	}

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slSnapshotIdx.ordered()
	list := make([]*ServerlessSnapshot, 0, len(keys))

	for _, name := range keys {
		snap, ok := b.slSnapshots.Get(name)
		if !ok || !p.matches(snap) {
			continue
		}

		list = append(list, cloneServerlessSnapshot(snap))
	}

	maxResults := p.MaxResults
	nextToken := p.NextToken

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*ServerlessSnapshot{}, ""
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

// DeleteServerlessSnapshot deletes a serverless snapshot.
func (b *InMemoryBackend) DeleteServerlessSnapshot(
	snapshotName string,
) (*ServerlessSnapshot, error) {
	b.mu.Lock("DeleteServerlessSnapshot")
	defer b.mu.Unlock()

	snap, ok := b.slSnapshots.Get(snapshotName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: snapshot %q not found",
			ErrServerlessSnapshotNotFound,
			snapshotName,
		)
	}

	cp := cloneServerlessSnapshot(snap)
	b.slSnapshots.Delete(snapshotName)
	b.slSnapshotIdx.remove(snapshotName)

	return cp, nil
}

func cloneServerlessSnapshot(snap *ServerlessSnapshot) *ServerlessSnapshot {
	cp := *snap
	cp.AccountsWithRestoreAccess = cloneStrings(snap.AccountsWithRestoreAccess)

	return &cp
}
