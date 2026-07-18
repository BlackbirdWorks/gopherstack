package redshift

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Serverless Snapshots
// ---------------------------------------------------------------------------

// CreateServerlessSnapshot creates a snapshot of a serverless namespace.
func (b *InMemoryBackend) CreateServerlessSnapshot(
	snapshotName, namespaceName string,
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
		SnapshotCreateTime: time.Now(),
		SnapshotArn:        snapArn,
		SnapshotName:       snapshotName,
		NamespaceName:      namespaceName,
		NamespaceArn:       ns.NamespaceArn,
		Status:             slStatusAvailable,
		AdminUsername:      ns.AdminUsername,
	}
	b.slSnapshots.Put(snap)
	b.slSnapshotIdx.insert(snapshotName)

	return cloneServerlessSnapshot(snap), nil
}

// GetServerlessSnapshot returns a serverless snapshot by name.
func (b *InMemoryBackend) GetServerlessSnapshot(snapshotName string) (*ServerlessSnapshot, error) {
	b.mu.RLock("GetServerlessSnapshot")
	defer b.mu.RUnlock()

	snap, ok := b.slSnapshots.Get(snapshotName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: snapshot %q not found",
			ErrServerlessSnapshotNotFound,
			snapshotName,
		)
	}

	return cloneServerlessSnapshot(snap), nil
}

// ListServerlessSnapshots returns snapshots, optionally filtered by namespace name.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListServerlessSnapshots(
	namespaceName string,
	maxResults int,
	nextToken string,
) ([]*ServerlessSnapshot, string) {
	b.mu.RLock("ListServerlessSnapshots")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slSnapshotIdx.ordered()
	list := make([]*ServerlessSnapshot, 0, len(keys))

	for _, name := range keys {
		snap, ok := b.slSnapshots.Get(name)
		if !ok {
			continue
		}

		if namespaceName == "" || snap.NamespaceName == namespaceName {
			list = append(list, cloneServerlessSnapshot(snap))
		}
	}

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
	cp.AccountsWithRestoreAccess = make([]string, len(snap.AccountsWithRestoreAccess))
	copy(cp.AccountsWithRestoreAccess, snap.AccountsWithRestoreAccess)

	return &cp
}
