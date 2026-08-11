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
func (b *InMemoryBackend) GetServerlessSnapshot(nameOrArn string) (*ServerlessSnapshot, error) {
	b.mu.RLock("GetServerlessSnapshot")
	defer b.mu.RUnlock()

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
	cp.AccountsWithRestoreAccess = cloneStrings(snap.AccountsWithRestoreAccess)

	return &cp
}
