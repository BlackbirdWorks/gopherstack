package redshift

import (
	"fmt"
	"sort"
)

// ---- Snapshot Schedules ----

// CreateSnapshotSchedule creates a new snapshot schedule.
func (b *InMemoryBackend) CreateSnapshotSchedule(
	scheduleID, description string,
	definitions []string,
	tagMap map[string]string,
) (*SnapshotSchedule, error) {
	if scheduleID == "" {
		return nil, fmt.Errorf("%w: ScheduleIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSnapshotSchedule")
	defer b.mu.Unlock()

	if _, exists := b.snapshotSchedules.Get(scheduleID); exists {
		return nil, fmt.Errorf("%w: schedule %s already exists", ErrSnapshotScheduleAlreadyExists, scheduleID)
	}

	defCopy := make([]string, len(definitions))
	copy(defCopy, definitions)

	sched := &SnapshotSchedule{
		ScheduleIdentifier:  scheduleID,
		Description:         description,
		ScheduleDefinitions: defCopy,
		Tags:                tagMap,
	}
	b.snapshotSchedules.Put(sched)

	cp := b.cloneSnapshotSchedule(sched)

	return cp, nil
}

// DeleteSnapshotSchedule deletes the named snapshot schedule.
func (b *InMemoryBackend) DeleteSnapshotSchedule(scheduleID string) error {
	if scheduleID == "" {
		return fmt.Errorf("%w: ScheduleIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSnapshotSchedule")
	defer b.mu.Unlock()

	if _, exists := b.snapshotSchedules.Get(scheduleID); !exists {
		return fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
	}

	b.snapshotSchedules.Delete(scheduleID)

	return nil
}

// DescribeSnapshotSchedules returns snapshot schedules, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeSnapshotSchedules(scheduleID string) ([]SnapshotSchedule, error) {
	b.mu.RLock("DescribeSnapshotSchedules")
	defer b.mu.RUnlock()

	if scheduleID != "" {
		s, exists := b.snapshotSchedules.Get(scheduleID)
		if !exists {
			return nil, fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
		}

		return []SnapshotSchedule{*b.cloneSnapshotSchedule(s)}, nil
	}

	result := make([]SnapshotSchedule, 0, b.snapshotSchedules.Len())

	for _, s := range b.snapshotSchedules.All() {
		result = append(result, *b.cloneSnapshotSchedule(s))
	}

	return result, nil
}

// ModifySnapshotSchedule updates the schedule definitions for an existing snapshot schedule.
func (b *InMemoryBackend) ModifySnapshotSchedule(scheduleID string, definitions []string) (*SnapshotSchedule, error) {
	if scheduleID == "" {
		return nil, fmt.Errorf("%w: ScheduleIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySnapshotSchedule")
	defer b.mu.Unlock()

	sched, exists := b.snapshotSchedules.Get(scheduleID)
	if !exists {
		return nil, fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
	}

	defCopy := make([]string, len(definitions))
	copy(defCopy, definitions)
	sched.ScheduleDefinitions = defCopy

	return b.cloneSnapshotSchedule(sched), nil
}

// ModifyClusterSnapshotSchedule associates or disassociates a snapshot schedule with
// a cluster by setting/clearing Cluster.SnapshotScheduleIdentifier -- the real wire
// field (confirmed against aws-sdk-go-v2/service/redshift@v1.62.3/types.Cluster).
// SnapshotSchedule.AssociatedClusters (see snapshotScheduleAssociatedClusters) is
// derived by scanning for clusters whose SnapshotScheduleIdentifier matches, rather
// than stored redundantly on the schedule.
func (b *InMemoryBackend) ModifyClusterSnapshotSchedule(clusterID, scheduleID string, disassociate bool) error {
	if clusterID == "" {
		return fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterSnapshotSchedule")
	defer b.mu.Unlock()

	c, exists := b.clusters.Get(clusterID)
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if disassociate {
		c.SnapshotScheduleIdentifier = ""
		c.SnapshotScheduleState = ""

		return nil
	}

	if scheduleID == "" {
		return fmt.Errorf("%w: ScheduleIdentifier is required unless DisassociateSchedule is set", ErrInvalidParameter)
	}

	if _, scheduleExists := b.snapshotSchedules.Get(scheduleID); !scheduleExists {
		return fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
	}

	c.SnapshotScheduleIdentifier = scheduleID
	c.SnapshotScheduleState = dataShareStatusActive

	return nil
}

// snapshotScheduleAssociatedClusters returns the identifiers of every cluster
// currently associated with scheduleID, sorted for deterministic output. Caller
// must hold b.mu (read or write).
func (b *InMemoryBackend) snapshotScheduleAssociatedClusters(scheduleID string) []string {
	var ids []string

	for _, c := range b.clusters.All() {
		if c.SnapshotScheduleIdentifier == scheduleID {
			ids = append(ids, c.ClusterIdentifier)
		}
	}

	sort.Strings(ids)

	return ids
}

// cloneSnapshotSchedule returns a deep copy of a SnapshotSchedule, with
// AssociatedClusters freshly computed from current cluster state. Caller must hold
// b.mu (read or write).
func (b *InMemoryBackend) cloneSnapshotSchedule(s *SnapshotSchedule) *SnapshotSchedule {
	cp := *s
	cp.ScheduleDefinitions = make([]string, len(s.ScheduleDefinitions))
	copy(cp.ScheduleDefinitions, s.ScheduleDefinitions)
	cp.AssociatedClusters = b.snapshotScheduleAssociatedClusters(s.ScheduleIdentifier)

	return &cp
}
