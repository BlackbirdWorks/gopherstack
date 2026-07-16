package redshift

import "fmt"

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

	cp := cloneSnapshotSchedule(sched)

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

		return []SnapshotSchedule{*cloneSnapshotSchedule(s)}, nil
	}

	result := make([]SnapshotSchedule, 0, b.snapshotSchedules.Len())

	for _, s := range b.snapshotSchedules.All() {
		result = append(result, *cloneSnapshotSchedule(s))
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

	return cloneSnapshotSchedule(sched), nil
}

// ModifyClusterSnapshotSchedule associates or disassociates a snapshot schedule with a cluster.
func (b *InMemoryBackend) ModifyClusterSnapshotSchedule(clusterID, scheduleID string, disassociate bool) error {
	if clusterID == "" {
		return fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterSnapshotSchedule")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if !disassociate && scheduleID != "" {
		if _, exists := b.snapshotSchedules.Get(scheduleID); !exists {
			return fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
		}
	}

	return nil
}

// cloneSnapshotSchedule returns a deep copy of a SnapshotSchedule.
func cloneSnapshotSchedule(s *SnapshotSchedule) *SnapshotSchedule {
	cp := *s
	cp.ScheduleDefinitions = make([]string, len(s.ScheduleDefinitions))
	copy(cp.ScheduleDefinitions, s.ScheduleDefinitions)

	return &cp
}
