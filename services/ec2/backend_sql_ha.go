package ec2

import (
	"fmt"
	"sort"
	"time"
)

// backend_sql_ha.go implements the SQL Server High Availability standby
// detection family: EnableInstanceSQLHaStandbyDetections,
// DisableInstanceSQLHaStandbyDetections, DescribeInstanceSQLHaStates, and
// DescribeInstanceSQLHaHistoryStates.

// SQL Server HA status constants, mirroring the AWS-modeled HaStatus enum.
const (
	sqlHaStatusProcessing = "processing"
	sqlHaStatusActive     = "active"
)

// RegisteredSQLHaInstance represents an instance registered for SQL Server
// High Availability standby detection monitoring.
type RegisteredSQLHaInstance struct {
	LastUpdatedTime       time.Time         `json:"lastUpdatedTime"`
	Tags                  map[string]string `json:"tags,omitempty"`
	InstanceID            string            `json:"instanceId,omitempty"`
	HaStatus              string            `json:"haStatus,omitempty"`
	ProcessingStatus      string            `json:"processingStatus,omitempty"`
	SQLServerCredentials  string            `json:"sqlServerCredentials,omitempty"`
	SQLServerLicenseUsage string            `json:"sqlServerLicenseUsage,omitempty"`
}

// resetSQLHaMapsLocked re-initialises the SQL HA registration/history maps.
// Must be called with b.mu held.
func (b *InMemoryBackend) resetSQLHaMapsLocked() {
	b.sqlHaRegistrations = make(map[string]*RegisteredSQLHaInstance)
	b.sqlHaHistory = make(map[string][]*RegisteredSQLHaInstance)
}

// recordSQLHaHistoryLocked appends a snapshot of reg to its instance's history
// log. Must be called with b.mu held.
func (b *InMemoryBackend) recordSQLHaHistoryLocked(reg *RegisteredSQLHaInstance) {
	cp := *reg
	b.sqlHaHistory[reg.InstanceID] = append(b.sqlHaHistory[reg.InstanceID], &cp)
}

// EnableInstanceSQLHaStandbyDetections registers one or more instances for SQL
// Server High Availability standby detection monitoring.
func (b *InMemoryBackend) EnableInstanceSQLHaStandbyDetections(
	instanceIDs []string, sqlServerCredentials string,
) ([]*RegisteredSQLHaInstance, error) {
	if len(instanceIDs) == 0 {
		return nil, fmt.Errorf("%w: InstanceIds is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableInstanceSQLHaStandbyDetections")
	defer b.mu.Unlock()

	for _, id := range instanceIDs {
		if _, ok := b.instances[id]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}
	}

	out := make([]*RegisteredSQLHaInstance, 0, len(instanceIDs))

	for _, id := range instanceIDs {
		reg := &RegisteredSQLHaInstance{
			InstanceID:            id,
			HaStatus:              sqlHaStatusProcessing,
			LastUpdatedTime:       time.Now().UTC(),
			ProcessingStatus:      "Enabling SQL Server High Availability standby detection monitoring",
			SQLServerCredentials:  sqlServerCredentials,
			SQLServerLicenseUsage: "full",
		}
		b.sqlHaRegistrations[id] = reg
		b.recordSQLHaHistoryLocked(reg)

		cp := *reg
		out = append(out, &cp)
	}

	return out, nil
}

// DisableInstanceSQLHaStandbyDetections unregisters one or more instances from
// SQL Server High Availability standby detection monitoring.
func (b *InMemoryBackend) DisableInstanceSQLHaStandbyDetections(
	instanceIDs []string,
) ([]*RegisteredSQLHaInstance, error) {
	if len(instanceIDs) == 0 {
		return nil, fmt.Errorf("%w: InstanceIds is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableInstanceSQLHaStandbyDetections")
	defer b.mu.Unlock()

	out := make([]*RegisteredSQLHaInstance, 0, len(instanceIDs))

	for _, id := range instanceIDs {
		reg, ok := b.sqlHaRegistrations[id]
		if !ok {
			continue
		}

		cp := *reg
		cp.ProcessingStatus = "Disabled from SQL Server High Availability standby detection monitoring"
		cp.LastUpdatedTime = time.Now().UTC()
		b.recordSQLHaHistoryLocked(&cp)
		delete(b.sqlHaRegistrations, id)

		out = append(out, &cp)
	}

	return out, nil
}

// DescribeInstanceSQLHaStates returns the current SQL Server High Availability
// registration state for the given instances (or all registered instances if
// ids is empty). Registrations that are still "processing" settle to "active"
// as a side effect of being described.
func (b *InMemoryBackend) DescribeInstanceSQLHaStates(ids []string) []*RegisteredSQLHaInstance {
	b.mu.Lock("DescribeInstanceSQLHaStates")
	defer b.mu.Unlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*RegisteredSQLHaInstance, 0, len(b.sqlHaRegistrations))

	for _, reg := range b.sqlHaRegistrations {
		if len(idSet) > 0 && !idSet[reg.InstanceID] {
			continue
		}

		if reg.HaStatus == sqlHaStatusProcessing {
			reg.HaStatus = sqlHaStatusActive
			reg.ProcessingStatus = "Active"
			reg.LastUpdatedTime = time.Now().UTC()
			b.recordSQLHaHistoryLocked(reg)
		}

		cp := *reg
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// DescribeInstanceSQLHaHistoryStates returns historical SQL Server High
// Availability state transitions for the given instances (or all instances
// with history if ids is empty), optionally bounded by [startTime, endTime].
func (b *InMemoryBackend) DescribeInstanceSQLHaHistoryStates(
	ids []string, startTime, endTime time.Time,
) []*RegisteredSQLHaInstance {
	b.mu.RLock("DescribeInstanceSQLHaHistoryStates")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*RegisteredSQLHaInstance, 0)

	for instanceID, history := range b.sqlHaHistory {
		if len(idSet) > 0 && !idSet[instanceID] {
			continue
		}

		for _, entry := range history {
			if !startTime.IsZero() && entry.LastUpdatedTime.Before(startTime) {
				continue
			}

			if !endTime.IsZero() && entry.LastUpdatedTime.After(endTime) {
				continue
			}

			cp := *entry
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].InstanceID != out[j].InstanceID {
			return out[i].InstanceID < out[j].InstanceID
		}

		return out[i].LastUpdatedTime.Before(out[j].LastUpdatedTime)
	})

	return out
}
