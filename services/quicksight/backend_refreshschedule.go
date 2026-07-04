package quicksight

import (
	"sort"
	"time"
)

func isValidIngestionType(refreshType string) bool {
	switch refreshType {
	case "INCREMENTAL_REFRESH", "FULL_REFRESH":
		return true
	}

	return false
}

// refreshSchedulesLocked returns ds.RefreshSchedules, lazily initializing it for
// datasets created (or restored from a snapshot) before this map existed.
// Callers must hold b.mu.
func refreshSchedulesLocked(ds *storedDataSet) map[string]*storedRefreshSchedule {
	if ds.RefreshSchedules == nil {
		ds.RefreshSchedules = make(map[string]*storedRefreshSchedule)
	}

	return ds.RefreshSchedules
}

// ---- DataSet refresh schedules ----

func (b *InMemoryBackend) CreateRefreshSchedule(
	accountID, datasetID, scheduleID, refreshType, startAfterDateTime string,
	scheduleFrequency map[string]any,
) (*RefreshSchedule, error) {
	if scheduleID == "" || !isValidIngestionType(refreshType) {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateRefreshSchedule")
	defer b.mu.Unlock()

	ds, ok := b.dataSets[dataSetKey(accountID, datasetID)]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	schedules := refreshSchedulesLocked(ds)
	if _, exists := schedules[scheduleID]; exists {
		return nil, ErrRefreshScheduleAlreadyExists
	}

	s := &storedRefreshSchedule{
		ScheduleID:         scheduleID,
		Arn:                ds.Arn + "/refresh-schedule/" + scheduleID,
		RefreshType:        refreshType,
		StartAfterDateTime: startAfterDateTime,
		ScheduleFrequency:  scheduleFrequency,
	}
	schedules[scheduleID] = s

	return s.toRefreshSchedule(), nil
}

func (b *InMemoryBackend) DescribeRefreshSchedule(accountID, datasetID, scheduleID string) (*RefreshSchedule, error) {
	b.mu.RLock("DescribeRefreshSchedule")
	defer b.mu.RUnlock()

	ds, ok := b.dataSets[dataSetKey(accountID, datasetID)]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	s, ok := ds.RefreshSchedules[scheduleID]
	if !ok {
		return nil, ErrRefreshScheduleNotFound
	}

	return s.toRefreshSchedule(), nil
}

func (b *InMemoryBackend) UpdateRefreshSchedule(
	accountID, datasetID, scheduleID, refreshType, startAfterDateTime string,
	scheduleFrequency map[string]any,
) (*RefreshSchedule, error) {
	b.mu.Lock("UpdateRefreshSchedule")
	defer b.mu.Unlock()

	ds, ok := b.dataSets[dataSetKey(accountID, datasetID)]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	schedules := refreshSchedulesLocked(ds)
	s, ok := schedules[scheduleID]
	if !ok {
		return nil, ErrRefreshScheduleNotFound
	}

	if refreshType != "" {
		if !isValidIngestionType(refreshType) {
			return nil, ErrValidation
		}
		s.RefreshType = refreshType
	}
	if startAfterDateTime != "" {
		s.StartAfterDateTime = startAfterDateTime
	}
	if scheduleFrequency != nil {
		s.ScheduleFrequency = scheduleFrequency
	}

	return s.toRefreshSchedule(), nil
}

func (b *InMemoryBackend) DeleteRefreshSchedule(accountID, datasetID, scheduleID string) error {
	b.mu.Lock("DeleteRefreshSchedule")
	defer b.mu.Unlock()

	ds, ok := b.dataSets[dataSetKey(accountID, datasetID)]
	if !ok {
		return ErrDataSetNotFound
	}

	schedules := refreshSchedulesLocked(ds)
	if _, exists := schedules[scheduleID]; !exists {
		return ErrRefreshScheduleNotFound
	}
	delete(schedules, scheduleID)

	return nil
}

func (b *InMemoryBackend) ListRefreshSchedules(accountID, datasetID string) ([]*RefreshSchedule, error) {
	b.mu.RLock("ListRefreshSchedules")
	defer b.mu.RUnlock()

	ds, ok := b.dataSets[dataSetKey(accountID, datasetID)]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	ids := make([]string, 0, len(ds.RefreshSchedules))
	for id := range ds.RefreshSchedules {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	result := make([]*RefreshSchedule, 0, len(ids))
	for _, id := range ids {
		result = append(result, ds.RefreshSchedules[id].toRefreshSchedule())
	}

	return result, nil
}

// ---- DataSet refresh properties ----

func (b *InMemoryBackend) PutDataSetRefreshProperties(
	accountID, datasetID string,
	refreshConfiguration, failureConfiguration map[string]any,
) (*DataSetRefreshProperties, error) {
	b.mu.Lock("PutDataSetRefreshProperties")
	defer b.mu.Unlock()

	ds, ok := b.dataSets[dataSetKey(accountID, datasetID)]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	ds.RefreshProperties = &storedDataSetRefreshProperties{
		RefreshConfiguration: refreshConfiguration,
		FailureConfiguration: failureConfiguration,
	}
	ds.LastUpdatedTime = time.Now().UTC()

	return ds.RefreshProperties.toDataSetRefreshProperties(), nil
}

func (b *InMemoryBackend) DescribeDataSetRefreshProperties(
	accountID, datasetID string,
) (*DataSetRefreshProperties, error) {
	b.mu.RLock("DescribeDataSetRefreshProperties")
	defer b.mu.RUnlock()

	ds, ok := b.dataSets[dataSetKey(accountID, datasetID)]
	if !ok {
		return nil, ErrDataSetNotFound
	}

	if ds.RefreshProperties == nil {
		return nil, ErrDataSetRefreshPropertiesNotFound
	}

	return ds.RefreshProperties.toDataSetRefreshProperties(), nil
}

func (b *InMemoryBackend) DeleteDataSetRefreshProperties(accountID, datasetID string) error {
	b.mu.Lock("DeleteDataSetRefreshProperties")
	defer b.mu.Unlock()

	ds, ok := b.dataSets[dataSetKey(accountID, datasetID)]
	if !ok {
		return ErrDataSetNotFound
	}

	if ds.RefreshProperties == nil {
		return ErrDataSetRefreshPropertiesNotFound
	}
	ds.RefreshProperties = nil

	return nil
}
