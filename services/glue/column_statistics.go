package glue

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

var ErrColumnStatTaskRunNotFound = fmt.Errorf("column statistics task run not found: %w", ErrNotFound)

// columnStatTaskKey builds the key for task settings.
func columnStatTaskKey(dbName, tableName string) string {
	return dbName + "|" + tableName
}

// CreateColumnStatisticsTaskSettings stores task settings.
func (b *InMemoryBackend) CreateColumnStatisticsTaskSettings(
	dbName, tableName, roleArn string,
	columns []string,
) (*ColumnStatisticsTaskSettings, error) {
	b.mu.Lock("CreateColumnStatisticsTaskSettings")
	defer b.mu.Unlock()

	settings := &ColumnStatisticsTaskSettings{
		DatabaseName:   dbName,
		TableName:      tableName,
		ColumnNameList: columns,
		RoleArn:        roleArn,
	}
	b.columnStatTaskSettings.Put(settings)
	cp := *settings

	return &cp, nil
}

// GetColumnStatisticsTaskSettings returns task settings.
func (b *InMemoryBackend) GetColumnStatisticsTaskSettings(
	dbName, tableName string,
) (*ColumnStatisticsTaskSettings, error) {
	b.mu.RLock("GetColumnStatisticsTaskSettings")
	defer b.mu.RUnlock()

	key := columnStatTaskKey(dbName, tableName)
	s, ok := b.columnStatTaskSettings.Get(key)

	if !ok {
		return &ColumnStatisticsTaskSettings{DatabaseName: dbName, TableName: tableName}, nil
	}

	cp := *s

	return &cp, nil
}

// UpdateColumnStatisticsTaskSettings updates task settings.
func (b *InMemoryBackend) UpdateColumnStatisticsTaskSettings(
	dbName, tableName, roleArn string,
) error {
	b.mu.Lock("UpdateColumnStatisticsTaskSettings")
	defer b.mu.Unlock()

	key := columnStatTaskKey(dbName, tableName)
	if s, ok := b.columnStatTaskSettings.Get(key); ok {
		s.RoleArn = roleArn
	}

	return nil
}

// DeleteColumnStatisticsTaskSettings removes task settings.
func (b *InMemoryBackend) DeleteColumnStatisticsTaskSettings(dbName, tableName string) error {
	b.mu.Lock("DeleteColumnStatisticsTaskSettings")
	defer b.mu.Unlock()

	b.columnStatTaskSettings.Delete(columnStatTaskKey(dbName, tableName))

	return nil
}

// StartColumnStatisticsTaskRunSchedule enables the run schedule for a table's
// column statistics task settings. The settings must already exist (created via
// CreateColumnStatisticsTaskSettings), matching AWS's requirement that a
// schedule can only be attached to existing task settings.
func (b *InMemoryBackend) StartColumnStatisticsTaskRunSchedule(dbName, tableName string) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("%w: DatabaseName and TableName are required", ErrValidation)
	}

	b.mu.Lock("StartColumnStatisticsTaskRunSchedule")
	defer b.mu.Unlock()

	s, ok := b.columnStatTaskSettings.Get(columnStatTaskKey(dbName, tableName))
	if !ok {
		return fmt.Errorf(
			"column statistics task settings not found for %s.%s: %w", dbName, tableName, ErrNotFound,
		)
	}

	s.Schedule.State = stateScheduled

	return nil
}

// StopColumnStatisticsTaskRunSchedule disables the run schedule for a table's
// column statistics task settings.
func (b *InMemoryBackend) StopColumnStatisticsTaskRunSchedule(dbName, tableName string) error {
	if dbName == "" || tableName == "" {
		return fmt.Errorf("%w: DatabaseName and TableName are required", ErrValidation)
	}

	b.mu.Lock("StopColumnStatisticsTaskRunSchedule")
	defer b.mu.Unlock()

	s, ok := b.columnStatTaskSettings.Get(columnStatTaskKey(dbName, tableName))
	if !ok {
		return fmt.Errorf(
			"column statistics task settings not found for %s.%s: %w", dbName, tableName, ErrNotFound,
		)
	}

	s.Schedule.State = stateNotScheduled

	return nil
}

// StartColumnStatisticsTaskRun starts a column statistics task run.
func (b *InMemoryBackend) StartColumnStatisticsTaskRun(dbName, tableName string) (*ColumnStatisticsTaskRun, error) {
	b.mu.Lock("StartColumnStatisticsTaskRun")
	defer b.mu.Unlock()

	runID := "cstr-" + uuid.NewString()[:8]
	run := &ColumnStatisticsTaskRun{
		DatabaseName:              dbName,
		TableName:                 tableName,
		ColumnStatisticsTaskRunID: runID,
		Status:                    "STARTED",
		StartedOn:                 float64(time.Now().Unix()),
	}
	b.columnStatTaskRuns.Put(run)
	cp := *run

	return &cp, nil
}

// StopColumnStatisticsTaskRun stops a task run.
func (b *InMemoryBackend) StopColumnStatisticsTaskRun(runID string) error {
	b.mu.Lock("StopColumnStatisticsTaskRun")
	defer b.mu.Unlock()

	r, ok := b.columnStatTaskRuns.Get(runID)
	if !ok {
		return ErrColumnStatTaskRunNotFound
	}

	r.Status = stateStopped

	return nil
}

// GetColumnStatisticsTaskRun returns a task run.
func (b *InMemoryBackend) GetColumnStatisticsTaskRun(runID string) (*ColumnStatisticsTaskRun, error) {
	b.mu.RLock("GetColumnStatisticsTaskRun")
	defer b.mu.RUnlock()

	r, ok := b.columnStatTaskRuns.Get(runID)
	if !ok {
		return nil, ErrColumnStatTaskRunNotFound
	}

	cp := *r

	return &cp, nil
}

// ListColumnStatisticsTaskRuns returns all task runs.
func (b *InMemoryBackend) ListColumnStatisticsTaskRuns() []*ColumnStatisticsTaskRun {
	b.mu.RLock("ListColumnStatisticsTaskRuns")
	defer b.mu.RUnlock()

	src := b.columnStatTaskRuns.All()
	runs := make([]*ColumnStatisticsTaskRun, 0, len(src))
	for _, r := range src {
		cp := *r
		runs = append(runs, &cp)
	}

	sort.Slice(runs, func(i, k int) bool {
		return runs[i].StartedOn < runs[k].StartedOn
	})

	return runs
}

// GetColumnStatisticsTaskRuns returns task runs as a slice (alias for ListColumnStatisticsTaskRuns).
func (b *InMemoryBackend) GetColumnStatisticsTaskRuns() []*ColumnStatisticsTaskRun {
	return b.ListColumnStatisticsTaskRuns()
}

func cloneColumnStats(cs *ColumnStatistics) *ColumnStatistics {
	cp := *cs

	return &cp
}

func (b *InMemoryBackend) columnStatsKey(dbName, tableName, colName string) string {
	return dbName + "|" + tableName + "|" + colName
}

func (b *InMemoryBackend) UpdateColumnStatisticsForTable(
	dbName, tableName string,
	stats []*ColumnStatistics,
) error {
	b.mu.Lock("UpdateColumnStatisticsForTable")
	defer b.mu.Unlock()

	if !b.databases.Has(dbName) {
		return fmt.Errorf("database %q not found: %w", dbName, ErrNotFound)
	}
	for _, cs := range stats {
		key := b.columnStatsKey(dbName, tableName, cs.ColumnName)
		cp := cloneColumnStats(cs)
		cp.AnalyzedTime = float64(time.Now().Unix())
		b.tableColumnStats[key] = cp
	}

	return nil
}

func (b *InMemoryBackend) GetColumnStatisticsForTable(
	dbName, tableName string,
	columnNames []string,
) ([]*ColumnStatistics, error) {
	b.mu.RLock("GetColumnStatisticsForTable")
	defer b.mu.RUnlock()

	var out []*ColumnStatistics
	if len(columnNames) == 0 {
		prefix := dbName + "|" + tableName + "|"
		for key, cs := range b.tableColumnStats {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				out = append(out, cloneColumnStats(cs))
			}
		}
	} else {
		for _, col := range columnNames {
			key := b.columnStatsKey(dbName, tableName, col)
			if cs, ok := b.tableColumnStats[key]; ok {
				out = append(out, cloneColumnStats(cs))
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ColumnName < out[j].ColumnName })

	return out, nil
}

func (b *InMemoryBackend) DeleteColumnStatisticsForTable(
	dbName, tableName, columnName string,
) error {
	b.mu.Lock("DeleteColumnStatisticsForTable")
	defer b.mu.Unlock()

	key := b.columnStatsKey(dbName, tableName, columnName)
	delete(b.tableColumnStats, key)

	return nil
}

func (b *InMemoryBackend) UpdateColumnStatisticsForPartition(
	dbName, tableName string,
	partitionValues []string,
	stats []*ColumnStatistics,
) error {
	b.mu.Lock("UpdateColumnStatisticsForPartition")
	defer b.mu.Unlock()

	pk := partitionKey(dbName, tableName, partitionValues)
	for _, cs := range stats {
		key := pk + "|" + cs.ColumnName
		cp := cloneColumnStats(cs)
		cp.AnalyzedTime = float64(time.Now().Unix())
		b.partitionColumnStats[key] = cp
	}

	return nil
}

func (b *InMemoryBackend) GetColumnStatisticsForPartition(
	dbName, tableName string,
	partitionValues []string,
	columnNames []string,
) ([]*ColumnStatistics, error) {
	b.mu.RLock("GetColumnStatisticsForPartition")
	defer b.mu.RUnlock()

	pk := partitionKey(dbName, tableName, partitionValues)
	var out []*ColumnStatistics
	if len(columnNames) == 0 {
		prefix := pk + "|"
		for key, cs := range b.partitionColumnStats {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				out = append(out, cloneColumnStats(cs))
			}
		}
	} else {
		for _, col := range columnNames {
			key := pk + "|" + col
			if cs, ok := b.partitionColumnStats[key]; ok {
				out = append(out, cloneColumnStats(cs))
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ColumnName < out[j].ColumnName })

	return out, nil
}

func (b *InMemoryBackend) DeleteColumnStatisticsForPartition(
	dbName, tableName string,
	partitionValues []string,
	columnName string,
) error {
	b.mu.Lock("DeleteColumnStatisticsForPartition")
	defer b.mu.Unlock()

	pk := partitionKey(dbName, tableName, partitionValues)
	key := pk + "|" + columnName
	delete(b.partitionColumnStats, key)

	return nil
}
