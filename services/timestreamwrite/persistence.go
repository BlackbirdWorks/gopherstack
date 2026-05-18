package timestreamwrite

import (
	"encoding/json"
	"log/slog"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// backendSnapshot is the serialisable representation of the backend state.
type backendSnapshot struct {
	Databases      map[string]*Database           `json:"databases"`
	Tables         map[string]map[string]*Table   `json:"tables"`
	Records        map[string]map[string][]Record `json:"records"`
	Tags           map[string]map[string]string   `json:"tags"`
	BatchLoadTasks map[string]*BatchLoadTask      `json:"batch_load_tasks"`
	NextTaskID     int                            `json:"next_task_id"`
}

// Snapshot serialises the current backend state into a JSON byte slice.
//
// Holds the global write lock to serialise against WriteRecords, which only
// holds the global read lock and writes to the inner b.records map; concurrent
// map iteration here and map write there would be a fatal Go data race.
func (b *InMemoryBackend) Snapshot() ([]byte, error) {
	b.mu.Lock("Snapshot")
	defer b.mu.Unlock()

	snap := backendSnapshot{
		Databases:      make(map[string]*Database, len(b.databases)),
		Tables:         make(map[string]map[string]*Table, len(b.tables)),
		Records:        make(map[string]map[string][]Record, len(b.records)),
		Tags:           make(map[string]map[string]string, len(b.tags)),
		BatchLoadTasks: make(map[string]*BatchLoadTask, len(b.batchLoadTasks)),
		NextTaskID:     b.nextTaskID,
	}

	for k, v := range b.databases {
		cp := *v
		snap.Databases[k] = &cp
	}

	for dbName, tbls := range b.tables {
		snap.Tables[dbName] = make(map[string]*Table, len(tbls))
		for tblName, tbl := range tbls {
			cp := *tbl
			snap.Tables[dbName][tblName] = &cp
		}
	}

	for dbName, tblSlots := range b.records {
		snap.Records[dbName] = make(map[string][]Record, len(tblSlots))
		for tblName, slot := range tblSlots {
			out := make([]Record, len(slot.records))
			for i, r := range slot.records {
				dims := make([]Dimension, len(r.Dimensions))
				copy(dims, r.Dimensions)
				r.Dimensions = dims
				out[i] = r
			}

			snap.Records[dbName][tblName] = out
		}
	}

	for arn, tagMap := range b.tags {
		snap.Tags[arn] = make(map[string]string, len(tagMap))
		maps.Copy(snap.Tags[arn], tagMap)
	}

	for id, task := range b.batchLoadTasks {
		cp := *task
		snap.BatchLoadTasks[id] = &cp
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("timestreamwrite: snapshot marshal failed", "error", err)
	}

	return data, err
}

// Restore replaces the backend state with the data from a previous Snapshot call.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		slog.Default().Warn("timestreamwrite: restore unmarshal failed", "error", err)

		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close all existing per-table mutexes before discarding the records map;
	// otherwise lockmetrics' global registry leaks one entry per pre-restore table.
	b.closeAllTableMutexesLocked()

	b.nextTaskID = snap.NextTaskID
	b.databases = snap.Databases
	b.tables = snap.Tables
	b.tags = snap.Tags
	b.batchLoadTasks = snap.BatchLoadTasks

	// Convert snapshot's flat record map back into per-table slots so that
	// each table owns its own mutex and slice independent of the enclosing maps.
	// Rebuild the dedup index from the restored records so that version-based
	// upsert works correctly after restore.
	b.records = make(map[string]map[string]*tableRecords, len(snap.Records))
	for dbName, tblRecs := range snap.Records {
		b.records[dbName] = make(map[string]*tableRecords, len(tblRecs))
		for tblName, recs := range tblRecs {
			idx := make(map[string]int, len(recs))
			for i, r := range recs {
				idx[recordKey(r)] = i
			}

			b.records[dbName][tblName] = &tableRecords{
				mu:          lockmetrics.New("timestreamwrite.table"),
				records:     recs,
				recordIndex: idx,
			}
		}
	}

	b.ensureNonNilMapsFromSnapshot()

	return nil
}

// ensureNonNilMapsFromSnapshot initialises any nil maps that may result from
// restoring a snapshot with missing fields, and ensures each table has a slot
// with an initialised mutex even if its records slice is missing.
func (b *InMemoryBackend) ensureNonNilMapsFromSnapshot() {
	if b.databases == nil {
		b.databases = make(map[string]*Database)
	}

	if b.tables == nil {
		b.tables = make(map[string]map[string]*Table)
	}

	if b.records == nil {
		b.records = make(map[string]map[string]*tableRecords)
	}

	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	if b.batchLoadTasks == nil {
		b.batchLoadTasks = make(map[string]*BatchLoadTask)
	}

	// Make sure every restored table has a corresponding records slot so
	// WriteRecords always finds an initialised *tableRecords with a mutex.
	for dbName, tbls := range b.tables {
		if b.records[dbName] == nil {
			b.records[dbName] = make(map[string]*tableRecords, len(tbls))
		}

		for tblName := range tbls {
			if b.records[dbName][tblName] == nil {
				b.records[dbName][tblName] = &tableRecords{
					mu:          lockmetrics.New("timestreamwrite.table"),
					recordIndex: make(map[string]int),
				}
			} else if b.records[dbName][tblName].recordIndex == nil {
				b.records[dbName][tblName].recordIndex = make(map[string]int)
			}
		}
	}
}
