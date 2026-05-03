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
func (b *InMemoryBackend) Snapshot() ([]byte, error) {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

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

	for dbName, tblRecords := range b.records {
		snap.Records[dbName] = make(map[string][]Record, len(tblRecords))
		for tblName, recs := range tblRecords {
			out := make([]Record, len(recs))
			for i, r := range recs {
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

	b.nextTaskID = snap.NextTaskID
	b.databases = snap.Databases
	b.tables = snap.Tables
	b.records = snap.Records
	b.tags = snap.Tags
	b.batchLoadTasks = snap.BatchLoadTasks

	b.ensureNonNilMapsFromSnapshot()

	return nil
}

// ensureNonNilMapsFromSnapshot initialises any nil maps that may result from
// restoring a snapshot with missing fields, and rebuilds the tableMus index.
func (b *InMemoryBackend) ensureNonNilMapsFromSnapshot() {
	if b.databases == nil {
		b.databases = make(map[string]*Database)
	}

	if b.tables == nil {
		b.tables = make(map[string]map[string]*Table)
	}

	if b.records == nil {
		b.records = make(map[string]map[string][]Record)
	}

	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	if b.batchLoadTasks == nil {
		b.batchLoadTasks = make(map[string]*BatchLoadTask)
	}

	// Rebuild per-table mutexes from the restored table map.
	b.tableMus = make(map[string]*lockmetrics.RWMutex)
	for _, tbls := range b.tables {
		for _, tbl := range tbls {
			b.tableMus[tbl.ARN] = lockmetrics.New("timestreamwrite.table")
		}
	}
}
