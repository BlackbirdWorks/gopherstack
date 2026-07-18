package timestreamwrite

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// scheduledQueryARNFragment identifies scheduled-query ARNs from the Timestream
// Query service.  These are accepted by TagResource so that the unified write-service
// tag store can hold tags for both resource types.
const scheduledQueryARNFragment = "scheduled-query/"

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:       lockmetrics.New("timestreamwrite"),
		registry: store.NewRegistry(),
	}
	registerAllTables(b)
	b.ensureNonNilMaps()

	return b
}

// Reset clears all stored state, returning the backend to its initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.closeAllTableMutexesLocked()
	b.nextTaskID = 0
	b.registry.ResetAll()
	b.ensureNonNilMapsLocked()
}

// closeAllTableMutexesLocked closes every per-table lockmetrics.RWMutex held in
// b.records. Must be called with b.mu held in write mode before any operation
// that abandons the records map (Reset, Restore) or before the receiver is
// discarded; otherwise the mutexes leak in lockmetrics' liveCollector registry.
func (b *InMemoryBackend) closeAllTableMutexesLocked() {
	for _, dbSlots := range b.records {
		for _, slot := range dbSlots {
			if slot != nil && slot.mu != nil {
				slot.mu.Close()
			}
		}
	}
}

// AccountID returns the simulated AWS account ID.
func (b *InMemoryBackend) AccountID() string { return config.DefaultAccountID }

// Region returns the simulated AWS region.
func (b *InMemoryBackend) Region() string { return config.DefaultRegion }

// ensureNonNilMaps initialises the raw (non-store.Table) maps (called
// without lock held during construction or restore). databases, tables, and
// batchLoadTasks are store.Table-backed and are always non-nil after
// registerAllTables; they are cleared via b.registry.ResetAll(), not
// reallocation.
func (b *InMemoryBackend) ensureNonNilMaps() {
	b.records = make(map[string]map[string]*tableRecords)
	b.tags = make(map[string]map[string]string)
}

// ensureNonNilMapsLocked initialises the raw (non-store.Table) maps when the
// lock is already held.
func (b *InMemoryBackend) ensureNonNilMapsLocked() {
	b.records = make(map[string]map[string]*tableRecords)
	b.tags = make(map[string]map[string]string)
}

func databaseARN(name string) string {
	return arn.Build("timestream", config.DefaultRegion, config.DefaultAccountID, fmt.Sprintf("database/%s", name))
}

func tableARN(dbName, tblName string) string {
	return arn.Build(
		"timestream", config.DefaultRegion, config.DefaultAccountID,
		fmt.Sprintf("database/%s/table/%s", dbName, tblName),
	)
}

// isKnownARN reports whether the ARN is registered in the backend (database, table,
// or batch-load-task ARNs derived from the resource maps) or belongs to an external
// Timestream resource type (e.g. scheduled-query) that shares the same API endpoint.
// Must be called with at least a read-lock held.
func (b *InMemoryBackend) isKnownARNLocked(arn string) bool {
	// Accept scheduled-query ARNs from the Timestream Query service which shares
	// the same TagResource endpoint.
	if strings.Contains(arn, scheduledQueryARNFragment) {
		return true
	}

	// Check against database ARNs.
	for _, db := range b.databases.All() {
		if db.ARN == arn {
			return true
		}

		// Check against table ARNs.
		for _, tbl := range b.tablesByDatabase.Get(db.DatabaseName) {
			if tbl.ARN == arn {
				return true
			}
		}
	}

	return false
}
