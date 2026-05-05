package glue

import (
	"encoding/json"
	"maps"
)

type backendSnapshot struct {
	Databases           map[string]*Database                 `json:"databases"`
	Tables              map[string]*Table                    `json:"tables"`
	Crawlers            map[string]*Crawler                  `json:"crawlers"`
	Jobs                map[string]*Job                      `json:"jobs"`
	Partitions          map[string]*Partition                `json:"partitions"`
	TableVersions       map[string]*TableVersion             `json:"tableVersions"`
	Connections         map[string]*Connection               `json:"connections"`
	Blueprints          map[string]*Blueprint                `json:"blueprints"`
	CustomEntityTypes   map[string]*CustomEntityType         `json:"customEntityTypes"`
	DataQualityResult   map[string]*DataQualityResult        `json:"dataQualityResult"`
	DevEndpoints        map[string]*DevEndpoint              `json:"devEndpoints"`
	JobRuns             map[string][]*JobRun                 `json:"jobRuns"`
	JobBookmarks        map[string]*JobBookmark              `json:"jobBookmarks"`
	DataQualityRulesets map[string]*DataQualityRuleset       `json:"dataQualityRulesets"`
	DataQualityEvalRuns map[string]*DataQualityEvaluationRun `json:"dataQualityEvalRuns"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Databases: copyMap(b.databases, cloneDatabase),
		Tables: copyMap(b.tables, func(t *Table) *Table {
			cp := *t

			return &cp
		}),
		Crawlers: copyMap(b.crawlers, cloneCrawler),
		Jobs:     copyMap(b.jobs, cloneJob),
		Partitions: copyMap(b.partitions, func(p *Partition) *Partition {
			cp := *p
			cp.Values = append([]string(nil), p.Values...)

			return &cp
		}),
		TableVersions: copyMap(b.tableVersions, func(tv *TableVersion) *TableVersion {
			cp := *tv

			return &cp
		}),
		Connections: maps.Clone(b.connections),
		Blueprints:  maps.Clone(b.blueprints),
		CustomEntityTypes: copyMap(b.customEntityTypes, func(c *CustomEntityType) *CustomEntityType {
			cp := *c
			cp.ContextWords = append([]string(nil), c.ContextWords...)

			return &cp
		}),
		DataQualityResult: maps.Clone(b.dataQualityResult),
		DevEndpoints:      maps.Clone(b.devEndpoints),
		JobRuns:           copyJobRunsMap(b.jobRuns),
		JobBookmarks:      maps.Clone(b.jobBookmarks),
		DataQualityRulesets: copyMap(b.dataQualityRulesets, func(r *DataQualityRuleset) *DataQualityRuleset {
			cp := *r
			cp.Tags = maps.Clone(r.Tags)

			return &cp
		}),
		DataQualityEvalRuns: copyMap(
			b.dataQualityEvalRuns,
			func(run *DataQualityEvaluationRun) *DataQualityEvaluationRun {
				cp := *run
				cp.RulesetNames = append([]string(nil), run.RulesetNames...)

				return &cp
			},
		),
	}

	data, err := json.Marshal(snap)
	if err != nil {
		// Marshal failure is unexpected for a pure in-memory struct.
		// Return nil to indicate no snapshot is available.
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	initSnapshotDefaults(&snap)
	b.restoreFromSnapshot(snap)

	return nil
}

// initSnapshotDefaults ensures all snapshot maps are non-nil.
func initSnapshotDefaults(snap *backendSnapshot) {
	initSnapshotCoreDefaults(snap)
	initSnapshotExtDefaults(snap)
}

// initSnapshotCoreDefaults initializes core maps to non-nil.
func initSnapshotCoreDefaults(snap *backendSnapshot) {
	if snap.Databases == nil {
		snap.Databases = make(map[string]*Database)
	}
	if snap.Tables == nil {
		snap.Tables = make(map[string]*Table)
	}
	if snap.Crawlers == nil {
		snap.Crawlers = make(map[string]*Crawler)
	}
	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*Job)
	}
	if snap.Partitions == nil {
		snap.Partitions = make(map[string]*Partition)
	}
	if snap.TableVersions == nil {
		snap.TableVersions = make(map[string]*TableVersion)
	}
	if snap.Connections == nil {
		snap.Connections = make(map[string]*Connection)
	}
	if snap.Blueprints == nil {
		snap.Blueprints = make(map[string]*Blueprint)
	}
}

// initSnapshotExtDefaults initializes extended maps to non-nil.
func initSnapshotExtDefaults(snap *backendSnapshot) {
	if snap.CustomEntityTypes == nil {
		snap.CustomEntityTypes = make(map[string]*CustomEntityType)
	}
	if snap.DataQualityResult == nil {
		snap.DataQualityResult = make(map[string]*DataQualityResult)
	}
	if snap.DevEndpoints == nil {
		snap.DevEndpoints = make(map[string]*DevEndpoint)
	}
	if snap.JobRuns == nil {
		snap.JobRuns = make(map[string][]*JobRun)
	}
	if snap.JobBookmarks == nil {
		snap.JobBookmarks = make(map[string]*JobBookmark)
	}
	if snap.DataQualityRulesets == nil {
		snap.DataQualityRulesets = make(map[string]*DataQualityRuleset)
	}
	if snap.DataQualityEvalRuns == nil {
		snap.DataQualityEvalRuns = make(map[string]*DataQualityEvaluationRun)
	}
}

// restoreFromSnapshot copies snapshot data into the backend (caller holds lock).
func (b *InMemoryBackend) restoreFromSnapshot(snap backendSnapshot) {
	b.databases = copyMap(snap.Databases, cloneDatabase)
	b.tables = copyMap(snap.Tables, func(t *Table) *Table {
		cp := *t

		return &cp
	})
	b.crawlers = copyMap(snap.Crawlers, cloneCrawler)
	b.jobs = copyMap(snap.Jobs, cloneJob)
	b.partitions = copyMap(snap.Partitions, func(p *Partition) *Partition {
		cp := *p
		cp.Values = append([]string(nil), p.Values...)

		return &cp
	})
	b.tableVersions = copyMap(snap.TableVersions, func(tv *TableVersion) *TableVersion {
		cp := *tv

		return &cp
	})
	b.connections = maps.Clone(snap.Connections)
	b.blueprints = maps.Clone(snap.Blueprints)
	b.customEntityTypes = copyMap(snap.CustomEntityTypes, func(c *CustomEntityType) *CustomEntityType {
		cp := *c
		cp.ContextWords = append([]string(nil), c.ContextWords...)

		return &cp
	})
	b.dataQualityResult = maps.Clone(snap.DataQualityResult)
	b.devEndpoints = maps.Clone(snap.DevEndpoints)
	b.jobRuns = copyJobRunsMap(snap.JobRuns)
	b.jobBookmarks = maps.Clone(snap.JobBookmarks)
	b.dataQualityRulesets = copyMap(snap.DataQualityRulesets, func(r *DataQualityRuleset) *DataQualityRuleset {
		cp := *r
		cp.Tags = maps.Clone(r.Tags)

		return &cp
	})
	b.dataQualityEvalRuns = copyMap(
		snap.DataQualityEvalRuns,
		func(run *DataQualityEvaluationRun) *DataQualityEvaluationRun {
			cp := *run
			cp.RulesetNames = append([]string(nil), run.RulesetNames...)

			return &cp
		},
	)
}

// copyMap creates a deep copy of a map using the provided clone function.
func copyMap[V any](src map[string]V, clone func(V) V) map[string]V {
	dst := make(map[string]V, len(src))
	for k, v := range src {
		dst[k] = clone(v)
	}

	return dst
}

// Snapshot implements Snapshottable by delegating to the backend when it
// supports it.
func (h *Handler) Snapshot() []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements Snapshottable by delegating to the backend when it
// supports it.
func (h *Handler) Restore(data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(data)
	}

	return nil
}

// copyJobRunsMap deep-copies the jobRuns map (map[string][]*JobRun).
func copyJobRunsMap(src map[string][]*JobRun) map[string][]*JobRun {
	dst := make(map[string][]*JobRun, len(src))
	for k, runs := range src {
		cp := make([]*JobRun, len(runs))
		for i, r := range runs {
			rc := *r
			rc.Arguments = maps.Clone(r.Arguments)
			cp[i] = &rc
		}
		dst[k] = cp
	}

	return dst
}
