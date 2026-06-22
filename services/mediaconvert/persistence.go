package mediaconvert

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Queues        map[string]*Queue            `json:"queues"`
	JobTemplates  map[string]*JobTemplate      `json:"jobTemplates"`
	Jobs          map[string]*Job              `json:"jobs"`
	Presets       map[string]*Preset           `json:"presets"`
	Tags          map[string]map[string]string `json:"tags"`
	Certificates  map[string]struct{}          `json:"certificates"`
	QueueCounters map[string]*queueJobCounter  `json:"queueCounters,omitempty"`
	TokenIndex    map[string]*tokenEntry       `json:"tokenIndex,omitempty"`
	Policy        *Policy                      `json:"policy,omitempty"`
	AccountID     string                       `json:"accountID"`
	Region        string                       `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// Deep copies are taken before serialisation to avoid races with concurrent writes.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Deep-copy each resource to avoid sharing references.
	queues := make(map[string]*Queue, len(b.queues))
	for k, v := range b.queues {
		queues[k] = cloneQueue(v)
	}

	jobTemplates := make(map[string]*JobTemplate, len(b.jobTemplates))
	for k, v := range b.jobTemplates {
		jobTemplates[k] = cloneJobTemplate(v)
	}

	jobs := make(map[string]*Job, len(b.jobs))
	for k, v := range b.jobs {
		jobs[k] = cloneJob(v)
	}

	presets := make(map[string]*Preset, len(b.presets))
	for k, v := range b.presets {
		presets[k] = clonePreset(v)
	}

	tags := make(map[string]map[string]string, len(b.tags))
	for k, v := range b.tags {
		tags[k] = nonNilTagsCopy(v)
	}

	certs := make(map[string]struct{}, len(b.certificates))
	for k := range b.certificates {
		certs[k] = struct{}{}
	}

	queueCounters := make(map[string]*queueJobCounter, len(b.queueCounters))
	for k, v := range b.queueCounters {
		cp := *v
		queueCounters[k] = &cp
	}

	tokenIndex := make(map[string]*tokenEntry, len(b.tokenIndex))
	for k, v := range b.tokenIndex {
		cp := *v
		tokenIndex[k] = &cp
	}

	var pol *Policy
	if b.policy != nil {
		cp := *b.policy
		pol = &cp
	}

	snap := backendSnapshot{
		Queues:        queues,
		JobTemplates:  jobTemplates,
		Jobs:          jobs,
		Presets:       presets,
		Tags:          tags,
		Certificates:  certs,
		QueueCounters: queueCounters,
		TokenIndex:    tokenIndex,
		Policy:        pol,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mediaconvert: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "mediaconvert", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.queries = make(map[string]*jobsQuery)
	b.queues = snap.Queues
	b.jobTemplates = snap.JobTemplates
	b.jobs = snap.Jobs
	b.presets = snap.Presets
	b.tags = snap.Tags
	b.certificates = snap.Certificates
	b.queueCounters = snap.QueueCounters
	b.tokenIndex = snap.TokenIndex
	b.policy = snap.Policy
	b.accountID = snap.AccountID
	b.region = snap.Region

	if len(b.queueCounters) == 0 {
		b.rebuildCountersLocked()
	}

	return nil
}

// ensureNonNilMaps initialises any nil maps in the snapshot to avoid nil-map panics.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Queues == nil {
		snap.Queues = make(map[string]*Queue)
	}

	if snap.JobTemplates == nil {
		snap.JobTemplates = make(map[string]*JobTemplate)
	}

	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*Job)
	}

	if snap.Presets == nil {
		snap.Presets = make(map[string]*Preset)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	if snap.Certificates == nil {
		snap.Certificates = make(map[string]struct{})
	}

	if snap.QueueCounters == nil {
		snap.QueueCounters = make(map[string]*queueJobCounter)
	}

	if snap.TokenIndex == nil {
		snap.TokenIndex = make(map[string]*tokenEntry)
	}
}
