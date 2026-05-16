package emr

import (
	"encoding/json"
	"log/slog"
)

// clusterExtra holds the unexported cluster fields that are persisted separately.
type clusterExtra struct {
	ManagedScalingPolicy  *ManagedScalingPolicy  `json:"managedScalingPolicy,omitempty"`
	AutoTerminationPolicy *AutoTerminationPolicy `json:"autoTerminationPolicy,omitempty"`
	InstanceGroups        []InstanceGroup        `json:"instanceGroups,omitempty"`
	InstanceFleets        []InstanceFleet        `json:"instanceFleets,omitempty"`
	Steps                 []Step                 `json:"steps,omitempty"`
}

type backendSnapshot struct {
	Clusters              map[string]*Cluster               `json:"clusters"`
	ClusterExtras         map[string]*clusterExtra          `json:"clusterExtras,omitempty"`
	ArnIndex              map[string]string                 `json:"arnIndex"`
	SecurityConfigs       map[string]*SecurityConfiguration `json:"securityConfigs"`
	Studios               map[string]*Studio                `json:"studios"`
	StudioSessionMappings map[string]*StudioSessionMapping  `json:"studioSessionMappings"`
	PersistentAppUIs      map[string]*PersistentAppUI       `json:"persistentAppUIs"`
	NotebookExecutions    map[string]*NotebookExecution     `json:"notebookExecutions,omitempty"`
	BlockPublicAccess     *BlockPublicAccessConfiguration   `json:"blockPublicAccess,omitempty"`
	BlockPublicAccessMeta *blockPublicAccessMeta            `json:"blockPublicAccessMeta,omitempty"`
	AccountID             string                            `json:"accountID"`
	Region                string                            `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.Clusters == nil {
		s.Clusters = make(map[string]*Cluster)
	}

	if s.ClusterExtras == nil {
		s.ClusterExtras = make(map[string]*clusterExtra)
	}

	if s.ArnIndex == nil {
		s.ArnIndex = make(map[string]string)
	}

	if s.SecurityConfigs == nil {
		s.SecurityConfigs = make(map[string]*SecurityConfiguration)
	}

	if s.Studios == nil {
		s.Studios = make(map[string]*Studio)
	}

	if s.StudioSessionMappings == nil {
		s.StudioSessionMappings = make(map[string]*StudioSessionMapping)
	}

	if s.PersistentAppUIs == nil {
		s.PersistentAppUIs = make(map[string]*PersistentAppUI)
	}

	if s.NotebookExecutions == nil {
		s.NotebookExecutions = make(map[string]*NotebookExecution)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	extras := make(map[string]*clusterExtra, len(b.clusters))

	for id, c := range b.clusters {
		extras[id] = extractClusterExtra(c)
	}

	var bpa *BlockPublicAccessConfiguration
	if b.blockPublicAccess != nil {
		cp := *b.blockPublicAccess
		bpa = &cp
	}

	var bpam *blockPublicAccessMeta
	if b.blockPublicAccessMeta != nil {
		cp := *b.blockPublicAccessMeta
		bpam = &cp
	}

	snap := backendSnapshot{
		Clusters:              b.clusters,
		ClusterExtras:         extras,
		ArnIndex:              b.arnIndex,
		SecurityConfigs:       b.securityConfigs,
		Studios:               b.studios,
		StudioSessionMappings: b.studioSessionMappings,
		PersistentAppUIs:      b.persistentAppUIs,
		NotebookExecutions:    b.notebookExecutions,
		BlockPublicAccess:     bpa,
		BlockPublicAccessMeta: bpam,
		AccountID:             b.accountID,
		Region:                b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("emr: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

func extractClusterExtra(c *Cluster) *clusterExtra {
	ex := &clusterExtra{
		InstanceGroups: make([]InstanceGroup, len(c.instanceGroups)),
		InstanceFleets: make([]InstanceFleet, len(c.instanceFleets)),
		Steps:          make([]Step, len(c.steps)),
	}

	copy(ex.InstanceGroups, c.instanceGroups)
	copy(ex.InstanceFleets, c.instanceFleets)
	copy(ex.Steps, c.steps)

	if c.managedScalingPolicy != nil {
		cp := *c.managedScalingPolicy
		ex.ManagedScalingPolicy = &cp
	}

	if c.autoTerminationPolicy != nil {
		cp := *c.autoTerminationPolicy
		ex.AutoTerminationPolicy = &cp
	}

	return ex
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()
	applyClusterExtras(snap.Clusters, snap.ClusterExtras)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.clusters = snap.Clusters
	b.arnIndex = snap.ArnIndex
	b.securityConfigs = snap.SecurityConfigs
	b.studios = snap.Studios
	b.studioSessionMappings = snap.StudioSessionMappings
	b.persistentAppUIs = snap.PersistentAppUIs
	b.notebookExecutions = snap.NotebookExecutions
	b.blockPublicAccess = snap.BlockPublicAccess
	b.blockPublicAccessMeta = snap.BlockPublicAccessMeta
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// applyClusterExtras re-hydrates the unexported cluster fields from the extras map.
func applyClusterExtras(clusters map[string]*Cluster, extras map[string]*clusterExtra) {
	for id, c := range clusters {
		ex, ok := extras[id]
		if !ok {
			continue
		}

		c.instanceGroups = ex.InstanceGroups
		c.instanceFleets = ex.InstanceFleets
		c.steps = ex.Steps
		c.managedScalingPolicy = ex.ManagedScalingPolicy
		c.autoTerminationPolicy = ex.AutoTerminationPolicy
	}
}
