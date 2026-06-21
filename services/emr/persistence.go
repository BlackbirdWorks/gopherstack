package emr

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// clusterExtra holds the unexported cluster fields that are persisted separately.
type clusterExtra struct {
	ManagedScalingPolicy  *ManagedScalingPolicy   `json:"managedScalingPolicy,omitempty"`
	AutoTerminationPolicy *AutoTerminationPolicy  `json:"autoTerminationPolicy,omitempty"`
	InstanceGroups        []InstanceGroup         `json:"instanceGroups,omitempty"`
	InstanceFleets        []InstanceFleet         `json:"instanceFleets,omitempty"`
	Steps                 []Step                  `json:"steps,omitempty"`
	BootstrapActions      []BootstrapActionConfig `json:"bootstrapActions,omitempty"`
}

// backendSnapshot mirrors the region-nested backend maps (outer key = region).
type backendSnapshot struct {
	Clusters              map[string]map[string]*Cluster               `json:"clusters"`
	ClusterExtras         map[string]map[string]*clusterExtra          `json:"clusterExtras,omitempty"`
	ArnIndex              map[string]map[string]string                 `json:"arnIndex"`
	SecurityConfigs       map[string]map[string]*SecurityConfiguration `json:"securityConfigs"`
	Studios               map[string]map[string]*Studio                `json:"studios"`
	StudioSessionMappings map[string]map[string]*StudioSessionMapping  `json:"studioSessionMappings"`
	PersistentAppUIs      map[string]map[string]*PersistentAppUI       `json:"persistentAppUIs"`
	NotebookExecutions    map[string]map[string]*NotebookExecution     `json:"notebookExecutions,omitempty"`
	BlockPublicAccess     map[string]*BlockPublicAccessConfiguration   `json:"blockPublicAccess,omitempty"`
	BlockPublicAccessMeta map[string]*blockPublicAccessMeta            `json:"blockPublicAccessMeta,omitempty"`
	AccountID             string                                       `json:"accountID"`
	Region                string                                       `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.Clusters == nil {
		s.Clusters = make(map[string]map[string]*Cluster)
	}

	if s.ClusterExtras == nil {
		s.ClusterExtras = make(map[string]map[string]*clusterExtra)
	}

	if s.ArnIndex == nil {
		s.ArnIndex = make(map[string]map[string]string)
	}

	if s.SecurityConfigs == nil {
		s.SecurityConfigs = make(map[string]map[string]*SecurityConfiguration)
	}

	if s.Studios == nil {
		s.Studios = make(map[string]map[string]*Studio)
	}

	if s.StudioSessionMappings == nil {
		s.StudioSessionMappings = make(map[string]map[string]*StudioSessionMapping)
	}

	if s.PersistentAppUIs == nil {
		s.PersistentAppUIs = make(map[string]map[string]*PersistentAppUI)
	}

	if s.NotebookExecutions == nil {
		s.NotebookExecutions = make(map[string]map[string]*NotebookExecution)
	}

	if s.BlockPublicAccess == nil {
		s.BlockPublicAccess = make(map[string]*BlockPublicAccessConfiguration)
	}

	if s.BlockPublicAccessMeta == nil {
		s.BlockPublicAccessMeta = make(map[string]*blockPublicAccessMeta)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	extras := make(map[string]map[string]*clusterExtra, len(b.clusters))

	for region, clusters := range b.clusters {
		regionExtras := make(map[string]*clusterExtra, len(clusters))
		for id, c := range clusters {
			regionExtras[id] = extractClusterExtra(c)
		}

		extras[region] = regionExtras
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
		BlockPublicAccess:     cloneBlockPublicAccess(b.blockPublicAccess),
		BlockPublicAccessMeta: cloneBlockPublicAccessMeta(b.blockPublicAccessMeta),
		AccountID:             b.accountID,
		Region:                b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "emr: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// cloneBlockPublicAccess deep-copies the per-region block-public-access configs.
func cloneBlockPublicAccess(
	src map[string]*BlockPublicAccessConfiguration,
) map[string]*BlockPublicAccessConfiguration {
	out := make(map[string]*BlockPublicAccessConfiguration, len(src))
	for region, cfg := range src {
		if cfg == nil {
			continue
		}

		cp := *cfg
		out[region] = &cp
	}

	return out
}

// cloneBlockPublicAccessMeta deep-copies the per-region block-public-access metadata.
func cloneBlockPublicAccessMeta(
	src map[string]*blockPublicAccessMeta,
) map[string]*blockPublicAccessMeta {
	out := make(map[string]*blockPublicAccessMeta, len(src))
	for region, meta := range src {
		if meta == nil {
			continue
		}

		cp := *meta
		out[region] = &cp
	}

	return out
}

func extractClusterExtra(c *Cluster) *clusterExtra {
	ex := &clusterExtra{
		InstanceGroups:   make([]InstanceGroup, len(c.instanceGroups)),
		InstanceFleets:   make([]InstanceFleet, len(c.instanceFleets)),
		Steps:            make([]Step, len(c.steps)),
		BootstrapActions: cloneBootstrapActions(c.bootstrapActions),
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
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	for region, clusters := range snap.Clusters {
		applyClusterExtras(clusters, snap.ClusterExtras[region])
	}

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
		c.bootstrapActions = ex.BootstrapActions
		c.managedScalingPolicy = ex.ManagedScalingPolicy
		c.autoTerminationPolicy = ex.AutoTerminationPolicy
	}
}
