package dax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// errSnapshotIntegrity is the sentinel error for snapshot referential integrity failures.
var errSnapshotIntegrity = errors.New("snapshot integrity violation")

type backendSnapshot struct {
	Clusters     map[string]*Cluster          `json:"clusters"`
	ParamGroups  map[string]*ParameterGroup   `json:"paramGroups"`
	SubnetGroups map[string]*SubnetGroup      `json:"subnetGroups"`
	Tags         map[string]map[string]string `json:"tags"`
	AccountID    string                       `json:"accountID"`
	Region       string                       `json:"region"`
	Events       []*Event                     `json:"events"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.Clusters == nil {
		s.Clusters = make(map[string]*Cluster)
	}

	if s.ParamGroups == nil {
		s.ParamGroups = make(map[string]*ParameterGroup)
	}

	if s.SubnetGroups == nil {
		s.SubnetGroups = make(map[string]*SubnetGroup)
	}

	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}

	if s.Events == nil {
		s.Events = make([]*Event, 0)
	}
}

// validateSnapshot checks referential integrity after deserialization.
func validateSnapshot(s *backendSnapshot) error {
	for name, cluster := range s.Clusters {
		if cluster.ParameterGroup.ParameterGroupName != "" {
			pgName := cluster.ParameterGroup.ParameterGroupName
			if _, ok := s.ParamGroups[pgName]; !ok {
				return fmt.Errorf(
					"%w: cluster %q references missing parameter group %q",
					errSnapshotIntegrity,
					name,
					pgName,
				)
			}
		}

		if cluster.SubnetGroupName != "" {
			if _, ok := s.SubnetGroups[cluster.SubnetGroupName]; !ok {
				return fmt.Errorf(
					"%w: cluster %q references missing subnet group %q",
					errSnapshotIntegrity,
					name,
					cluster.SubnetGroupName,
				)
			}
		}
	}

	return nil
}

// rebuildTagIndex rebuilds the tag index from cluster.Tags as the source of truth.
// This corrects any desync that can occur between the tag index and cluster.Tags.
func rebuildTagIndex(s *backendSnapshot) {
	for _, cluster := range s.Clusters {
		if len(cluster.Tags) == 0 {
			continue
		}

		arn := cluster.ClusterArn
		if s.Tags[arn] == nil {
			s.Tags[arn] = make(map[string]string)
		}

		maps.Copy(s.Tags[arn], cluster.Tags)
	}
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")

	snap := backendSnapshot{
		Clusters:     b.clusters,
		ParamGroups:  b.paramGroups,
		SubnetGroups: b.subnetGroups,
		Tags:         b.tags,
		Events:       b.events,
		AccountID:    b.AccountID,
		Region:       b.Region,
	}

	data, err := json.Marshal(snap)

	b.mu.RUnlock()

	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "dax: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "dax", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	if err := validateSnapshot(&snap); err != nil {
		return fmt.Errorf("snapshot integrity check failed: %w", err)
	}

	rebuildTagIndex(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.clusters = snap.Clusters
	b.paramGroups = snap.ParamGroups
	b.subnetGroups = snap.SubnetGroups
	b.tags = snap.Tags
	b.events = snap.Events
	b.AccountID = snap.AccountID
	b.Region = snap.Region

	return nil
}
