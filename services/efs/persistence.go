package efs

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// backendSnapshot serialises the backend state. All resource maps are nested by
// region (outer key = region) so region isolation survives snapshot/restore.
type backendSnapshot struct {
	FileSystems        map[string]map[string]*FileSystem               `json:"fileSystems"`
	MountTargets       map[string]map[string]*MountTarget              `json:"mountTargets"`
	AccessPoints       map[string]map[string]*AccessPoint              `json:"accessPoints"`
	LifecyclePolicies  map[string]map[string][]LifecyclePolicy         `json:"lifecyclePolicies"`
	ReplicationConfigs map[string]map[string]*ReplicationConfiguration `json:"replicationConfigs"`
	BackupPolicies     map[string]map[string]string                    `json:"backupPolicies"`
	FileSystemPolicies map[string]map[string]string                    `json:"fileSystemPolicies"`
	AccountID          string                                          `json:"accountID"`
	Region             string                                          `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.FileSystems == nil {
		s.FileSystems = make(map[string]map[string]*FileSystem)
	}
	if s.MountTargets == nil {
		s.MountTargets = make(map[string]map[string]*MountTarget)
	}
	if s.AccessPoints == nil {
		s.AccessPoints = make(map[string]map[string]*AccessPoint)
	}
	if s.LifecyclePolicies == nil {
		s.LifecyclePolicies = make(map[string]map[string][]LifecyclePolicy)
	}
	if s.ReplicationConfigs == nil {
		s.ReplicationConfigs = make(map[string]map[string]*ReplicationConfiguration)
	}
	if s.BackupPolicies == nil {
		s.BackupPolicies = make(map[string]map[string]string)
	}
	if s.FileSystemPolicies == nil {
		s.FileSystemPolicies = make(map[string]map[string]string)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		FileSystems:        b.fileSystems,
		MountTargets:       b.mountTargets,
		AccessPoints:       b.accessPoints,
		LifecyclePolicies:  b.lifecyclePolicies,
		ReplicationConfigs: b.replicationConfigs,
		BackupPolicies:     b.backupPolicies,
		FileSystemPolicies: b.fileSystemPolicies,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("efs: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.fileSystems = snap.FileSystems
	b.mountTargets = snap.MountTargets
	b.accessPoints = snap.AccessPoints
	b.lifecyclePolicies = snap.LifecyclePolicies
	b.replicationConfigs = snap.ReplicationConfigs
	b.backupPolicies = snap.BackupPolicies
	b.fileSystemPolicies = snap.FileSystemPolicies
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.rebuildARNIndexes()

	return nil
}

// rebuildARNIndexes reconstructs all region-nested ARN-keyed maps, the client-token
// index, and reinitialises nil tag registries.
func (b *InMemoryBackend) rebuildARNIndexes() {
	b.fileSystemsByARN = make(map[string]map[string]*FileSystem, len(b.fileSystems))

	for region, regionFS := range b.fileSystems {
		arnIndex := make(map[string]*FileSystem, len(regionFS))
		for _, fs := range regionFS {
			if fs.Tags == nil {
				fs.Tags = tags.New("efs.filesystem." + fs.FileSystemID + ".tags")
			}
			arnIndex[fs.FileSystemArn] = fs
		}
		b.fileSystemsByARN[region] = arnIndex
	}

	b.mountTargetsByARN = make(map[string]map[string]*MountTarget, len(b.mountTargets))

	for region, regionMT := range b.mountTargets {
		arnIndex := make(map[string]*MountTarget, len(regionMT))
		for _, mt := range regionMT {
			arnIndex[mt.MountTargetArn] = mt
		}
		b.mountTargetsByARN[region] = arnIndex
	}

	b.accessPointsByARN = make(map[string]map[string]*AccessPoint, len(b.accessPoints))
	b.accessPointsByClientToken = make(map[string]map[string]*AccessPoint, len(b.accessPoints))

	for region, regionAP := range b.accessPoints {
		arnIndex := make(map[string]*AccessPoint, len(regionAP))
		tokenIndex := make(map[string]*AccessPoint)
		for _, ap := range regionAP {
			if ap.Tags == nil {
				ap.Tags = tags.New("efs.accesspoint." + ap.AccessPointID + ".tags")
			}
			arnIndex[ap.AccessPointArn] = ap
			if ap.ClientToken != "" {
				tokenIndex[ap.ClientToken] = ap
			}
		}
		b.accessPointsByARN[region] = arnIndex
		b.accessPointsByClientToken[region] = tokenIndex
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
