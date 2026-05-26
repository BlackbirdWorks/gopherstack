package efs

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	FileSystems        map[string]*FileSystem               `json:"fileSystems"`
	MountTargets       map[string]*MountTarget              `json:"mountTargets"`
	AccessPoints       map[string]*AccessPoint              `json:"accessPoints"`
	LifecyclePolicies  map[string][]LifecyclePolicy         `json:"lifecyclePolicies"`
	ReplicationConfigs map[string]*ReplicationConfiguration `json:"replicationConfigs"`
	BackupPolicies     map[string]string                    `json:"backupPolicies"`
	FileSystemPolicies map[string]string                    `json:"fileSystemPolicies"`
	AccountID          string                               `json:"accountID"`
	Region             string                               `json:"region"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.FileSystems == nil {
		s.FileSystems = make(map[string]*FileSystem)
	}
	if s.MountTargets == nil {
		s.MountTargets = make(map[string]*MountTarget)
	}
	if s.AccessPoints == nil {
		s.AccessPoints = make(map[string]*AccessPoint)
	}
	if s.LifecyclePolicies == nil {
		s.LifecyclePolicies = make(map[string][]LifecyclePolicy)
	}
	if s.ReplicationConfigs == nil {
		s.ReplicationConfigs = make(map[string]*ReplicationConfiguration)
	}
	if s.BackupPolicies == nil {
		s.BackupPolicies = make(map[string]string)
	}
	if s.FileSystemPolicies == nil {
		s.FileSystemPolicies = make(map[string]string)
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

// rebuildARNIndexes reconstructs all ARN-keyed maps, client-token index, and reinitialises nil tag registries.
func (b *InMemoryBackend) rebuildARNIndexes() {
	b.fileSystemsByARN = make(map[string]*FileSystem, len(b.fileSystems))

	for _, fs := range b.fileSystems {
		if fs.Tags == nil {
			fs.Tags = tags.New("efs.filesystem." + fs.FileSystemID + ".tags")
		}
		b.fileSystemsByARN[fs.FileSystemArn] = fs
	}

	b.mountTargetsByARN = make(map[string]*MountTarget, len(b.mountTargets))

	for _, mt := range b.mountTargets {
		b.mountTargetsByARN[mt.MountTargetArn] = mt
	}

	b.accessPointsByARN = make(map[string]*AccessPoint, len(b.accessPoints))
	b.accessPointsByClientToken = make(map[string]*AccessPoint)

	for _, ap := range b.accessPoints {
		if ap.Tags == nil {
			ap.Tags = tags.New("efs.accesspoint." + ap.AccessPointID + ".tags")
		}
		b.accessPointsByARN[ap.AccessPointArn] = ap
		if ap.ClientToken != "" {
			b.accessPointsByClientToken[ap.ClientToken] = ap
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
