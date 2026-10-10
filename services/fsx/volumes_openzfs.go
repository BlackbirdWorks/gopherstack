package fsx

import (
	"fmt"
	"slices"
)

const (
	copyStrategyClone           = "CLONE"
	copyStrategyFullCopy        = "FULL_COPY"
	copyStrategyIncrementalCopy = "INCREMENTAL_COPY"

	optionDeleteIntermediateSnapshots = "DELETE_INTERMEDIATE_SNAPSHOTS"
	optionDeleteClonedVolumes         = "DELETE_CLONED_VOLUMES"
	optionDeleteIntermediateData      = "DELETE_INTERMEDIATE_DATA"
	optionDeleteChildVolumes          = "DELETE_CHILD_VOLUMES_AND_SNAPSHOTS"

	openZFSMaxRecordSizeKiB      = 1024
	openZFSMinRecordSizeKiB      = 4
	openZFSTieringMinRecordKiB   = 128
	openZFSTieringMaxRecordKiB   = 4096
	storageTypeIntelligentTiered = "INTELLIGENT_TIERING"
	unsetCapacity                = -1
)

// OpenZFSNfsExport mirrors types.OpenZFSNfsExport.
type OpenZFSNfsExport struct {
	ClientConfigurations []OpenZFSClientConfiguration `json:"ClientConfigurations"`
}

// OpenZFSClientConfiguration mirrors types.OpenZFSClientConfiguration.
type OpenZFSClientConfiguration struct {
	Clients string   `json:"Clients"`
	Options []string `json:"Options"`
}

// OpenZFSUserOrGroupQuota mirrors types.OpenZFSUserOrGroupQuota.
type OpenZFSUserOrGroupQuota struct {
	ID                      *int32 `json:"Id"`
	StorageCapacityQuotaGiB *int32 `json:"StorageCapacityQuotaGiB"`
	Type                    string `json:"Type"`
}

// OpenZFSOriginSnapshot mirrors types.OpenZFSOriginSnapshotConfiguration.
type OpenZFSOriginSnapshot struct {
	CopyStrategy string `json:"CopyStrategy"`
	SnapshotARN  string `json:"SnapshotARN"`
}

// openZFSVolumeSettings holds the mutable members shared by
// CreateOpenZFSVolumeConfiguration and UpdateOpenZFSVolumeConfiguration.
type openZFSVolumeSettings struct {
	CopyTagsToSnapshots           *bool                     `json:"CopyTagsToSnapshots,omitempty"`
	ReadOnly                      *bool                     `json:"ReadOnly,omitempty"`
	RecordSizeKiB                 *int32                    `json:"RecordSizeKiB,omitempty"`
	StorageCapacityQuotaGiB       *int32                    `json:"StorageCapacityQuotaGiB,omitempty"`
	StorageCapacityReservationGiB *int32                    `json:"StorageCapacityReservationGiB,omitempty"`
	DataCompressionType           string                    `json:"DataCompressionType,omitempty"`
	NfsExports                    []OpenZFSNfsExport        `json:"NfsExports,omitempty"`
	UserAndGroupQuotas            []OpenZFSUserOrGroupQuota `json:"UserAndGroupQuotas,omitempty"`
}

func validRecordSizeKiB(size int32, tiering bool) bool {
	lo, hi := int32(openZFSMinRecordSizeKiB), int32(openZFSMaxRecordSizeKiB)
	if tiering {
		lo, hi = openZFSTieringMinRecordKiB, openZFSTieringMaxRecordKiB
	}

	return size >= lo && size <= hi && size&(size-1) == 0
}

func validateNfsExports(exports []OpenZFSNfsExport) error {
	for _, e := range exports {
		if len(e.ClientConfigurations) == 0 {
			return fmt.Errorf("%w: NfsExports.ClientConfigurations is required", ErrValidation)
		}

		for _, c := range e.ClientConfigurations {
			if c.Clients == "" || len(c.Options) == 0 {
				return fmt.Errorf("%w: NfsExports client configuration needs Clients and Options", ErrValidation)
			}
		}
	}

	return nil
}

func validateUserAndGroupQuotas(quotas []OpenZFSUserOrGroupQuota) error {
	for _, q := range quotas {
		if q.ID == nil || q.StorageCapacityQuotaGiB == nil {
			return fmt.Errorf("%w: UserAndGroupQuotas entries need Id and StorageCapacityQuotaGiB", ErrValidation)
		}

		if q.Type != "USER" && q.Type != "GROUP" {
			return fmt.Errorf("%w: UserAndGroupQuotas Type %q must be USER or GROUP", ErrValidation, q.Type)
		}
	}

	return nil
}

func validateOpenZFSSettings(s *openZFSVolumeSettings, tiering bool) error {
	switch s.DataCompressionType {
	case "", "NONE", "ZSTD", "LZ4":
	default:
		return fmt.Errorf("%w: DataCompressionType %q must be NONE, ZSTD or LZ4", ErrValidation, s.DataCompressionType)
	}

	if s.RecordSizeKiB != nil && !validRecordSizeKiB(*s.RecordSizeKiB, tiering) {
		return fmt.Errorf("%w: RecordSizeKiB %d is not valid for this file system", ErrValidation, *s.RecordSizeKiB)
	}

	for _, n := range []*int32{s.StorageCapacityQuotaGiB, s.StorageCapacityReservationGiB} {
		if n != nil && *n < unsetCapacity {
			return fmt.Errorf("%w: storage capacity values must be -1 or greater", ErrValidation)
		}
	}

	if err := validateNfsExports(s.NfsExports); err != nil {
		return err
	}

	return validateUserAndGroupQuotas(s.UserAndGroupQuotas)
}

func capacityOrNil(n *int32, unsetAtZero bool) *int32 {
	if n == nil || *n == unsetCapacity || (unsetAtZero && *n == 0) {
		return nil
	}

	v := *n

	return &v
}

func applyOpenZFSSettings(cfg *OpenZFSVolumeConfiguration, s *openZFSVolumeSettings) {
	if s.CopyTagsToSnapshots != nil {
		cfg.CopyTagsToSnapshots = *s.CopyTagsToSnapshots
	}

	if s.ReadOnly != nil {
		cfg.ReadOnly = *s.ReadOnly
	}

	if s.RecordSizeKiB != nil {
		cfg.RecordSizeKiB = *s.RecordSizeKiB
	}

	if s.DataCompressionType != "" {
		cfg.DataCompressionType = s.DataCompressionType
	}

	if s.StorageCapacityQuotaGiB != nil {
		cfg.StorageCapacityQuotaGiB = capacityOrNil(s.StorageCapacityQuotaGiB, false)
	}

	if s.StorageCapacityReservationGiB != nil {
		cfg.StorageCapacityReservationGiB = capacityOrNil(s.StorageCapacityReservationGiB, true)
	}

	if s.NfsExports != nil {
		cfg.NfsExports = slices.Clone(s.NfsExports)
	}

	if s.UserAndGroupQuotas != nil {
		cfg.UserAndGroupQuotas = slices.Clone(s.UserAndGroupQuotas)
	}
}

func cloneOpenZFSConfig(c *OpenZFSVolumeConfiguration) *OpenZFSVolumeConfiguration {
	if c == nil {
		return nil
	}

	out := *c
	out.NfsExports = slices.Clone(c.NfsExports)
	out.UserAndGroupQuotas = slices.Clone(c.UserAndGroupQuotas)

	if c.OriginSnapshot != nil {
		origin := *c.OriginSnapshot
		out.OriginSnapshot = &origin
	}

	out.StorageCapacityQuotaGiB = capacityOrNil(c.StorageCapacityQuotaGiB, false)
	out.StorageCapacityReservationGiB = capacityOrNil(c.StorageCapacityReservationGiB, false)

	return &out
}

func (b *InMemoryBackend) fileSystemIsTiered(fileSystemID string) bool {
	fs, ok := b.fileSystems.Get(fileSystemID)

	return ok && fs.StorageType == storageTypeIntelligentTiered
}

// buildOpenZFSVolumeLocked validates cfg and renders the stored OpenZFS block
// for a new volume under fileSystemID. Caller must hold b.mu.
func (b *InMemoryBackend) buildOpenZFSVolumeLocked(
	cfg *createOpenZFSVolumeConfigInput,
	fileSystemID string,
) (*OpenZFSVolumeConfiguration, error) {
	if err := validateOpenZFSSettings(&cfg.openZFSVolumeSettings, b.fileSystemIsTiered(fileSystemID)); err != nil {
		return nil, err
	}

	out := &OpenZFSVolumeConfiguration{ParentVolumeID: cfg.ParentVolumeID}

	if o := cfg.OriginSnapshot; o != nil {
		if o.SnapshotARN == "" {
			return nil, fmt.Errorf("%w: OriginSnapshot.SnapshotARN is required", ErrValidation)
		}

		if o.CopyStrategy != copyStrategyClone && o.CopyStrategy != copyStrategyFullCopy {
			return nil, fmt.Errorf("%w: OriginSnapshot.CopyStrategy must be CLONE or FULL_COPY", ErrValidation)
		}

		if !b.snapshots.Has(snapshotIDFromARN(o.SnapshotARN)) {
			return nil, fmt.Errorf("%w: snapshot %q not found", ErrValidation, o.SnapshotARN)
		}

		out.OriginSnapshot = &OpenZFSOriginSnapshot{CopyStrategy: o.CopyStrategy, SnapshotARN: o.SnapshotARN}
	}

	applyOpenZFSSettings(out, &cfg.openZFSVolumeSettings)

	return out, nil
}

type updateOpenZFSVolumeConfigInput struct {
	openZFSVolumeSettings
}

func (b *InMemoryBackend) updateOpenZFSVolumeLocked(v *storedVolume, in *updateOpenZFSVolumeConfigInput) error {
	if v.VolumeType != fileSystemTypeOpenZFS {
		return fmt.Errorf("%w: OpenZFSConfiguration applies only to OPENZFS volumes", ErrValidation)
	}

	if err := validateOpenZFSSettings(&in.openZFSVolumeSettings, b.fileSystemIsTiered(v.FileSystemID)); err != nil {
		return err
	}

	cfg := cloneOpenZFSConfig(v.OpenZFS)
	if cfg == nil {
		cfg = &OpenZFSVolumeConfiguration{}
	}

	applyOpenZFSSettings(cfg, &in.openZFSVolumeSettings)
	v.OpenZFS = cfg

	return nil
}

// volumeDependentsLocked returns child volumes of volumeID and volumes cloned
// from any snapshot of it. Caller must hold b.mu.
func (b *InMemoryBackend) volumeDependentsLocked(volumeID string) []string {
	snapARNs := make(map[string]struct{})

	b.snapshots.Range(func(s *storedSnapshot) bool {
		if s.VolumeID == volumeID {
			snapARNs[s.ResourceARN] = struct{}{}
		}

		return true
	})

	var ids []string

	b.volumes.Range(func(v *storedVolume) bool {
		if v.OpenZFS == nil || v.VolumeID == volumeID {
			return true
		}

		_, cloned := snapARNs[originSnapshotARN(v.OpenZFS)]
		if v.OpenZFS.ParentVolumeID == volumeID ||
			(cloned && v.OpenZFS.OriginSnapshot.CopyStrategy == copyStrategyClone) {
			ids = append(ids, v.VolumeID)
		}

		return true
	})

	slices.Sort(ids)

	return ids
}

func originSnapshotARN(c *OpenZFSVolumeConfiguration) string {
	if c == nil || c.OriginSnapshot == nil {
		return ""
	}

	return c.OriginSnapshot.SnapshotARN
}

// deleteVolumeTreeLocked deletes volumeID and, transitively, every dependent
// from volumeDependentsLocked. Caller must hold b.mu.
func (b *InMemoryBackend) deleteVolumeTreeLocked(volumeID string) {
	for _, dep := range b.volumeDependentsLocked(volumeID) {
		b.deleteVolumeTreeLocked(dep)
	}

	b.deleteVolumeLocked(volumeID)
}

func validateOptions(name string, options []string, allowed ...string) error {
	for _, o := range options {
		if !slices.Contains(allowed, o) {
			return fmt.Errorf("%w: %s value %q is not valid", ErrValidation, name, o)
		}
	}

	return nil
}

// restoreIntermediatesLocked enforces RestoreVolumeFromSnapshot's Options
// contract and removes what the options allow to be deleted. Caller must
// hold b.mu.
func (b *InMemoryBackend) restoreIntermediatesLocked(v *storedVolume, target *storedSnapshot, options []string) error {
	var newer []*storedSnapshot

	b.snapshots.Range(func(s *storedSnapshot) bool {
		if s.VolumeID == v.VolumeID && s.CreationTime.After(target.CreationTime) {
			newer = append(newer, s)
		}

		return true
	})

	if len(newer) == 0 {
		return nil
	}

	if !slices.Contains(options, optionDeleteIntermediateSnapshots) {
		return fmt.Errorf("%w: intermediate snapshots exist; use %s", ErrValidation, optionDeleteIntermediateSnapshots)
	}

	newerARNs := make(map[string]struct{}, len(newer))
	for _, s := range newer {
		newerARNs[s.ResourceARN] = struct{}{}
	}

	var clones []string

	b.volumes.Range(func(c *storedVolume) bool {
		if _, ok := newerARNs[originSnapshotARN(c.OpenZFS)]; ok {
			clones = append(clones, c.VolumeID)
		}

		return true
	})

	if len(clones) > 0 && !slices.Contains(options, optionDeleteClonedVolumes) {
		return fmt.Errorf("%w: dependent clone volumes exist; use %s", ErrValidation, optionDeleteClonedVolumes)
	}

	for _, id := range clones {
		b.deleteVolumeTreeLocked(id)
	}

	for _, s := range newer {
		delete(b.tags, s.ResourceARN)
		b.snapshots.Delete(s.SnapshotID)
	}

	return nil
}
