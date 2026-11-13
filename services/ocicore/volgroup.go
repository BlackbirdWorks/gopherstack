package ocicore

import (
	"slices"
)

const (
	maxGroupVolumes = 32

	groupFromIDs     = "volumeIds"
	groupFromBackup  = "volumeGroupBackupId"
	groupFromGroup   = "volumeGroupId"
	groupFromReplica = "volumeGroupReplicaId"
)

// CreateVolumeGroupIn is the CreateVolumeGroup request body.
type CreateVolumeGroupIn struct {
	TagsIn
	SourceDetails       *VolumeGroupSource `json:"sourceDetails"`
	CompartmentID       string             `json:"compartmentId"`
	AvailabilityDomain  string             `json:"availabilityDomain"`
	DisplayName         string             `json:"displayName"`
	BackupPolicyID      string             `json:"backupPolicyId"`
	VolumeGroupReplicas []any              `json:"volumeGroupReplicas"`
}

// groupMember is a volume or boot volume that belongs to a volume group.
type groupMember interface {
	resource
	member() (ad string, group *string, sizeMBs int64)
}

func (v *Volume) member() (string, *string, int64) {
	return v.AvailabilityDomain, &v.VolumeGroupID, v.SizeInMBs
}

func (v *BootVolume) member() (string, *string, int64) {
	return v.AvailabilityDomain, &v.VolumeGroupID, v.SizeInMBs
}

func (b *InMemoryBackend) findMember(id string) (groupMember, *APIError) {
	switch ocidKind(id) {
	case kindVolume:
		return getLive[*Volume](b, id)
	case kindBootVolume:
		return getLive[*BootVolume](b, id)
	}

	return nil, errParam("volumeIds entry " + id + " must be a volume or boot volume OCID")
}

// groupMembers validates the ids a volume group is to hold; selfID is the group being updated.
func (b *InMemoryBackend) groupMembers(ad string, ids []string, selfID string) ([]groupMember, *APIError) {
	if len(ids) > maxGroupVolumes {
		return nil, errLimit("A volume group can have at most 32 volumes")
	}

	out := make([]groupMember, 0, len(ids))

	for i, id := range ids {
		if slices.Contains(ids[:i], id) {
			return nil, errParam("volumeIds lists " + id + " twice")
		}

		m, aerr := b.findMember(id)
		if aerr != nil {
			return nil, aerr
		}

		mad, grp, _ := m.member()

		switch {
		case m.base().LifecycleState != statAvailable:
			return nil, errState("The volume " + id + " is " + m.base().LifecycleState)
		case mad != ad:
			return nil, errParam("The volume " + id + " is in a different availability domain than the volume group")
		case *grp != "" && *grp != selfID:
			return nil, errConflict("The volume " + id + " already belongs to volume group " + *grp)
		}

		out = append(out, m)
	}

	return out, nil
}

func (b *InMemoryBackend) groupSize(ids []string) int64 {
	var mbs int64

	for _, id := range ids {
		if m, ok := b.res[id].(groupMember); ok {
			_, _, sz := m.member()
			mbs += sz
		}
	}

	return mbs
}

func (g *VolumeGroup) setSize(mbs int64) {
	g.SizeInMBs, g.SizeInGBs = mbs, mbs/bytesGB
}

// sourceMembers resolves the volumes a new group starts with, creating restored or cloned ones as needed.
func (b *InMemoryBackend) sourceMembers(g *VolumeGroup, src *VolumeGroupSource) ([]string, *APIError) {
	switch src.Type {
	case groupFromIDs:
		ms, aerr := b.groupMembers(g.AvailabilityDomain, src.VolumeIDs, "")
		if aerr != nil {
			return nil, aerr
		}

		ids := make([]string, 0, len(ms))
		for _, m := range ms {
			ids = append(ids, m.base().ID)
		}

		return ids, nil
	case groupFromBackup:
		return b.restoreGroup(g, src.VolumeGroupBackupID)
	case groupFromGroup:
		return b.cloneGroup(g, src.VolumeGroupID)
	case groupFromReplica:
		return nil, errNotFound(src.VolumeGroupReplicaID)
	}

	return nil, errParam("sourceDetails.type must be volumeIds, volumeGroupBackupId or volumeGroupId")
}

func (b *InMemoryBackend) restoreGroup(g *VolumeGroup, backupID string) ([]string, *APIError) {
	gb, aerr := getLive[*VolumeGroupBackup](b, backupID)
	if aerr != nil {
		return nil, aerr
	}

	if gb.LifecycleState != statAvailable {
		return nil, errState("The volume group backup " + gb.ID + " is " + gb.LifecycleState)
	}

	ids := make([]string, 0, len(gb.VolumeBackupIDs))

	for _, id := range gb.VolumeBackupIDs {
		switch bk := b.res[id].(type) {
		case *VolumeBackup:
			ids = append(ids, b.restoreVolume(g, bk).ID)
		case *BootVolumeBackup:
			ids = append(ids, b.restoreBootVolume(g, bk).ID)
		}
	}

	return ids, nil
}

func (b *InMemoryBackend) restoreVolume(g *VolumeGroup, bk *VolumeBackup) *Volume {
	v := &Volume{
		Base:               b.newBase(kindVolume, g.CompartmentID, b.restoredName(bk.VolumeID), TagsIn{}),
		AvailabilityDomain: g.AvailabilityDomain,
		SizeInGBs:          bk.SizeInGBs,
		SizeInMBs:          bk.SizeInMBs,
		VpusPerGB:          defaultVpus,
		SourceDetails:      &VolumeSource{Type: srcVolumeBackup, ID: bk.ID},
		VolumeGroupID:      g.ID,
		KmsKeyID:           bk.KmsKeyID,
	}
	b.moveTo(v, statRestoring, statAvailable)
	b.add(v)

	return v
}

func (b *InMemoryBackend) restoreBootVolume(g *VolumeGroup, bk *BootVolumeBackup) *BootVolume {
	bv := &BootVolume{
		Base:               b.newBase(kindBootVolume, g.CompartmentID, b.restoredName(bk.BootVolumeID), TagsIn{}),
		AvailabilityDomain: g.AvailabilityDomain, ImageID: bk.ImageID, SizeInGBs: bk.SizeInGBs,
		SizeInMBs: bk.SizeInGBs * bytesGB, VpusPerGB: defaultVpus, KmsKeyID: bk.KmsKeyID, VolumeGroupID: g.ID,
		SourceDetails: &VolumeSource{Type: srcBootVolumeBackup, ID: bk.ID},
	}
	b.moveTo(bv, statRestoring, statAvailable)
	b.add(bv)

	return bv
}

func (b *InMemoryBackend) restoredName(sourceID string) string {
	if m, ok := b.res[sourceID]; ok {
		return m.base().DisplayName + " (restored)"
	}

	return "restored volume"
}

func (b *InMemoryBackend) cloneGroup(g *VolumeGroup, groupID string) ([]string, *APIError) {
	src, aerr := getLive[*VolumeGroup](b, groupID)
	if aerr != nil {
		return nil, aerr
	}

	if src.LifecycleState != statAvailable {
		return nil, errState("The source volume group " + src.ID + " is " + src.LifecycleState)
	}

	if src.AvailabilityDomain != g.AvailabilityDomain {
		return nil, errParam("A volume group can only be cloned within its availability domain")
	}

	ids := make([]string, 0, len(src.VolumeIDs))

	for _, id := range src.VolumeIDs {
		switch m := b.res[id].(type) {
		case *Volume:
			c := *m
			ids = append(ids, b.cloneVolume(g, &c).ID)
		case *BootVolume:
			c := *m
			ids = append(ids, b.cloneBootVolume(g, &c).ID)
		}
	}

	return ids, nil
}

func (b *InMemoryBackend) cloneVolume(g *VolumeGroup, src *Volume) *Volume {
	v := &Volume{
		Base:               b.newBase(kindVolume, g.CompartmentID, src.DisplayName+" (clone)", TagsIn{}),
		AvailabilityDomain: g.AvailabilityDomain,
		SizeInGBs:          src.SizeInGBs,
		SizeInMBs:          src.SizeInMBs,
		VpusPerGB:          src.VpusPerGB,
		SourceDetails:      &VolumeSource{Type: srcVolume, ID: src.ID},
		VolumeGroupID:      g.ID,
		KmsKeyID:           src.KmsKeyID,
	}
	b.moveTo(v, statProvisioning, statAvailable)
	b.add(v)

	return v
}

func (b *InMemoryBackend) cloneBootVolume(g *VolumeGroup, src *BootVolume) *BootVolume {
	bv := &BootVolume{
		Base:               b.newBase(kindBootVolume, g.CompartmentID, src.DisplayName+" (clone)", TagsIn{}),
		AvailabilityDomain: g.AvailabilityDomain,
		ImageID:            src.ImageID,
		SizeInGBs:          src.SizeInGBs,
		SizeInMBs:          src.SizeInMBs,
		VpusPerGB:          src.VpusPerGB,
		KmsKeyID:           src.KmsKeyID,
		VolumeGroupID:      g.ID,
		SourceDetails:      &VolumeSource{Type: srcBootVolume, ID: src.ID},
	}
	b.moveTo(bv, statProvisioning, statAvailable)
	b.add(bv)

	return bv
}

// CreateVolumeGroup creates a volume group from volumes, a volume group backup or another volume group.
func (b *InMemoryBackend) CreateVolumeGroup(in CreateVolumeGroupIn) (resource, string, *APIError) {
	if aerr := b.volumeGroupChecks(in); aerr != nil {
		return nil, "", aerr
	}

	g := &VolumeGroup{
		Base:               b.newBase(kindVolumeGroup, in.CompartmentID, in.DisplayName, in.TagsIn),
		AvailabilityDomain: in.AvailabilityDomain, SourceDetails: in.SourceDetails, VolumeIDs: []string{},
	}
	if in.DisplayName == "" {
		g.DisplayName = "volume-group" + b.nowUTC().Format("20060102-150405")
	}

	ids, aerr := b.sourceMembers(g, in.SourceDetails)
	if aerr != nil {
		return nil, "", aerr
	}

	g.IsHydrated = in.SourceDetails.Type == groupFromIDs
	b.adopt(g, ids)
	b.add(g)

	if in.BackupPolicyID != "" {
		b.assignPolicy(g.ID, in.BackupPolicyID, g.CompartmentID)
	}

	return g.view(), g.Internal.Etag, nil
}

func (b *InMemoryBackend) volumeGroupChecks(in CreateVolumeGroupIn) *APIError {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return aerr
	}

	if in.AvailabilityDomain == "" {
		return errMissing("availabilityDomain is required")
	}

	if !blockAdRe.MatchString(in.AvailabilityDomain) {
		return errParam("availabilityDomain " + in.AvailabilityDomain + " is not a valid availability domain")
	}

	if in.SourceDetails == nil {
		return errMissing("sourceDetails is required")
	}

	if len(in.VolumeGroupReplicas) > 0 {
		return errParam("volumeGroupReplicas is not supported")
	}

	if in.BackupPolicyID != "" {
		if _, aerr := b.findPolicy(in.BackupPolicyID); aerr != nil {
			return aerr
		}
	}

	return nil
}

// adopt makes the volumes members of the group and refreshes its size.
func (b *InMemoryBackend) adopt(g *VolumeGroup, ids []string) {
	g.VolumeIDs = slices.Clone(ids)
	if g.VolumeIDs == nil {
		g.VolumeIDs = []string{}
	}

	for _, id := range ids {
		if m, ok := b.res[id].(groupMember); ok {
			_, grp, _ := m.member()
			*grp = g.ID
			b.bump(m)
		}
	}

	g.setSize(b.groupSize(ids))
}

// UpdateVolumeGroupIn is the UpdateVolumeGroup request body.
type UpdateVolumeGroupIn struct {
	UpdateIn
	VolumeIDs           *[]string `json:"volumeIds"`
	VolumeGroupReplicas []any     `json:"volumeGroupReplicas"`
}

// UpdateVolumeGroup renames a group or replaces its set of volumes.
func (b *InMemoryBackend) UpdateVolumeGroup(id, ifMatch string, in UpdateVolumeGroupIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(g *VolumeGroup) *APIError {
		if len(in.VolumeGroupReplicas) > 0 {
			return errParam("volumeGroupReplicas is not supported")
		}

		if in.VolumeIDs == nil {
			return nil
		}

		if _, aerr := b.groupMembers(g.AvailabilityDomain, *in.VolumeIDs, g.ID); aerr != nil {
			return aerr
		}

		b.leaveGroup(g)
		b.adopt(g, *in.VolumeIDs)

		return nil
	})
}

// leaveGroup detaches every member from the group.
func (b *InMemoryBackend) leaveGroup(g *VolumeGroup) {
	for _, id := range g.VolumeIDs {
		if m, ok := b.res[id].(groupMember); ok {
			_, grp, _ := m.member()
			if *grp == g.ID {
				*grp = ""
				b.bump(m)
			}
		}
	}
}

// leaveVolumeGroup removes a volume that is being deleted from its group.
func (b *InMemoryBackend) leaveVolumeGroup(m groupMember) {
	_, grp, _ := m.member()
	if *grp == "" {
		return
	}

	if g, ok := b.res[*grp].(*VolumeGroup); ok {
		g.VolumeIDs = slices.DeleteFunc(slices.Clone(g.VolumeIDs), func(id string) bool { return id == m.base().ID })
		g.setSize(b.groupSize(g.VolumeIDs))
		b.bump(g)
	}

	*grp = ""
}

// CreateVolumeGroupBackupIn is the CreateVolumeGroupBackup request body.
type CreateVolumeGroupBackupIn struct {
	TagsIn
	RetentionIn
	VolumeGroupID string `json:"volumeGroupId"`
	CompartmentID string `json:"compartmentId"`
	DisplayName   string `json:"displayName"`
	Type          string `json:"type"`
}

// CreateVolumeGroupBackup backs up every member of a volume group; the member backups share its lifecycle.
func (b *InMemoryBackend) CreateVolumeGroupBackup(in CreateVolumeGroupBackupIn) (resource, string, *APIError) {
	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.VolumeGroupID == "" {
		return nil, "", errMissing("volumeGroupId is required")
	}

	if in.Type != "" && in.Type != backupFull && in.Type != backupIncremental {
		return nil, "", errParam("type must be FULL or INCREMENTAL")
	}

	g, aerr := getLive[*VolumeGroup](b, in.VolumeGroupID)
	if aerr != nil {
		return nil, "", aerr
	}

	if g.LifecycleState != statAvailable {
		return nil, "", errState("The volume group " + g.ID + " is " + g.LifecycleState)
	}

	comp := cmpOrStr(in.CompartmentID, g.CompartmentID)
	if aerr = b.checkCompartment(comp); aerr != nil {
		return nil, "", aerr
	}

	gb, aerr := b.snapshotGroup(g, in, comp)
	if aerr != nil {
		return nil, "", aerr
	}

	return gb.view(), gb.Internal.Etag, nil
}

func (b *InMemoryBackend) snapshotGroup(
	g *VolumeGroup,
	in CreateVolumeGroupBackupIn,
	comp string,
) (*VolumeGroupBackup, *APIError) {
	typ := cmpOrStr(in.Type, backupIncremental)
	if len(liveOf(b, func(x *VolumeGroupBackup) bool { return x.VolumeGroupID == g.ID })) == 0 {
		typ = backupFull
	}

	gb := &VolumeGroupBackup{
		Base: b.newBase(kindGroupBackup, comp, in.DisplayName, in.TagsIn), Type: typ, VolumeGroupID: g.ID,
		SourceType: sourceManual, TimeRequestReceived: b.nowUTC(), VolumeBackupIDs: []string{},
	}
	if in.DisplayName == "" {
		gb.DisplayName = "volume-group-backup" + b.nowUTC().Format("20060102-150405")
	}

	if aerr := gb.apply(in.RetentionIn, gb.TimeCreated); aerr != nil {
		return nil, aerr
	}

	for _, id := range g.VolumeIDs {
		bid, aerr := b.backupMember(id, gb, in, comp)
		if aerr != nil {
			b.dropGroupBackup(gb)

			return nil, aerr
		}

		if bid != "" {
			gb.VolumeBackupIDs = append(gb.VolumeBackupIDs, bid)
		}
	}

	gb.SizeInMBs = b.groupSize(g.VolumeIDs)
	gb.SizeInGBs = gb.SizeInMBs / bytesGB
	b.moveTo(gb, statCreating, statAvailable)
	b.add(gb)

	return gb, nil
}

// backupMember snapshots one group member for a group backup and returns the member backup id.
func (b *InMemoryBackend) backupMember(
	id string,
	gb *VolumeGroupBackup,
	in CreateVolumeGroupBackupIn,
	comp string,
) (string, *APIError) {
	switch m := b.res[id].(type) {
	case *Volume:
		bk, aerr := b.snapshotVolume(
			m,
			CreateVolumeBackupIn{TagsIn: in.TagsIn, RetentionIn: in.RetentionIn, Type: in.Type},
			comp,
		)
		if aerr != nil {
			return "", aerr
		}

		bk.VolumeGroupBackupID = gb.ID

		return bk.ID, nil
	case *BootVolume:
		bk, aerr := b.snapshotBootVolume(m,
			CreateBootVolumeBackupIn{TagsIn: in.TagsIn, RetentionIn: in.RetentionIn, Type: in.Type}, comp)
		if aerr != nil {
			return "", aerr
		}

		bk.VolumeGroupBackupID = gb.ID

		return bk.ID, nil
	}

	return "", nil
}

func (b *InMemoryBackend) dropGroupBackup(gb *VolumeGroupBackup) {
	for _, id := range gb.VolumeBackupIDs {
		delete(b.res, id)
	}
}

// UpdateVolumeGroupBackupIn is the UpdateVolumeGroupBackup request body.
type UpdateVolumeGroupBackupIn struct {
	RetentionIn
	UpdateIn
}

// UpdateVolumeGroupBackup changes a group backup's name, tags and retention settings; members follow retention.
func (b *InMemoryBackend) UpdateVolumeGroupBackup(
	id, ifMatch string, in UpdateVolumeGroupBackupIn,
) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(gb *VolumeGroupBackup) *APIError {
		if aerr := gb.apply(in.RetentionIn, gb.TimeCreated); aerr != nil {
			return aerr
		}

		for _, mid := range gb.VolumeBackupIDs {
			switch m := b.res[mid].(type) {
			case *VolumeBackup:
				m.Retention = gb.Retention
			case *BootVolumeBackup:
				m.Retention = gb.Retention
			}
		}

		return nil
	})
}

// groupBackupMembers returns the live member backups of a group backup.
func (b *InMemoryBackend) groupBackupMembers(gb *VolumeGroupBackup) []resource {
	var out []resource

	for _, id := range gb.VolumeBackupIDs {
		if m, ok := b.res[id]; ok && live(m) {
			out = append(out, m)
		}
	}

	return out
}

func (b *InMemoryBackend) deleteGroupBackupCheck(gb *VolumeGroupBackup) *APIError {
	if aerr := b.retentionBlock("volume group backup", gb.ID, &gb.Retention); aerr != nil {
		return aerr
	}

	for _, m := range b.groupBackupMembers(gb) {
		switch bk := m.(type) {
		case *VolumeBackup:
			if aerr := b.retentionBlock("volume backup", bk.ID, &bk.Retention); aerr != nil {
				return aerr
			}
		case *BootVolumeBackup:
			if aerr := b.retentionBlock("boot volume backup", bk.ID, &bk.Retention); aerr != nil {
				return aerr
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) terminateGroupBackup(gb *VolumeGroupBackup) {
	for _, m := range b.groupBackupMembers(gb) {
		b.markTerminating(m)
	}
}

// memberBackupBlock refuses to delete a backup that still belongs to a live group backup.
func (b *InMemoryBackend) memberBackupBlock(id, groupBackupID string) *APIError {
	if groupBackupID == "" {
		return nil
	}

	if gb, ok := b.res[groupBackupID].(*VolumeGroupBackup); ok && live(gb) {
		return errConflict(
			"The backup " + id + " belongs to volume group backup " + gb.ID + ": delete the group backup instead",
		)
	}

	return nil
}
