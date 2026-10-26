package ocicore

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	backupFull        = "FULL"
	backupIncremental = "INCREMENTAL"
)

// CreateVolumeBackupIn is the CreateVolumeBackup request body.
type CreateVolumeBackupIn struct {
	TagsIn
	DisplayName string `json:"displayName"`
	Type        string `json:"type"`
	VolumeID    string `json:"volumeId"`
	KmsKeyID    string `json:"kmsKeyId"`
}

// CreateVolumeBackup snapshots a volume; the first backup of a volume is always FULL.
func (b *InMemoryBackend) CreateVolumeBackup(in CreateVolumeBackupIn) (resource, string, *APIError) {
	b.mu.Lock("CreateVolumeBackup")
	defer b.mu.Unlock()

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.VolumeID == "" {
		return nil, "", errMissing("volumeId is required")
	}

	if in.Type != "" && in.Type != backupFull && in.Type != backupIncremental {
		return nil, "", errParam("type must be FULL or INCREMENTAL")
	}

	vol, aerr := getLive[*Volume](b, in.VolumeID)
	if aerr != nil {
		return nil, "", aerr
	}

	if vol.LifecycleState != statAvailable {
		return nil, "", errState("The volume " + vol.ID + " is " + vol.LifecycleState)
	}

	typ := cmpOrStr(in.Type, backupIncremental)
	if len(liveOf(b, func(x *VolumeBackup) bool { return x.VolumeID == vol.ID })) == 0 {
		typ = backupFull
	}

	bk := &VolumeBackup{
		Base:                b.newBase(kindBackup, vol.CompartmentID, in.DisplayName, in.TagsIn),
		Type:                typ,
		VolumeID:            vol.ID,
		SourceType:          "MANUAL",
		KmsKeyID:            in.KmsKeyID,
		SizeInGBs:           vol.SizeInGBs,
		SizeInMBs:           vol.SizeInMBs,
		TimeRequestReceived: b.nowUTC(),
	}
	if in.DisplayName == "" {
		bk.DisplayName = "backup" + b.nowUTC().Format("20060102-150405")
	}

	b.moveTo(bk, statCreating, statAvailable)
	b.add(bk)

	return bk.view(), bk.Internal.Etag, nil
}

// UpdateVolumeBackup changes a backup's name and tags.
func (b *InMemoryBackend) UpdateVolumeBackup(id, ifMatch string, in UpdateIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*VolumeBackup) *APIError { return nil })
}

func (b *InMemoryBackend) findPolicy(id string) (*VolumeBackupPolicy, *APIError) {
	if i := slices.IndexFunc(predefinedPolicies(), func(p *VolumeBackupPolicy) bool { return p.ID == id }); i >= 0 {
		return predefinedPolicies()[i], nil
	}

	return get[*VolumeBackupPolicy](b, id)
}

func validSchedules(in []BackupSchedule) *APIError {
	for _, s := range in {
		if s.BackupType != backupFull && s.BackupType != backupIncremental {
			return errParam("schedule backupType must be FULL or INCREMENTAL")
		}

		if !slices.Contains([]string{"ONE_HOUR", "ONE_DAY", "ONE_WEEK", "ONE_MONTH", "ONE_YEAR"}, s.Period) {
			return errParam("schedule period must be ONE_HOUR, ONE_DAY, ONE_WEEK, ONE_MONTH or ONE_YEAR")
		}

		if s.RetentionSeconds < 0 {
			return errParam("schedule retentionSeconds must not be negative")
		}
	}

	return nil
}

// CreateVolumeBackupPolicyIn is the CreateVolumeBackupPolicy request body.
type CreateVolumeBackupPolicyIn struct {
	TagsIn
	CompartmentID     string           `json:"compartmentId"`
	DisplayName       string           `json:"displayName"`
	DestinationRegion string           `json:"destinationRegion"`
	Schedules         []BackupSchedule `json:"schedules"`
}

// CreateVolumeBackupPolicy stores a user defined policy; its schedules are never executed.
func (b *InMemoryBackend) CreateVolumeBackupPolicy(in CreateVolumeBackupPolicyIn) (resource, string, *APIError) {
	b.mu.Lock("CreateVolumeBackupPolicy")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validSchedules(in.Schedules); aerr != nil {
		return nil, "", aerr
	}

	p := &VolumeBackupPolicy{
		Base:      b.newBase(kindPolicy, in.CompartmentID, in.DisplayName, in.TagsIn),
		Schedules: nonNilSchedules(in.Schedules), DestinationRegion: in.DestinationRegion,
	}
	b.ready(p)
	b.add(p)

	return p.view(), p.Internal.Etag, nil
}

func nonNilSchedules(s []BackupSchedule) []BackupSchedule {
	if s == nil {
		return []BackupSchedule{}
	}

	return s
}

// UpdateVolumeBackupPolicyIn is the UpdateVolumeBackupPolicy request body.
type UpdateVolumeBackupPolicyIn struct {
	Schedules *[]BackupSchedule `json:"schedules"`
	UpdateIn
}

// UpdateVolumeBackupPolicy changes a user defined policy.
func (b *InMemoryBackend) UpdateVolumeBackupPolicy(
	id, ifMatch string,
	in UpdateVolumeBackupPolicyIn,
) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(p *VolumeBackupPolicy) *APIError {
		if in.Schedules == nil {
			return nil
		}

		if aerr := validSchedules(*in.Schedules); aerr != nil {
			return aerr
		}

		p.Schedules = nonNilSchedules(*in.Schedules)

		return nil
	})
}

// GetVolumeBackupPolicy returns a predefined or user defined policy.
func (b *InMemoryBackend) GetVolumeBackupPolicy(id string) (resource, string, *APIError) {
	b.mu.RLock("GetVolumeBackupPolicy")
	defer b.mu.RUnlock()

	p, aerr := b.findPolicy(id)
	if aerr != nil {
		return nil, "", aerr
	}

	return p.view(), p.Internal.Etag, nil
}

// ListVolumeBackupPolicies lists the predefined policies plus the custom ones of a compartment.
func (b *InMemoryBackend) ListVolumeBackupPolicies(
	compartment, pageToken string,
	limit int,
) (page.Page[resource], *APIError) {
	b.mu.RLock("ListVolumeBackupPolicies")
	defer b.mu.RUnlock()

	all := predefinedPolicies()

	if compartment != "" {
		if aerr := b.checkCompartment(compartment); aerr != nil {
			return page.Page[resource]{}, aerr
		}

		custom := liveOf(b, func(p *VolumeBackupPolicy) bool { return p.CompartmentID == compartment })
		sortRes(custom, ListOpts{Order: "ASC"})
		all = append(all, custom...)
	}

	pg := page.New(all, pageToken, limit, defaultListLimit)
	out := make([]resource, 0, len(pg.Data))

	for _, p := range pg.Data {
		out = append(out, p.view())
	}

	return page.Page[resource]{Data: out, Next: pg.Next}, nil
}

// DeleteVolumeBackupPolicy deletes a user defined policy that has no assignments.
func (b *InMemoryBackend) DeleteVolumeBackupPolicy(id, ifMatch string) *APIError {
	b.mu.Lock("DeleteVolumeBackupPolicy")
	defer b.mu.Unlock()

	p, aerr := get[*VolumeBackupPolicy](b, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return aerr
	}

	if a := firstID(liveOf(b, func(x *BackupPolicyAssignment) bool { return x.PolicyID == id })); a != "" {
		return errConflict("The backup policy " + id + " is assigned (" + a + ")")
	}

	delete(b.res, id)

	return nil
}

// CreateAssignmentIn is the CreateVolumeBackupPolicyAssignment request body.
type CreateAssignmentIn struct {
	AssetID  string `json:"assetId"`
	PolicyID string `json:"policyId"`
}

func (b *InMemoryBackend) assignPolicy(assetID, policyID, compartment string) *BackupPolicyAssignment {
	a := &BackupPolicyAssignment{
		Base:    b.newBase(kindAssign, compartment, "", TagsIn{}),
		AssetID: assetID, PolicyID: policyID,
	}
	b.ready(a)
	b.add(a)

	return a
}

func (b *InMemoryBackend) dropAssignments(assetID string) {
	for _, a := range liveOf(b, func(x *BackupPolicyAssignment) bool { return x.AssetID == assetID }) {
		delete(b.res, a.ID)
	}
}

// CreateVolumeBackupPolicyAssignment assigns a policy to a volume or boot volume (one policy per asset).
func (b *InMemoryBackend) CreateVolumeBackupPolicyAssignment(in CreateAssignmentIn) (resource, string, *APIError) {
	b.mu.Lock("CreateVolumeBackupPolicyAssignment")
	defer b.mu.Unlock()

	if in.AssetID == "" || in.PolicyID == "" {
		return nil, "", errMissing("assetId and policyId are required")
	}

	if _, aerr := b.findPolicy(in.PolicyID); aerr != nil {
		return nil, "", aerr
	}

	var comp string

	switch ocidKind(in.AssetID) {
	case kindVolume:
		v, aerr := getLive[*Volume](b, in.AssetID)
		if aerr != nil {
			return nil, "", aerr
		}

		comp = v.CompartmentID
	case kindBootVolume:
		v, aerr := getLive[*BootVolume](b, in.AssetID)
		if aerr != nil {
			return nil, "", aerr
		}

		comp = v.CompartmentID
	default:
		return nil, "", errParam("assetId must be a volume or boot volume OCID")
	}

	if len(liveOf(b, func(x *BackupPolicyAssignment) bool { return x.AssetID == in.AssetID })) > 0 {
		return nil, "", errConflict("The asset " + in.AssetID + " already has a backup policy assigned")
	}

	a := b.assignPolicy(in.AssetID, in.PolicyID, comp)

	return a.view(), a.Internal.Etag, nil
}

// ListPolicyAssignments lists the policy assignments of an asset.
func (b *InMemoryBackend) ListPolicyAssignments(assetID, pageToken string, limit int) (page.Page[resource], *APIError) {
	if assetID == "" {
		return page.Page[resource]{}, errMissing("assetId is required")
	}

	b.mu.RLock("ListPolicyAssignments")
	defer b.mu.RUnlock()

	if _, ok := b.res[assetID]; !ok {
		return page.Page[resource]{}, errNotFound(assetID)
	}

	all := liveOf(b, func(x *BackupPolicyAssignment) bool { return x.AssetID == assetID })
	pg := page.New(all, pageToken, limit, defaultListLimit)
	out := make([]resource, 0, len(pg.Data))

	for _, a := range pg.Data {
		out = append(out, a.view())
	}

	return page.Page[resource]{Data: out, Next: pg.Next}, nil
}

// DeleteVolumeBackupPolicyAssignment removes an assignment.
func (b *InMemoryBackend) DeleteVolumeBackupPolicyAssignment(id, ifMatch string) *APIError {
	b.mu.Lock("DeleteVolumeBackupPolicyAssignment")
	defer b.mu.Unlock()

	a, aerr := get[*BackupPolicyAssignment](b, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(a, ifMatch); aerr != nil {
		return aerr
	}

	delete(b.res, id)

	return nil
}
