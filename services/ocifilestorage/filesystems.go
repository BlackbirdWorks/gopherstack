package ocifilestorage

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	cloneAttached  = "ATTACHED"
	cloneDetaching = "DETACHING"
	cloneDetached  = "DETACHED"
	hydrationSteps = 4
	quotaEnabled   = "ENABLED"
	quotaDisabled  = "DISABLED"
	quotaEnabling  = "ENABLING"
	quotaDisabling = "DISABLING"
	notBlocked     = "NOT_BLOCKED"
)

type createFSIn struct {
	tagsIn
	DisplayName                *string `json:"displayName"`
	SourceSnapshotID           *string `json:"sourceSnapshotId"`
	AreQuotaRulesEnabled       *bool   `json:"areQuotaRulesEnabled"`
	AvailabilityDomain         string  `json:"availabilityDomain"`
	CompartmentID              string  `json:"compartmentId"`
	KmsKeyID                   string  `json:"kmsKeyId"`
	CloneAttachStatus          string  `json:"cloneAttachStatus"`
	FilesystemSnapshotPolicyID string  `json:"filesystemSnapshotPolicyId"`
	Locks                      []lock  `json:"locks"`
}

type updateFSIn struct {
	tagsIn
	DisplayName                *string `json:"displayName"`
	KmsKeyID                   *string `json:"kmsKeyId"`
	FilesystemSnapshotPolicyID *string `json:"filesystemSnapshotPolicyId"`
}

// FSFilter selects file systems in a list.
type FSFilter struct {
	Compartment, AD, State, DisplayName, ID, SourceSnapshot, Parent, Policy string
}

func (b *InMemoryBackend) fsView(e *entry[*FileSystem]) FileSystem {
	v := *e.W
	v.Int, v.base = nil, cloneBase(e.W.base)

	if b.dp != nil {
		v.MeteredBytes = b.dp.Usage(v.ID)
	}

	v.IsTargetable = b.targetable(v.ID)

	return v
}

// targetable reports whether a file system is unexported, unreplicated and free of user snapshots.
func (b *InMemoryBackend) targetable(id string) bool {
	for _, x := range b.exps {
		if x.W.FileSystemID == id && x.W.State != stDeleted {
			return false
		}
	}

	for _, r := range b.reps {
		if r.W.TargetID == id && r.W.State != stDeleted {
			return false
		}
	}

	for _, sn := range b.snaps {
		if sn.W.FileSystemID == id && sn.W.State != stDeleted && sn.W.SnapshotType == snapUser {
			return false
		}
	}

	return true
}

func fsSummary(v FileSystem) map[string]any {
	return project(v, "meteredBytes", "compartmentId", "displayName", "id", "lifecycleState", "timeCreated",
		"availabilityDomain", "locks", "freeformTags", "definedTags", "systemTags", "replicationSourceCount",
		"kmsKeyId", "sourceDetails", "isCloneParent", "isHydrated", "lifecycleDetails", "cloneAttachStatus",
		"quotaEnforcementState", "compartmentQuotaEnforcementState")
}

func (b *InMemoryBackend) checkPolicyFor(policyID, ad string) *APIError {
	if policyID == "" {
		return nil
	}

	p, aerr := b.pols.usable(policyID)
	if aerr != nil {
		return aerr
	}

	if p.W.AvailabilityDomain != ad {
		return errParam("The snapshot policy " + policyID + " is in a different availability domain")
	}

	return nil
}

func (b *InMemoryBackend) retryHit(kind, token string) string {
	if token == "" {
		return ""
	}

	return b.tokens[kind+"|"+token]
}

func (b *InMemoryBackend) remember(kind, token, id string) {
	if token != "" {
		b.tokens[kind+"|"+token] = id
	}
}

// CreateFileSystem creates a file system, optionally cloned from a snapshot.
func (b *InMemoryBackend) CreateFileSystem(in createFSIn, token string) (FileSystem, string, error) {
	if in.AvailabilityDomain == "" {
		return FileSystem{}, "", errMissing("availabilityDomain is required")
	}

	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	if id := b.retryHit(kindFS, token); id != "" {
		if e, ok := b.fss[id]; ok {
			return b.fsView(e), e.Etag, nil
		}
	}

	fs, aerr := b.newFileSystem(in)
	if aerr != nil {
		return FileSystem{}, "", aerr
	}

	if b.dp != nil && fs.SourceDetails == nil {
		if err := b.dp.CreateFS(fs.ID); err != nil {
			return FileSystem{}, "", internalErr(err)
		}
	}

	e := put(b, b.fss, fs, stActive)
	b.remember(kindFS, token, fs.ID)

	return b.fsView(e), e.Etag, nil
}

func (b *InMemoryBackend) newFileSystem(in createFSIn) (*FileSystem, *APIError) {
	name := "FileSystem-" + b.nowUTC().Format("20060102-150405")
	if in.DisplayName != nil {
		name = *in.DisplayName
	}

	for _, aerr := range []*APIError{
		b.checkCompartment(in.CompartmentID), checkName(name),
		b.checkPolicyFor(in.FilesystemSnapshotPolicyID, in.AvailabilityDomain), checkLocks(in.Locks),
	} {
		if aerr != nil {
			return nil, aerr
		}
	}

	fs := &FileSystem{
		base: b.newBase(kindFS, in.tagsIn, in.Locks), CompartmentID: in.CompartmentID, DisplayName: name,
		AvailabilityDomain: in.AvailabilityDomain, KmsKeyID: in.KmsKeyID, IsTargetable: true,
		FilesystemSnapshotPolicyID: in.FilesystemSnapshotPolicyID, QuotaEnforcementState: quotaDisabled,
		CompartmentQuotaEnforcementState: notBlocked, AreQuotaRulesEnabled: deref(in.AreQuotaRulesEnabled, false),
	}

	if fs.AreQuotaRulesEnabled {
		fs.QuotaEnforcementState = quotaEnabled
	}

	switch {
	case in.SourceSnapshotID != nil && *in.SourceSnapshotID != "":
		if aerr := b.cloneInto(fs, *in.SourceSnapshotID, in.CloneAttachStatus); aerr != nil {
			return nil, aerr
		}
	case in.CloneAttachStatus != "":
		return nil, errParam("cloneAttachStatus applies only when sourceSnapshotId is given")
	}

	return fs, nil
}

// cloneInto turns fs into a clone of the snapshot and copies the data.
func (b *InMemoryBackend) cloneInto(fs *FileSystem, snapID, attach string) *APIError {
	if attach != "" && attach != "ATTACH" && attach != "DETACH" {
		return errParam("cloneAttachStatus must be ATTACH or DETACH")
	}

	sn, aerr := b.snaps.usable(snapID)
	if aerr != nil {
		return aerr
	}

	if sn.W.State != stActive {
		return errState("The snapshot " + snapID + " is in state " + sn.W.State)
	}

	parent, aerr := b.fss.usable(sn.W.FileSystemID)
	if aerr != nil {
		return aerr
	}

	if parent.W.AvailabilityDomain != fs.AvailabilityDomain {
		return errParam("A clone must be created in the availability domain of its source file system")
	}

	if b.dp != nil {
		if err := b.dp.CloneFrom(parent.W.ID, sn.W.Name, fs.ID); err != nil {
			return internalErr(err)
		}
	}

	parent.W.IsCloneParent = true
	parent.W.CloneCount++
	parent.Etag = newEtag()
	sn.W.IsCloneSource = true
	sn.Etag = newEtag()

	hydrated := b.transition <= 0
	at := b.nowUTC().Add(hydrationSteps * b.transition)
	fs.SourceDetails = &sourceDetails{ParentFileSystemID: parent.W.ID, SourceSnapshotID: snapID}
	fs.CloneAttachStatus, fs.IsHydrated = cloneAttached, &hydrated
	fs.Int = &fsInt{HydratesAt: &at, DetachOnHydrate: attach == "DETACH"}

	if hydrated {
		fs.Int.HydratesAt = nil
		b.finishHydration(fs)
	}

	return nil
}

func (b *InMemoryBackend) finishHydration(fs *FileSystem) {
	yes := true
	fs.IsHydrated = &yes

	if fs.Int != nil && fs.Int.DetachOnHydrate {
		fs.Int.DetachOnHydrate = false
		b.detachNow(fs)
	}
}

func (b *InMemoryBackend) detachNow(fs *FileSystem) {
	fs.CloneAttachStatus = cloneDetached

	if fs.SourceDetails == nil {
		return
	}

	if p, ok := b.fss[fs.SourceDetails.ParentFileSystemID]; ok {
		p.W.CloneCount = max(0, p.W.CloneCount-1)
		p.Etag = newEtag()
	}
}

// sweepFileSystems finishes hydration and clone detachment and enforces policy snapshots' parents.
func (b *InMemoryBackend) sweepFileSystems(now time.Time) {
	for _, e := range b.fss {
		in := e.W.Int
		if in == nil {
			continue
		}

		if in.HydratesAt != nil && !in.HydratesAt.After(now) {
			in.HydratesAt = nil
			b.finishHydration(e.W)
			e.Etag = newEtag()
		}

		if in.DetachAt != nil && !in.DetachAt.After(now) {
			in.DetachAt = nil
			b.detachNow(e.W)
			e.Etag = newEtag()
		}
	}
}

// GetFileSystem returns one file system.
func (b *InMemoryBackend) GetFileSystem(id string) (FileSystem, string, error) {
	b.mu.RLock("GetFileSystem")
	defer b.mu.RUnlock()

	e, aerr := b.fss.get(id)
	if aerr != nil {
		return FileSystem{}, "", aerr
	}

	return b.fsView(e), e.Etag, nil
}

// ListFileSystems lists the file systems of a compartment and availability domain.
func (b *InMemoryBackend) ListFileSystems(f FSFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListFileSystems")
	defer b.mu.RUnlock()

	scoped := MTFilter{Compartment: f.Compartment, AD: f.AD, State: f.State, DisplayName: f.DisplayName, ID: f.ID}

	return listScoped(b, b.fss, scoped, o, scope[*FileSystem]{
		fields: func(w *FileSystem) (string, string, string) {
			return w.CompartmentID, w.AvailabilityDomain, w.DisplayName
		},
		extra:  f.matches,
		render: func(e *entry[*FileSystem]) map[string]any { return fsSummary(b.fsView(e)) },
	})
}

func (f FSFilter) matches(w *FileSystem) bool {
	sd := w.SourceDetails

	return (f.Policy == "" || w.FilesystemSnapshotPolicyID == f.Policy) &&
		(f.SourceSnapshot == "" || sd != nil && sd.SourceSnapshotID == f.SourceSnapshot) &&
		(f.Parent == "" || sd != nil && sd.ParentFileSystemID == f.Parent)
}

// UpdateFileSystem changes a file system's name, tags, key or snapshot policy.
func (b *InMemoryBackend) UpdateFileSystem(
	id, ifMatch string,
	override bool,
	in updateFSIn,
) (FileSystem, string, error) {
	b.mu.Lock("UpdateFileSystem")
	defer b.mu.Unlock()

	e, aerr := b.fss.usable(id)
	if aerr != nil {
		return FileSystem{}, "", aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, override)); aerr != nil {
		return FileSystem{}, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = checkName(*in.DisplayName); aerr != nil {
			return FileSystem{}, "", aerr
		}
	}

	if in.FilesystemSnapshotPolicyID != nil {
		if aerr = b.checkPolicyFor(*in.FilesystemSnapshotPolicyID, e.W.AvailabilityDomain); aerr != nil {
			return FileSystem{}, "", aerr
		}

		e.W.FilesystemSnapshotPolicyID = *in.FilesystemSnapshotPolicyID
	}

	if in.DisplayName != nil {
		e.W.DisplayName = *in.DisplayName
	}

	if in.KmsKeyID != nil {
		e.W.KmsKeyID = *in.KmsKeyID
	}

	applyTags(&e.W.base, in.tagsIn)
	e.Etag = newEtag()

	return b.fsView(e), e.Etag, nil
}

func firstErr(errs ...*APIError) *APIError {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

// DeleteFileSystem deletes a file system together with its snapshots.
func (b *InMemoryBackend) DeleteFileSystem(id, ifMatch string, override, detachChildren bool) error {
	b.mu.Lock("DeleteFileSystem")
	defer b.mu.Unlock()

	e, aerr := b.fss.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(
		checkEtag(e.Etag, ifMatch), guardDelete(&e.W.base, override), b.fsDeletable(e.W, detachChildren, override),
	); aerr != nil {
		return aerr
	}

	b.releaseFileSystem(e.W)
	begin(b, e, stDeleting, stDeleted)

	return nil
}

// releaseFileSystem detaches clones and removes snapshots, quota rules and data. Callers hold the write lock.
func (b *InMemoryBackend) releaseFileSystem(fs *FileSystem) {
	for _, c := range b.fss {
		if c.W.SourceDetails != nil && c.W.SourceDetails.ParentFileSystemID == fs.ID &&
			c.W.CloneAttachStatus == cloneAttached {
			b.detachNow(c.W)
			c.Etag = newEtag()
		}
	}

	for _, sn := range b.snaps {
		if sn.W.FileSystemID == fs.ID && sn.W.State != stDeleted {
			b.dropSnapshotData(sn.W)
			begin(b, sn, stDeleting, stDeleted)
		}
	}

	for qid, q := range b.quotas {
		if q.W.FileSystemID == fs.ID {
			delete(b.quotas, qid)
		}
	}

	if b.dp != nil {
		b.dp.RemoveFS(fs.ID)
	}

	if fs.SourceDetails != nil && fs.CloneAttachStatus == cloneAttached {
		b.detachNow(fs)
	}
}

func (b *InMemoryBackend) fsDeletable(fs *FileSystem, detachChildren, override bool) *APIError {
	for _, x := range b.exps {
		if x.W.FileSystemID == fs.ID && x.W.State != stDeleted && x.W.State != stDeleting {
			return errConflict("The file system " + fs.ID + " is exported; delete its exports first")
		}
	}

	for _, r := range b.reps {
		if (r.W.SourceID == fs.ID || r.W.TargetID == fs.ID) && r.W.State != stDeleted {
			return errConflict("The file system " + fs.ID + " takes part in the replication " + r.W.ID)
		}
	}

	if fs.CloneCount > 0 && !detachChildren {
		return errConflict(
			"The file system " + fs.ID + " has attached clones; set canDetachChildFileSystem to detach them",
		)
	}

	return b.snapshotsDeletable(fs.ID, override)
}

func (b *InMemoryBackend) snapshotsDeletable(fsID string, override bool) *APIError {
	now := b.nowUTC()

	for _, sn := range b.snaps {
		if sn.W.FileSystemID != fsID || sn.W.State == stDeleted {
			continue
		}

		if aerr := guardDelete(&sn.W.base, override); aerr != nil {
			return aerr
		}

		if lockHeld(sn.W, now, override) {
			return errLocked(sn.W.ID)
		}
	}

	return nil
}

// ChangeFileSystemCompartment moves a file system to another compartment.
func (b *InMemoryBackend) ChangeFileSystemCompartment(id, ifMatch, compartment string) error {
	b.mu.Lock("ChangeFileSystemCompartment")
	defer b.mu.Unlock()

	e, aerr := b.fss.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(
		checkEtag(e.Etag, ifMatch),
		guardChange(&e.W.base, false),
		b.checkCompartment(compartment),
	); aerr != nil {
		return aerr
	}

	e.W.CompartmentID = compartment
	e.Etag = newEtag()

	return nil
}

// DetachClone detaches a clone from its parent.
func (b *InMemoryBackend) DetachClone(id, ifMatch string) error {
	b.mu.Lock("DetachClone")
	defer b.mu.Unlock()

	e, aerr := b.fss.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(e.Etag, ifMatch); aerr != nil {
		return aerr
	}

	if e.W.SourceDetails == nil || e.W.CloneAttachStatus != cloneAttached {
		return errState("The file system " + id + " is not an attached clone")
	}

	e.W.CloneAttachStatus = cloneDetaching
	at := b.nowUTC().Add(b.transition)
	e.Etag = newEtag()

	if e.W.Int == nil {
		e.W.Int = &fsInt{}
	}

	if b.transition <= 0 {
		b.detachNow(e.W)

		return nil
	}

	e.W.Int.DetachAt = &at

	return nil
}

type toggleQuotaIn struct {
	AreQuotaRulesEnabled *bool `json:"areQuotaRulesEnabled"`
}

// ToggleQuotaRules enables or disables quota enforcement on a file system.
func (b *InMemoryBackend) ToggleQuotaRules(id, ifMatch string, in toggleQuotaIn) error {
	if in.AreQuotaRulesEnabled == nil {
		return errMissing("areQuotaRulesEnabled is required")
	}

	b.mu.Lock("ToggleQuotaRules")
	defer b.mu.Unlock()

	e, aerr := b.fss.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, false)); aerr != nil {
		return aerr
	}

	e.W.AreQuotaRulesEnabled = *in.AreQuotaRulesEnabled
	e.W.QuotaEnforcementState = quotaEnabled

	if !*in.AreQuotaRulesEnabled {
		e.W.QuotaEnforcementState = quotaDisabled
	}

	e.Etag = newEtag()

	return nil
}

// ReplicationEstimate is the EstimateReplication result.
type ReplicationEstimate struct {
	AllowedTargetRegions              []string `json:"allowedTargetRegions"`
	ChangeRateInMBps                  int      `json:"changeRateInMBps"`
	MinimumSupportedIntervalInMinutes int      `json:"minimumSupportedIntervalInMinutes"`
	EstimatedBaseCopyTimeInMinutes    int      `json:"estimatedBaseCopyTimeInMinutes"`
	IsReplicationSupported            bool     `json:"isReplicationSupported"`
}

const (
	minReplicationMinutes = 15
	defaultChangeRate     = 1
	baseCopyMBps          = 100
	bytesPerMB            = 1 << 20
	secondsPerMinute      = 60
	regionName            = "us-ashburn-1"
)

// EstimateReplication estimates the base copy time of a file system from its metered size.
func (b *InMemoryBackend) EstimateReplication(id string, rate *int) (ReplicationEstimate, error) {
	b.mu.RLock("EstimateReplication")
	defer b.mu.RUnlock()

	e, aerr := b.fss.usable(id)
	if aerr != nil {
		return ReplicationEstimate{}, aerr
	}

	cr := defaultChangeRate
	if rate != nil {
		if *rate < 0 {
			return ReplicationEstimate{}, errParam("changeRateInMBps must not be negative")
		}

		cr = *rate
	}

	mb := b.fsView(e).MeteredBytes / bytesPerMB
	mins := int((mb + baseCopyMBps*secondsPerMinute - 1) / (baseCopyMBps * secondsPerMinute))

	return ReplicationEstimate{
		IsReplicationSupported: true, ChangeRateInMBps: cr, MinimumSupportedIntervalInMinutes: minReplicationMinutes,
		EstimatedBaseCopyTimeInMinutes: mins, AllowedTargetRegions: []string{regionName},
	}, nil
}
