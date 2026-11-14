package ocifilestorage

import (
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	snapUser        = "USER"
	snapPolicy      = "POLICY_BASED"
	snapReplication = "REPLICATION"
	modeGovernance  = "GOVERNANCE"
	modeCompliance  = "COMPLIANCE"
	maxLockDays     = 36500
	maxCoolOffDays  = 365
	defaultCoolOff  = 14
	hoursPerDay     = 24
	maxSnapNameLen  = 255
)

type createSnapshotIn struct {
	tagsIn
	ExpirationTime      *time.Time    `json:"expirationTime"`
	LockDurationDetails *lockDuration `json:"lockDurationDetails"`
	FileSystemID        string        `json:"fileSystemId"`
	Name                string        `json:"name"`
	Locks               []lock        `json:"locks"`
}

type updateSnapshotIn struct {
	tagsIn
	ExpirationTime      *time.Time    `json:"expirationTime"`
	LockDurationDetails *lockDuration `json:"lockDurationDetails"`
}

// SnapshotFilter selects snapshots in a list.
type SnapshotFilter struct {
	FileSystem, Policy, Compartment, State, ID string
}

func validSnapshotName(n string) *APIError {
	switch {
	case n == "":
		return errMissing("name is required")
	case len(n) > maxSnapNameLen:
		return errParam("name must be at most 255 bytes long")
	case n == "." || n == "..", strings.ContainsAny(n, "/\x00"):
		return errParam("name must not be . or .. and must not contain a slash")
	}

	return nil
}

func checkLockDuration(d *lockDuration) *APIError {
	if d == nil {
		return nil
	}

	if d.LockMode != modeGovernance && d.LockMode != modeCompliance {
		return errParam("lockMode must be GOVERNANCE or COMPLIANCE")
	}

	if d.LockDuration != nil && (*d.LockDuration < 0 || *d.LockDuration > maxLockDays) {
		return errParam("lockDuration must be between 0 and 36500 days")
	}

	if d.CoolOffDuration != nil && (*d.CoolOffDuration < 0 || *d.CoolOffDuration > maxCoolOffDays) {
		return errParam("coolOffDuration must be between 0 and 365 days")
	}

	if d.CoolOffDuration != nil && d.LockMode != modeCompliance {
		return errParam("coolOffDuration applies only to COMPLIANCE mode")
	}

	return nil
}

func (b *InMemoryBackend) snapView(e *entry[*Snapshot]) Snapshot {
	v := *e.W
	v.base = cloneBase(e.W.base)

	return v
}

func snapSummary(v Snapshot) map[string]any {
	return project(v, "fileSystemId", "id", "lifecycleState", "name", "timeCreated", "locks", "lockDurationDetails",
		"snapshotType", "snapshotTime", "expirationTime", "provenanceId", "isCloneSource", "lifecycleDetails",
		"freeformTags", "definedTags", "systemTags", "exclusiveBytes")
}

// lockHeld reports whether the duration lock still prevents deleting sn at now.
func lockHeld(sn *Snapshot, now time.Time, override bool) bool {
	d := sn.LockDurationDetails
	if d == nil {
		return false
	}

	indefinite := d.LockDuration == nil || *d.LockDuration == 0
	until := sn.TimeCreated.Add(time.Duration(deref(d.LockDuration, 0)) * hoursPerDay * time.Hour)

	if d.LockMode == modeCompliance {
		cool := sn.TimeCreated.Add(time.Duration(deref(d.CoolOffDuration, defaultCoolOff)) * hoursPerDay * time.Hour)
		if now.Before(cool) {
			return false
		}

		return indefinite || now.Before(until)
	}

	return !override && (indefinite || now.Before(until))
}

// CreateSnapshot takes a user snapshot of a file system.
func (b *InMemoryBackend) CreateSnapshot(in createSnapshotIn, token string) (Snapshot, string, error) {
	if in.FileSystemID == "" {
		return Snapshot{}, "", errMissing("fileSystemId is required")
	}

	if aerr := validSnapshotName(in.Name); aerr != nil {
		return Snapshot{}, "", aerr
	}

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	if id := b.retryHit(kindSnapshot, token); id != "" {
		if e, ok := b.snaps[id]; ok {
			return b.snapView(e), e.Etag, nil
		}
	}

	sn, aerr := b.takeSnapshot(in, snapUser, "")
	if aerr != nil {
		return Snapshot{}, "", aerr
	}

	b.remember(kindSnapshot, token, sn.W.ID)

	return b.snapView(sn), sn.Etag, nil
}

// takeSnapshot records and copies a snapshot of any type. Callers hold the write lock.
func (b *InMemoryBackend) takeSnapshot(in createSnapshotIn, typ, policyID string) (*entry[*Snapshot], *APIError) {
	fs, aerr := b.fss.usable(in.FileSystemID)
	if aerr != nil {
		return nil, aerr
	}

	if fs.W.State != stActive {
		return nil, errState("The file system " + fs.W.ID + " is in state " + fs.W.State)
	}

	for _, x := range b.snaps {
		if x.W.FileSystemID == fs.W.ID && x.W.State != stDeleted && x.W.Name == in.Name {
			return nil, errConflict("A snapshot named " + in.Name + " already exists on the file system")
		}
	}

	now := b.nowUTC()
	if in.ExpirationTime != nil && !in.ExpirationTime.After(now) {
		return nil, errParam("expirationTime must be in the future")
	}

	if aerr = checkLockDuration(in.LockDurationDetails); aerr != nil {
		return nil, aerr
	}

	for _, l := range in.Locks {
		if aerr = checkLock(l); aerr != nil {
			return nil, aerr
		}
	}

	sn := &Snapshot{
		base: b.newBase(kindSnapshot, in.tagsIn, in.Locks), FileSystemID: fs.W.ID, Name: in.Name, SnapshotType: typ,
		SnapshotTime: now, ExpirationTime: in.ExpirationTime, LockDurationDetails: in.LockDurationDetails,
		FilesystemSnapshotPolicyID: policyID,
	}
	sn.ProvenanceID = sn.ID

	if in.LockDurationDetails != nil {
		locked := now
		sn.TimeLocked = &locked
	}

	if b.dp != nil {
		if err := b.dp.TakeSnapshot(fs.W.ID, in.Name); err != nil {
			return nil, internalErr(err)
		}
	}

	return put(b, b.snaps, sn, stActive), nil
}

// GetSnapshot returns one snapshot.
func (b *InMemoryBackend) GetSnapshot(id string) (Snapshot, string, error) {
	b.mu.RLock("GetSnapshot")
	defer b.mu.RUnlock()

	e, aerr := b.snaps.get(id)
	if aerr != nil {
		return Snapshot{}, "", aerr
	}

	return b.snapView(e), e.Etag, nil
}

// ListSnapshots lists snapshots of a file system or snapshot policy.
func (b *InMemoryBackend) ListSnapshots(f SnapshotFilter, o ListOpts) (page.Page[map[string]any], error) {
	if f.FileSystem == "" && f.Policy == "" && f.ID == "" {
		return page.Page[map[string]any]{}, errMissing("fileSystemId, filesystemSnapshotPolicyId or id is required")
	}

	b.mu.RLock("ListSnapshots")
	defer b.mu.RUnlock()

	keep := func(e *entry[*Snapshot]) bool {
		w := e.W

		switch {
		case f.FileSystem != "" && w.FileSystemID != f.FileSystem,
			f.Policy != "" && w.FilesystemSnapshotPolicyID != f.Policy:
			return false
		case f.State != "" && w.State != f.State, f.ID != "" && w.ID != f.ID:
			return false
		case f.Compartment != "":
			fs, ok := b.fss[w.FileSystemID]

			return ok && fs.W.CompartmentID == f.Compartment
		}

		return true
	}

	return listOf(
		b.snaps,
		keep,
		ListOpts{Page: o.Page, Limit: o.Limit, Order: o.Order},
		func(w *Snapshot) string { return w.Name },
		func(e *entry[*Snapshot]) map[string]any { return snapSummary(b.snapView(e)) },
	), nil
}

// UpdateSnapshot changes a snapshot's tags, expiration or lock duration.
func (b *InMemoryBackend) UpdateSnapshot(id, ifMatch string, in updateSnapshotIn) (Snapshot, string, error) {
	b.mu.Lock("UpdateSnapshot")
	defer b.mu.Unlock()

	e, aerr := b.snaps.usable(id)
	if aerr != nil {
		return Snapshot{}, "", aerr
	}

	if aerr = firstErr(
		checkEtag(e.Etag, ifMatch),
		guardChange(&e.W.base, false),
		checkLockDuration(in.LockDurationDetails),
	); aerr != nil {
		return Snapshot{}, "", aerr
	}

	if e.W.SnapshotType == snapReplication {
		return Snapshot{}, "", errState("Replication snapshots cannot be modified")
	}

	if in.ExpirationTime != nil && !in.ExpirationTime.After(b.nowUTC()) {
		return Snapshot{}, "", errParam("expirationTime must be in the future")
	}

	if in.ExpirationTime != nil {
		e.W.ExpirationTime = in.ExpirationTime
	}

	if in.LockDurationDetails != nil {
		if aerr = b.relock(e.W, in.LockDurationDetails); aerr != nil {
			return Snapshot{}, "", aerr
		}
	}

	applyTags(&e.W.base, in.tagsIn)
	e.Etag = newEtag()

	return b.snapView(e), e.Etag, nil
}

// relock replaces the duration lock; a COMPLIANCE lock past its cool-off can only grow.
func (b *InMemoryBackend) relock(sn *Snapshot, d *lockDuration) *APIError {
	old := sn.LockDurationDetails
	now := b.nowUTC()

	if old != nil && old.LockMode == modeCompliance && lockHeld(sn, now, false) {
		if d.LockMode != modeCompliance || deref(d.LockDuration, 0) < deref(old.LockDuration, 0) ||
			deref(old.LockDuration, 0) == 0 && deref(d.LockDuration, 0) != 0 {
			return errConflict("A COMPLIANCE lock past its cool-off period can only be extended")
		}
	}

	sn.LockDurationDetails = d
	locked := now
	sn.TimeLocked = &locked

	return nil
}

func (b *InMemoryBackend) dropSnapshotData(sn *Snapshot) {
	if b.dp != nil {
		b.dp.DropSnapshot(sn.FileSystemID, sn.Name)
	}
}

// DeleteSnapshot deletes a snapshot unless it is locked.
func (b *InMemoryBackend) DeleteSnapshot(id, ifMatch string, override bool) error {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	e, aerr := b.snaps.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardDelete(&e.W.base, override)); aerr != nil {
		return aerr
	}

	if e.W.SnapshotType == snapReplication {
		return errState("Replication snapshots are managed by the replication")
	}

	if lockHeld(e.W, b.nowUTC(), override) {
		return errLocked(id)
	}

	b.dropSnapshotData(e.W)
	begin(b, e, stDeleting, stDeleted)

	return nil
}

// sweepSnapshots deletes snapshots whose expiration time has passed.
func (b *InMemoryBackend) sweepSnapshots(now time.Time) {
	for _, e := range b.snaps {
		w := e.W
		if w.State != stActive || w.ExpirationTime == nil || w.ExpirationTime.After(now) || lockHeld(w, now, false) {
			continue
		}

		if len(w.Locks) > 0 {
			continue
		}

		b.dropSnapshotData(w)
		begin(b, e, stDeleting, stDeleted)
	}
}
