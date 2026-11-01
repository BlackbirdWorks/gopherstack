package ocifilestorage

import (
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	deltaIdle         = "IDLE"
	deltaCapturing    = "CAPTURING"
	deltaTransferring = "TRANSFERRING"
	deltaApplying     = "APPLYING"
	modeFinishAny     = "FINISH_CYCLE_IF_CAPTURING_OR_APPLYING"
	modeOneMore       = "ONE_MORE_CYCLE"
	modeFinishApply   = "FINISH_CYCLE_IF_APPLYING"
	defaultInterval   = 60
	progressStep      = 33
	progressStepApply = 66
	progressDone      = 100
)

type createReplicationIn struct {
	tagsIn
	DisplayName         *string `json:"displayName"`
	ReplicationInterval *int64  `json:"replicationInterval"`
	CompartmentID       string  `json:"compartmentId"`
	SourceID            string  `json:"sourceId"`
	TargetID            string  `json:"targetId"`
	Locks               []lock  `json:"locks"`
}

type updateReplicationIn struct {
	tagsIn
	DisplayName         *string `json:"displayName"`
	ReplicationInterval *int64  `json:"replicationInterval"`
}

// ReplicationFilter selects replications and replication targets in a list.
type ReplicationFilter struct {
	MTFilter
	FileSystem string
}

func (b *InMemoryBackend) repView(e *entry[*Replication]) Replication {
	v := *e.W
	v.Int, v.base = nil, cloneBase(e.W.base)

	return v
}

func repSummary(v Replication) map[string]any {
	return project(
		v,
		"id",
		"lifecycleState",
		"displayName",
		"timeCreated",
		"availabilityDomain",
		"compartmentId",
		"locks",
		"replicationInterval",
		"freeformTags",
		"definedTags",
		"systemTags",
		"lifecycleDetails",
		"recoveryPointTime",
	)
}

func tgtSummary(v ReplicationTarget) map[string]any {
	return project(v, "id", "lifecycleState", "displayName", "timeCreated", "availabilityDomain", "compartmentId",
		"freeformTags", "definedTags", "systemTags", "lifecycleDetails", "recoveryPointTime")
}

func (b *InMemoryBackend) isReplicationTarget(fsID string) bool {
	for _, r := range b.reps {
		if r.W.TargetID == fsID && r.W.State != stDeleted {
			return true
		}
	}

	return false
}

func checkInterval(v int64) *APIError {
	if v < minReplicationMinutes {
		return errParam("replicationInterval must be at least 15 minutes")
	}

	return nil
}

// CreateReplication replicates a file system into an unused target file system.
func (b *InMemoryBackend) CreateReplication(in createReplicationIn, token string) (Replication, string, error) {
	if in.SourceID == "" || in.TargetID == "" {
		return Replication{}, "", errMissing("sourceId and targetId are required")
	}

	b.mu.Lock("CreateReplication")
	defer b.mu.Unlock()

	if id := b.retryHit(kindRepl, token); id != "" {
		if e, ok := b.reps[id]; ok {
			return b.repView(e), e.Etag, nil
		}
	}

	ends, aerr := b.replicationEnds(in)
	if aerr != nil {
		return Replication{}, "", aerr
	}

	src, dst, interval := ends.src, ends.dst, ends.interval

	for _, l := range in.Locks {
		if aerr = checkLock(l); aerr != nil {
			return Replication{}, "", aerr
		}
	}

	name := "replication-" + b.nowUTC().Format("20060102-150405")
	if in.DisplayName != nil {
		name = *in.DisplayName
	}

	if aerr = checkName(name); aerr != nil {
		return Replication{}, "", aerr
	}

	now := b.nowUTC()
	tgt := &ReplicationTarget{
		base: b.newBase(kindReplTgt, in.tagsIn, nil), CompartmentID: dst.W.CompartmentID, DisplayName: name,
		SourceID: src.W.ID, TargetID: dst.W.ID, AvailabilityDomain: dst.W.AvailabilityDomain, DeltaStatus: deltaIdle,
	}
	rep := &Replication{
		base:                b.newBase(kindRepl, in.tagsIn, in.Locks),
		CompartmentID:       in.CompartmentID,
		DisplayName:         name,
		SourceID:            src.W.ID,
		TargetID:            dst.W.ID,
		ReplicationTargetID: tgt.ID,
		AvailabilityDomain:  src.W.AvailabilityDomain,
		ReplicationInterval: interval,
		DeltaStatus:         deltaIdle,
		Int:                 &repInt{NextCycle: &now},
	}
	tgt.ReplicationID = rep.ID

	dst.W.ReplicationTargetID = tgt.ID
	dst.Etag = newEtag()
	src.W.ReplicationSourceCount++
	src.Etag = newEtag()

	put(b, b.tgts, tgt, stActive)
	e := put(b, b.reps, rep, stActive)
	b.remember(kindRepl, token, rep.ID)

	return b.repView(e), e.Etag, nil
}

type replicationEnds struct {
	src, dst *entry[*FileSystem]
	interval int64
}

func (b *InMemoryBackend) replicationEnds(in createReplicationIn) (replicationEnds, *APIError) {
	var ends replicationEnds

	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return ends, aerr
	}

	if in.SourceID == in.TargetID {
		return ends, errParam("sourceId and targetId must differ")
	}

	var aerr *APIError

	if ends.src, aerr = b.fss.usable(in.SourceID); aerr != nil {
		return ends, aerr
	}

	if ends.dst, aerr = b.fss.usable(in.TargetID); aerr != nil {
		return ends, aerr
	}

	if aerr = b.checkReplicable(ends.src.W, ends.dst.W); aerr != nil {
		return ends, aerr
	}

	ends.interval = deref(in.ReplicationInterval, defaultInterval)

	return ends, checkInterval(ends.interval)
}

func (b *InMemoryBackend) checkReplicable(src, dst *FileSystem) *APIError {
	if src.State != stActive || dst.State != stActive {
		return errState("The source and target file systems must be ACTIVE")
	}

	if !b.targetable(dst.ID) {
		return errConflict("The file system " + dst.ID + " is not targetable: it is exported, " +
			"already a replication target or holds user snapshots")
	}

	if b.isReplicationTarget(src.ID) {
		return errConflict("The source file system is the target of another replication")
	}

	for _, r := range b.reps {
		if r.W.State != stDeleted && r.W.SourceID == dst.ID {
			return errConflict("The target file system is the source of another replication")
		}
	}

	return nil
}

// GetReplication returns one replication.
func (b *InMemoryBackend) GetReplication(id string) (Replication, string, error) {
	b.mu.RLock("GetReplication")
	defer b.mu.RUnlock()

	e, aerr := b.reps.get(id)
	if aerr != nil {
		return Replication{}, "", aerr
	}

	return b.repView(e), e.Etag, nil
}

// ListReplications lists replications of a compartment and availability domain.
func (b *InMemoryBackend) ListReplications(f ReplicationFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListReplications")
	defer b.mu.RUnlock()

	return listScoped(b, b.reps, f.MTFilter, o, scope[*Replication]{
		fields: func(w *Replication) (string, string, string) {
			return w.CompartmentID, w.AvailabilityDomain, w.DisplayName
		},
		extra: func(w *Replication) bool {
			return f.FileSystem == "" || w.SourceID == f.FileSystem || w.TargetID == f.FileSystem
		},
		render: func(e *entry[*Replication]) map[string]any { return repSummary(b.repView(e)) },
	})
}

// UpdateReplication changes a replication's name, interval and tags.
func (b *InMemoryBackend) UpdateReplication(
	id, ifMatch string,
	override bool,
	in updateReplicationIn,
) (Replication, string, error) {
	b.mu.Lock("UpdateReplication")
	defer b.mu.Unlock()

	e, aerr := b.reps.usable(id)
	if aerr != nil {
		return Replication{}, "", aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, override)); aerr != nil {
		return Replication{}, "", aerr
	}

	if in.ReplicationInterval != nil {
		if aerr = checkInterval(*in.ReplicationInterval); aerr != nil {
			return Replication{}, "", aerr
		}
	}

	if in.DisplayName != nil {
		if aerr = checkName(*in.DisplayName); aerr != nil {
			return Replication{}, "", aerr
		}

		e.W.DisplayName = *in.DisplayName

		if t, ok := b.tgts[e.W.ReplicationTargetID]; ok {
			t.W.DisplayName = *in.DisplayName
			t.Etag = newEtag()
		}
	}

	if in.ReplicationInterval != nil {
		e.W.ReplicationInterval = *in.ReplicationInterval
	}

	applyTags(&e.W.base, in.tagsIn)
	e.Etag = newEtag()

	return b.repView(e), e.Etag, nil
}

// DeleteReplication deletes a replication according to the delete mode.
func (b *InMemoryBackend) DeleteReplication(id, ifMatch, mode string, override bool) error {
	if mode != "" && mode != modeFinishAny && mode != modeOneMore && mode != modeFinishApply {
		return errParam(
			"deleteMode must be FINISH_CYCLE_IF_CAPTURING_OR_APPLYING, ONE_MORE_CYCLE or FINISH_CYCLE_IF_APPLYING",
		)
	}

	b.mu.Lock("DeleteReplication")
	defer b.mu.Unlock()

	e, aerr := b.reps.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardDelete(&e.W.base, override)); aerr != nil {
		return aerr
	}

	e.W.State, e.Etag = stDeleting, newEtag()
	idle := e.W.DeltaStatus == deltaIdle

	switch {
	case mode == modeOneMore:
		e.W.Int.FinalPass = true

		if idle {
			now := b.nowUTC()
			e.W.Int.NextCycle = &now
		}
	case idle, mode == modeFinishApply && e.W.DeltaStatus != deltaApplying:
		b.finishReplicationDelete(e)
	default:
		e.W.Int.FinalPass = true
	}

	return nil
}

func (b *InMemoryBackend) finishReplicationDelete(e *entry[*Replication]) {
	r := e.W

	if dst, ok := b.fss[r.TargetID]; ok {
		dst.W.ReplicationTargetID = ""
		dst.Etag = newEtag()
	}

	if src, ok := b.fss[r.SourceID]; ok {
		src.W.ReplicationSourceCount = max(0, src.W.ReplicationSourceCount-1)
		src.Etag = newEtag()
	}

	for _, sn := range b.snaps {
		if sn.W.SnapshotType == snapReplication && sn.W.FileSystemID == r.SourceID && sn.W.State == stActive {
			b.dropSnapshotData(sn.W)
			begin(b, sn, stDeleting, stDeleted)
		}
	}

	if t, ok := b.tgts[r.ReplicationTargetID]; ok && t.W.State != stDeleted {
		begin(b, t, stDeleting, stDeleted)
	}

	r.Int.NextCycle, r.Int.PhaseAt, r.Int.FinalPass = nil, nil, false
	begin(b, e, stDeleting, stDeleted)
}

// ChangeReplicationCompartment moves a replication.
func (b *InMemoryBackend) ChangeReplicationCompartment(id, ifMatch, compartment string) error {
	b.mu.Lock("ChangeReplicationCompartment")
	defer b.mu.Unlock()

	e, aerr := b.reps.usable(id)
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

// GetReplicationTarget returns one replication target.
func (b *InMemoryBackend) GetReplicationTarget(id string) (ReplicationTarget, string, error) {
	b.mu.RLock("GetReplicationTarget")
	defer b.mu.RUnlock()

	e, aerr := b.tgts.get(id)
	if aerr != nil {
		return ReplicationTarget{}, "", aerr
	}

	v := *e.W
	v.base = cloneBase(e.W.base)

	return v, e.Etag, nil
}

// ListReplicationTargets lists replication targets of a compartment and availability domain.
func (b *InMemoryBackend) ListReplicationTargets(f MTFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListReplicationTargets")
	defer b.mu.RUnlock()

	return listScoped(b, b.tgts, f, o, scope[*ReplicationTarget]{
		fields: func(w *ReplicationTarget) (string, string, string) {
			return w.CompartmentID, w.AvailabilityDomain, w.DisplayName
		},
		render: func(e *entry[*ReplicationTarget]) map[string]any {
			v := *e.W
			v.base = cloneBase(e.W.base)

			return tgtSummary(v)
		},
	})
}

// DeleteReplicationTarget deletes a replication target whose replication is gone.
func (b *InMemoryBackend) DeleteReplicationTarget(id, ifMatch string) error {
	b.mu.Lock("DeleteReplicationTarget")
	defer b.mu.Unlock()

	e, aerr := b.tgts.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(e.Etag, ifMatch); aerr != nil {
		return aerr
	}

	if r, ok := b.reps[e.W.ReplicationID]; ok && r.W.State != stDeleted {
		return errConflict(
			"The replication target " + id + " belongs to the replication " + r.W.ID + "; delete the replication instead",
		)
	}

	begin(b, e, stDeleting, stDeleted)

	return nil
}

// sweepReplications advances replication cycles on the backend clock.
func (b *InMemoryBackend) sweepReplications(now time.Time) {
	for _, e := range b.reps {
		r := e.W
		if (r.State != stActive && r.State != stDeleting) || r.Int == nil {
			continue
		}

		switch {
		case r.DeltaStatus != deltaIdle:
			if r.Int.PhaseAt != nil && !r.Int.PhaseAt.After(now) {
				b.advanceCycle(e, now)
			}
		case r.Int.NextCycle != nil && !r.Int.NextCycle.After(now):
			b.startCycle(e, now)
		}
	}
}

func (b *InMemoryBackend) startCycle(e *entry[*Replication], now time.Time) {
	r := e.W
	r.Int.Number++
	name := "replication-snapshot-" + strconv.Itoa(r.Int.Number) + "-" + now.UTC().Format("20060102150405")

	_, aerr := b.takeSnapshot(createSnapshotIn{FileSystemID: r.SourceID, Name: name}, snapReplication, "")
	if aerr != nil {
		r.LifecycleDetails = aerr.Message
		next := now.Add(minutes(r.ReplicationInterval))
		r.Int.NextCycle = &next

		return
	}

	r.LifecycleDetails = ""
	r.Int.NextCycle = nil
	b.setDelta(e, deltaCapturing, 0, now)
}

func (b *InMemoryBackend) setDelta(e *entry[*Replication], status string, progress int64, now time.Time) {
	r := e.W
	r.DeltaStatus, r.DeltaProgress = status, &progress
	at := now.Add(b.transition)
	r.Int.PhaseAt = &at
	e.Etag = newEtag()

	if t, ok := b.tgts[r.ReplicationTargetID]; ok {
		t.W.DeltaStatus, t.W.DeltaProgress = status, &progress
		t.Etag = newEtag()
	}
}

func (b *InMemoryBackend) advanceCycle(e *entry[*Replication], now time.Time) {
	r := e.W

	switch r.DeltaStatus {
	case deltaCapturing:
		b.setDelta(e, deltaTransferring, progressStep, now)
	case deltaTransferring:
		b.setDelta(e, deltaApplying, progressStepApply, now)
	case deltaApplying:
		b.completeCycle(e, now)
	}
}

func (b *InMemoryBackend) completeCycle(e *entry[*Replication], now time.Time) {
	r := e.W

	var latest *entry[*Snapshot]

	for _, sn := range b.snaps {
		if sn.W.SnapshotType == snapReplication && sn.W.FileSystemID == r.SourceID && sn.W.State == stActive &&
			(latest == nil || sn.Seq > latest.Seq) {
			latest = sn
		}
	}

	if latest != nil {
		b.applyReplicated(e, latest, now)
	}

	done := int64(progressDone)
	r.DeltaStatus, r.DeltaProgress, r.Int.PhaseAt = deltaIdle, &done, nil
	next := now.Add(minutes(r.ReplicationInterval))
	r.Int.NextCycle = &next
	e.Etag = newEtag()

	if t, ok := b.tgts[r.ReplicationTargetID]; ok {
		t.W.DeltaStatus, t.W.DeltaProgress = deltaIdle, &done
		t.W.LastSnapshotID, t.W.RecoveryPointTime = r.LastSnapshotID, r.RecoveryPointTime
		t.Etag = newEtag()
	}

	if r.State == stDeleting {
		b.finishReplicationDelete(e)
	}
}

func (b *InMemoryBackend) applyReplicated(e *entry[*Replication], latest *entry[*Snapshot], now time.Time) {
	r := e.W

	if b.dp != nil {
		if err := b.dp.ReplicateSnapshot(r.SourceID, latest.W.Name, r.TargetID); err != nil {
			r.LifecycleDetails = "data copy failed: " + err.Error()
			next := now.Add(minutes(r.ReplicationInterval))
			r.Int.NextCycle = &next

			return
		}
	}

	for _, sn := range b.snaps {
		if sn != latest && sn.W.SnapshotType == snapReplication && sn.W.FileSystemID == r.SourceID &&
			sn.W.State == stActive {
			b.dropSnapshotData(sn.W)
			begin(b, sn, stDeleting, stDeleted)
		}
	}

	at := latest.W.SnapshotTime
	r.LastSnapshotID, r.RecoveryPointTime = latest.W.ID, &at
}
