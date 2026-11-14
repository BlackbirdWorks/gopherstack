package ocimysql

import (
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maintManual    = "MANUAL"
	maintShape     = "SHAPE"
	maintDatabase  = "DATABASE"
	maintHardware  = "HARDWARE"
	maintSucceeded = "SUCCEEDED"
	scopeAll       = "ALL"
	scopeNonPrim   = "ALL_BUT_PRIMARY"
	scopePrimary   = "PRIMARY_ONLY"
)

// MaintenanceEvent records an upgrade or shape change the service performed on a DB system.
type MaintenanceEvent struct {
	TimeCreated   time.Time  `json:"timeCreated"`
	TimeStarted   *time.Time `json:"timeStarted,omitempty"`
	TimeEnded     *time.Time `json:"timeEnded,omitempty"`
	ID            string     `json:"id"`
	DBSystemID    string     `json:"dbSystemId"`
	CompartmentID string     `json:"compartmentId"`
	VersionBefore string     `json:"mysqlVersionBeforeMaintenance"`
	VersionAfter  string     `json:"mysqlVersionAfterMaintenance"`
	Type          string     `json:"maintenanceType"`
	Action        string     `json:"maintenanceAction"`
	Status        string     `json:"maintenanceStatus"`
	Scope         string     `json:"maintenanceScope,omitempty"`
	Notes         string     `json:"maintenanceNotes,omitempty"`
	Seq           int64      `json:"seq"`
}

// ControlledRec is the last controlled update processed on a DB system.
type ControlledRec struct {
	Version string `json:"targetMysqlVersion,omitempty"`
	Targets string `json:"targetDbInstances,omitempty"`
}

// ControlledUpdateIn is the ControlledUpdateDbSystem body.
type ControlledUpdateIn struct {
	TargetMysqlVersion string `json:"targetMysqlVersion"`
	TargetDBInstances  string `json:"targetDbInstances"`
}

func (b *InMemoryBackend) recordMaintenance(s *DBSystem, e MaintenanceEvent, ended *time.Time) {
	now := b.nowUTC()
	e.ID, e.DBSystemID, e.CompartmentID = newOCID("mysqlmaintenanceevent"), s.ID, s.CompartmentID
	e.TimeCreated, e.TimeStarted, e.TimeEnded, e.Status, e.Seq = now, &now, ended, maintSucceeded, b.nextSeq()
	b.events[e.ID] = &e
}

// recordUpdateEvents notes the version upgrade and the shape change an update applied.
func (b *InMemoryBackend) recordUpdateEvents(before, after *DBSystem, ended *time.Time, scope string) {
	if before.Version != after.Version {
		b.recordMaintenance(after, MaintenanceEvent{
			VersionBefore: before.Version, VersionAfter: after.Version, Type: maintManual, Action: maintDatabase,
			Scope: orStr(scope, scopeAll), Notes: "MySQL upgraded from " + before.Version + " to " + after.Version,
		}, ended)
	}

	if before.Shape != after.Shape {
		b.recordMaintenance(after, MaintenanceEvent{
			VersionBefore: before.Version, VersionAfter: after.Version, Type: maintShape, Action: maintHardware,
			Scope: scopeAll, Notes: "Shape changed from " + before.Shape + " to " + after.Shape,
		}, ended)
	}
}

// MaintenanceListIn are the ListMaintenanceEvents filters.
type MaintenanceListIn struct {
	DBSystemID, Before, After, Type, Action, Status string
	ListOpts
}

// ListMaintenanceEvents lists the maintenance events of a DB system.
func (b *InMemoryBackend) ListMaintenanceEvents(in MaintenanceListIn) (page.Page[Doc], error) {
	if aerr := firstErr(
		validListOpts(in.ListOpts, keyTimeCreated, "timeScheduled", "timeStarted", "timeEnded"),
		validEnum("maintenanceType", in.Type, "AUTOMATIC", maintManual, maintShape),
		validEnum("maintenanceAction", in.Action, maintDatabase, "OS_UPDATE", "ONLINE_UPDATE", maintHardware),
		validEnum("maintenanceStatus", in.Status, maintSucceeded, stateFailed, "CANCELED"),
	); aerr != nil {
		return page.Page[Doc]{}, aerr
	}

	b.mu.RLock("ListMaintenanceEvents")
	defer b.mu.RUnlock()

	if s, ok := b.systems[in.DBSystemID]; !ok || s.State == stateDeleted {
		return page.Page[Doc]{}, errNotFound(in.DBSystemID)
	}

	var all []*MaintenanceEvent

	for _, e := range b.events {
		if e.DBSystemID == in.DBSystemID && e.matches(in) {
			all = append(all, e)
		}
	}

	pg := paginate(all, in.ListOpts, func(e *MaintenanceEvent) sortKeys {
		return sortKeys{created: eventTime(e, in.SortBy), seq: e.Seq}
	})

	out := make([]Doc, 0, len(pg.Data))
	for _, e := range pg.Data {
		out = append(out, e.doc())
	}

	return page.Page[Doc]{Data: out, Next: pg.Next}, nil
}

func eventTime(e *MaintenanceEvent, by string) time.Time {
	var t *time.Time

	switch strings.ToLower(by) {
	case "timestarted":
		t = e.TimeStarted
	case "timeended":
		t = e.TimeEnded
	}

	if t == nil {
		return e.TimeCreated
	}

	return *t
}

func (e *MaintenanceEvent) matches(in MaintenanceListIn) bool {
	return (in.Before == "" || e.VersionBefore == in.Before) && (in.After == "" || e.VersionAfter == in.After) &&
		(in.Type == "" || strings.EqualFold(e.Type, in.Type)) &&
		(in.Action == "" || strings.EqualFold(e.Action, in.Action)) &&
		(in.Status == "" || strings.EqualFold(e.Status, in.Status))
}

func (e *MaintenanceEvent) doc() Doc {
	d := Doc{
		paramDBSystem: e.DBSystemID, keyCompartment: e.CompartmentID, keyTimeCreated: e.TimeCreated,
		"mysqlVersionBeforeMaintenance": e.VersionBefore, "mysqlVersionAfterMaintenance": e.VersionAfter,
		"maintenanceType": e.Type, "maintenanceAction": e.Action, "maintenanceStatus": e.Status,
	}

	if e.Notes != "" {
		d["maintenanceNotes"] = e.Notes
	}

	if e.Scope != "" {
		d["maintenanceScope"] = e.Scope
	}

	if e.TimeStarted != nil {
		d["timeStarted"] = *e.TimeStarted
	}

	if e.TimeEnded != nil {
		d["timeEnded"] = *e.TimeEnded
	}

	return d
}

func (b *InMemoryBackend) controlledTarget(s *DBSystem, in ControlledUpdateIn) (string, *APIError) {
	if aerr := validEnum("targetDbInstances", in.TargetDBInstances, scopeNonPrim, scopePrimary); aerr != nil {
		return "", aerr
	}

	switch {
	case !s.HA:
		return "", errParam("a controlled update needs a highly available DB system")
	case in.TargetMysqlVersion != "" && !isKnownVersion(in.TargetMysqlVersion):
		return "", errParam("targetMysqlVersion " + in.TargetMysqlVersion + " is not supported; see ListVersions")
	case in.TargetMysqlVersion != "" && versionBefore(in.TargetMysqlVersion, s.Version):
		return "", errParam("targetMysqlVersion cannot be lower than " + s.Version)
	}

	target := orStr(in.TargetMysqlVersion, newestInFamily(s.Version))
	last := s.Controlled

	if strings.EqualFold(in.TargetDBInstances, scopePrimary) &&
		(last == nil || last.Targets != scopeNonPrim || last.Version != target) {
		return "", errState("PRIMARY_ONLY needs a previous ALL_BUT_PRIMARY update to " + target)
	}

	return target, nil
}

func newestInFamily(version string) string {
	f, ok := familyOf(version)
	if !ok {
		return version
	}

	return f.versions[len(f.versions)-1]
}

// ControlledUpdateDBSystem updates the chosen subset of a highly available DB system's instances.
func (b *InMemoryBackend) ControlledUpdateDBSystem(id, ifMatch, token string, in ControlledUpdateIn) (string, error) {
	b.mu.Lock("ControlledUpdateDbSystem")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("ControlledUpdateDbSystem/"+id, token, b.nowUTC()); ok {
		return v, nil
	}

	s, aerr := b.mutable(id, ifMatch, stateActive)
	if aerr != nil {
		return "", aerr
	}

	target, aerr := b.controlledTarget(s, in)
	if aerr != nil {
		return "", aerr
	}

	scope := strings.ToUpper(in.TargetDBInstances)
	rec := &ControlledRec{Version: target, Targets: scope}

	var (
		wr  string
		err error
	)

	if scope == scopeNonPrim {
		wr = b.controlSecondaries(s, target)
	} else if wr, err = b.updateLocked(id, "", UpdateDBSystemIn{MysqlVersion: &target, scope: scope}); err != nil {
		return "", err
	}

	s.Controlled = rec
	b.retry.Remember("ControlledUpdateDbSystem/"+id, token, wr, b.nowUTC())

	return wr, nil
}

// controlSecondaries records the ALL_BUT_PRIMARY step: the emulator runs one server per DB system, so no secondary
// instance exists to upgrade.
func (b *InMemoryBackend) controlSecondaries(s *DBSystem, target string) string {
	logger.Load(b.runCtx).InfoContext(b.runCtx, "ocimysql: controlled update would upgrade the secondary instances",
		"dbSystem", s.ID, "targetMysqlVersion", target)

	now := b.nowUTC()
	b.recordMaintenance(s, MaintenanceEvent{
		VersionBefore: s.Version,
		VersionAfter:  s.Version,
		Type:          maintManual,
		Action:        maintDatabase,
		Scope:         scopeNonPrim,
		Notes:         "Secondary instances updated to " + target + "; the primary stays on " + s.Version,
	}, &now)

	s.TimeUpdated, s.Etag = now, newEtag()
	wr := b.newWR(wrUpdateDBSystem, s.CompartmentID, entityDBSystem, s.ID, actionUpdated, nil)
	b.finishWR(wr, "")

	return wr
}

func (h *Handler) controlledUpdate(q *req) error {
	var in ControlledUpdateIn
	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	wr, err := h.Backend.ControlledUpdateDBSystem(q.param(paramDBSystem), q.ifMatch(), q.r.Header.Get(headerRetry), in)
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

func (h *Handler) listMaintenanceEvents(q *req) error {
	pg, err := h.Backend.ListMaintenanceEvents(MaintenanceListIn{
		DBSystemID: q.param(paramDBSystem), Before: q.query("mysqlVersionBeforeMaintenance"),
		After: q.query("mysqlVersionAfterMaintenance"), Type: q.query("maintenanceType"),
		Action: q.query("maintenanceAction"), Status: q.query("maintenanceStatus"), ListOpts: q.listOpts(),
	})

	return collectionReply(q, pg, err)
}
