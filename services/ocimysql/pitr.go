package ocimysql

import (
	"strings"
	"time"
)

// pitrPlan is the recovery a new DB system is created from.
type pitrPlan struct {
	at      time.Time
	srcID   string
	admin   string
	pass    string
	version string
	gbs     int
}

// pitrWindow is the span of recovery points of a DB system with PITR on: from the later of the PITR enablement and
// the start of the backup retention period to now, or to the stop/deletion time while the system is not running.
func (b *InMemoryBackend) pitrWindow(s *DBSystem) (time.Time, time.Time) {
	now := b.nowUTC()
	latest := now

	if s.PitrFreeze != nil {
		latest = *s.PitrFreeze
	}

	earliest := s.TimeCreated
	if s.PitrBase != nil {
		earliest = *s.PitrBase
	}

	if floor := latest.AddDate(0, 0, -s.BackupPolicy.RetentionInDays); earliest.Before(floor) {
		earliest = floor
	}

	return earliest, latest
}

func validatePitrPolicy(shape string, p BackupPolicy) *APIError {
	switch {
	case !p.PitrEnabled:
		return nil
	case shape == shapeFree:
		return errParam("point-in-time recovery is not supported on " + shapeFree)
	case !p.IsEnabled:
		return errParam("backupPolicy.pitrPolicy requires automatic backups (backupPolicy.isEnabled)")
	}

	return nil
}

// resolveSource validates the create source and, for PITR, the recovery it asks for.
func (b *InMemoryBackend) resolveSource(in *CreateDBSystemIn) (*Source, *Backup, *pitrPlan, *APIError) {
	if in.Source == nil || !strings.EqualFold(in.Source.SourceType, sourcePitr) {
		src, bk, aerr := b.checkSource(in.Source)

		return src, bk, nil, aerr
	}

	pp, aerr := b.checkPitrSource(in)
	if aerr != nil {
		return nil, nil, nil, aerr
	}

	src := &Source{SourceType: sourcePitr, DBSystemID: pp.srcID, RecoveryPoint: in.Source.RecoveryPoint}

	if in.MysqlVersion == "" {
		in.MysqlVersion = pp.version
	}

	if in.DataStorageGBs == nil {
		in.DataStorageGBs = &pp.gbs
	}

	return src, nil, pp, nil
}

func (b *InMemoryBackend) checkPitrSource(in *CreateDBSystemIn) (*pitrPlan, *APIError) {
	if in.Source.DBSystemID == "" {
		return nil, errMissing("source.dbSystemId is required for sourceType PITR")
	}

	b.settleID(in.Source.DBSystemID)

	s, ok := b.systems[in.Source.DBSystemID]
	if !ok {
		return nil, errParam("source.dbSystemId " + in.Source.DBSystemID + " does not reference an existing DB system")
	}

	if aerr := b.pitrSourceReady(s); aerr != nil {
		return nil, aerr
	}

	earliest, latest := b.pitrWindow(s)
	at := latest

	if rp := in.Source.RecoveryPoint; rp != nil {
		at = rp.UTC()
	}

	if at.Before(earliest.Truncate(time.Second)) || at.After(latest) {
		return nil, errParam("source.recoveryPoint must be between " + earliest.Format(time.RFC3339) + " and " +
			latest.Format(time.RFC3339))
	}

	if in.MysqlVersion != "" && isKnownVersion(in.MysqlVersion) && versionBefore(in.MysqlVersion, s.Version) {
		return nil, errParam("mysqlVersion cannot be lower than the source's " + s.Version)
	}

	return &pitrPlan{at: at, srcID: s.ID, admin: s.AdminUser, pass: s.AdminPass, version: s.Version, gbs: s.DataGB}, nil
}

// pitrSourceReady says why s cannot be recovered from, if it cannot.
func (b *InMemoryBackend) pitrSourceReady(s *DBSystem) *APIError {
	switch {
	case !s.BackupPolicy.PitrEnabled:
		return errParam("DB system " + s.ID + " does not have point-in-time recovery enabled")
	case s.State == stateDeleted && s.Deletion.AutomaticBackupRetention != retainBackups:
		return errParam("DB system " + s.ID + " was deleted without retaining its automatic backups")
	case s.State == stateDeleted && b.eng != nil && b.eng.retained[s.ID] == nil:
		return errParam("DB system " + s.ID + " was deleted and its retained recovery data has expired")
	case s.State == stateDeleted:
		return nil
	case s.State != stateActive && s.State != stateInactive:
		return errState("DB system " + s.ID + " is in state " + s.State + "; recovery requires ACTIVE or INACTIVE")
	}

	return nil
}

func (b *InMemoryBackend) freezePitr(s *DBSystem) {
	if s.BackupPolicy.PitrEnabled {
		t := b.nowUTC()
		s.PitrFreeze = &t
	}
}

func (b *InMemoryBackend) pitrToggled(cur, n *DBSystem) bool {
	return cur.BackupPolicy.PitrEnabled != n.BackupPolicy.PitrEnabled
}

// applyPitrState records when PITR was switched on or off; the engine reports the real base time later.
func (b *InMemoryBackend) applyPitrState(n *DBSystem) {
	if !n.BackupPolicy.PitrEnabled {
		n.PitrBase, n.PitrFreeze = nil, nil

		return
	}

	t := b.nowUTC()
	n.PitrBase = &t
}
