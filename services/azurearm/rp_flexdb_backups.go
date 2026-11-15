package azurearm

import (
	"regexp"
	"strings"
	"time"
)

const (
	flexBackupsType   = "backups"
	flexBackupsV2Type = "backupsV2"
	flexBackupFull    = "FULL"
	flexBackupAuto    = "Automatic"
	flexBackupOnDmd   = "Full"
)

var flexBackupNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

// flexBackup is one restore point of a server: the first automatic backup is taken when the server is created
// (Learn: postgresql backup-restore and mysql concepts-backup-restore), MySQL adds on-demand ones.
type flexBackup struct {
	CompletedAt time.Time `json:"completedAt"`
	Name        string    `json:"name"`
	Source      string    `json:"source"`
}

func flexAutoBackup(at time.Time) flexBackup {
	return flexBackup{Name: "backup_" + at.UTC().Format("20060102T150405Z"), Source: flexBackupAuto, CompletedAt: at}
}

// liveBackups lists the backups still inside the retention window.
func (p *FlexDBProvider) liveBackups(rec *flexServer) []flexBackup {
	out := make([]flexBackup, 0, len(rec.Backups))
	cutoff := p.now().AddDate(0, 0, -rec.Retention)

	for _, b := range rec.Backups {
		if !b.CompletedAt.Before(cutoff) {
			out = append(out, b)
		}
	}

	return out
}

func (p *FlexDBProvider) backupItemBody(id ResourceID, b flexBackup, v2 bool) map[string]any {
	rid := id
	rid.Names = []string{id.Names[0], b.Name}

	props := map[string]any{
		"backupType": flexBackupFull, "completedTime": b.CompletedAt.UTC().Format(time.RFC3339Nano), "source": b.Source,
	}

	typ := flexBackupsType

	if v2 {
		typ = flexBackupsV2Type
		props["backupNameV2"] = b.Name
		props[fieldProvisioningState] = flexStatusOK
	}

	return p.childEnvelope(rid, typ, props)
}

func (p *FlexDBProvider) getBackup(id ResourceID, rec *flexServer, v2 bool) (map[string]any, error) {
	for _, b := range p.liveBackups(rec) {
		if strings.EqualFold(b.Name, id.LeafName()) {
			return p.backupItemBody(id, b, v2), nil
		}
	}

	return nil, p.childNotFound(id)
}

// writeBackup takes an on-demand backup (MySQL backups PUT, answered 200 with the finished backup).
func (p *FlexDBProvider) writeBackup(id ResourceID) (flexResult, error) {
	if p.flavor.kind != flexKindMySQL {
		return flexResult{}, errUnsupportedFlexType(id)
	}

	name := id.LeafName()
	if !flexBackupNameRE.MatchString(name) {
		return flexResult{}, flexInvalid("The backup name '%s' is not valid.", name)
	}

	p.mu.Lock("writeBackup")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return flexResult{}, err
	}

	if p.stateOf(rec) != flexStateReady {
		return flexResult{}, p.notReady(rec, "backup")
	}

	if _, dup := rec.Backups[strings.ToLower(name)]; dup {
		return flexResult{}, flexConflict(flexCodeConflict, "The backup '"+name+"' already exists.")
	}

	b := flexBackup{Name: name, Source: flexBackupOnDmd, CompletedAt: p.now().UTC()}
	if rec.Backups == nil {
		rec.Backups = map[string]flexBackup{}
	}

	rec.Backups[strings.ToLower(name)] = b

	return flexResult{body: p.backupItemBody(id, b, false)}, nil
}
