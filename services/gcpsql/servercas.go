package gcpsql

import (
	"context"
	"net/http"
)

const (
	kindListServerCas    = "sql#instancesListServerCas"
	availabilityRegional = "REGIONAL"
	logGeneralTable      = "MYSQL_GENERAL_TABLE"
	logSlowTable         = "MYSQL_SLOW_TABLE"
)

// ServerCas is the instances.listServerCas response.
type ServerCas struct {
	Kind          string     `json:"kind"`
	ActiveVersion string     `json:"activeVersion,omitempty"`
	Certs         []*SslCert `json:"certs"`
}

func (rec *instRec) serverCas() *ServerCas {
	out := &ServerCas{Kind: kindListServerCas}

	if active := rec.Inst.ServerCaCert; active != nil {
		cp := *active
		out.ActiveVersion = cp.Sha1Fingerprint
		out.Certs = append(out.Certs, &cp)
	}

	for _, c := range []*SslCert{rec.NextCA, rec.PrevCA} {
		if c != nil {
			cp := *c
			out.Certs = append(out.Certs, &cp)
		}
	}

	return out
}

// ListServerCas lists the active server CA, the CA waiting to be rotated in and the CA rotated out.
func (b *InMemoryBackend) ListServerCas(project, instance string) (*ServerCas, error) {
	b.mu.RLock("ListServerCas")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	return rec.serverCas(), nil
}

// AddServerCa prepares a rotation; a CA that was added but never rotated in is replaced.
func (b *InMemoryBackend) AddServerCa(project, instance string) (*Operation, error) {
	pem, key, info, err := newServerCA(project, instance, b.now())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("AddServerCa")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	rec.NextCACert, rec.NextCAKey, rec.NextCA = pem, key, info

	return b.submitLocked(rec, project, "UPDATE", opMeta{}, func(context.Context) error { return nil }), nil
}

// RotateServerCa makes the CA added by AddServerCa the one that signs and is reported as the server CA.
func (b *InMemoryBackend) RotateServerCa(project, instance, nextVersion string) (*Operation, error) {
	b.mu.Lock("RotateServerCa")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	if rec.NextCA == nil {
		return nil, errInvalid("There is no server CA waiting to be rotated in; call addServerCa first.")
	}

	if nextVersion != "" && nextVersion != rec.NextCA.Sha1Fingerprint {
		return nil, errInvalid("The server CA version %q does not exist.", nextVersion)
	}

	rec.PrevCA = rec.Inst.ServerCaCert
	rec.CACert, rec.CAKey, rec.Inst.ServerCaCert = rec.NextCACert, rec.NextCAKey, rec.NextCA
	rec.NextCACert, rec.NextCAKey, rec.NextCA = "", "", nil
	b.touchLocked(rec)

	return b.submitLocked(rec, project, "UPDATE", opMeta{}, func(context.Context) error { return nil }), nil
}

// ResetSslConfig deletes every client certificate and issues a new server CA.
func (b *InMemoryBackend) ResetSslConfig(project, instance string) (*Operation, error) {
	pem, key, info, err := newServerCA(project, instance, b.now())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ResetSslConfig")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	rec.Certs = map[string]*certRec{}
	rec.CACert, rec.CAKey, rec.Inst.ServerCaCert = pem, key, info
	rec.NextCACert, rec.NextCAKey, rec.NextCA, rec.PrevCA = "", "", nil, nil
	b.touchLocked(rec)

	return b.submitLocked(rec, project, "UPDATE", opMeta{}, func(context.Context) error { return nil }), nil
}

// Failover swaps the primary and standby zones of a REGIONAL instance.
func (b *InMemoryBackend) Failover(project, instance string, settingsVersion string) (*Operation, error) {
	b.mu.Lock("Failover")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	regional := settingString(rec.Inst.Settings, "availabilityType") == availabilityRegional
	if !regional || rec.Inst.MasterInstanceName != "" {
		return nil, errInvalid("Failover requires a high availability (REGIONAL) primary instance.")
	}

	if settingsVersion != "" && settingsVersion != settingString(rec.Inst.Settings, "settingsVersion") {
		return nil, newErr(http.StatusPreconditionFailed, "conditionNotMet", "Precondition check failed.")
	}

	return b.submitLocked(rec, project, "FAILOVER", opMeta{}, func(context.Context) error {
		b.mu.Lock("Failover.task")
		defer b.mu.Unlock()

		rec.Inst.GceZone, rec.Inst.SecondaryGceZone = rec.Inst.SecondaryGceZone, rec.Inst.GceZone
		b.touchLocked(rec)

		return nil
	}), nil
}

// TruncateLog empties a MySQL general or slow log table.
func (b *InMemoryBackend) TruncateLog(project, instance, logType string) (*Operation, error) {
	table := ""

	switch logType {
	case logGeneralTable:
		table = "mysql.general_log"
	case logSlowTable:
		table = "mysql.slow_log"
	default:
		return nil, errInvalid("logType must be %s or %s.", logGeneralTable, logSlowTable)
	}

	b.mu.Lock("TruncateLog")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	if kind, _, perr := parseVersion(rec.Inst.DatabaseVersion); perr != nil || kind != kindMySQL {
		return nil, errInvalid("Log tables can only be truncated on MySQL instances.")
	}

	return b.submitLocked(rec, project, "TRUNCATE_LOG", opMeta{}, func(ctx context.Context) error {
		eng := b.engineOf(rec)
		if eng == nil {
			return nil
		}

		_, qerr := eng.Query(ctx, "mysql", "TRUNCATE TABLE "+table)

		return qerr
	}), nil
}

// syncZones keeps the standby zone of a REGIONAL instance in step with its availability type.
func syncZones(rec *instRec) {
	if settingString(rec.Inst.Settings, "availabilityType") != availabilityRegional {
		rec.Inst.SecondaryGceZone = ""

		return
	}

	if z := settingString(asMap(rec.Inst.Settings["locationPreference"]), "secondaryZone"); z != "" {
		rec.Inst.SecondaryGceZone = z

		return
	}

	if rec.Inst.SecondaryGceZone != "" {
		return
	}

	for _, suffix := range []string{"-b", "-c", "-d", "-a"} {
		if z := rec.Inst.Region + suffix; z != rec.Inst.GceZone {
			rec.Inst.SecondaryGceZone = z

			return
		}
	}
}
