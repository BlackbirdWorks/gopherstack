package ocimysql

import (
	"context"
)

const (
	shutdownFast = "FAST"
	shutdownSlow = "SLOW"
)

// serverSettings are the DB system fields the MySQL server itself enforces.
type serverSettings struct {
	databaseMode  string
	accessMode    string
	crashRecovery string
}

func settingsOf(s *DBSystem) serverSettings {
	return serverSettings{databaseMode: s.DatabaseMode, accessMode: s.AccessMode, crashRecovery: s.CrashRecovery}
}

func (v serverSettings) isDefault() bool {
	return v.databaseMode != modeReadOnly && v.accessMode != accessRestricted && v.crashRecovery != disabled
}

func onOff(on bool) string {
	if on {
		return "ON"
	}

	return "OFF"
}

// innodbShutdown maps the SDK's InnoDbShutdownModeEnum to innodb_fast_shutdown (SLOW 0, FAST 1, IMMEDIATE 2).
func innodbShutdown(mode string) string {
	switch mode {
	case shutdownSlow:
		return "0"
	case shutdownFast:
		return "1"
	}

	return "2"
}

// applyServerSettings makes the server follow databaseMode (read_only and super_read_only), accessMode
// (offline_mode, as documented on DbSystemAccessModeEnum) and crashRecovery (redo log, double write buffer and
// binary log syncing, as documented on CrashRecoveryStatusEnum).
func applyServerSettings(ctx context.Context, inst Instance, v serverSettings) error {
	q, ok := inst.(Querier)
	if !ok {
		return nil
	}

	crash := v.crashRecovery != disabled

	sync := 0
	if crash {
		sync = 1
	}

	restart, err := applyVariables(ctx, q, map[string]any{"innodbDoublewrite": crash, "syncBinlog": sync})
	if err != nil {
		return err
	}

	if restart {
		if err = inst.Stop(ctx); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}

		if err = inst.Start(ctx); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	redo := "DISABLE"
	if crash {
		redo = "ENABLE"
	}

	ro := v.databaseMode == modeReadOnly
	stmts := []string{
		"ALTER INSTANCE " + redo + " INNODB REDO_LOG",
		"SET GLOBAL offline_mode = " + onOff(v.accessMode == accessRestricted),
	}

	if ro {
		stmts = append(stmts, "SET GLOBAL read_only = ON", "SET GLOBAL super_read_only = ON")
	} else {
		stmts = append(stmts, "SET GLOBAL super_read_only = OFF", "SET GLOBAL read_only = OFF")
	}

	for _, st := range stmts {
		if _, err = q.Query(ctx, "", st); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	return nil
}

// stopInstance sets the InnoDB shutdown mode before the server stops.
func stopInstance(ctx context.Context, inst Instance, mode string) error {
	if q, ok := inst.(Querier); ok && mode != "" {
		if _, err := q.Query(ctx, "", "SET GLOBAL innodb_fast_shutdown = "+innodbShutdown(mode)); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	return inst.Stop(ctx) //nolint:wrapcheck // engine errors carry the cause
}

// syncSettings applies a DB system's server settings to its running server.
func (b *InMemoryBackend) syncSettings(ctx context.Context, id string, force bool) error {
	b.mu.RLock("syncSettings")
	s, ok := b.systems[id]

	var v serverSettings
	if ok {
		v = settingsOf(s)
	}
	b.mu.RUnlock()

	inst := b.instance(id)
	if !ok || inst == nil || (!force && v.isDefault()) {
		return nil
	}

	return applyServerSettings(ctx, inst, v)
}
