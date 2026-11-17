package azurearm

import (
	"context"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	flexMaxBackups = 100
	flexSweepEvery = time.Minute
	flexTopoPrefix = "topo:"
	flexCfgPrefix  = "cfg:"
)

// flexSeed tells the create job where a restored or replica server gets its data from.
type flexSeed struct {
	srcKey string
	mode   string
	backup flexBackup
}

func (b flexBackup) ref() flexDumpRef {
	return flexDumpRef{File: b.File, SHA256: b.SHA256, Bytes: b.Bytes}
}

// SetDumpDir sets where database dumps (backups and replica seeds) are kept and how large one may grow. An empty
// dir uses a temporary directory that is removed on Close.
func (p *FlexDBProvider) SetDumpDir(dir string, maxBytes int64) {
	p.mu.Lock("SetDumpDir")
	defer p.mu.Unlock()

	p.dumps.close()
	p.dumps = newFlexDumpStore(dir, maxBytes)
}

func (p *FlexDBProvider) dumpStore() *flexDumpStore {
	p.mu.RLock("dumpStore")
	defer p.mu.RUnlock()

	return p.dumps
}

func (p *FlexDBProvider) dumpToStore(
	ctx context.Context,
	eng FlexEngine,
	key string,
	dbs []string,
) (flexDumpRef, error) {
	return p.dumpStore().write(p.flavor.kind, func(w io.Writer) error { return eng.Dump(ctx, key, dbs, w) })
}

func (p *FlexDBProvider) dumpDatabases(rec *flexServer) []string {
	out := make([]string, 0, len(rec.Databases))

	for _, d := range p.userDatabases(rec) {
		out = append(out, d.Name)
	}

	return out
}

func (p *FlexDBProvider) dropFiles(files []string) {
	st := p.dumpStore()

	for _, f := range files {
		st.remove(f)
	}
}

func backupFiles(m map[string]flexBackup) []string {
	var out []string

	for _, b := range m {
		if b.File != "" {
			out = append(out, b.File)
		}
	}

	return out
}

// takeBackup dumps a running server into a new backup. The provider lock is not held while the engine works; the
// server is looked up again before the backup is recorded.
func (p *FlexDBProvider) takeBackup(ctx context.Context, key, name, source string) (flexBackup, error) {
	p.mu.RLock("takeBackup")

	eng := p.engine
	rec, ok := p.res[key]

	if eng == nil || !ok {
		p.mu.RUnlock()

		return flexBackup{}, flexInvalid("The server is not available for a backup.")
	}

	if p.opPending(rec.OpID) || p.stateOf(rec) != flexStateReady {
		err := p.notReady(rec, "backup")
		p.mu.RUnlock()

		return flexBackup{}, err
	}

	created, at, dbs := rec.CreatedAt, p.now().UTC(), p.dumpDatabases(rec)
	b := flexBackup{
		Name: name, Source: source, CompletedAt: at, Databases: maps.Clone(rec.Databases),
		Config: maps.Clone(rec.Config),
	}

	p.mu.RUnlock()

	ref, err := p.dumpToStore(ctx, eng, key, dbs)
	if err != nil {
		return flexBackup{}, flexBackupError(err)
	}

	b.File, b.SHA256, b.Bytes = ref.File, ref.SHA256, ref.Bytes

	if err = p.recordBackup(key, created, b); err != nil {
		p.dropFiles([]string{ref.File})

		return flexBackup{}, err
	}

	return b, nil
}

func (p *FlexDBProvider) recordBackup(key string, created time.Time, b flexBackup) error {
	p.mu.Lock("recordBackup")
	defer p.mu.Unlock()

	rec, ok := p.res[key]
	if !ok || !rec.CreatedAt.Equal(created) {
		return flexInvalid("The server was deleted while the backup ran.")
	}

	k := strings.ToLower(b.Name)

	if _, dup := rec.Backups[k]; dup {
		return flexConflict(flexCodeConflict, "The backup '"+b.Name+"' already exists.")
	}

	if len(rec.Backups) >= flexMaxBackups {
		return flexInvalid("The server already has the maximum of %d backups.", flexMaxBackups)
	}

	if rec.Backups == nil {
		rec.Backups = map[string]flexBackup{}
	}

	rec.Backups[k] = b

	return nil
}

func flexBackupError(err error) error {
	if isFlexDumpTooLarge(err) {
		return flexInvalid("The server's data exceeds the size limit of the emulator's backups.")
	}

	return flexEngineError()
}

// attachDump stores a dump of the server's present state with the backup name, which was recorded without data.
func (p *FlexDBProvider) attachDump(ctx context.Context, key, name string) error {
	p.mu.RLock("attachDump")

	eng, rec := p.engine, p.res[key]
	if eng == nil || rec == nil {
		p.mu.RUnlock()

		return nil
	}

	dbs := p.dumpDatabases(rec)
	p.mu.RUnlock()

	ref, err := p.dumpToStore(ctx, eng, key, dbs)
	if err != nil {
		return err
	}

	p.mu.Lock("attachDump")

	attached := false

	if cur, ok := p.res[key]; ok {
		if b, found := cur.Backups[strings.ToLower(name)]; found && b.File == "" {
			b.File, b.SHA256, b.Bytes = ref.File, ref.SHA256, ref.Bytes
			cur.Backups[strings.ToLower(name)] = b
			attached = true
		}
	}

	p.mu.Unlock()

	if !attached {
		p.dropFiles([]string{ref.File})
	}

	return nil
}

func (p *FlexDBProvider) dueForBackup(rec *flexServer, now time.Time) bool {
	if p.opPending(rec.OpID) || p.stateOf(rec) != flexStateReady {
		return false
	}

	var newest time.Time

	for _, b := range rec.Backups {
		if b.Source == flexBackupAuto && b.CompletedAt.After(newest) {
			newest = b.CompletedAt
		}
	}

	return now.Sub(newest) >= flexBackupHours*time.Hour
}

func (p *FlexDBProvider) expireBackups(rec *flexServer, now time.Time) []string {
	cutoff := now.AddDate(0, 0, -rec.Retention)

	var files []string

	for k, b := range rec.Backups {
		if b.CompletedAt.Before(cutoff) {
			if b.File != "" {
				files = append(files, b.File)
			}

			delete(rec.Backups, k)
		}
	}

	return files
}

// RunBackupCycle removes backups past their retention and takes the daily automatic backup of every running server
// that has none from the last 24 hours, all on the provider's clock.
func (p *FlexDBProvider) RunBackupCycle(ctx context.Context) {
	p.mu.Lock("RunBackupCycle")

	now, eng := p.now(), p.engine

	var due []string

	files := make([]string, 0, len(p.res))

	for key, rec := range p.res {
		files = append(files, p.expireBackups(rec, now)...)

		if eng != nil && p.dueForBackup(rec, now) {
			due = append(due, key)
		}
	}

	p.mu.Unlock()

	p.dropFiles(files)
	slices.Sort(due)

	for _, key := range due {
		if _, err := p.takeBackup(ctx, key, flexAutoBackup(now).Name, flexBackupAuto); err != nil && ctx.Err() == nil {
			logger.Load(ctx).WarnContext(ctx, "azurearm: automatic flexible server backup failed", "server", key)
		}
	}
}

func (p *FlexDBProvider) startJanitor() {
	p.janitor.Do(func() {
		p.wg.Go(func() {
			t := time.NewTicker(flexSweepEvery)
			defer t.Stop()

			for {
				select {
				case <-p.ctx.Done():
					return
				case <-t.C:
					p.RunBackupCycle(p.ctx)
				}
			}
		})
	})
}

// chooseBackup picks the newest backup with stored data completed at or before at (any, when at is zero).
func (p *FlexDBProvider) chooseBackup(src *flexServer, at time.Time) (flexBackup, bool) {
	var best flexBackup

	found := false

	for _, b := range p.liveBackups(src) {
		if b.File == "" || (!at.IsZero() && b.CompletedAt.After(at)) || !p.dumps.exists(b.File) {
			continue
		}

		if !found || b.CompletedAt.After(best.CompletedAt) {
			best, found = b, true
		}
	}

	return best, found
}

func chain(steps ...func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, s := range steps {
			if err := s(ctx); err != nil {
				return err
			}
		}

		return nil
	}
}

func (p *FlexDBProvider) createJob(
	eng FlexEngine, key string, spec FlexSpec, rec *flexServer, pw string,
) func(context.Context) error {
	dbs, cfg, seed, login := p.userDatabases(rec), maps.Clone(rec.Config), rec.Seed, rec.Login

	var first string

	for _, b := range rec.Backups {
		first = b.Name
	}

	return func(ctx context.Context) error {
		if _, err := eng.Ensure(ctx, key, spec); err != nil {
			return err //nolint:wrapcheck // engine errors are reduced to a fixed message by the operation
		}

		if seed == nil {
			if err := p.createEngineDatabases(ctx, eng, key, dbs); err != nil {
				return err
			}

			if err := p.attachDump(ctx, key, first); err != nil && ctx.Err() == nil {
				logger.Load(ctx).WarnContext(ctx, "azurearm: first flexible server backup has no data", "server", key)
			}

			return nil
		}

		if err := p.seedServer(ctx, eng, key, login, pw, dbs, cfg, seed); err != nil {
			_ = eng.Delete(context.WithoutCancel(ctx), key)

			return flexSeedError(err)
		}

		return nil
	}
}

func flexSeedError(err error) error {
	return &flexError{
		redisCodeUnavailable, "The server could not be seeded with the source data and was not created: " +
			flexSeedReason(err), 503,
	}
}

func flexSeedReason(err error) string {
	switch {
	case isFlexDumpCorrupt(err):
		return "the stored backup failed its checksum."
	case isFlexDumpTooLarge(err):
		return "the data exceeds the size limit of the emulator."
	}

	return "the database engine refused the load."
}

func (p *FlexDBProvider) seedServer(
	ctx context.Context,
	eng FlexEngine,
	key, login, pw string,
	dbs []flexDatabase,
	cfg map[string]string,
	seed *flexSeed,
) error {
	if err := eng.CopyAdmin(ctx, seed.srcKey, key); err != nil {
		return err //nolint:wrapcheck // reduced to a fixed message by the operation
	}

	if pw != "" {
		if err := eng.SetPassword(ctx, key, login, pw); err != nil {
			return err //nolint:wrapcheck // as above
		}
	}

	if err := p.createEngineDatabases(ctx, eng, key, dbs); err != nil {
		return err
	}

	if err := p.loadSeed(ctx, eng, key, seed); err != nil {
		return err
	}

	if err := p.pushStored(ctx, eng, key, cfg); err != nil {
		return err
	}

	if seed.mode == flexModeReplica {
		return eng.SetReadOnly(ctx, key, true) //nolint:wrapcheck // as above
	}

	return nil
}

func (p *FlexDBProvider) loadSeed(ctx context.Context, eng FlexEngine, key string, seed *flexSeed) error {
	ref := seed.backup.ref()

	if seed.mode == flexModeReplica {
		release, err := p.gate.acquire(ctx, flexTopoPrefix+seed.srcKey)
		if err != nil {
			return err
		}
		defer release()

		dbs, ok := p.sourceDatabases(seed.srcKey)
		if !ok {
			return errFlexNoServer
		}

		if ref, err = p.dumpToStore(ctx, eng, seed.srcKey, dbs); err != nil {
			return err
		}
		defer p.dropFiles([]string{ref.File})
	}

	rd, err := p.dumpStore().read(ref)
	if err != nil {
		return err
	}
	defer rd.Close()

	if err = eng.Restore(ctx, key, rd); err != nil {
		return err //nolint:wrapcheck // reduced to a fixed message by the operation
	}

	return rd.Verify()
}

func (p *FlexDBProvider) sourceDatabases(srcKey string) ([]string, bool) {
	p.mu.RLock("sourceDatabases")
	defer p.mu.RUnlock()

	rec, ok := p.res[srcKey]
	if !ok {
		return nil, false
	}

	return p.dumpDatabases(rec), true
}

// pushStored applies the stored parameter overrides to a server that was just loaded and restarts it when one of
// them is static.
func (p *FlexDBProvider) pushStored(ctx context.Context, eng FlexEngine, key string, cfg map[string]string) error {
	var (
		params []FlexParam
		static bool
	)

	for _, name := range slices.Sorted(maps.Keys(cfg)) {
		def, ok := p.flavor.config(name)
		if !ok || def.engineless {
			continue
		}

		params = append(params, FlexParam{Name: def.name, Value: cfg[name], Static: def.static})
		static = static || def.static
	}

	if len(params) == 0 {
		return nil
	}

	if err := eng.ApplyParameters(ctx, key, params); err != nil {
		return err //nolint:wrapcheck // reduced to a fixed message by the operation
	}

	if static {
		return eng.Restart(ctx, key) //nolint:wrapcheck // as above
	}

	return nil
}

func (p *FlexDBProvider) promoteJob(eng FlexEngine, key, srcKey string) func(context.Context) error {
	return func(ctx context.Context) error {
		release, err := p.gate.acquire(ctx, flexTopoPrefix+srcKey)
		if err != nil {
			return err
		}
		defer release()

		return eng.SetReadOnly(ctx, key, false) //nolint:wrapcheck // reduced to a fixed message by the operation
	}
}
