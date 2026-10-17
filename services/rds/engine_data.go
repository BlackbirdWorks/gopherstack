package rds

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	dumpChunkChars = 60000

	cmdEnv           = "env"
	snapKindInstance = "i:"
	snapKindCluster  = "c:"
)

var (
	errEngineExec = errors.New("engine command failed")
	errDumpDBName = errors.New("database name is not dumpable")
	errSeedSource = errors.New("seed source unavailable")

	safeDBName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_$]{0,63}$`)
)

// EngineExecRuntime is the optional runtime capability used to dump and load database contents.
type EngineExecRuntime interface {
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// engineSeed describes data a new unit loads once its engine is up.
type engineSeed struct {
	dump     string
	fromKey  string
	password string
	dbName   string
}

func (s engineSeed) empty() bool { return s.dump == "" && s.fromKey == "" }

// engineData is the logical content captured with a snapshot.
type engineData struct {
	dump     string
	password string
	dbName   string
	captured bool
}

type unitInfo struct {
	login       EngineLogin
	containerID string
}

func snapDataKey(kind, snapshotID string) string { return kind + normalizeID(snapshotID) }

func (e *dbEngine) execer() (EngineExecRuntime, bool) {
	x, ok := e.cfg.Runtime.(EngineExecRuntime)

	return x, ok
}

func (e *dbEngine) run(ctx context.Context, x EngineExecRuntime, id string, cmd []string) (string, error) {
	res, err := x.Exec(ctx, id, cmd)
	if err != nil {
		return "", fmt.Errorf("exec: %w", err)
	}

	if res.ExitCode != 0 {
		return "", fmt.Errorf("%w: exit status %d", errEngineExec, res.ExitCode)
	}

	return res.Stdout, nil
}

// mysqlClients returns the client and dump binaries shipped in the kind's image.
func mysqlClients(kind string) (string, string) {
	if kind == engineMariaDB {
		return "mariadb", "mariadb-dump"
	}

	return "mysql", "mysqldump"
}

func pgDatabase(l EngineLogin) (string, error) {
	db := l.dbOrDefault()
	if !safeDBName.MatchString(db) {
		return "", fmt.Errorf("%w", errDumpDBName)
	}

	return db, nil
}

// dumpUnit captures the unit's logical content with the engine's own dump tool.
func (e *dbEngine) dumpUnit(ctx context.Context, in unitInfo) (string, error) {
	x, ok := e.execer()
	if !ok || in.containerID == "" {
		return "", nil
	}

	l := in.login
	if l.Kind == enginePostgres {
		db, err := pgDatabase(l)
		if err != nil {
			return "", err
		}

		return e.run(ctx, x, in.containerID, []string{
			cmdEnv, "PGPASSWORD=" + l.Password, "pg_dump", "-U", l.User, "-d", db, "--no-owner", "--no-privileges",
		})
	}

	client, dumper := mysqlClients(l.Kind)
	env := "MYSQL_PWD=" + l.Password

	out, err := e.run(ctx, x, in.containerID, []string{
		cmdEnv, env, client, "-u" + mysqlRootUser, "-N", "-B", "-e", "SHOW DATABASES",
	})
	if err != nil {
		return "", err
	}

	var names []string

	for name := range strings.FieldsSeq(out) {
		if safeDBName.MatchString(name) && !isMySQLSystemSchema(name) {
			names = append(names, name)
		}
	}

	if len(names) == 0 {
		return "", nil
	}

	cmd := append([]string{
		cmdEnv, env, dumper, "-u" + mysqlRootUser, "--single-transaction", "--routines", "--events", "--triggers",
		"--databases",
	}, names...)

	return e.run(ctx, x, in.containerID, cmd)
}

func isMySQLSystemSchema(name string) bool {
	switch strings.ToLower(name) {
	case "information_schema", "mysql", "performance_schema", "sys":
		return true
	default:
		return false
	}
}

const (
	pgLoadScript = `f="$0"; base64 -d "$f" > "$f.sql" && ` +
		`psql -q -v ON_ERROR_STOP=1 --single-transaction -U "$1" -d "$2" -f "$f.sql"; rc=$?; ` +
		`rm -f "$f" "$f.sql"; exit $rc`
	mysqlLoadScript = `f="$0"; base64 -d "$f" > "$f.sql" && "$1" -u"$2" < "$f.sql"; rc=$?; ` +
		`rm -f "$f" "$f.sql"; exit $rc`
	appendChunkScript = `printf %s "$1" >> "$0"`
)

// loadDump replays a dump into the unit by staging it in the container and feeding the engine's client.
func (e *dbEngine) loadDump(ctx context.Context, in unitInfo, dump string) error {
	x, ok := e.execer()
	if !ok || in.containerID == "" || dump == "" {
		return nil
	}

	l := in.login
	if l.Kind == enginePostgres {
		if _, err := pgDatabase(l); err != nil {
			return err
		}
	}

	path := "/tmp/rds-seed-" + randomPassword()
	enc := base64.StdEncoding.EncodeToString([]byte(dump))

	for len(enc) > 0 {
		n := min(dumpChunkChars, len(enc))
		if _, err := e.run(ctx, x, in.containerID, []string{"sh", "-c", appendChunkScript, path, enc[:n]}); err != nil {
			return err
		}

		enc = enc[n:]
	}

	if l.Kind == enginePostgres {
		db, _ := pgDatabase(l)

		_, err := e.run(ctx, x, in.containerID, []string{
			cmdEnv, "PGPASSWORD=" + l.Password, "sh", "-c", pgLoadScript, path, l.User, db,
		})

		return err
	}

	client, _ := mysqlClients(l.Kind)
	_, err := e.run(ctx, x, in.containerID, []string{
		cmdEnv, "MYSQL_PWD=" + l.Password, "sh", "-c", mysqlLoadScript, path, client, mysqlRootUser,
	})

	return err
}

func (b *InMemoryBackend) unitInfoLocked(lb *liveDB) unitInfo {
	return unitInfo{login: b.engine.login(lb), containerID: lb.containerID}
}

// captureUnit dumps the unit lb outside b.mu; captured is false when nothing could be dumped.
func (b *InMemoryBackend) captureUnit(lb *liveDB, in unitInfo) (engineData, error) {
	if _, can := b.engine.execer(); !can {
		return engineData{password: in.login.Password, dbName: in.login.DBName, captured: true}, nil
	}

	ctx, cancel := lb.opContext(b.engine.cfg.StartTimeout)
	defer cancel()

	dump, err := b.engine.dumpUnit(ctx, in)
	if errors.Is(err, container.ErrExecUnsupported) {
		return engineData{password: in.login.Password, dbName: in.login.DBName, captured: true}, nil
	}

	if err != nil {
		return engineData{}, fmt.Errorf("%w: %w", ErrInvalidDBInstanceState, err)
	}

	return engineData{dump: dump, password: in.login.Password, dbName: in.login.DBName, captured: true}, nil
}

// captureInstanceData dumps the engine behind instance id before a snapshot; captured is false without a ready engine.
func (b *InMemoryBackend) captureInstanceData(id string) (engineData, error) {
	b.mu.RLock("captureInstanceData")

	var (
		lb *liveDB
		in unitInfo
	)

	if inst, found := b.instances.Get(normalizeID(id)); found {
		if lb = b.instanceUnitLocked(inst); lb != nil && lb.ready {
			in = b.unitInfoLocked(lb)
		} else {
			lb = nil
		}
	}

	b.mu.RUnlock()

	if lb == nil {
		return engineData{}, nil
	}

	return b.captureUnit(lb, in)
}

// captureClusterData dumps the engine behind cluster id before a cluster snapshot.
func (b *InMemoryBackend) captureClusterData(id string) (engineData, error) {
	b.mu.RLock("captureClusterData")

	lb := b.clusterUnitLocked(id)

	var in unitInfo

	if lb != nil && lb.ready {
		in = b.unitInfoLocked(lb)
	} else {
		lb = nil
	}

	b.mu.RUnlock()

	if lb == nil {
		return engineData{}, nil
	}

	return b.captureUnit(lb, in)
}

func (b *InMemoryBackend) storeSnapDataLocked(kind, snapshotID string, d engineData) {
	if b.engine == nil || !d.captured {
		return
	}

	b.engine.data[snapDataKey(kind, snapshotID)] = d
}

func (b *InMemoryBackend) copySnapDataLocked(kind, from, to string) {
	if b.engine == nil {
		return
	}

	if d, ok := b.engine.data[snapDataKey(kind, from)]; ok {
		b.engine.data[snapDataKey(kind, to)] = d
	}
}

func (b *InMemoryBackend) dropSnapDataLocked(kind, snapshotID string) {
	if b.engine != nil {
		delete(b.engine.data, snapDataKey(kind, snapshotID))
	}
}

// snapSeedLocked returns the seed a restore of the snapshot loads; the zero value means an empty database.
func (b *InMemoryBackend) snapSeedLocked(kind, snapshotID string) engineSeed {
	if b.engine == nil {
		return engineSeed{}
	}

	d, ok := b.engine.data[snapDataKey(kind, snapshotID)]
	if !ok {
		return engineSeed{}
	}

	return engineSeed{dump: d.dump, password: d.password, dbName: d.dbName}
}

// liveSeedLocked returns the seed that copies the live unit lb (read replica, point-in-time restore).
func liveSeedLocked(lb *liveDB) engineSeed {
	if lb == nil {
		return engineSeed{}
	}

	return engineSeed{fromKey: lb.key, password: lb.password, dbName: lb.dbName}
}

// applySeed loads the unit's pending seed once, right after its engine first comes up.
func (b *InMemoryBackend) applySeed(ctx context.Context, lb *liveDB) error {
	b.mu.Lock("rdsEngineSeed")
	seed := lb.seed
	lb.seed = engineSeed{}
	in := unitInfo{}

	if b.attachedLocked(lb) {
		in = b.unitInfoLocked(lb)
	}
	b.mu.Unlock()

	if seed.empty() {
		return nil
	}

	dump := seed.dump
	if seed.fromKey != "" {
		var err error
		if dump, err = b.dumpLiveSource(ctx, seed.fromKey); err != nil {
			return err
		}
	}

	if err := b.engine.loadDump(ctx, in, dump); err != nil && !errors.Is(err, container.ErrExecUnsupported) {
		return err
	}

	return nil
}

// dumpLiveSource waits for the source unit to be ready, then dumps it.
func (b *InMemoryBackend) dumpLiveSource(ctx context.Context, key string) (string, error) {
	ticker := time.NewTicker(engineProbeInterval)
	defer ticker.Stop()

	for {
		b.mu.RLock("rdsSeedSource")
		src, ok := b.engine.units[key]

		var in unitInfo

		ready := ok && src.ready
		if ready {
			in = b.unitInfoLocked(src)
		}
		b.mu.RUnlock()

		if !ok {
			return "", fmt.Errorf("%w", errSeedSource)
		}

		if ready {
			return b.engine.dumpUnit(ctx, in)
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("%w: %w", errSeedSource, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (b *InMemoryBackend) captureFinalInstance(dst *engineData, skip bool, finalID, id string) error {
	if skip || finalID == "" {
		return nil
	}

	d, err := b.captureInstanceData(id)
	*dst = d

	return err
}

func (b *InMemoryBackend) captureFinalCluster(dst *engineData, skip bool, finalID, id string) error {
	if skip || finalID == "" {
		return nil
	}

	d, err := b.captureClusterData(id)
	*dst = d

	return err
}
