package gcpsql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	replUser     = "cloudsqlreplica"
	pitrMount    = "/gopherstack-pitr"
	pitrSrcMount = "/gopherstack-pitr-src"
	recoveryEnv  = "GS_RECOVERY_AT"
	rootUserKey  = "MYSQL_PWD="

	pgWrapper = `set -e
if [ -d ` + pitrMount + ` ]; then
  mkdir -p ` + pitrMount + `/wal
  chown postgres:postgres ` + pitrMount + ` ` + pitrMount + `/wal
fi
if [ -n "$` + recoveryEnv + `" ] && [ ! -s "$PGDATA/PG_VERSION" ]; then
  mkdir -p "$PGDATA"
  cp -a ` + pitrSrcMount + `/base/. "$PGDATA"/
  chown -R postgres:postgres "$PGDATA"
  chmod 700 "$PGDATA"
  touch "$PGDATA/recovery.signal"
  echo "restore_command = 'cp ` + pitrSrcMount + `/wal/%f %p'" >> "$PGDATA/postgresql.auto.conf"
  echo "recovery_target_time = '$` + recoveryEnv + `'" >> "$PGDATA/postgresql.auto.conf"
  echo "recovery_target_action = 'promote'" >> "$PGDATA/postgresql.auto.conf"
fi
exec docker-entrypoint.sh "$@"`
)

// sourceNet is the docker network a replication source shares with its replicas.
type sourceNet struct {
	id        string
	name      string
	connected atomic.Bool
}

func mysqlServerFlags(serverID uint32) []string {
	return []string{
		"--server-id=" + strconv.FormatUint(uint64(max(serverID, 1)), 10),
		"--gtid-mode=ON", "--enforce-gtid-consistency=ON",
	}
}

// pgLaunch fills the PostgreSQL part of the container spec: replication-ready flags, the data volume and, for
// replicas and recoveries, the entrypoint that seeds the data directory.
func (e *dockerEngine) pgLaunch(s *container.Spec) {
	args := []string{
		"postgres", "-c", "wal_level=replica", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10",
	}

	if e.volData != "" {
		s.Mounts = append(s.Mounts, e.volData+":"+pgDataDir)
	}

	switch {
	case e.spec.Replica != nil:
		r := e.spec.Replica
		s.Entrypoint = append([]string{
			"sh", "-c", dbrepl.PGReplicaScript(replUser), "gs-pg-replica", r.Host, strconv.Itoa(r.Port),
		}, args...)
		s.Env = append(s.Env, dbrepl.PasswordEnv+"="+r.Password)
	case e.volPITR != "":
		s.Mounts = append(s.Mounts, e.volPITR+":"+pitrMount)

		if rec := e.spec.Recovery; rec != nil {
			s.Mounts = append(s.Mounts, rec.Volume+":"+pitrSrcMount+":ro")
			s.Env = append(s.Env, recoveryEnv+"="+rec.At.UTC().Format("2006-01-02 15:04:05.000000+00"))
		}

		s.Entrypoint = append([]string{"sh", "-c", pgWrapper, "gs-pg"}, args...)
	default:
		s.Cmd = args
	}
}

func (e *dockerEngine) psql(ctx context.Context, sqlText string) (string, error) {
	return e.runOut(
		ctx,
		[]string{psqlBin, "-tA", "-q", "-v", "ON_ERROR_STOP=1", "-U", pgAdminUser, "-d", pgAdminUser, "-c", sqlText},
	)
}

func (e *dockerEngine) mysqlCmd(ctx context.Context, stmt string) (string, error) {
	return e.runOutEnv(
		ctx,
		[]string{mysqlClient, "-u" + mysqlAdminUser, "-N", "-B", "-e", stmt},
		e.mysqlEnv(),
	)
}

// netSource is the source whose replication network this engine joins: its primary, or the engine it recovers from.
func (e *dockerEngine) netSource() *ReplicaSource {
	if e.spec.Replica != nil {
		return e.spec.Replica
	}

	if e.spec.Recovery != nil {
		return e.spec.Recovery.Source
	}

	return nil
}

func (e *dockerEngine) replicaPass() string {
	e.mu.Lock("replicaPass")
	defer e.mu.Unlock()

	if e.replPass == "" {
		e.replPass = randomPassword()[:30]
	}

	return e.replPass
}

// ReplicationSource implements Replicator.
func (e *dockerEngine) ReplicationSource(ctx context.Context) (ReplicaSource, error) {
	nr, ok := e.rt.(container.NetworkRuntime)
	if !ok {
		return ReplicaSource{}, errNoReplication
	}

	pass := e.replicaPass()
	port := pgPort

	if e.kind == kindMySQL {
		port = mysqlPort

		if _, err := e.mysqlCmd(ctx, dbrepl.MySQLReplicaUserSQL(replUser, pass)); err != nil {
			return ReplicaSource{}, err
		}
	} else if err := e.preparePGSource(ctx, pass); err != nil {
		return ReplicaSource{}, err
	}

	n, err := e.sourceNetwork(ctx, nr)
	if err != nil {
		return ReplicaSource{}, err
	}

	p, _ := strconv.Atoi(port)

	return ReplicaSource{Network: n.name, NetworkID: n.id, Host: e.containerName(), Port: p, Password: pass}, nil
}

func (e *dockerEngine) containerName() string {
	e.mu.RLock("containerName")
	defer e.mu.RUnlock()

	return e.ctrName
}

func (e *dockerEngine) preparePGSource(ctx context.Context, pass string) error {
	if _, err := e.psql(ctx, dbrepl.PGReplicaRoleSQL(replUser, pass)); err != nil {
		return err
	}

	if err := e.run(ctx, []string{"sh", "-c", dbrepl.PGHBAScript(replUser), "pg-hba"}); err != nil {
		return err
	}

	_, err := e.psql(ctx, "SELECT pg_reload_conf()")

	return err
}

func (e *dockerEngine) sourceNetwork(ctx context.Context, nr container.NetworkRuntime) (*sourceNet, error) {
	e.mu.Lock("sourceNetwork")
	defer e.mu.Unlock()

	if e.net == nil {
		name := "gopherstack-gcpsql-net-" + sanitizeName(e.id) + "-" + randomPassword()[:8]

		id, err := nr.CreateNetwork(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("create replication network: %w", err)
		}

		e.net = &sourceNet{id: id, name: name}
		e.netID = id
	}

	if !e.net.connected.Load() {
		if err := nr.ConnectNetwork(ctx, e.net.id, e.ctrID); err != nil {
			return nil, fmt.Errorf("connect source to replication network: %w", err)
		}

		e.net.connected.Store(true)
	}

	return e.net, nil
}

// releaseNetwork removes the replication network; a network still in use by another engine is removed by
// whichever engine closes last.
func (e *dockerEngine) releaseNetwork(ctx context.Context) {
	nr, ok := e.rt.(container.NetworkRuntime)
	if !ok {
		return
	}

	e.mu.RLock("releaseNetwork")
	id := e.netID
	e.mu.RUnlock()

	if id != "" {
		_ = nr.RemoveNetwork(ctx, id)
	}
}

// ReplicaSeed implements Replicator.
func (e *dockerEngine) ReplicaSeed(ctx context.Context, dbs []string) ([]byte, error) {
	if e.kind == kindPostgres {
		return nil, nil
	}

	if len(dbs) == 0 {
		out, err := e.mysqlCmd(ctx, "SELECT @@GLOBAL.gtid_executed")
		if err != nil {
			return nil, err
		}

		gtid := strings.ReplaceAll(strings.Join(strings.Fields(out), ""), `\n`, "")
		if gtid == "" || gtid == "NULL" {
			return nil, nil
		}

		return []byte("SET @@GLOBAL.GTID_PURGED='" + gtid + "';\n"), nil
	}

	cmd := append([]string{
		mysqldumpBin, "-u" + mysqlAdminUser, argSingleTx, "--skip-dump-date",
		argRoutines, "--events", "--triggers", "--set-gtid-purged=ON", argAddDrop,
		"-r", dumpMount + "/seed.sql", argDatabases,
	}, dbs...)

	if _, err := e.runOutEnv(ctx, cmd, e.mysqlEnv()); err != nil {
		return nil, err
	}

	return e.readDump("seed.sql")
}

func (e *dockerEngine) mysqlResetStmt() string {
	if strings.HasSuffix(e.image, ":8.4") {
		return "RESET BINARY LOGS AND GTIDS"
	}

	return "RESET MASTER"
}

// AttachReplica implements Replicator; PostgreSQL replicas already stream from pg_basebackup's recovery config.
func (e *dockerEngine) AttachReplica(ctx context.Context, src ReplicaSource, seed []byte) error {
	e.mu.Lock("AttachReplica")
	e.spec.Replica, e.netID = &src, src.NetworkID
	e.mu.Unlock()

	if e.kind == kindPostgres {
		return e.repointPG(ctx, src)
	}

	change := dbrepl.MySQLChangeSourceSQL(src.Host, src.Port, replUser, src.Password)

	if out, err := e.mysqlCmd(ctx, "SHOW REPLICA STATUS"); err == nil && strings.TrimSpace(out) != "" {
		_, err = e.mysqlCmd(ctx, "STOP REPLICA; "+change)

		return e.guardReplica(ctx, err)
	}

	if _, err := e.mysqlCmd(ctx, e.mysqlResetStmt()); err != nil {
		return err
	}

	if len(seed) > 0 {
		if err := e.restoreFile(ctx, "", seed); err != nil {
			return err
		}
	}

	_, err := e.mysqlCmd(ctx, change)

	return e.guardReplica(ctx, err)
}

func (e *dockerEngine) guardReplica(ctx context.Context, prev error) error {
	if prev != nil {
		return prev
	}

	if _, err := e.mysqlCmd(ctx, "SET GLOBAL super_read_only=ON"); err != nil {
		return err
	}

	e.mu.Lock("guardReplica")
	e.attached = true
	e.mu.Unlock()

	return nil
}

// Persistent reports whether the engine keeps its data in a volume that survives the container.
func (e *dockerEngine) Persistent() bool { return e.volData != "" }

func (e *dockerEngine) conninfo(src ReplicaSource) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s", src.Host, src.Port, replUser, src.Password)
}

// repointPG aims a streaming replica at its source's current container name, which changes across restarts.
func (e *dockerEngine) repointPG(ctx context.Context, src ReplicaSource) error {
	if _, err := e.psql(ctx, "ALTER SYSTEM SET primary_conninfo = '"+e.conninfo(src)+"'"); err != nil {
		return err
	}

	_, err := e.psql(ctx, "SELECT pg_reload_conf()")

	return err
}

// Promote implements Replicator.
func (e *dockerEngine) Promote(ctx context.Context) error {
	if e.kind == kindPostgres {
		out, err := e.psql(ctx, "SELECT pg_promote(true, 60)")
		if err != nil {
			return err
		}

		if strings.TrimSpace(out) != "t" {
			return fmt.Errorf("%w: pg_promote did not complete", errExec)
		}
	} else if _, err := e.mysqlCmd(ctx,
		"STOP REPLICA; RESET REPLICA ALL; SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF"); err != nil {
		return err
	}

	e.mu.Lock("Promote")
	e.promoted, e.spec.Replica = true, nil
	e.mu.Unlock()

	return nil
}

// SetReplication implements Replicator.
func (e *dockerEngine) SetReplication(ctx context.Context, on bool) error {
	if e.kind == kindMySQL {
		stmt := "STOP REPLICA"
		if on {
			stmt = "START REPLICA"
		}

		_, err := e.mysqlCmd(ctx, stmt)

		return err
	}

	e.mu.RLock("SetReplication")
	src := e.spec.Replica
	e.mu.RUnlock()

	if src == nil {
		return errNoReplication
	}

	conn := ""
	if on {
		conn = e.conninfo(*src)
	}

	if _, err := e.psql(ctx, "ALTER SYSTEM SET primary_conninfo = '"+conn+"'"); err != nil {
		return err
	}

	_, err := e.psql(ctx, "SELECT pg_reload_conf()")

	return err
}

// ReplicationStatus implements Replicator.
func (e *dockerEngine) ReplicationStatus(ctx context.Context) (dbrepl.Status, error) {
	db, err := e.open(e.adminDB())
	if err != nil {
		return dbrepl.Status{}, err
	}
	defer db.Close()

	return dbrepl.QueryStatus(ctx, db, e.kind == kindPostgres)
}

// reguard re-applies the read-only guard a MySQL replica loses on container restart.
func (e *dockerEngine) reguard(ctx context.Context) {
	e.mu.RLock("reguard")
	replica := e.attached && !e.promoted && e.kind == kindMySQL
	e.mu.RUnlock()

	if replica {
		if _, err := e.mysqlCmd(ctx, "SET GLOBAL super_read_only=ON"); err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpsql: replica read-only guard failed", "instance", e.id, "error", err)
		}
	}
}

// Purge implements Purger: it closes the engine and deletes its persistent volumes.
func (e *dockerEngine) Purge(ctx context.Context) {
	e.Close(ctx)
	e.RemoveVolumes(ctx)
}

// RemoveVolumes implements VolumeRemover; the engine must be closed first.
func (e *dockerEngine) RemoveVolumes(ctx context.Context) {
	if e.vols == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownLimit)
	defer cancel()

	for _, v := range []string{e.volData, e.volPITR} {
		if v == "" {
			continue
		}

		if err := e.vols.RemoveVolume(ctx, v); err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpsql: volume removal failed", "volume", v, "error", err)
		}
	}
}
