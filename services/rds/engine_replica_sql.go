package rds

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
)

// ReplicaStatus is a read replica's replication health as reported by its engine.
type ReplicaStatus = dbrepl.Status

func (e *dbEngine) mysqlExec(ctx context.Context, in unitInfo, stmt string) error {
	x, ok := e.execer()
	if !ok {
		return container.ErrExecUnsupported
	}

	client, _ := mysqlClients(in.login.Kind)
	_, err := e.runEnv(ctx, x, in.containerID, []string{
		client, "-u" + mysqlRootUser, "-e", stmt,
	}, mysqlPwdEnv(in.login.Password))

	return err
}

func mysqlResetStmt(lb *liveDB) string {
	if lb.kind == engineMariaDB || !strings.HasSuffix(engineImage(lb.kind, lb.version), ":8.4") {
		return "RESET MASTER"
	}

	return "RESET BINARY LOGS AND GTIDS"
}

func mysqlChangeSourceStmt(kind string, plan replPlan) string {
	if kind == engineMariaDB {
		return fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', "+
			"MASTER_PASSWORD='%s', MASTER_USE_GTID=slave_pos; START REPLICA",
			plan.srcHost, mysqlContainerPort, replUser, plan.srcPass)
	}

	return dbrepl.MySQLChangeSourceSQL(plan.srcHost, mysqlContainerPort, replUser, plan.srcPass)
}

// seedReplica finishes a MySQL/MariaDB replica: it loads a consistent dump of the source and starts replication
// from the dump's position. Postgres replicas arrive complete from pg_basebackup.
func (b *InMemoryBackend) seedReplica(ctx context.Context, lb *liveDB, seed engineSeed, in unitInfo) error {
	if lb.kind == enginePostgres {
		return nil
	}

	b.mu.RLock("rdsReplicaSeed")
	plan := lb.repl.plan
	b.mu.RUnlock()

	_, srcIn, err := b.awaitLiveSource(ctx, seed.fromKey)
	if err != nil {
		if b.standaloneAfterSourceLoss(ctx, lb) {
			return nil
		}

		return err
	}

	dump, err := b.engine.dumpUnitFor(ctx, srcIn, true)
	if err != nil {
		return err
	}

	e := b.engine
	if err = e.mysqlExec(ctx, in, mysqlResetStmt(lb)); err != nil {
		return err
	}

	if err = e.loadDump(ctx, in, dump); err != nil {
		return err
	}

	return e.mysqlExec(ctx, in, mysqlChangeSourceStmt(lb.kind, plan))
}

// guardReplica keeps a MySQL/MariaDB replica read-only; it runs on every start because the setting is not persisted.
func (b *InMemoryBackend) guardReplica(ctx context.Context, lb *liveDB) error {
	b.mu.RLock("rdsReplicaGuard")
	isReplica := lb.repl != nil && !lb.repl.promoting && lb.kind != enginePostgres
	in := b.unitInfoLocked(lb)
	b.mu.RUnlock()

	if !isReplica {
		return nil
	}

	stmt, err := replicaReadOnlyStmt(lb, in.login.User)
	if err != nil {
		return err
	}

	err = b.engine.mysqlExec(ctx, in, stmt)
	if errors.Is(err, container.ErrExecUnsupported) {
		return nil
	}

	return err
}

// replicaReadOnlyStmt makes the replica reject writes. MariaDB has no super_read_only, so the master user also
// loses the privilege that bypasses read_only (SUPER before 10.11, READ ONLY ADMIN since).
func replicaReadOnlyStmt(lb *liveDB, user string) (string, error) {
	if lb.kind != engineMariaDB {
		return "SET GLOBAL super_read_only=ON", nil
	}

	stmt := "SET GLOBAL read_only=ON"
	if user == mysqlRootUser {
		return stmt, nil
	}

	acct, err := mysqlAccount(user)
	if err != nil {
		return "", err
	}

	priv := "READ ONLY ADMIN"
	if strings.HasSuffix(engineImage(lb.kind, lb.version), ":10.6") {
		priv = "SUPER"
	}

	return stmt + "; REVOKE " + priv + " ON *.* FROM " + acct, nil
}

var mysqlUserRe = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

// mysqlAccount renders user as a quoted 'user'@'%' account; only the RDS master-username alphabet is accepted.
func mysqlAccount(user string) (string, error) {
	if !mysqlUserRe.MatchString(user) {
		return "", fmt.Errorf("%w: unsupported master username %q", errEngineExec, user)
	}

	return "'" + strings.ReplaceAll(user, "'", "''") + "'@'%'", nil
}

// promoteEngine detaches a replica from its source and makes it writable.
func (e *dbEngine) promoteEngine(ctx context.Context, in unitInfo) error {
	x, ok := e.execer()
	if !ok {
		return container.ErrExecUnsupported
	}

	l := in.login
	if l.Kind == enginePostgres {
		db, err := pgDatabase(l)
		if err != nil {
			return err
		}

		out, err := e.runEnv(ctx, x, in.containerID, []string{
			"psql", "-tA", "-U", l.User, "-d", db,
			"-c", "SELECT pg_promote(true, 60)",
		}, pgPwdEnv(l.Password))
		if err != nil {
			return err
		}

		if strings.TrimSpace(out) != "t" {
			return fmt.Errorf("%w: pg_promote did not complete", errEngineExec)
		}

		return nil
	}

	stmts := "STOP REPLICA; RESET REPLICA ALL; SET GLOBAL read_only=OFF"
	if l.Kind != engineMariaDB {
		stmts += "; SET GLOBAL super_read_only=OFF"
	} else if l.User != mysqlRootUser {
		acct, aerr := mysqlAccount(l.User)
		if aerr != nil {
			return aerr
		}

		stmts += "; GRANT ALL PRIVILEGES ON *.* TO " + acct + " WITH GRANT OPTION"
	}

	return e.mysqlExec(ctx, in, stmts)
}

func queryReplicaStatus(ctx context.Context, l EngineLogin) (ReplicaStatus, error) {
	db, err := openLogin(l)
	if err != nil {
		return ReplicaStatus{}, err
	}

	defer func() { _ = db.Close() }()

	return dbrepl.QueryStatus(ctx, db, l.Kind == enginePostgres)
}
