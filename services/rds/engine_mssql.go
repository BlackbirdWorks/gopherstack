package rds

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
)

const (
	engineSQLServer = "sqlserver"

	sqlServerContainerPort = 1433
	sqlServerSAUser        = "sa"
	sqlServerMinPassword   = 8
	sqlServerMaxPassword   = 128
	sqlServerDefaultMajor  = "16"
	sqlServerFingerprint   = "fp:"
	sqlServerBackupExt     = ".bak"
	sqlServerDumpFields    = 2
	sqlServerSAPrefix      = "Gs1!"
	sqlServerSASuffixChars = 24
)

var (
	errSQLServerArch = errors.New("SQL Server containers are amd64-only and this host is not amd64")
	errSQLServerDump = errors.New("malformed SQL Server dump")
)

// sqlServerEngine reports whether an RDS engine name is one of the SQL Server editions this engine can run.
func sqlServerEngine(engine string) bool {
	switch engine {
	case "sqlserver-ee", "sqlserver-se", "sqlserver-ex", "sqlserver-web", "sqlserver-dev-ee":
		return true
	default:
		return false
	}
}

// sqlServerEdition maps an RDS edition to MSSQL_PID: Express for ex, Developer (full feature set, non-production
// licence) for everything else.
func sqlServerEdition(engine string) string {
	if engine == "sqlserver-ex" {
		return "Express"
	}

	return "Developer"
}

// sqlServerImage maps the engine version's major (14=2017, 15=2019, 16=2022) to a published image tag.
func sqlServerImage(version string) string {
	major, _, _ := strings.Cut(version, ".")

	year := map[string]string{"14": "2017", "15": "2019", "16": "2022"}[major]
	if year == "" {
		year = "2022"
	}

	return "mcr.microsoft.com/mssql/server:" + year + "-latest"
}

// sqlServerSAPassword is the container's internal sa password: it satisfies SQL Server's complexity rule
// whatever the master password is, and derives from the container name so it needs no storage.
func sqlServerSAPassword(ctrName string) string {
	sum := sha256.Sum256([]byte("gopherstack-sa:" + ctrName))

	return sqlServerSAPrefix + hex.EncodeToString(sum[:])[:sqlServerSASuffixChars]
}

func sqlServerEnv(lb *liveDB, engine string) []string {
	return []string{
		"ACCEPT_EULA=Y",
		"MSSQL_PID=" + sqlServerEdition(engine),
		"MSSQL_SA_PASSWORD=" + sqlServerSAPassword(lb.ctrName),
	}
}

// validateSQLServerCreate applies the checks RDS makes on a SQL Server CreateDBInstance when the engine is real.
func (b *InMemoryBackend) validateSQLServerCreate(engine, dbName, password, masterUser string) error {
	if b.engine == nil || b.engine.kindOf(engine) != engineSQLServer {
		return nil
	}

	if dbName != "" {
		return fmt.Errorf("%w: DBName must be null for engine %s", ErrInvalidParameterCombination, engine)
	}

	if strings.EqualFold(masterUser, sqlServerSAUser) {
		return fmt.Errorf("%w: MasterUsername %q is a reserved word", ErrInvalidParameter, masterUser)
	}

	if password == "" {
		return nil
	}

	if n := len(password); n < sqlServerMinPassword || n > sqlServerMaxPassword {
		return fmt.Errorf("%w: The parameter MasterUserPassword is not a valid password because it must be "+
			"%d to %d characters long", ErrInvalidParameter, sqlServerMinPassword, sqlServerMaxPassword)
	}

	return nil
}

func openSQLServer(addr, user, password string) (*sql.DB, error) {
	u := url.URL{
		Scheme:   engineSQLServer,
		User:     url.UserPassword(user, password),
		Host:     addr,
		RawQuery: "database=master&encrypt=disable&connection+timeout=5",
	}

	c, err := mssql.NewConnector(u.String())
	if err != nil {
		return nil, fmt.Errorf("sqlserver connector: %w", err)
	}

	return sql.OpenDB(c), nil
}

const ensureMasterLoginSQL = `
DECLARE @s nvarchar(max);
IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @p1)
  SET @s = N'ALTER LOGIN ' + QUOTENAME(@p1) + N' WITH PASSWORD = ' + QUOTENAME(@p2, '''') + N', CHECK_POLICY = OFF';
ELSE
  SET @s = N'CREATE LOGIN ' + QUOTENAME(@p1) + N' WITH PASSWORD = ' + QUOTENAME(@p2, '''') + N', CHECK_POLICY = OFF';
EXEC (@s);
SET @s = N'ALTER SERVER ROLE sysadmin ADD MEMBER ' + QUOTENAME(@p1);
EXEC (@s);`

// probeSQLServer provisions the master login through sa (idempotent), then proves it by logging in as it.
func probeSQLServer(ctx context.Context, l EngineLogin) error {
	sa, err := openSQLServer(l.Addr, sqlServerSAUser, l.AdminPassword)
	if err != nil {
		return err
	}

	defer func() { _ = sa.Close() }()

	if _, err = sa.ExecContext(ctx, ensureMasterLoginSQL, l.User, l.Password); err != nil {
		return fmt.Errorf("provision master login: %w", err)
	}

	return selectOne(ctx, l)
}

func setSQLServerPassword(ctx context.Context, db *sql.DB, newPassword string) error {
	_, err := db.ExecContext(ctx, `DECLARE @s nvarchar(max) = N'ALTER LOGIN ' + QUOTENAME(SUSER_SNAME()) + `+
		`N' WITH PASSWORD = ' + QUOTENAME(@p1, '''') + N', CHECK_POLICY = OFF'; EXEC (@s);`, newPassword)

	return err
}

const (
	sqlServerLoadScript = `f="$0"; base64 -d "$f" > "$f.bak"; rc=$?; rm -f "$f"; exit $rc`
	sqlServerListSQL    = `SELECT d.name, CONVERT(varchar(64), s.log_end_lsn) FROM sys.databases d ` +
		`CROSS APPLY sys.dm_db_log_stats(d.database_id) s WHERE d.database_id > 4 AND d.state = 0 ORDER BY d.name`
	sqlServerBackupSQL = `DECLARE @s nvarchar(max) = N'BACKUP DATABASE ' + QUOTENAME(@p1) + N' TO DISK = N' + ` +
		`QUOTENAME(@p2, '''') + N' WITH INIT, COPY_ONLY'; EXEC (@s);`
	sqlServerRestoreSQL = `DECLARE @s nvarchar(max) = N'RESTORE DATABASE ' + QUOTENAME(@p1) + N' FROM DISK = N' + ` +
		`QUOTENAME(@p2, '''') + N' WITH REPLACE'; EXEC (@s); ` +
		`SET @s = N'ALTER AUTHORIZATION ON DATABASE::' + QUOTENAME(@p1) + N' TO ' + QUOTENAME(@p3); EXEC (@s);`
)

type sqlServerDB struct{ name, lsn string }

func listSQLServerDBs(ctx context.Context, db *sql.DB) ([]sqlServerDB, error) {
	rows, err := db.QueryContext(ctx, sqlServerListSQL)
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var out []sqlServerDB

	for rows.Next() {
		var d sqlServerDB
		if err = rows.Scan(&d.name, &d.lsn); err != nil {
			return nil, fmt.Errorf("scan database: %w", err)
		}

		out = append(out, d)
	}

	return out, rows.Err()
}

// dumpSQLServer captures every user database as a native BACKUP file read back from the container. The dump is
// a fingerprint line (the databases' log end LSNs, so unchanged data dedupes as one restore point) followed by
// one "name<TAB>base64(.bak)" line per database.
func (e *dbEngine) dumpSQLServer(ctx context.Context, x EngineExecRuntime, in unitInfo) (string, error) {
	l := in.login

	db, err := openSQLServer(l.Addr, l.User, l.Password)
	if err != nil {
		return "", err
	}

	defer func() { _ = db.Close() }()

	dbs, err := listSQLServerDBs(ctx, db)
	if err != nil || len(dbs) == 0 {
		return "", err
	}

	h := sha256.New()
	lines := make([]string, 0, len(dbs)+1)

	for _, d := range dbs {
		_, _ = fmt.Fprintf(h, "%s=%s;", d.name, d.lsn)

		enc, derr := e.backupSQLServerDB(ctx, x, in, db, d.name)
		if derr != nil {
			return "", derr
		}

		lines = append(lines, d.name+"\t"+enc)
	}

	return sqlServerFingerprint + hex.EncodeToString(h.Sum(nil)) + "\n" + strings.Join(lines, "\n"), nil
}

func (e *dbEngine) backupSQLServerDB(
	ctx context.Context, x EngineExecRuntime, in unitInfo, db *sql.DB, name string,
) (string, error) {
	path := "/tmp/rds-bak-" + randomPassword() + sqlServerBackupExt

	defer func() { _, _ = e.run(ctx, x, in.containerID, []string{"rm", "-f", path}) }()

	if _, err := db.ExecContext(ctx, sqlServerBackupSQL, name, path); err != nil {
		return "", fmt.Errorf("backup database %s: %w", name, err)
	}

	out, err := e.run(ctx, x, in.containerID, []string{"base64", "-w0", path})
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(out), nil
}

// loadSQLServerDump restores each database of a dumpSQLServer dump from a staged .bak file.
func (e *dbEngine) loadSQLServerDump(ctx context.Context, x EngineExecRuntime, in unitInfo, dump string) error {
	l := in.login

	db, err := openSQLServer(l.Addr, l.User, l.Password)
	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	for line := range strings.SplitSeq(dump, "\n") {
		if line == "" || strings.HasPrefix(line, sqlServerFingerprint) {
			continue
		}

		parts := strings.SplitN(line, "\t", sqlServerDumpFields)
		if len(parts) != sqlServerDumpFields {
			return fmt.Errorf("%w", errSQLServerDump)
		}

		if err = e.restoreSQLServerDB(ctx, x, in, db, parts[0], parts[1]); err != nil {
			return err
		}
	}

	return nil
}

func (e *dbEngine) restoreSQLServerDB(
	ctx context.Context, x EngineExecRuntime, in unitInfo, db *sql.DB, name, enc string,
) error {
	staged := "/tmp/rds-seed-" + randomPassword()
	bak := staged + sqlServerBackupExt

	defer func() { _, _ = e.run(ctx, x, in.containerID, []string{"rm", "-f", staged, bak}) }()

	if _, err := base64.StdEncoding.DecodeString(enc); err != nil {
		return fmt.Errorf("%w: %w", errSQLServerDump, err)
	}

	for len(enc) > 0 {
		n := min(dumpChunkChars, len(enc))
		if _, err := e.run(ctx, x, in.containerID,
			[]string{"sh", "-c", appendChunkScript, staged, enc[:n]}); err != nil {
			return err
		}

		enc = enc[n:]
	}

	if _, err := e.run(ctx, x, in.containerID, []string{"sh", "-c", sqlServerLoadScript, staged}); err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, sqlServerRestoreSQL, name, bak, in.login.User); err != nil {
		return fmt.Errorf("restore database %s: %w", name, err)
	}

	return nil
}
