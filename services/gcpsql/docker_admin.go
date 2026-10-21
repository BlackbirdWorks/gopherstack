package gcpsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const connectTimeout = 5 * time.Second

var errBadValue = errors.New("value contains a NUL byte")

func (e *dockerEngine) open(db string) (*sql.DB, error) {
	h, p := e.Endpoint()

	if e.kind == kindPostgres {
		cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable connect_timeout=5",
			h, p, pgAdminUser, db))
		if err != nil {
			return nil, fmt.Errorf("pg config: %w", err)
		}

		cfg.Password = e.pass()

		return stdlib.OpenDB(*cfg), nil
	}

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = mysqlAdminUser, e.pass(), "tcp", e.addr(), db
	cfg.InterpolateParams = true
	cfg.Timeout = connectTimeout

	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}

	return sql.OpenDB(conn), nil
}

func (e *dockerEngine) exec(ctx context.Context, db, stmt string, args ...any) error {
	conn, err := e.open(db)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	return nil
}

func (e *dockerEngine) bootstrap(ctx context.Context) error {
	if e.kind != kindPostgres {
		return nil
	}

	if err := e.exec(ctx, pgAdminUser, "CREATE ROLE "+pgSuperRole+" NOLOGIN CREATEDB CREATEROLE"); err != nil {
		return err
	}

	for _, db := range []string{pgAdminUser, "template1"} {
		if err := e.exec(ctx, db, "GRANT ALL ON SCHEMA public TO "+pgSuperRole); err != nil {
			return err
		}
	}

	return nil
}

func pgLiteral(s string) (string, error) {
	if strings.ContainsRune(s, 0) {
		return "", errBadValue
	}

	return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
}

func myIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// CreateDatabase implements Engine.
func (e *dockerEngine) CreateDatabase(ctx context.Context, name, charset, collation string) error {
	if e.kind == kindPostgres {
		return e.exec(ctx, pgAdminUser, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	}

	stmt := "CREATE DATABASE " + myIdent(name)

	if charset != "" && charsetRe.MatchString(charset) {
		stmt += " CHARACTER SET " + charset
	}

	if collation != "" && charsetRe.MatchString(collation) {
		stmt += " COLLATE " + collation
	}

	return e.exec(ctx, "", stmt)
}

// DropDatabase implements Engine.
func (e *dockerEngine) DropDatabase(ctx context.Context, name string) error {
	if e.kind == kindPostgres {
		return e.exec(ctx, pgAdminUser, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	}

	return e.exec(ctx, "", "DROP DATABASE "+myIdent(name))
}

// ListDatabases implements Engine.
func (e *dockerEngine) ListDatabases(ctx context.Context) ([]string, error) {
	query := "SHOW DATABASES"
	if e.kind == kindPostgres {
		query = "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY 1"
	}

	rows, err := e.Query(ctx, e.adminDB(), query)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(rows))

	for _, r := range rows[1:] {
		out = append(out, r[0])
	}

	return out, nil
}

// UpsertUser implements Engine.
func (e *dockerEngine) UpsertUser(ctx context.Context, u UserSpec) error {
	var err error
	if e.kind == kindPostgres {
		err = e.upsertPGUser(ctx, u)
	} else {
		err = e.upsertMySQLUser(ctx, u)
	}

	if err == nil && u.Password != "" && u.Name == e.adminUser() {
		e.mu.Lock("UpsertUser")
		e.password = u.Password
		e.mu.Unlock()
	}

	return err
}

func (e *dockerEngine) adminUser() string {
	if e.kind == kindPostgres {
		return pgAdminUser
	}

	return mysqlAdminUser
}

func (e *dockerEngine) upsertPGUser(ctx context.Context, u UserSpec) error {
	ident := pgx.Identifier{u.Name}.Sanitize()
	pw := ""

	if u.Password != "" {
		lit, err := pgLiteral(u.Password)
		if err != nil {
			return err
		}

		pw = " PASSWORD " + lit
	}

	conn, err := e.open(pgAdminUser)
	if err != nil {
		return err
	}
	defer conn.Close()

	var exists bool
	if err = conn.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", u.Name).
		Scan(&exists); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	stmts := []string{"ALTER ROLE " + ident + " WITH LOGIN" + pw}
	if !exists {
		stmts = []string{"CREATE ROLE " + ident + " WITH LOGIN" + pw, "GRANT " + pgSuperRole + " TO " + ident}
	}

	for _, s := range stmts {
		if _, err = conn.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("sql: %w", err)
		}
	}

	return nil
}

func (e *dockerEngine) upsertMySQLUser(ctx context.Context, u UserSpec) error {
	conn, err := e.open("")
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, "CREATE USER IF NOT EXISTS ?@?", u.Name, u.Host); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	if u.Password != "" {
		if _, err = conn.ExecContext(ctx, "ALTER USER ?@? IDENTIFIED BY ?", u.Name, u.Host, u.Password); err != nil {
			return fmt.Errorf("sql: %w", err)
		}
	}

	if _, err = conn.ExecContext(
		ctx,
		"GRANT ALL PRIVILEGES ON *.* TO ?@? WITH GRANT OPTION",
		u.Name,
		u.Host,
	); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	return nil
}

// DropUser implements Engine.
func (e *dockerEngine) DropUser(ctx context.Context, name, host string) error {
	if e.kind == kindPostgres {
		return e.exec(ctx, pgAdminUser, "DROP ROLE "+pgx.Identifier{name}.Sanitize())
	}

	return e.exec(ctx, "", "DROP USER ?@?", name, host)
}

// Restart implements Engine.
func (e *dockerEngine) Restart(ctx context.Context, flags []Flag) error {
	if err := e.applyFlags(ctx, flags); err != nil {
		return err
	}

	if err := e.Stop(ctx); err != nil {
		return err
	}

	return e.Start(ctx)
}

func (e *dockerEngine) applyFlags(ctx context.Context, flags []Flag) error {
	if e.kind == kindPostgres {
		return e.applyPGFlags(ctx, flags)
	}

	_ = e.exec(ctx, "", "RESET PERSIST")

	for _, f := range flags {
		if !flagNameRe.MatchString(f.Name) {
			return errInvalid("flag name %q is invalid", f.Name)
		}

		if err := e.setMySQLFlag(ctx, f); err != nil {
			return err
		}
	}

	return nil
}

var mysqlBareValue = regexp.MustCompile(`^(-?[0-9]+(\.[0-9]+)?|ON|OFF|TRUE|FALSE)$`)

// setMySQLFlag persists a server variable; numbers and booleans must not be quoted.
func (e *dockerEngine) setMySQLFlag(ctx context.Context, f Flag) error {
	stmt := "SET PERSIST_ONLY " + f.Name + " = "

	if mysqlBareValue.MatchString(strings.ToUpper(f.Value)) {
		return e.exec(ctx, "", stmt+strings.ToUpper(f.Value))
	}

	return e.exec(ctx, "", stmt+"?", f.Value)
}

func (e *dockerEngine) applyPGFlags(ctx context.Context, flags []Flag) error {
	if err := e.exec(ctx, pgAdminUser, "ALTER SYSTEM RESET ALL"); err != nil {
		return err
	}

	for _, f := range flags {
		if !flagNameRe.MatchString(f.Name) {
			return errInvalid("flag name %q is invalid", f.Name)
		}

		lit, err := pgLiteral(f.Value)
		if err != nil {
			return err
		}

		if err = e.exec(ctx, pgAdminUser, "ALTER SYSTEM SET "+f.Name+" = "+lit); err != nil {
			return err
		}
	}

	return nil
}
