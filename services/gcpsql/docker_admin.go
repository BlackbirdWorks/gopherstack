package gcpsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const connectTimeout = 5 * time.Second

var (
	errBadValue = errors.New("value contains a NUL byte")
	identRe     = regexp.MustCompile(`^[\p{L}\p{N}_ $.@#-]{1,128}$`)
)

func (e *dockerEngine) open(db string) (*sql.DB, error) {
	h, p := e.Endpoint()

	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable connect_timeout=5",
		h, p, pgAdminUser, db))
	if err != nil {
		return nil, fmt.Errorf("pg config: %w", err)
	}

	cfg.Password = e.pass()

	return stdlib.OpenDB(*cfg), nil
}

func (e *dockerEngine) exec(ctx context.Context, db, stmt string) error {
	conn, err := e.open(db)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	return nil
}

func (e *dockerEngine) bootstrap(ctx context.Context) error {
	if err := e.exec(ctx, pgAdminUser, "DO $$ BEGIN CREATE ROLE "+pgSuperRole+
		" NOLOGIN CREATEDB CREATEROLE; EXCEPTION WHEN duplicate_object THEN NULL; END $$"); err != nil {
		return err
	}

	for _, db := range []string{pgAdminUser, "template1"} {
		if err := e.exec(ctx, db, "GRANT ALL ON SCHEMA public TO "+pgSuperRole); err != nil {
			return err
		}
	}

	return nil
}

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// quoteIdent has the server quote s, so no client-built string reaches the DDL statement.
func (e *dockerEngine) quoteIdent(ctx context.Context, q rowQuerier, s string) (string, error) {
	if !identRe.MatchString(s) {
		return "", errInvalid("identifier %q is invalid", s)
	}

	var out string
	if err := q.QueryRowContext(ctx, "SELECT quote_ident($1::text)", s).Scan(&out); err != nil {
		return "", fmt.Errorf("sql: %w", err)
	}

	return out, nil
}

// quoteDotted quotes each dot-separated part of s and rejoins them.
func (e *dockerEngine) quoteDotted(ctx context.Context, q rowQuerier, s string) (string, error) {
	parts := strings.Split(s, ".")

	for i, p := range parts {
		qp, err := e.quoteIdent(ctx, q, p)
		if err != nil {
			return "", err
		}

		parts[i] = qp
	}

	return strings.Join(parts, "."), nil
}

// quoteLiteral has the PostgreSQL server quote s as a string literal.
func (e *dockerEngine) quoteLiteral(ctx context.Context, q rowQuerier, s string) (string, error) {
	if strings.ContainsRune(s, 0) {
		return "", errBadValue
	}

	var out string
	if err := q.QueryRowContext(ctx, "SELECT quote_literal($1::text)", s).Scan(&out); err != nil {
		return "", fmt.Errorf("sql: %w", err)
	}

	return out, nil
}

func (e *dockerEngine) adminConn() (*sql.DB, error) { return e.open(e.adminDB()) }

// CreateDatabase implements Engine.
func (e *dockerEngine) CreateDatabase(ctx context.Context, name, _, _ string) error {
	conn, err := e.adminConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	ident, err := e.quoteIdent(ctx, conn, name)
	if err != nil {
		return err
	}

	return e.exec(ctx, pgAdminUser, "CREATE DATABASE "+ident)
}

// DropDatabase implements Engine.
func (e *dockerEngine) DropDatabase(ctx context.Context, name string) error {
	conn, err := e.adminConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	ident, err := e.quoteIdent(ctx, conn, name)
	if err != nil {
		return err
	}

	return e.exec(ctx, pgAdminUser, "DROP DATABASE "+ident+" WITH (FORCE)")
}

// ListDatabases implements Engine.
func (e *dockerEngine) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := e.Query(ctx, pgAdminUser, "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY 1")
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
	err := e.upsertPGUser(ctx, u)

	if err == nil && u.Password != "" && u.Name == pgAdminUser {
		e.mu.Lock("UpsertUser")
		e.password = u.Password
		e.mu.Unlock()
	}

	return err
}

func (e *dockerEngine) upsertPGUser(ctx context.Context, u UserSpec) error {
	conn, err := e.open(pgAdminUser)
	if err != nil {
		return err
	}
	defer conn.Close()

	ident, err := e.quoteIdent(ctx, conn, u.Name)
	if err != nil {
		return err
	}

	pw := ""

	if u.Password != "" {
		lit, litErr := e.quoteLiteral(ctx, conn, u.Password)
		if litErr != nil {
			return litErr
		}

		pw = " PASSWORD " + lit
	}

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

// DropUser implements Engine.
func (e *dockerEngine) DropUser(ctx context.Context, name, _ string) error {
	conn, err := e.open(pgAdminUser)
	if err != nil {
		return err
	}
	defer conn.Close()

	ident, err := e.quoteIdent(ctx, conn, name)
	if err != nil {
		return err
	}

	return e.exec(ctx, pgAdminUser, "DROP ROLE "+ident)
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
	return e.applyPGFlags(ctx, flags)
}

func (e *dockerEngine) applyPGFlags(ctx context.Context, flags []Flag) error {
	if err := e.exec(ctx, pgAdminUser, "ALTER SYSTEM RESET ALL"); err != nil {
		return err
	}

	conn, err := e.open(pgAdminUser)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, f := range flags {
		if !flagNameRe.MatchString(f.Name) {
			return errInvalid("flag name %q is invalid", f.Name)
		}

		name, nameErr := e.quoteDotted(ctx, conn, f.Name)
		if nameErr != nil {
			return nameErr
		}

		lit, litErr := e.quoteLiteral(ctx, conn, f.Value)
		if litErr != nil {
			return litErr
		}

		if err = e.exec(ctx, pgAdminUser, "ALTER SYSTEM SET "+name+" = "+lit); err != nil {
			return err
		}
	}

	return nil
}
