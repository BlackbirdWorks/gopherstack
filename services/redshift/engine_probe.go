package redshift

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

func openPostgres(l EngineLogin) (*sql.DB, error) {
	name := l.DBName
	if name == "" {
		name = l.User
	}

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(l.User, l.Password),
		Host:     l.Addr,
		Path:     "/" + name,
		RawQuery: "sslmode=disable&connect_timeout=5",
	}

	return sql.Open("pgx", u.String())
}

// probePostgres proves the engine answers and the master login works with a real SELECT 1.
func probePostgres(ctx context.Context, l EngineLogin) error {
	db, err := openPostgres(l)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}

	defer func() { _ = db.Close() }()

	var one int
	if err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("probe postgres: %w", err)
	}

	return nil
}

func alterMasterPassword(ctx context.Context, c PasswordChange) error {
	db, err := openPostgres(c.Login)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}

	defer func() { _ = db.Close() }()

	var stmt string

	err = db.QueryRowContext(ctx, "SELECT format('ALTER USER %I PASSWORD %L', $1::text, $2::text)",
		c.Login.User, c.NewPassword).Scan(&stmt)
	if err != nil {
		return fmt.Errorf("quote alter user: %w", err)
	}

	if _, err = db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("alter user: %w", err)
	}

	return nil
}
