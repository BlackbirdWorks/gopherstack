package rds

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

func openLogin(l EngineLogin) (*sql.DB, error) {
	if l.Kind == enginePostgres {
		u := url.URL{
			Scheme:   postgresScheme,
			User:     url.UserPassword(l.User, l.Password),
			Host:     l.Addr,
			Path:     "/" + l.dbOrDefault(),
			RawQuery: "sslmode=disable&connect_timeout=5",
		}

		return sql.Open("pgx", u.String())
	}

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = l.User, l.Password, "tcp", l.Addr
	cfg.DBName, cfg.InterpolateParams = l.DBName, true

	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql connector: %w", err)
	}

	return sql.OpenDB(conn), nil
}

func (l EngineLogin) dbOrDefault() string {
	if l.DBName != "" {
		return l.DBName
	}

	return l.User
}

// probeEngine proves the engine is up and the master login works with a real SELECT 1.
func probeEngine(ctx context.Context, l EngineLogin) error {
	if l.Kind != enginePostgres {
		if err := grantMasterPrivileges(ctx, l); err != nil {
			return err
		}
	}

	return selectOne(ctx, l)
}

func selectOne(ctx context.Context, l EngineLogin) error {
	db, err := openLogin(l)
	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	var one int
	if err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("select 1: %w", err)
	}

	return nil
}

// grantMasterPrivileges gives the MySQL/MariaDB master user server-wide rights like an RDS master user.
func grantMasterPrivileges(ctx context.Context, l EngineLogin) error {
	if l.User == mysqlRootUser {
		return nil
	}

	root := EngineLogin{Kind: l.Kind, Addr: l.Addr, User: mysqlRootUser, Password: l.Password}

	db, err := openLogin(root)
	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	if _, err = db.ExecContext(ctx, "GRANT ALL PRIVILEGES ON *.* TO `"+l.User+"`@'%' WITH GRANT OPTION"); err != nil {
		return fmt.Errorf("grant master privileges: %w", err)
	}

	return nil
}

// setEnginePassword changes the login's own password inside the engine.
func setEnginePassword(ctx context.Context, l EngineLogin, newPassword string) error {
	db, err := openLogin(l)
	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	if l.Kind == enginePostgres {
		var stmt string
		err = db.QueryRowContext(ctx,
			"SELECT format('ALTER ROLE CURRENT_USER PASSWORD %L', $1::text)", newPassword).Scan(&stmt)
		if err != nil {
			return fmt.Errorf("quote password: %w", err)
		}

		_, err = db.ExecContext(ctx, stmt)
	} else {
		_, err = db.ExecContext(ctx, "ALTER USER CURRENT_USER() IDENTIFIED BY ?", newPassword)
	}

	if err != nil {
		return fmt.Errorf("alter password: %w", err)
	}

	return nil
}

// silenceDriverLogs stops the MySQL driver printing every refused probe while the container boots.
func silenceDriverLogs() {
	_ = mysql.SetLogger(discardLogger{})
}

type discardLogger struct{}

func (discardLogger) Print(...any) {}
