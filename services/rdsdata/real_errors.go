package rdsdata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	pgAuthFailedCode  = "28P01"
	pgInvalidAuthCode = "28000"
	pgCanceledCode    = "57014"
	pgConnClassPrefix = "08"
	pgShutdownPrefix  = "57P"

	mysqlAccessDenied     = 1045
	mysqlDBAccessDenied   = 1044
	mysqlLockWaitTimeout  = 1205
	mysqlMaxExecutionTime = 3024
	mysqlQueryInterrupted = 1317
)

// mapDriverError converts driver failures into the Data API's modeled exceptions
// (API_ExecuteStatement.html Errors) without echoing connection details.
func mapDriverError(err error) error {
	if err == nil {
		return nil
	}

	var (
		api  *apiError
		pg   *pgconn.PgError
		my   *mysql.MySQLError
		nerr net.Error
	)

	switch {
	case errors.As(err, &api):
		return err
	case errors.Is(err, context.DeadlineExceeded):
		return errStatementTimeout
	case errors.Is(err, sql.ErrTxDone):
		return fmt.Errorf("%w: transaction is no longer active", ErrTransactionNotFound)
	case errors.As(err, &pg):
		return mapPGError(pg)
	case errors.As(err, &my):
		return mapMySQLError(my)
	case errors.As(err, &nerr), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, driver.ErrBadConn), errors.Is(err, mysql.ErrInvalidConn):
		return errDatabaseUnavailable
	case isConnectError(err):
		return errDatabaseUnavailable
	default:
		return errDatabaseError("the database returned an error")
	}
}

func mapPGError(pg *pgconn.PgError) error {
	switch {
	case pg.Code == pgAuthFailedCode || pg.Code == pgInvalidAuthCode:
		return errInvalidSecret("the credentials in the secret were rejected by the database")
	case pg.Code == pgCanceledCode:
		return errStatementTimeout
	case strings.HasPrefix(pg.Code, pgConnClassPrefix), strings.HasPrefix(pg.Code, pgShutdownPrefix):
		return errDatabaseUnavailable
	default:
		return errDatabaseError(fmt.Sprintf("%s (SQLSTATE %s)", pg.Message, pg.Code))
	}
}

func mapMySQLError(my *mysql.MySQLError) error {
	switch my.Number {
	case mysqlAccessDenied, mysqlDBAccessDenied:
		return errInvalidSecret("the credentials in the secret were rejected by the database")
	case mysqlLockWaitTimeout, mysqlMaxExecutionTime, mysqlQueryInterrupted:
		return errStatementTimeout
	default:
		return errDatabaseError(fmt.Sprintf("%s (error %d)", my.Message, my.Number))
	}
}

func isConnectError(err error) bool {
	_, ok := errors.AsType[*pgconn.ConnectError](err)

	return ok
}
