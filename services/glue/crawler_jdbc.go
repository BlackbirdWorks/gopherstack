package glue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	jdbcTimeout = 10 * time.Second
	engPostgres = "postgresql"
	engMySQL    = "mysql"
	engRedshift = "redshift"
	pgTablePart = 2
)

var (
	errJDBCSecret       = errors.New("cannot resolve the connection's SECRET_ID")
	errJDBCSecretBody   = errors.New("secret has no username and password")
	errJDBCNoConnection = errors.New("connection not found")
	errJDBCNoURL        = errors.New("connection has no JDBC_CONNECTION_URL")
	errJDBCEngine       = errors.New("unsupported JDBC engine")
)

type jdbcEndpoint struct {
	engine, host, port, database, user, password, secretID string
}

func parseJDBCURL(conn *Connection) (jdbcEndpoint, error) {
	raw := conn.ConnectionProperties["JDBC_CONNECTION_URL"]
	if raw == "" {
		return jdbcEndpoint{}, errJDBCNoURL
	}

	rest, ok := strings.CutPrefix(raw, "jdbc:")
	if !ok {
		return jdbcEndpoint{}, fmt.Errorf("%w: %q", errJDBCEngine, raw)
	}

	u, err := url.Parse(rest)
	if err != nil {
		return jdbcEndpoint{}, fmt.Errorf("parse JDBC URL: %w", err)
	}

	ep := jdbcEndpoint{
		host: u.Hostname(), port: u.Port(), database: strings.TrimPrefix(u.Path, "/"),
		user: conn.ConnectionProperties["USERNAME"], password: conn.ConnectionProperties["PASSWORD"],
		secretID: conn.ConnectionProperties["SECRET_ID"],
	}

	switch u.Scheme {
	case engPostgres, "postgres":
		ep.engine = engPostgres
	case engMySQL:
		ep.engine = engMySQL
	case engRedshift:
		ep.engine = engRedshift
	default:
		return jdbcEndpoint{}, fmt.Errorf("%w: %q", errJDBCEngine, u.Scheme)
	}

	if ep.port == "" {
		ep.port = map[string]string{engPostgres: "5432", engMySQL: "3306", engRedshift: "5439"}[ep.engine]
	}

	return ep, nil
}

func (ep jdbcEndpoint) open(database string) (*sql.DB, error) {
	addr := net.JoinHostPort(ep.host, ep.port)

	if ep.engine == engMySQL {
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = ep.user, ep.password, "tcp", addr, database
		cfg.Timeout = jdbcTimeout

		connector, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, err
		}

		return sql.OpenDB(connector), nil
	}

	dsn := (&url.URL{
		Scheme: "postgres", User: url.UserPassword(ep.user, ep.password), Host: addr, Path: "/" + database,
		RawQuery: "sslmode=prefer&connect_timeout=10",
	}).String()

	return sql.Open("pgx", dsn)
}

type jdbcColumn struct {
	schema, table, name, dataType, udt string
	precision, scale                   sql.NullInt64
}

func jdbcPattern(parts []string, i int) string {
	if i < len(parts) && parts[i] != "" {
		return parts[i]
	}

	return "%"
}

func (r *crawlRun) crawlJDBC(t JDBCTarget) error {
	conn, ok := r.conns[t.ConnectionName]
	if !ok {
		return fmt.Errorf("%w: %q", errJDBCNoConnection, t.ConnectionName)
	}

	ep, err := parseJDBCURL(conn)
	if err != nil {
		return err
	}

	parts := strings.Split(strings.Trim(t.Path, "/"), "/")
	dbName := ep.database

	if p := jdbcPattern(parts, 0); !strings.Contains(p, "%") {
		dbName = p
	}

	ctx, cancel := context.WithTimeout(context.Background(), jdbcTimeout)
	defer cancel()

	if ep.secretID != "" {
		if ep.user, ep.password, err = r.resolveSecretCredentials(ctx, ep.secretID); err != nil {
			return err
		}
	}

	db, err := ep.open(dbName)
	if err != nil {
		return fmt.Errorf("connect to JDBC data store: %w", err)
	}
	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to JDBC data store %s:%s: %w", ep.host, ep.port, err)
	}

	cols, err := ep.introspect(ctx, db, dbName, parts)
	if err != nil {
		return fmt.Errorf("introspect JDBC data store: %w", err)
	}

	return r.writeJDBCTables(t, ep, dbName, cols)
}

func (ep jdbcEndpoint) introspect(
	ctx context.Context,
	db *sql.DB,
	dbName string,
	parts []string,
) ([]jdbcColumn, error) {
	var (
		rows *sql.Rows
		err  error
	)

	if ep.engine == engMySQL {
		rows, err = db.QueryContext(ctx, `SELECT table_schema, table_name, column_name, data_type, column_type,
			numeric_precision, numeric_scale FROM information_schema.columns
			WHERE table_schema LIKE ? AND table_name LIKE ?
			AND table_schema NOT IN ('mysql','sys','performance_schema','information_schema')
			ORDER BY table_schema, table_name, ordinal_position`, dbName, jdbcPattern(parts, 1))
	} else {
		rows, err = db.QueryContext(ctx, `SELECT table_schema, table_name, column_name, data_type, udt_name,
			numeric_precision, numeric_scale FROM information_schema.columns
			WHERE table_catalog = $1 AND table_schema LIKE $2 AND table_name LIKE $3
			AND table_schema NOT IN ('pg_catalog','information_schema')
			ORDER BY table_schema, table_name, ordinal_position`, dbName, jdbcPattern(parts, 1), jdbcPattern(parts, pgTablePart))
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []jdbcColumn

	for rows.Next() {
		var c jdbcColumn
		if err = rows.Scan(&c.schema, &c.table, &c.name, &c.dataType, &c.udt, &c.precision, &c.scale); err != nil {
			return nil, err
		}

		out = append(out, c)
	}

	return out, rows.Err()
}

func jdbcExcluded(c jdbcColumn, engine string, excl []string) bool {
	cands := []string{c.table, c.schema + "." + c.table, c.schema + "/" + c.table}
	if engine == engMySQL {
		cands = []string{c.table}
	}

	for _, p := range excl {
		p = strings.ReplaceAll(p, "%", "*")

		for _, cand := range cands {
			if ok, _ := path.Match(p, cand); ok {
				return true
			}
		}
	}

	return false
}

func (r *crawlRun) writeJDBCTables(t JDBCTarget, ep jdbcEndpoint, dbName string, cols []jdbcColumn) error {
	type tableCols struct {
		name, location string
		cols           []Column
	}

	var order []string

	tables := map[string]*tableCols{}

	for _, c := range cols {
		if jdbcExcluded(c, ep.engine, t.Exclusions) {
			continue
		}

		key, location, name := c.schema+"."+c.table, dbName+"."+c.schema+"."+c.table, dbName+"_"+c.schema+"_"+c.table
		if ep.engine == engMySQL {
			key, location, name = c.table, dbName+"."+c.table, dbName+"_"+c.table
		}

		tc := tables[key]
		if tc == nil {
			tc = &tableCols{name: name, location: location}
			tables[key] = tc
			order = append(order, key)
		}

		tc.cols = append(tc.cols, Column{Name: c.name, Type: jdbcHiveType(ep.engine, c)})
	}

	for _, key := range order {
		tc := tables[key]
		params := map[string]string{
			paramClassification: ep.engine, paramConnectionName: t.ConnectionName,
			paramTypeOfData: typeOfDataTable, crawlerManagedParam: r.c.Name,
		}

		r.upsert(&Table{
			Name: crawlTableName(
				r.c.TablePrefix,
				tc.name,
			),
			DatabaseName: strs.Fold(r.c.DatabaseName),
			Parameters:   params,
			StorageDescriptor: StorageDescriptor{
				Columns: tc.cols, Location: tc.location,
				Parameters: map[string]string{paramConnectionName: t.ConnectionName, crawlerManagedParam: r.c.Name},
			},
		})
	}

	return nil
}

func jdbcHiveType(engine string, c jdbcColumn) string {
	if engine == engMySQL {
		return mysqlHiveType(c)
	}

	return pgHiveType(c)
}

func pgHiveType(c jdbcColumn) string {
	if c.dataType == "ARRAY" {
		return "array<" + pgScalarType(strings.TrimPrefix(c.udt, "_"), c) + ">"
	}

	return pgScalarType(c.udt, c)
}

func pgScalarType(udt string, c jdbcColumn) string {
	switch udt {
	case "int2":
		return typeSmallint
	case "int4":
		return avroInt
	case "int8":
		return typeBigint
	case "float4":
		return avroFloat
	case "float8":
		return avroDouble
	case "bool":
		return avroBoolean
	case typeDate:
		return typeDate
	case typeTimestamp, "timestamptz":
		return typeTimestamp
	case "bytea":
		return typeBinary
	case "numeric":
		if c.precision.Valid {
			return fmt.Sprintf("decimal(%d,%d)", c.precision.Int64, c.scale.Int64)
		}

		return avroDouble
	default:
		return avroString
	}
}

func mysqlHiveType(c jdbcColumn) string {
	switch c.dataType {
	case typeTinyint, typeSmallint, avroInt, avroFloat, avroDouble, typeDate:
		return c.dataType
	case "mediumint":
		return avroInt
	case "bigint":
		return typeBigint
	case "decimal":
		return fmt.Sprintf("decimal(%d,%d)", c.precision.Int64, c.scale.Int64)
	case "datetime", typeTimestamp:
		return typeTimestamp
	case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary":
		return typeBinary
	default:
		return avroString
	}
}

func (r *crawlRun) resolveSecretCredentials(ctx context.Context, secretID string) (string, string, error) {
	if r.secrets == nil {
		return "", "", fmt.Errorf("%w %q: Secrets Manager is not available", errJDBCSecret, secretID)
	}

	raw, err := r.secrets.SecretString(ctx, r.region, secretID)
	if err != nil {
		return "", "", fmt.Errorf("%w %q: %w", errJDBCSecret, secretID, err)
	}

	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err = decodeJSON([]byte(raw), &body); err != nil || body.Username == "" {
		return "", "", fmt.Errorf("%w %q: %w", errJDBCSecret, secretID, errJDBCSecretBody)
	}

	return body.Username, body.Password, nil
}
