package dmsengine

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	sqldriver "github.com/go-sql-driver/mysql"
)

var errBinlog = errors.New("binary log capture unavailable")

type mysqlSource struct {
	db  *sql.DB
	cfg EndpointConfig
}

func mysqlDB(ep EndpointConfig, clientFound bool) (*sql.DB, error) {
	c := sqldriver.NewConfig()
	c.User, c.Passwd, c.Net, c.Addr, c.DBName = ep.User, ep.Password, "tcp", ep.addr(), ep.Database
	c.Timeout = DefaultConnectTimeout
	c.ClientFoundRows = clientFound
	c.Loc = time.UTC

	if tlsRequired(ep.SSLMode) {
		c.TLS = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12} //nolint:gosec // local emulator CA
	}

	conn, err := sqldriver.NewConnector(c)
	if err != nil {
		return nil, err
	}

	return sql.OpenDB(conn), nil
}

func openMySQLSource(_ context.Context, ep EndpointConfig) (Source, error) {
	db, err := mysqlDB(ep, false)
	if err != nil {
		return nil, err
	}

	return &mysqlSource{db: db, cfg: ep}, nil
}

func (s *mysqlSource) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *mysqlSource) Close() error { return s.db.Close() }

func (s *mysqlSource) Release(context.Context) error { return nil }

const mysqlColumnsSQL = "SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.COLUMN_TYPE, c.IS_NULLABLE " +
	"FROM information_schema.COLUMNS c JOIN information_schema.TABLES t " +
	"ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_TYPE = 'BASE TABLE' " +
	"WHERE c.TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys') " +
	"ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION"

const mysqlKeysSQL = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE " +
	"WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys') " +
	"ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"

func (s *mysqlSource) Tables(ctx context.Context) ([]Table, error) {
	rows, err := s.db.QueryContext(ctx, mysqlColumnsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		out   []Table
		index = map[string]int{}
	)

	for rows.Next() {
		var schema, table, col, typ, nullable string
		if err = rows.Scan(&schema, &table, &col, &typ, &nullable); err != nil {
			return nil, err
		}

		if s.cfg.Database != "" && schema != s.cfg.Database {
			continue
		}

		k := schema + "." + table
		i, ok := index[k]

		if !ok {
			i = len(out)
			index[k] = i
			out = append(out, Table{Schema: schema, Name: table})
		}

		out[i].Columns = append(out[i].Columns, Column{Name: col, Native: typ, Kind: KindOf(typ), Nullable: nullable == "YES"})
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return out, s.attachKeys(ctx, out, index)
}

func (s *mysqlSource) attachKeys(ctx context.Context, tables []Table, index map[string]int) error {
	rows, err := s.db.QueryContext(ctx, mysqlKeysSQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table, col string
		if err = rows.Scan(&schema, &table, &col); err != nil {
			return err
		}

		if i, ok := index[schema+"."+table]; ok {
			tables[i].PK = append(tables[i].PK, col)
		}
	}

	return rows.Err()
}

func (s *mysqlSource) Scan(ctx context.Context, t Table, fn func(Row) error) error {
	names := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		names[i] = QuoteIdent(FamilyMySQL, c.Name)
	}

	q := "SELECT " + strings.Join(names, ", ") + " FROM " + QuoteIdent(FamilyMySQL, t.Schema) + "." +
		QuoteIdent(FamilyMySQL, t.Name)

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanRows(rows, t, fn)
}

func scanRows(rows *sql.Rows, t Table, fn func(Row) error) error {
	vals := make([]any, len(t.Columns))
	ptrs := make([]any, len(vals))

	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		row := make(Row, len(vals))
		for i, v := range vals {
			row[i] = textForm(v, t.Columns[i].Kind)
		}

		if err := fn(row); err != nil {
			return err
		}
	}

	return rows.Err()
}

func textForm(v any, k Kind) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		if k == KindBytes {
			return append([]byte(nil), x...)
		}

		return string(x)
	case string:
		if k == KindBytes {
			return []byte(x)
		}

		return x
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04:05.999999")
	case bool:
		return strconv.FormatBool(x)
	default:
		return fmt.Sprint(x)
	}
}

func (s *mysqlSource) firstRow(ctx context.Context, q string) ([]any, error) {
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, rows.Err()
	}

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))

	for i := range vals {
		ptrs[i] = &vals[i]
	}

	return vals, rows.Scan(ptrs...)
}

func (s *mysqlSource) Position(ctx context.Context) (string, error) {
	var format string
	if err := s.db.QueryRowContext(ctx, "SELECT @@GLOBAL.binlog_format").Scan(&format); err != nil {
		return "", err
	}

	if !strings.EqualFold(format, "ROW") {
		return "", fmt.Errorf("%w: binlog_format is %s, ROW is required for CDC", errBinlog, format)
	}

	row, err := s.firstRow(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		row, err = s.firstRow(ctx, "SHOW MASTER STATUS")
	}

	if err != nil {
		return "", err
	}

	if len(row) < 2 {
		return "", fmt.Errorf("%w: binary logging is not enabled on the source", errBinlog)
	}

	return fmt.Sprintf("%s:%s", anyText(row[0]), anyText(row[1])), nil
}

func anyText(v any) string {
	if b, ok := v.([]byte); ok {
		return string(b)
	}

	return fmt.Sprint(v)
}

func parseBinlogPosition(p string) (mysql.Position, bool) {
	i := strings.LastIndexByte(p, ':')
	if i <= 0 {
		return mysql.Position{}, false
	}

	pos, err := strconv.ParseUint(p[i+1:], 10, 32)
	if err != nil {
		return mysql.Position{}, false
	}

	return mysql.Position{Name: p[:i], Pos: uint32(pos)}, true
}

func (s *mysqlSource) startPosition(ctx context.Context, start StreamStart) (mysql.Position, error) {
	if p, ok := parseBinlogPosition(start.Position); ok {
		return p, nil
	}

	if !start.Time.IsZero() {
		row, err := s.firstRow(ctx, "SHOW BINARY LOGS")
		if err != nil || len(row) == 0 {
			return mysql.Position{}, fmt.Errorf("%w: cannot list binary logs", errBinlog)
		}

		return mysql.Position{Name: anyText(row[0]), Pos: 4}, nil
	}

	cur, err := s.Position(ctx)
	if err != nil {
		return mysql.Position{}, err
	}

	p, _ := parseBinlogPosition(cur)

	return p, nil
}

func (s *mysqlSource) isMariaDB(ctx context.Context) bool {
	var v string

	return s.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v) == nil && strings.Contains(strings.ToLower(v), "mariadb")
}

func (s *mysqlSource) syncerConfig(ctx context.Context) (replication.BinlogSyncerConfig, error) {
	host, portStr, err := net.SplitHostPort(s.cfg.addr())
	if err != nil {
		return replication.BinlogSyncerConfig{}, err
	}

	port, _ := strconv.ParseUint(portStr, 10, 16)
	flavor := mysql.MySQLFlavor

	if s.isMariaDB(ctx) {
		flavor = mysql.MariaDBFlavor
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: 2_000_000 + rand.Uint32N(1_000_000), //nolint:gosec // replica ids need uniqueness, not secrecy
		Flavor:   flavor, Host: host, Port: uint16(port), User: s.cfg.User, Password: s.cfg.Password,
		UseDecimal: true, TimestampStringLocation: time.UTC,
	}

	if tlsRequired(s.cfg.SSLMode) {
		cfg.TLSConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12} //nolint:gosec // local emulator CA
	}

	return cfg, nil
}

func (s *mysqlSource) Stream(
	ctx context.Context, start StreamStart, tables func(string, string) (Table, bool), fn func(Change) error,
) error {
	pos, err := s.startPosition(ctx, start)
	if err != nil {
		return err
	}

	cfg, err := s.syncerConfig(ctx)
	if err != nil {
		return err
	}

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("start binlog sync: %w", err)
	}

	file := pos.Name

	for {
		ev, gerr := streamer.GetEvent(ctx)
		if gerr != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // cancellation is the normal stop
			}

			return gerr
		}

		ts := time.Unix(int64(ev.Header.Timestamp), 0).UTC()

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			file = string(e.NextLogName)
		case *replication.XIDEvent:
			if ts.Before(start.Time) {
				continue
			}

			if err = fn(Change{Time: ts, Commit: true, Position: binlogPos(file, ev.Header.LogPos)}); err != nil {
				return err
			}
		case *replication.RowsEvent:
			if ts.Before(start.Time) {
				continue
			}

			if err = s.emitRows(ev, e, file, ts, tables, fn); err != nil {
				return err
			}
		}
	}
}

func binlogPos(file string, pos uint32) string { return fmt.Sprintf("%s:%d", file, pos) }

func (s *mysqlSource) emitRows(
	ev *replication.BinlogEvent, e *replication.RowsEvent, file string, ts time.Time,
	tables func(string, string) (Table, bool), fn func(Change) error,
) error {
	t, ok := tables(string(e.Table.Schema), string(e.Table.Table))
	if !ok {
		return nil
	}

	base := Change{Time: ts, Schema: t.Schema, Table: t.Name, Position: binlogPos(file, ev.Header.LogPos)}
	conv := func(r []any) Row { return binlogRow(r, t) }

	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		for _, r := range e.Rows {
			c := base
			c.Op, c.After = OpInsert, conv(r)

			if err := fn(c); err != nil {
				return err
			}
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
		replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		for _, r := range e.Rows {
			c := base
			c.Op, c.Before = OpDelete, conv(r)

			if err := fn(c); err != nil {
				return err
			}
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		for i := 0; i+1 < len(e.Rows); i += 2 {
			c := base
			c.Op, c.Before, c.After = OpUpdate, conv(e.Rows[i]), conv(e.Rows[i+1])

			if err := fn(c); err != nil {
				return err
			}
		}
	}

	return nil
}

func binlogRow(vals []any, t Table) Row {
	out := make(Row, len(vals))

	for i, v := range vals {
		var col Column
		if i < len(t.Columns) {
			col = t.Columns[i]
		}

		out[i] = binlogValue(v, col)
	}

	return out
}

func binlogValue(v any, col Column) any {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return textForm(x, col.Kind)
	case []byte:
		return textForm(x, col.Kind)
	case int8:
		return intText(int64(x), 8, col)
	case int16:
		return intText(int64(x), 16, col)
	case int32:
		return intText(int64(x), 32, col)
	case int:
		return intText(int64(x), 32, col)
	case int64:
		return intText(x, 64, col)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case fmt.Stringer:
		return x.String()
	default:
		return textForm(v, col.Kind)
	}
}

var unsignedBits = map[string]uint{"tinyint": 8, "smallint": 16, "mediumint": 24, "int": 32, "integer": 32, "bigint": 64}

func intText(v int64, width uint, col Column) string {
	p := parseNative(col.Native)

	switch p.base {
	case "enum":
		return enumName(p.args, v)
	case "set":
		return setNames(p.args, v)
	}

	if p.unsigned && v < 0 {
		bits := unsignedBits[p.base]
		if bits == 0 {
			bits = width
		}

		if bits >= 64 {
			return strconv.FormatUint(uint64(v), 10)
		}

		return strconv.FormatInt(v+(int64(1)<<bits), 10)
	}

	return strconv.FormatInt(v, 10)
}

func enumMembers(args string) []string {
	var out []string

	for _, m := range strings.Split(args, "','") {
		out = append(out, strings.Trim(m, "'"))
	}

	return out
}

func enumName(args string, idx int64) string {
	m := enumMembers(args)
	if idx < 1 || int(idx) > len(m) {
		return ""
	}

	return m[idx-1]
}

func setNames(args string, mask int64) string {
	var names []string

	for i, m := range enumMembers(args) {
		if mask&(1<<uint(i)) != 0 {
			names = append(names, m)
		}
	}

	return strings.Join(names, ",")
}
