package dmsengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

var errLogical = errors.New("logical decoding unavailable")

const defaultPGPoll = 250 * time.Millisecond

type pgSource struct {
	db  *sql.DB
	cfg EndpointConfig
}

func pgDB(ep EndpointConfig) (*sql.DB, error) {
	mode := "disable"
	if tlsRequired(ep.SSLMode) {
		mode = "require"
	}

	c, err := pgx.ParseConfig("sslmode=" + mode + " connect_timeout=" + strconv.Itoa(int(DefaultConnectTimeout.Seconds())))
	if err != nil {
		return nil, err
	}

	c.Host, c.User, c.Password, c.Database = ep.Server, ep.User, ep.Password, ep.Database
	if ep.Port != 0 {
		c.Port = uint16(ep.Port)
	}

	return stdlib.OpenDB(*c), nil
}

func openPGSource(_ context.Context, ep EndpointConfig) (*pgSource, error) {
	db, err := pgDB(ep)
	if err != nil {
		return nil, err
	}

	return &pgSource{db: db, cfg: ep}, nil
}

func (s *pgSource) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *pgSource) Close() error { return s.db.Close() }

const pgColumnsSQL = "SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull " +
	"FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
	"JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped " +
	"WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') " +
	"AND n.nspname NOT LIKE 'pg_toast%' ORDER BY n.nspname, c.relname, a.attnum"

const pgKeysSQL = "SELECT n.nspname, c.relname, a.attname FROM pg_index i " +
	"JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace " +
	"JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true " +
	"JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum " +
	"WHERE i.indisprimary AND n.nspname NOT IN ('pg_catalog','information_schema') ORDER BY n.nspname, c.relname, k.ord"

func (s *pgSource) Tables(ctx context.Context) ([]Table, error) {
	rows, err := s.db.QueryContext(ctx, pgColumnsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		out   []Table
		index = map[string]int{}
	)

	for rows.Next() {
		var schema, table, col, typ string

		var nullable bool
		if err = rows.Scan(&schema, &table, &col, &typ, &nullable); err != nil {
			return nil, err
		}

		k := schema + "." + table
		i, ok := index[k]

		if !ok {
			i = len(out)
			index[k] = i
			out = append(out, Table{Schema: schema, Name: table})
		}

		out[i].Columns = append(out[i].Columns, Column{Name: col, Native: typ, Kind: KindOf(typ), Nullable: nullable})
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	keys, err := s.db.QueryContext(ctx, pgKeysSQL)
	if err != nil {
		return nil, err
	}
	defer keys.Close()

	for keys.Next() {
		var schema, table, col string
		if err = keys.Scan(&schema, &table, &col); err != nil {
			return nil, err
		}

		if i, ok := index[schema+"."+table]; ok {
			out[i].PK = append(out[i].PK, col)
		}
	}

	return out, keys.Err()
}

func (s *pgSource) Scan(ctx context.Context, t Table, fn func(Row) error) error {
	names := make([]string, len(t.Columns))

	for i, c := range t.Columns {
		q := QuoteIdent(FamilyPostgres, c.Name)
		if c.Kind != KindBytes {
			q += "::text"
		}

		names[i] = q
	}

	q := "SELECT " + strings.Join(names, ", ") + " FROM " + QuoteIdent(FamilyPostgres, t.Schema) + "." +
		QuoteIdent(FamilyPostgres, t.Name)

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanRows(rows, t, fn)
}

func (s *pgSource) slot() string { return s.cfg.SlotName }

func (s *pgSource) Position(ctx context.Context) (string, error) {
	var level string
	if err := s.db.QueryRowContext(ctx, "SHOW wal_level").Scan(&level); err != nil {
		return "", err
	}

	if level != "logical" {
		return "", fmt.Errorf("%w: wal_level is %s, logical is required for CDC", errLogical, level)
	}

	var exists bool

	err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", s.slot()).
		Scan(&exists)
	if err != nil {
		return "", err
	}

	if !exists {
		if _, err = s.db.ExecContext(ctx, "SELECT pg_create_logical_replication_slot($1, 'test_decoding')", s.slot()); err != nil {
			return "", err
		}
	}

	var lsn string
	if err = s.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
		return "", err
	}

	return lsn, nil
}

func (s *pgSource) Release(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx,
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", s.slot())

	return err
}

func (s *pgSource) Stream(
	ctx context.Context, start StreamStart, tables func(string, string) (Table, bool), fn func(Change) error,
) error {
	if start.Position == "" && start.Time.IsZero() {
		if _, err := s.Position(ctx); err != nil {
			return err
		}
	}

	poll := s.cfg.PollInterval
	if poll <= 0 {
		poll = defaultPGPoll
	}

	tick := time.NewTicker(poll)
	defer tick.Stop()

	for {
		if err := s.drain(ctx, start, tables, fn); err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (s *pgSource) drain(
	ctx context.Context, start StreamStart, tables func(string, string) (Table, bool), fn func(Change) error,
) error {
	rows, err := s.db.QueryContext(ctx,
		"SELECT lsn::text, data FROM pg_logical_slot_get_changes($1, NULL, NULL, 'include-timestamp', '1', 'skip-empty-xacts', '1')",
		s.slot())
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		batch  []decoded
		stamp  time.Time
		failed error
	)

	for rows.Next() {
		var lsn, data string
		if err = rows.Scan(&lsn, &data); err != nil {
			return err
		}

		d, ok := parseDecoded(lsn, data, tables)
		if !ok {
			continue
		}

		if d.commit {
			stamp = d.when
			failed = emitBatch(batch, stamp, start, fn, d.lsn)
			batch = batch[:0]

			if failed != nil {
				return failed
			}

			continue
		}

		batch = append(batch, d)
	}

	return rows.Err()
}

type decoded struct {
	when   time.Time
	change Change
	lsn    string
	commit bool
}

func emitBatch(batch []decoded, stamp time.Time, start StreamStart, fn func(Change) error, commitLSN string) error {
	if stamp.Before(start.Time) {
		return nil
	}

	for _, d := range batch {
		d.change.Time, d.change.Position = stamp, d.lsn
		if err := fn(d.change); err != nil {
			return err
		}
	}

	return fn(Change{Time: stamp, Commit: true, Position: commitLSN})
}

var commitRe = regexp.MustCompile(`^COMMIT \d+(?: \(at (.+)\))?$`)

func parseDecoded(lsn, data string, tables func(string, string) (Table, bool)) (decoded, bool) {
	if m := commitRe.FindStringSubmatch(data); m != nil {
		when := time.Now().UTC()
		if t, err := time.Parse("2006-01-02 15:04:05.999999-07", m[1]); err == nil {
			when = t.UTC()
		}

		return decoded{commit: true, when: when, lsn: lsn}, true
	}

	rest, ok := strings.CutPrefix(data, "table ")
	if !ok {
		return decoded{}, false
	}

	schema, table, rest, ok := splitQualified(rest)
	if !ok {
		return decoded{}, false
	}

	t, ok := tables(schema, table)
	if !ok {
		return decoded{}, false
	}

	opName, body, _ := strings.Cut(strings.TrimPrefix(rest, " "), ": ")
	c := Change{Schema: t.Schema, Table: t.Name}

	switch opName {
	case "INSERT":
		c.Op, c.After = OpInsert, tupleRow(parseTuple(body), t)
	case "DELETE":
		c.Op, c.Before = OpDelete, tupleRow(parseTuple(body), t)
	case "UPDATE":
		c.Op = OpUpdate

		if oldPart, newPart, hasOld := strings.Cut(body, " new-tuple: "); hasOld {
			c.Before = tupleRow(parseTuple(strings.TrimPrefix(oldPart, "old-key: ")), t)
			c.After = tupleRow(parseTuple(newPart), t)
		} else {
			c.After = tupleRow(parseTuple(body), t)
		}
	default:
		return decoded{}, false
	}

	return decoded{change: c, lsn: lsn}, true
}

func splitQualified(s string) (string, string, string, bool) {
	schema, rest, ok := readIdent(s)
	if !ok || !strings.HasPrefix(rest, ".") {
		return "", "", "", false
	}

	table, rest, ok := readIdent(rest[1:])
	if !ok || !strings.HasPrefix(rest, ":") {
		return "", "", "", false
	}

	return schema, table, rest[1:], true
}

func readIdent(s string) (string, string, bool) {
	if strings.HasPrefix(s, `"`) {
		var b strings.Builder

		for i := 1; i < len(s); i++ {
			if s[i] == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					b.WriteByte('"')
					i++

					continue
				}

				return b.String(), s[i+1:], true
			}

			b.WriteByte(s[i])
		}

		return "", "", false
	}

	end := strings.IndexAny(s, ".:[ ")
	if end < 0 {
		return "", "", false
	}

	return s[:end], s[end:], true
}

// parseTuple reads `name[type]:value ...` pairs; values are quoted (” escapes) or bare tokens.
func parseTuple(s string) map[string]any {
	out := map[string]any{}
	s = strings.TrimSpace(s)

	for s != "" {
		name, rest, ok := readIdent(s)
		if !ok || !strings.HasPrefix(rest, "[") {
			break
		}

		depth, i := 0, 0

		for ; i < len(rest); i++ {
			if rest[i] == '[' {
				depth++
			} else if rest[i] == ']' {
				depth--
				if depth == 0 {
					break
				}
			}
		}

		if i >= len(rest) || i+1 >= len(rest) || rest[i+1] != ':' {
			break
		}

		val, tail := readValue(rest[i+2:])
		out[name] = val
		s = strings.TrimLeft(tail, " ")
	}

	return out
}

func readValue(s string) (any, string) {
	if strings.HasPrefix(s, "'") {
		var b strings.Builder

		for i := 1; i < len(s); i++ {
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					b.WriteByte('\'')
					i++

					continue
				}

				return b.String(), s[i+1:]
			}

			b.WriteByte(s[i])
		}

		return b.String(), ""
	}

	tok, tail, _ := strings.Cut(s, " ")
	if tok == "null" {
		return nil, tail
	}

	return tok, tail
}

func tupleRow(m map[string]any, t Table) Row {
	r := make(Row, len(t.Columns))

	for i, c := range t.Columns {
		v := m[c.Name]
		if c.Kind == KindBytes {
			if str, ok := v.(string); ok {
				v = decodeBytea(str)
			}
		}

		r[i] = v
	}

	return r
}

func decodeBytea(s string) any {
	h, ok := strings.CutPrefix(s, `\x`)
	if !ok {
		return []byte(s)
	}

	out := make([]byte, 0, len(h)/2)

	for i := 0; i+1 < len(h); i += 2 {
		n, err := strconv.ParseUint(h[i:i+2], 16, 8)
		if err != nil {
			return []byte(s)
		}

		out = append(out, byte(n))
	}

	return out
}
