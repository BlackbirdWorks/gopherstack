package timestreamquery

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

const (
	engineTablePrefix = "ts_"
	// minMeteredBytes is the 10 MB minimum Timestream bills for any query that scans data.
	minMeteredBytes = 10 * 1024 * 1024
)

var (
	reNoColumn   = regexp.MustCompile(`no such column: (\S+)`)
	reNoFunction = regexp.MustCompile(`no such function: (\S+)`)
	reSQLiteTag  = regexp.MustCompile(`^SQL logic error: |\s\(\d+\)$`)
)

func validationf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, args...))
}

func tableKey(db, table string) string { return db + "\x00" + table }

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// execute runs a SELECT, DESCRIBE or SHOW statement against the written records.
func (b *InMemoryBackend) execute(
	ctx context.Context, query string, vars map[string]string,
) (*resultSet, error) {
	query = strings.TrimSpace(strings.TrimRight(strings.TrimSpace(query), ";"))
	region := getRegion(ctx, b.defaultRegion)

	switch firstWord(query) {
	case "DESCRIBE":
		return b.describe(region, query)
	case "SHOW":
		return b.show(region, query)
	case "EXPLAIN":
		return nil, validationf("line 1:1: EXPLAIN is not supported")
	}

	now := b.clock()
	src := b.recordSource()

	tables, scanned, err := loadSourceTables(region, src, query, now)
	if err != nil {
		return nil, err
	}

	translated, err := translate(rewriteTimestream(query, now, vars), tables)
	if err != nil {
		return nil, err
	}

	rs, err := runTranslated(ctx, translated, tables)
	if err != nil {
		return nil, err
	}

	rs.scanned = scanned
	rs.tables = len(tables)

	return rs, nil
}

func firstWord(q string) string {
	end := strings.IndexFunc(q, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' })
	if end < 0 {
		end = len(q)
	}

	return strings.ToUpper(q[:end])
}

type tableData struct {
	sqlName string
	schema  tableSchema
	records []SourceRecord
}

type loadedSet map[string]*tableData

func loadSourceTables(
	region string, src RecordSource, query string, now time.Time,
) (loadedSet, int64, error) {
	set := loadedSet{}

	var scanned int64

	for _, ref := range tableRefs(query) {
		key := tableKey(ref.database, ref.table)
		if _, dup := set[key]; dup {
			continue
		}

		var recs []SourceRecord

		if src != nil {
			var err error

			recs, err = src.TableRecords(region, ref.database, ref.table, now)
			if err != nil {
				return nil, 0, validationf("line 1:1: Table '%s.%s' does not exist", ref.database, ref.table)
			}
		}

		sort.SliceStable(recs, func(i, j int) bool { return recs[i].Time.Before(recs[j].Time) })

		for _, r := range recs {
			scanned += recordBytes(r)
		}

		set[key] = &tableData{
			sqlName: engineTablePrefix + strconv.Itoa(len(set)), schema: deriveSchema(recs), records: recs,
		}
	}

	return set, scanned, nil
}

func translate(q string, set loadedSet) (string, error) {
	resolve := func(parts []string) (string, string, bool) {
		if len(parts) != qualifiedParts {
			return "", "", false
		}

		t, ok := set[tableKey(parts[0], parts[1])]
		if !ok {
			return "", "", false
		}

		return "main", t.sqlName, true
	}

	out, unresolved, err := athena.TranslateTrino(q, resolve)
	if err != nil {
		return "", validationf("%s", err.Error())
	}

	if len(unresolved) > 0 {
		if !strings.Contains(unresolved[0], ".") {
			return "", validationf("line 1:15: Schema must be specified when session schema is not set")
		}

		return "", validationf("line 1:15: Table '%s' does not exist", unresolved[0])
	}

	return out, nil
}

func runTranslated(ctx context.Context, q string, set loadedSet) (*resultSet, error) {
	registerTimestreamFuncs()

	db, err := athena.OpenEngineDB()
	if err != nil {
		return nil, fmt.Errorf("timestreamquery: open engine: %w", err)
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("timestreamquery: engine connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	for _, t := range set {
		if err = loadTable(ctx, conn, t); err != nil {
			return nil, fmt.Errorf("timestreamquery: load table: %w", err)
		}
	}

	for _, p := range []string{"PRAGMA case_sensitive_like=ON", "PRAGMA query_only=ON"} {
		if _, err = conn.ExecContext(ctx, p); err != nil {
			return nil, fmt.Errorf("timestreamquery: engine pragma: %w", err)
		}
	}

	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return nil, mapEngineError(err)
	}
	defer func() { _ = rows.Close() }()

	rs, err := readRows(rows)
	if err != nil {
		return nil, mapEngineError(err)
	}

	return rs, nil
}

func loadTable(ctx context.Context, conn *sql.Conn, t *tableData) error {
	decls := make([]string, len(t.schema.cols))
	marks := make([]string, len(t.schema.cols))

	for i, c := range t.schema.cols {
		decls[i] = quoteIdent(c.name) + " " + declOf(c.typ)
		marks[i] = "?"
	}

	target := quoteIdent("main") + "." + quoteIdent(t.sqlName)
	ddl := "CREATE TABLE " + target + " (" + strings.Join(decls, ", ") + ")" //nolint:gosec // quoted identifiers
	if _, err := conn.ExecContext(ctx, ddl); err != nil {
		return err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err = insertRecords(ctx, tx, target, strings.Join(marks, ","), t); err != nil {
		_ = tx.Rollback()

		return err
	}

	return tx.Commit()
}

func insertRecords(ctx context.Context, tx *sql.Tx, target, marks string, t *tableData) error {
	insert := "INSERT INTO " + target + " VALUES (" + marks + ")" //nolint:gosec // quoted identifiers

	stmt, err := tx.PrepareContext(ctx, insert)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for _, r := range t.records {
		if _, err = stmt.ExecContext(ctx, t.schema.rowValues(r)...); err != nil {
			return err
		}
	}

	return nil
}

// mapEngineError turns SQLite failures into Timestream-style ValidationExceptions.
func mapEngineError(err error) error {
	msg := err.Error()

	if m := reNoColumn.FindStringSubmatch(msg); m != nil {
		return validationf("line 1:8: Column '%s' does not exist", strings.Trim(m[1], `"`))
	}

	if m := reNoFunction.FindStringSubmatch(msg); m != nil {
		return validationf("line 1:8: Function '%s' not registered", m[1])
	}

	return validationf("line 1:1: %s", reSQLiteTag.ReplaceAllString(msg, ""))
}
