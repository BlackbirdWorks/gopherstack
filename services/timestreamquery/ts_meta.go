package timestreamquery

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
)

func varcharResult(names []string, rows [][]string) *resultSet {
	rs := &resultSet{}
	for _, n := range names {
		rs.cols = append(rs.cols, scalarColumnInfo(n, ScalarTypeVarchar))
	}

	for _, r := range rows {
		data := make([]Datum, len(r))
		for i, v := range r {
			data[i] = ScalarDatum(v)
		}

		rs.rows = append(rs.rows, Row{Data: data})
	}

	return rs
}

// afterKeyword returns the text following the first whole-word kw in q.
func afterKeyword(q, kw string) (string, bool) {
	i := indexKeyword(strings.ToUpper(q), kw)
	if i < 0 {
		return "", false
	}

	return q[i+len(kw):], true
}

func (b *InMemoryBackend) tableSchemaFor(region, ref string, src RecordSource) (tableSchema, error) {
	r, ok := parseRefAfter(ref, 0)
	if !ok {
		return tableSchema{}, validationf("line 1:1: expected database.table")
	}

	if src == nil {
		return deriveSchema(nil), nil
	}

	if !src.DatabaseExists(region, r.database) {
		return tableSchema{}, validationf("line 1:1: Database '%s' does not exist", r.database)
	}

	recs, err := src.TableRecords(region, r.database, r.table, b.clock())
	if err != nil {
		return tableSchema{}, validationf("line 1:1: Table '%s.%s' does not exist", r.database, r.table)
	}

	return deriveSchema(recs), nil
}

func (b *InMemoryBackend) describe(region, q string) (*resultSet, error) {
	schema, err := b.tableSchemaFor(region, strings.TrimSpace(q[len("DESCRIBE"):]), b.recordSource())
	if err != nil {
		return nil, err
	}

	rows := make([][]string, 0, len(schema.cols))
	for _, c := range schema.cols {
		rows = append(rows, []string{c.name, c.typ, c.attr})
	}

	return varcharResult([]string{"Column", "Type", "Timestream attribute type"}, rows), nil
}

func likeMatcher(q string) func(string) bool {
	rest, ok := afterKeyword(q, "LIKE")
	if !ok {
		return func(string) bool { return true }
	}

	rest = strings.TrimSpace(rest)
	if len(rest) < 2 || rest[0] != '\'' {
		return func(string) bool { return true }
	}

	pat := rest[1:]
	if end := strings.IndexByte(pat, '\''); end >= 0 {
		pat = pat[:end]
	}

	re := regexp.MustCompile("^" + strings.NewReplacer("%", ".*", "_", ".").Replace(regexp.QuoteMeta(pat)) + "$")

	return re.MatchString
}

func (b *InMemoryBackend) show(region, q string) (*resultSet, error) {
	src := b.recordSource()
	words := strings.Fields(strings.ToUpper(q))

	if len(words) < argsTwo {
		return nil, validationf("line 1:5: mismatched input '<EOF>'. Expecting: 'DATABASES', 'MEASURES', 'TABLES'")
	}

	switch words[1] {
	case "DATABASES":
		return b.showDatabases(region, q, src), nil
	case "TABLES":
		return b.showTables(region, q, src)
	case "MEASURES":
		return b.showMeasures(region, q, src)
	default:
		return nil, validationf(
			"line 1:6: mismatched input '%s'. Expecting: 'DATABASES', 'MEASURES', 'TABLES'", strings.ToLower(words[1]))
	}
}

func (b *InMemoryBackend) showDatabases(region, q string, src RecordSource) *resultSet {
	var rows [][]string

	if src != nil {
		match := likeMatcher(q)

		for _, d := range src.ListDatabases(region) {
			if match(d) {
				rows = append(rows, []string{d})
			}
		}
	}

	return varcharResult([]string{"DatabaseName"}, rows)
}

func (b *InMemoryBackend) showTables(region, q string, src RecordSource) (*resultSet, error) {
	var rows [][]string

	rest, hasFrom := afterKeyword(q, "FROM")
	if src != nil && hasFrom {
		db, _, ok := parseIdent(strings.TrimSpace(rest), 0)
		if !ok {
			return nil, validationf("line 1:1: expected database name")
		}

		if !src.DatabaseExists(region, db) {
			return nil, validationf("line 1:1: Database '%s' does not exist", db)
		}

		match := likeMatcher(q)

		for _, t := range src.ListTables(region, db) {
			if match(t) {
				rows = append(rows, []string{t})
			}
		}
	}

	return varcharResult([]string{"TableName"}, rows), nil
}

func (b *InMemoryBackend) showMeasures(region, q string, src RecordSource) (*resultSet, error) {
	rest, ok := afterKeyword(q, "FROM")
	if !ok {
		return nil, validationf("line 1:14: mismatched input '<EOF>'. Expecting: 'FROM'")
	}

	schema, err := b.tableSchemaFor(region, strings.TrimSpace(rest), src)
	if err != nil {
		return nil, err
	}

	types := map[string]string{}
	for _, c := range schema.cols {
		types[c.name] = c.typ
	}

	match := likeMatcher(q)

	var rows [][]string

	for _, m := range schema.measures {
		if !match(m.name) {
			continue
		}

		dims := make([]map[string]string, 0, len(m.dims))
		sorted := append([]string(nil), m.dims...)
		sort.Strings(sorted)

		for _, d := range sorted {
			dims = append(dims, map[string]string{"dimension_name": d, "data_type": types[d]})
		}

		enc, _ := json.Marshal(dims)
		rows = append(rows, []string{m.name, m.typ, string(enc)})
	}

	return varcharResult([]string{"measure_name", "data_type", "Dimensions"}, rows), nil
}
