package bqsql

import (
	"context"
	"database/sql"
	"strings"
)

type dmlTarget struct {
	td    *TableData
	lt    *loadedTable
	rel   *relation
	alias string
}

func (c *compiler) dmlTarget(path []string, alias string, sc *scope) (*dmlTarget, error) {
	ref, err := c.resolveRef(path)
	if err != nil {
		return nil, err
	}

	td, err := c.lookupTable(ref)
	if err != nil {
		return nil, err
	}

	if td.ViewQuery != "" {
		return nil, invalid("Cannot modify view %s", ref)
	}

	if alias == "" {
		alias = ref.Table
	}

	t := &dmlTarget{td: td, lt: c.load(td), alias: alias}
	t.rel = &relation{alias: alias}

	for _, f := range td.Fields {
		t.rel.cols = append(
			t.rel.cols,
			&colInfo{name: f.Name, typ: f.SQLType(), sql: quoteIdent(alias) + "." + quoteIdent(f.Name)},
		)
	}

	sc.rels = append(sc.rels, t.rel)

	return t, nil
}

func stmtName(k StmtKind) string {
	switch k {
	case StmtInsert:
		return litInsert
	case StmtUpdate:
		return litUpdate
	case StmtDelete:
		return litDelete
	case StmtMerge:
		return "MERGE"
	case StmtTruncate:
		return "TRUNCATE_TABLE"
	case StmtQuery, StmtCreateTable, StmtCreateView, StmtCreateSchema, StmtDrop, StmtAlter:
		return litSelect
	}

	return litSelect
}

type dmlStep struct {
	sql  string
	kind byte
}

func (c *compiler) runDML(ctx context.Context, st *Statement, dryRun bool) (*Result, error) {
	sc := &scope{}

	var (
		target *dmlTarget
		steps  []dmlStep
		err    error
	)

	switch st.Kind {
	case StmtInsert:
		target, steps, err = c.planInsert(st, sc)
	case StmtUpdate:
		target, steps, err = c.planUpdate(st, sc)
	case StmtDelete:
		target, steps, err = c.planDelete(st, sc)
	case StmtMerge:
		target, steps, err = c.planMerge(st, sc)
	case StmtTruncate:
		target, steps, err = c.planTruncate(st, sc)
	case StmtQuery, StmtCreateTable, StmtCreateView, StmtCreateSchema, StmtDrop, StmtAlter:
		err = unsupported("statement")
	}

	if err != nil {
		return nil, err
	}

	res := &Result{Kind: st.Kind, Statement: stmtName(st.Kind)}
	if dryRun {
		return res, nil
	}

	db, err := openDB(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err = c.materialize(ctx, db); err != nil {
		return nil, err
	}

	if err = runSteps(ctx, db, steps, res); err != nil {
		return nil, err
	}

	rows, err := queryRows(ctx, db, selectAll(target), target.td.Fields)
	if err != nil {
		return nil, err
	}

	if err = checkRequired(target.td.Fields, rows); err != nil {
		return nil, err
	}

	res.Effects = []Effect{{Op: OpWrite, Ref: target.td.Ref, Fields: target.td.Fields, Rows: rows}}

	return res, nil
}

func selectAll(t *dmlTarget) string {
	names := make([]string, len(t.td.Fields))
	for i, f := range t.td.Fields {
		names[i] = quoteIdent(f.Name)
	}

	return "SELECT " + strings.Join(names, ", ") + " FROM " + quoteIdent(t.lt.name) + " ORDER BY rowid"
}

func checkRequired(fields []Field, rows [][]any) error {
	for _, row := range rows {
		for i, f := range fields {
			if strings.EqualFold(f.Mode, ModeRequired) && row[i] == nil {
				return &Error{Kind: ErrRuntime, Message: "Required field " + f.Name + " cannot be null"}
			}
		}
	}

	return nil
}

func runSteps(ctx context.Context, db *sql.DB, steps []dmlStep, res *Result) error {
	for _, s := range steps {
		r, err := db.ExecContext(ctx, s.sql)
		if err != nil {
			return mapSQLiteErr(err)
		}

		n, _ := r.RowsAffected()

		switch s.kind {
		case 'I':
			res.Inserted += n
		case 'U':
			res.Updated += n
		case 'D':
			res.Deleted += n
		}
	}

	res.Affected = res.Inserted + res.Updated + res.Deleted

	return nil
}

func (c *compiler) planTruncate(st *Statement, sc *scope) (*dmlTarget, []dmlStep, error) {
	t, err := c.dmlTarget(st.target, "", sc)
	if err != nil {
		return nil, nil, err
	}

	return t, []dmlStep{{sql: "DELETE FROM " + quoteIdent(t.lt.name), kind: 'D'}}, nil
}

func (c *compiler) planInsert(st *Statement, sc *scope) (*dmlTarget, []dmlStep, error) {
	t, err := c.dmlTarget(st.target, "", sc)
	if err != nil {
		return nil, nil, err
	}

	cols := st.cols
	if len(cols) == 0 {
		for _, f := range t.td.Fields {
			cols = append(cols, f.Name)
		}
	}

	names := make([]string, len(cols))

	for i, name := range cols {
		f := findField(t.td.Fields, name)
		if f == nil {
			return nil, nil, invalid("Inserted row has wrong column count or unknown column %s", name)
		}

		names[i] = quoteIdent(f.Name)
	}

	head := "INSERT INTO " + quoteIdent(t.lt.name) + " (" + strings.Join(names, ", ") + ") "

	if st.hasValues {
		body, valErr := c.insertValues(st, t, len(cols))
		if valErr != nil {
			return nil, nil, valErr
		}

		return t, []dmlStep{{sql: head + body, kind: 'I'}}, nil
	}

	cq, err := c.compileQuery(st.query, nil)
	if err != nil {
		return nil, nil, err
	}

	if len(cq.cols) != len(cols) {
		return nil, nil, invalid("Inserted row has wrong column count; Has %d, expected %d", len(cq.cols), len(cols))
	}

	return t, []dmlStep{{sql: head + "SELECT * FROM (" + cq.sql + ")", kind: 'I'}}, nil
}

func findField(fields []Field, name string) *Field {
	for i := range fields {
		if strings.EqualFold(fields[i].Name, name) {
			return &fields[i]
		}
	}

	return nil
}

func (c *compiler) insertValues(st *Statement, t *dmlTarget, width int) (string, error) {
	rows := make([]string, len(st.rows))

	for i, row := range st.rows {
		if len(row) != width {
			return "", invalid("Inserted row has wrong column count; Has %d, expected %d", len(row), width)
		}

		vals := make([]string, width)

		for j, e := range row {
			g, err := c.expr(e, &scope{})
			if err != nil {
				return "", err
			}

			vals[j] = c.coerceInsert(g, t, st, j)
		}

		rows[i] = "(" + strings.Join(vals, ", ") + ")"
	}

	return "VALUES " + strings.Join(rows, ", "), nil
}

func (c *compiler) coerceInsert(g *gx, t *dmlTarget, st *Statement, j int) string {
	name := ""
	if len(st.cols) > 0 {
		name = st.cols[j]
	} else if j < len(t.td.Fields) {
		name = t.td.Fields[j].Name
	}

	if f := findField(t.td.Fields, name); f != nil {
		return coerceFor(g, f.SQLType()).sql
	}

	return g.sql
}

func (c *compiler) planUpdate(st *Statement, sc *scope) (*dmlTarget, []dmlStep, error) {
	t, err := c.dmlTarget(st.target, st.alias, sc)
	if err != nil {
		return nil, nil, err
	}

	fromSQL := ""

	if st.from != nil {
		if fromSQL, err = c.compileFrom(st.from, sc); err != nil {
			return nil, nil, err
		}
	}

	sets, err := c.setClauses(st.set, t, sc)
	if err != nil {
		return nil, nil, err
	}

	where, err := c.expr(st.where, sc)
	if err != nil {
		return nil, nil, err
	}

	sql := "UPDATE " + quoteIdent(t.lt.name) + " AS " + quoteIdent(t.alias) + " SET " + sets

	if fromSQL != "" {
		sql += " FROM " + fromSQL
	}

	return t, []dmlStep{{sql: sql + " WHERE " + where.sql, kind: 'U'}}, nil
}

func (c *compiler) setClauses(set []assignment, t *dmlTarget, sc *scope) (string, error) {
	parts := make([]string, len(set))

	for i, a := range set {
		f := findField(t.td.Fields, a.target[0])
		if f == nil {
			return "", invalid("Unrecognized name: %s", a.target[0])
		}

		v, err := c.expr(a.value, sc)
		if err != nil {
			return "", err
		}

		v = coerceFor(v, f.SQLType())
		col := quoteIdent(f.Name)

		if len(a.target) == 1 {
			parts[i] = col + " = " + v.sql

			continue
		}

		var pb strings.Builder

		pb.WriteString("$")

		for _, p := range a.target[1:] {
			pb.WriteString(`."` + p + `"`)
		}

		path := pb.String()

		parts[i] = col + " = json_set(" + col + ", " + quoteStr(path) + ", " + jsonVal(v) + ")"
	}

	return strings.Join(parts, ", "), nil
}

func (c *compiler) planDelete(st *Statement, sc *scope) (*dmlTarget, []dmlStep, error) {
	t, err := c.dmlTarget(st.target, st.alias, sc)
	if err != nil {
		return nil, nil, err
	}

	where, err := c.expr(st.where, sc)
	if err != nil {
		return nil, nil, err
	}

	sql := "DELETE FROM " + quoteIdent(t.lt.name) + " AS " + quoteIdent(t.alias) + " WHERE " + where.sql

	return t, []dmlStep{{sql: sql, kind: 'D'}}, nil
}
