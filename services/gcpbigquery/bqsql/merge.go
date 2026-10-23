package bqsql

import (
	"slices"
	"strings"
)

const (
	mergeSource = "_m_src"
	flagDone    = "__done"
	flagMatched = "__matched"
	flagNoMatch = "__nm"
)

func (c *compiler) planMerge(st *Statement, sc *scope) (*dmlTarget, []dmlStep, error) {
	t, err := c.dmlTarget(st.target, st.alias, sc)
	if err != nil {
		return nil, nil, err
	}

	srcRel, setup, err := c.mergeSource(st, sc)
	if err != nil {
		return nil, nil, err
	}

	srcAlias := srcRel.alias
	tq := quoteIdent(t.lt.name)
	sq := quoteIdent(mergeSource)

	on, err := c.expr(st.on, sc)
	if err != nil {
		return nil, nil, err
	}

	steps := append(
		slices.Clone(setup),
		dmlStep{sql: "ALTER TABLE " + tq + " ADD COLUMN " + flagDone + " INTEGER DEFAULT 0"},
		dmlStep{sql: "ALTER TABLE " + tq + " ADD COLUMN " + flagNoMatch + " INTEGER DEFAULT 0"},
		dmlStep{
			sql: "UPDATE " + sq + " AS " + quoteIdent(
				srcAlias,
			) + " SET " + flagMatched + " = 1 WHERE EXISTS (SELECT 1 FROM " +
				tq + " AS " + quoteIdent(
				t.alias,
			) + " WHERE " + on.sql + ")",
		},
		dmlStep{
			sql: "UPDATE " + tq + " AS " + quoteIdent(
				t.alias,
			) + " SET " + flagNoMatch + " = 1 WHERE NOT EXISTS (SELECT 1 FROM " +
				sq + " AS " + quoteIdent(
				srcAlias,
			) + " WHERE " + on.sql + ")",
		},
	)

	whens, err := c.mergeWhens(st, t, srcAlias, on.sql, sc)
	if err != nil {
		return nil, nil, err
	}

	return t, append(steps, whens...), nil
}

// mergeSource materialises the USING relation into a scratch table so ON and WHEN clauses can reference it.
func (c *compiler) mergeSource(st *Statement, sc *scope) (*relation, []dmlStep, error) {
	inner := &scope{}

	srcSQL, err := c.compileFrom(st.source, inner)
	if err != nil {
		return nil, nil, err
	}

	if len(inner.rels) != 1 {
		return nil, nil, invalid("MERGE source must be a table or a subquery")
	}

	rel := inner.rels[0]
	cols := make([]string, len(rel.cols))
	defs := make([]string, len(rel.cols))
	out := &relation{alias: rel.alias}

	for i, col := range rel.cols {
		cols[i] = col.sql + " AS " + quoteIdent(col.name)
		defs[i] = quoteIdent(col.name)
		out.cols = append(
			out.cols,
			&colInfo{name: col.name, typ: col.typ, sql: quoteIdent(rel.alias) + "." + quoteIdent(col.name)},
		)
	}

	steps := []dmlStep{
		{sql: "CREATE TABLE " + quoteIdent(mergeSource) + " AS SELECT " + strings.Join(cols, ", ") + " FROM " + srcSQL},
		{sql: "ALTER TABLE " + quoteIdent(mergeSource) + " ADD COLUMN " + flagMatched + " INTEGER DEFAULT 0"},
		{sql: "ALTER TABLE " + quoteIdent(mergeSource) + " ADD COLUMN " + flagDone + " INTEGER DEFAULT 0"},
	}

	sc.rels = append(sc.rels, out)

	return out, steps, nil
}

func (c *compiler) mergeWhens(st *Statement, t *dmlTarget, srcAlias, on string, sc *scope) ([]dmlStep, error) {
	var steps []dmlStep

	tq, sq := quoteIdent(t.lt.name), quoteIdent(mergeSource)
	ta, sa := quoteIdent(t.alias), quoteIdent(srcAlias)

	for _, w := range st.whens {
		cond := "1"

		if w.cond != nil {
			g, err := c.expr(w.cond, sc)
			if err != nil {
				return nil, err
			}

			cond = g.sql
		}

		var (
			s   []dmlStep
			err error
		)

		switch {
		case w.matched:
			s, err = c.mergeMatched(w, t, tq, sq, ta, sa, on, cond, sc)
		case w.bySource:
			s, err = c.mergeBySource(w, t, tq, ta, cond, sc)
		default:
			s, err = c.mergeInsert(w, t, tq, sq, sa, cond, sc)
		}

		if err != nil {
			return nil, err
		}

		steps = append(steps, s...)
	}

	return steps, nil
}

func (c *compiler) mergeMatched(
	w mergeWhen,
	t *dmlTarget,
	tq, sq, ta, sa, on, cond string,
	sc *scope,
) ([]dmlStep, error) {
	pred := ta + "." + flagDone + " = 0 AND (" + on + ") AND (" + cond + ")"

	if w.action == litDelete {
		sql := "DELETE FROM " + tq + " AS " + ta + " WHERE " + ta + "." + flagDone + " = 0 AND EXISTS (SELECT 1 FROM " +
			sq + " AS " + sa + " WHERE (" + on + ") AND (" + cond + "))"

		return []dmlStep{{sql: sql, kind: 'D'}}, nil
	}

	if w.action != litUpdate {
		return nil, invalid("WHEN MATCHED supports only UPDATE and DELETE")
	}

	sets, err := c.setClauses(w.set, t, sc)
	if err != nil {
		return nil, err
	}

	sql := "UPDATE " + tq + " AS " + ta + " SET " + sets + ", " + flagDone + " = 1 FROM " + sq + " AS " + sa +
		" WHERE " + pred

	return []dmlStep{{sql: sql, kind: 'U'}}, nil
}

func (c *compiler) mergeBySource(w mergeWhen, t *dmlTarget, tq, ta, cond string, sc *scope) ([]dmlStep, error) {
	pred := ta + "." + flagNoMatch + " = 1 AND " + ta + "." + flagDone + " = 0 AND (" + cond + ")"

	switch w.action {
	case litDelete:
		return []dmlStep{{sql: "DELETE FROM " + tq + " AS " + ta + " WHERE " + pred, kind: 'D'}}, nil
	case litUpdate:
		sets, err := c.setClauses(w.set, t, sc)
		if err != nil {
			return nil, err
		}

		return []dmlStep{
			{sql: "UPDATE " + tq + " AS " + ta + " SET " + sets + ", " + flagDone + " = 1 WHERE " + pred, kind: 'U'},
		}, nil
	}

	return nil, invalid("WHEN NOT MATCHED BY SOURCE supports only UPDATE and DELETE")
}

func (c *compiler) mergeInsert(w mergeWhen, t *dmlTarget, tq, sq, sa, cond string, sc *scope) ([]dmlStep, error) {
	if w.action != litInsert {
		return nil, invalid("WHEN NOT MATCHED supports only INSERT")
	}

	var cols, vals []string

	switch {
	case w.insertAll:
		for _, f := range t.td.Fields {
			cols = append(cols, quoteIdent(f.Name))
			vals = append(vals, sa+"."+quoteIdent(f.Name))
		}
	default:
		names := w.insertCol
		if len(names) == 0 {
			for _, f := range t.td.Fields {
				names = append(names, f.Name)
			}
		}

		if len(names) != len(w.insertVal) {
			return nil, invalid("MERGE INSERT has %d columns but %d values", len(names), len(w.insertVal))
		}

		for i, name := range names {
			f := findField(t.td.Fields, name)
			if f == nil {
				return nil, invalid("Unrecognized name: %s", name)
			}

			g, err := c.expr(w.insertVal[i], sc)
			if err != nil {
				return nil, err
			}

			cols = append(cols, quoteIdent(f.Name))
			vals = append(vals, coerceFor(g, f.SQLType()).sql)
		}
	}

	pred := sa + "." + flagMatched + " = 0 AND " + sa + "." + flagDone + " = 0 AND (" + cond + ")"

	return []dmlStep{
		{
			sql: "INSERT INTO " + tq + " (" + strings.Join(
				cols,
				", ",
			) + ") SELECT " + strings.Join(
				vals,
				", ",
			) + " FROM " + sq + " AS " + sa + " WHERE " + pred,
			kind: 'I',
		},
		{sql: "UPDATE " + sq + " AS " + sa + " SET " + flagDone + " = 1 WHERE " + pred},
	}, nil
}
