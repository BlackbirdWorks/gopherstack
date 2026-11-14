package athena

import (
	"context"
	"errors"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

type alterOp func(m *iceberg.TableMetadata, next *iceberg.TableMetadata) error

func alterFail(err error) statementOutcome {
	switch {
	case errors.Is(err, iceberg.ErrColumnNotFound):
		return stmtFail(athenaErrTypeEntityMiss, "COLUMN_NOT_FOUND: line 1:1: %s", err.Error())
	case errors.Is(err, iceberg.ErrColumnExists):
		return stmtFail(athenaErrTypeSyntax, "line 1:1: %s", err.Error())
	default:
		return stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: line 1:1: %s", err.Error())
	}
}

// parseAlterOp reads the action of ALTER TABLE t <action> for an Iceberg table.
func parseAlterOp(c *stmtCursor) (alterOp, bool) {
	switch {
	case c.acceptWords("add", "columns"):
		return parseAddColumns(c)
	case c.acceptWords("drop", "column"), c.acceptWords("drop"):
		return parseDropColumn(c)
	case c.acceptWords("rename", "column"):
		return parseRenameColumn(c)
	case c.acceptWords("change"):
		return parseChangeColumn(c)
	case c.acceptWords("set", "tblproperties"):
		return parseSetProps(c)
	default:
		return nil, false
	}
}

func parseAddColumns(c *stmtCursor) (alterOp, bool) {
	inner, ok := c.group()
	if !ok || !c.done() {
		return nil, false
	}

	cols := parseColumnTokens(inner)

	return func(m, next *iceberg.TableMetadata) error {
		s := m.CurrentSchema()

		for _, col := range cols {
			var err error
			if s, _, err = next.WithColumn(s, col.Name, col.Type, col.Comment); err != nil {
				return err
			}

			next.LastColumnID++
		}

		next.SetSchema(s)

		return nil
	}, len(cols) > 0
}

func parseDropColumn(c *stmtCursor) (alterOp, bool) {
	name := c.qualifiedName()
	if len(name) != 1 || !c.done() {
		return nil, false
	}

	return func(m, next *iceberg.TableMetadata) error {
		s, err := next.WithoutColumn(m.CurrentSchema(), name[0])
		if err != nil {
			return err
		}

		next.SetSchema(s)

		return nil
	}, true
}

func parseRenameColumn(c *stmtCursor) (alterOp, bool) {
	from := c.qualifiedName()
	if len(from) != 1 || !c.acceptWords("to") {
		return nil, false
	}

	to := c.qualifiedName()
	if len(to) != 1 || !c.done() {
		return nil, false
	}

	return func(m, next *iceberg.TableMetadata) error {
		s, err := next.WithRenamedColumn(m.CurrentSchema(), from[0], to[0], "")
		if err != nil {
			return err
		}

		next.SetSchema(s)

		return nil
	}, true
}

type changeSpec struct {
	oldName, newName, typ, comment, after string
	first                                 bool
}

func parseChangeSpec(c *stmtCursor) (changeSpec, bool) {
	c.acceptWords("column")

	oldN, newN := c.qualifiedName(), c.qualifiedName()
	if len(oldN) != 1 || len(newN) != 1 {
		return changeSpec{}, false
	}

	end := topIndex(c.toks, c.pos, "comment", "first", "after")
	cs := changeSpec{oldName: oldN[0], newName: newN[0], typ: typeText(c.toks[c.pos:end])}
	c.pos = end

	if c.acceptWords("comment") {
		cs.comment = c.stringLit()
	}

	if c.acceptWords("after") {
		a := c.qualifiedName()
		if len(a) != 1 {
			return cs, false
		}

		cs.after = a[0]
	} else {
		cs.first = c.acceptWords("first")
	}

	return cs, cs.typ != "" && c.done()
}

func parseChangeColumn(c *stmtCursor) (alterOp, bool) {
	cs, ok := parseChangeSpec(c)
	if !ok {
		return nil, false
	}

	return func(m, next *iceberg.TableMetadata) error {
		s, err := next.WithRenamedColumn(m.CurrentSchema(), cs.oldName, cs.newName, cs.typ)
		if err != nil {
			return err
		}

		for i := range s.Fields {
			if cs.comment != "" && strings.EqualFold(s.Fields[i].Name, cs.newName) {
				s.Fields[i].Doc = cs.comment
			}
		}

		if cs.first || cs.after != "" {
			if s, err = next.WithMovedColumn(s, cs.newName, cs.first, cs.after); err != nil {
				return err
			}
		}

		next.SetSchema(s)

		return nil
	}, true
}

func parseSetProps(c *stmtCursor) (alterOp, bool) {
	inner, ok := c.group()
	if !ok || !c.done() {
		return nil, false
	}

	raw := map[string]string{}

	for _, part := range splitCommas(inner) {
		sig := significant(part)
		if len(sig) != argsThree || !sig[1].isPunct("=") || sig[0].kind != tkString || sig[2].kind != tkString {
			return nil, false
		}

		raw[stringLiteral(sig[0])] = stringLiteral(sig[2])
	}

	props, err := icebergProps(raw)

	return func(_, next *iceberg.TableMetadata) error {
		if err != nil {
			return err
		}

		maps.Copy(next.Properties, props)

		return nil
	}, ok
}

// execIcebergAlter handles ALTER TABLE on Iceberg tables; handled=false defers other ALTER forms.
func (b *InMemoryBackend) execIcebergAlter(
	query string, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	c, err := newStmtCursor(query)
	if err != nil || !c.acceptWords("alter", "table") {
		return nil, stmtOK(), false
	}

	parts := c.qualifiedName()
	if parts == nil {
		return nil, stmtOK(), false
	}

	save := c.pos

	op, ok := parseAlterOp(c)
	if !ok {
		c.pos = save

		return nil, stmtOK(), false
	}

	tg, out, handled := b.icebergTargetFor(parts, qctx)
	if !handled || out.failed {
		return nil, out, handled
	}

	return &sqlResult{}, tg.alter(context.Background(), op), true
}

func (tg *icebergTarget) alter(ctx context.Context, op alterOp) statementOutcome {
	p := tg.wp

	plan, err := tg.tbl.meta.PlanMetadataUpdate(tg.tbl.location, func(next *iceberg.TableMetadata) error {
		return op(tg.tbl.meta, next)
	}, p.now().UTC())
	if err != nil {
		if _, ok := errors.AsType[*icebergError](err); ok {
			return engineFailure(err)
		}

		if se, ok := errors.AsType[*syntaxError](err); ok {
			return stmtFail(athenaErrTypeSyntax, "%s", se.msg)
		}

		return alterFail(err)
	}

	conflict, out := p.publishPlan(ctx, tg.tbl, plan)
	if out.failed {
		return out
	}

	if conflict {
		return stmtFail(athenaErrTypeSyntax,
			"ICEBERG_COMMIT_ERROR: Failed to commit Iceberg update to table: %s (concurrent modification)", p.display())
	}

	if setter, ok := p.cat.(tableColumnSetter); ok {
		if cols, cerr := icebergColumns(plan.Metadata.CurrentSchema(), p.display()); cerr == nil {
			_ = setter.SetTableColumns(p.db, p.table, cols)
		}
	}

	return stmtOK()
}
