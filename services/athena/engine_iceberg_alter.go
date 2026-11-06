package athena

import (
	"context"
	"errors"
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
	case c.acceptWords("drop", "column"), c.acceptWords("drop"):
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
	case c.acceptWords("rename", "column"):
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
	case c.acceptWords("change"):
		return parseChangeColumn(c)
	case c.acceptWords("set", "tblproperties"):
		return parseSetProps(c)
	default:
		return nil, false
	}
}

func parseChangeColumn(c *stmtCursor) (alterOp, bool) {
	c.acceptWords("column")

	oldN, newN := c.qualifiedName(), c.qualifiedName()
	if len(oldN) != 1 || len(newN) != 1 {
		return nil, false
	}

	end := topIndex(c.toks, c.pos, "comment", "first", "after")
	typ := typeText(c.toks[c.pos:end])
	c.pos = end

	if typ == "" {
		return nil, false
	}

	var (
		comment string
		first   bool
		after   string
	)

	if c.acceptWords("comment") {
		comment = c.stringLit()
	}

	switch {
	case c.acceptWords("first"):
		first = true
	case c.acceptWords("after"):
		a := c.qualifiedName()
		if len(a) != 1 {
			return nil, false
		}

		after = a[0]
	}

	if !c.done() {
		return nil, false
	}

	return func(m, next *iceberg.TableMetadata) error {
		s, err := next.WithRenamedColumn(m.CurrentSchema(), oldN[0], newN[0], typ)
		if err != nil {
			return err
		}

		if comment != "" {
			for i := range s.Fields {
				if strings.EqualFold(s.Fields[i].Name, newN[0]) {
					s.Fields[i].Doc = comment
				}
			}
		}

		if first || after != "" {
			if s, err = next.WithMovedColumn(s, newN[0], first, after); err != nil {
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

		for k, v := range props {
			next.Properties[k] = v
		}

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
		var ie *icebergError
		if errors.As(err, &ie) {
			return engineFailure(err)
		}

		var se *syntaxError
		if errors.As(err, &se) {
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
