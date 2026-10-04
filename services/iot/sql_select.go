package iot

type selectItem struct {
	expr  sqlNode
	alias []string
	star  bool
}

type setVar struct {
	expr sqlNode
	name string
}

// selectStmt is a parsed SELECT; from is set only for nested queries.
type selectStmt struct {
	from      sqlNode
	sets      []setVar
	where     sqlNode
	fromAlias string
	items     []selectItem
	value     bool
}

// bindVars evaluates the SET clause in order and stores each value for SELECT, WHERE and templates.
func (s *selectStmt) bindVars(c *sqlCtx) {
	if len(s.sets) == 0 {
		return
	}

	c.msg.vars = make(map[string]any, len(s.sets))
	total := 0

	for _, sv := range s.sets {
		v := sv.expr.eval(c)
		total += len(jsonString(v))

		if total > maxVarValueLen {
			c.msg.fail(errSQLFunction, "SET")

			return
		}

		c.msg.vars[sv.name] = v
	}
}

func (s *selectStmt) loneStar() bool {
	return !s.value && len(s.items) == 1 && s.items[0].star
}

// project builds the output value for the context's scope.
func (s *selectStmt) project(c *sqlCtx) any {
	if s.value {
		v := s.items[0].expr.eval(c)
		if _, isArr := v.([]any); isArr && !c.v2016 {
			return sqlUndefined{}
		}

		return v
	}

	out := newSQLObject()

	for _, it := range s.items {
		if it.star {
			mergeStar(out, c)

			continue
		}

		v := it.expr.eval(c)
		if isUndef(v) {
			continue
		}

		setPath(out, it.alias, v)
	}

	return out
}

func mergeStar(out *sqlObject, c *sqlCtx) {
	scope := c.scope
	if !c.loaded {
		scope = c.root()
	}

	if obj, ok := scope.(*sqlObject); ok {
		for _, k := range obj.keys {
			out.set(k, obj.vals[k])
		}
	}
}

func setPath(out *sqlObject, path []string, v any) {
	cur := out

	for _, seg := range path[:len(path)-1] {
		next, ok := cur.vals[seg].(*sqlObject)
		if !ok {
			next = newSQLObject()
			cur.set(seg, next)
		}

		cur = next
	}

	cur.set(path[len(path)-1], v)
}

// passes reports whether the WHERE clause (if any) selects the scope.
func (s *selectStmt) passes(c *sqlCtx) bool {
	if s.where == nil {
		return true
	}

	b, ok := toBoolConv(s.where.eval(c))

	return ok && b
}

// subSelectNode is a nested (SELECT ... FROM path) query.
type subSelectNode struct{ stmt *selectStmt }

func (n subSelectNode) eval(c *sqlCtx) any {
	src, ok := n.stmt.from.eval(c).([]any)
	if !ok {
		return sqlUndefined{}
	}

	out := []any{}

	for _, el := range src {
		ec := c.child(el, n.stmt.fromAlias)
		if !n.stmt.passes(ec) {
			continue
		}

		if v := n.stmt.project(ec); !isUndef(v) {
			out = append(out, v)
		}
	}

	return out
}

// marshalResult renders the projected value as the action payload.
func marshalResult(v any) []byte {
	if isUndef(v) {
		return nil
	}

	return []byte(jsonString(v))
}
