package bqsql

import (
	"strings"
)

const maxRoutineDepth = 32

type frame struct {
	vars   map[string]*gx
	tables map[string]*cteDef
}

func (c *compiler) topFrame() *frame {
	if len(c.frames) == 0 {
		return nil
	}

	return c.frames[len(c.frames)-1]
}

func parseExprText(src string) (*node, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}

	p := &parser{src: src, toks: toks}

	n, err := p.expr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tkEOF {
		return nil, p.unexpected("unexpected input after expression")
	}

	return n, nil
}

func (c *compiler) routineRef(parts []string) (TableRef, error) {
	switch len(parts) {
	case 1:
		if c.dataset == "" {
			return TableRef{}, invalid("Routine %q must be qualified with a dataset (e.g. dataset.routine).", parts[0])
		}

		return TableRef{Project: c.project, Dataset: c.dataset, Table: parts[0]}, nil
	case 2: //nolint:mnd // dataset.routine
		return TableRef{Project: c.project, Dataset: parts[0], Table: parts[1]}, nil
	case 3: //nolint:mnd // project.dataset.routine
		return TableRef{Project: parts[0], Dataset: parts[1], Table: parts[2]}, nil
	}

	return TableRef{}, invalid("Invalid routine name %q", strings.Join(parts, "."))
}

// findRoutine looks a routine up by name: temporary routines first, then the catalog.
func (c *compiler) findRoutine(parts []string) (*RoutineData, bool, error) {
	if len(parts) == 1 {
		if r, ok := c.in.Temp[strings.ToLower(parts[0])]; ok {
			return r, true, nil
		}
	}

	rc, ok := c.in.Catalog.(RoutineCatalog)
	if !ok {
		return nil, false, nil
	}

	ref, err := c.routineRef(parts)
	if err != nil {
		if len(parts) == 1 {
			return nil, false, nil
		}

		return nil, false, err
	}

	r, found := rc.Routine(ref)

	return r, found, nil
}

func nodeParts(n *node) []string {
	if len(n.parts) > 0 {
		return n.parts
	}

	return strings.Split(n.name, ".")
}

func (c *compiler) userCall(n *node, sc *scope) (*gx, error) {
	r, ok, err := c.findRoutine(nodeParts(n))
	if err != nil {
		return nil, err
	}

	if !ok || r.Kind != RoutineScalar {
		return nil, invalid("Function not found: %s at [1:%d]", strings.Join(nodeParts(n), "."), n.pos+1)
	}

	if n.distinct || n.over != nil || n.safe {
		return nil, invalid("Function %s does not support DISTINCT, SAFE. or OVER", strings.Join(nodeParts(n), "."))
	}

	if len(n.args) != len(r.Args) {
		return nil, invalid(
			"No matching signature for function %s. Number of arguments does not match: expected %d, got %d",
			strings.Join(nodeParts(n), "."), len(r.Args), len(n.args),
		)
	}

	args, err := c.args(n, sc)
	if err != nil {
		return nil, err
	}

	return c.inlineScalar(r, args)
}

// castGX casts g to t unless it already has that type.
func (c *compiler) castGX(g *gx, t Type) (*gx, error) {
	if g.typ.Kind == t.Kind && g.typ.Kind != KindUnknown {
		return g, nil
	}

	typ := g.typ

	return c.expr(&node{kind: nCast, castTo: &t, args: []*node{{kind: nRawSQL, str: g.sql, castTo: &typ}}}, nil)
}

func (c *compiler) bindArgs(r *RoutineData, args []*gx) (map[string]*gx, error) {
	vars := make(map[string]*gx, len(args))

	for i, a := range r.Args {
		g := args[i]

		if !a.AnyType && !a.IsTable {
			var err error

			if g, err = c.castGX(g, a.Type); err != nil {
				return nil, err
			}
		}

		vars[strings.ToLower(a.Name)] = g
	}

	return vars, nil
}

// withRoutine runs fn with the routine's frame active and its own project and dataset as defaults.
func (c *compiler) withRoutine(r *RoutineData, f *frame, fn func() error) error {
	if len(c.frames) >= maxRoutineDepth {
		return invalid("Routine %s is nested too deeply; recursion is not allowed", r.Ref.Table)
	}

	saveP, saveD, saveC := c.project, c.dataset, c.ctes

	if !r.Temp {
		c.project, c.dataset = r.Ref.Project, r.Ref.Dataset
	}

	c.ctes = nil
	c.frames = append(c.frames, f)

	defer func() {
		c.project, c.dataset, c.ctes = saveP, saveD, saveC
		c.frames = c.frames[:len(c.frames)-1]
	}()

	return fn()
}

func (c *compiler) inlineScalar(r *RoutineData, args []*gx) (*gx, error) {
	if r.Language != langSQL {
		return nil, unsupported("%s user-defined functions", r.Language)
	}

	body, err := parseExprText(r.Body)
	if err != nil {
		return nil, invalid("Function %s has an invalid body: %v", r.Ref.Table, err)
	}

	vars, err := c.bindArgs(r, args)
	if err != nil {
		return nil, err
	}

	var out *gx

	err = c.withRoutine(r, &frame{vars: vars}, func() error {
		g, gerr := c.expr(body, nil)
		if gerr != nil {
			return gerr
		}

		if r.Returns != nil {
			g, gerr = c.castGX(g, *r.Returns)
		}

		out = g

		return gerr
	})

	return out, err
}

func (c *compiler) fromRoutine(f *fromItem, sc *scope) (string, error) {
	r, ok, err := c.findRoutine(f.path)
	if err != nil {
		return "", err
	}

	if !ok || r.Kind != RoutineTable {
		return "", notFound("Not found: Table function %s", strings.Join(f.path, "."))
	}

	if len(f.args) != len(r.Args) {
		return "", invalid("Table function %s expects %d arguments, got %d", r.Ref.Table, len(r.Args), len(f.args))
	}

	fr := &frame{vars: map[string]*gx{}, tables: map[string]*cteDef{}}

	var prefix []string

	for i, a := range r.Args {
		if a.IsTable {
			def, sql, terr := c.tableArg(a, f.args[i], sc)
			if terr != nil {
				return "", terr
			}

			fr.tables[strings.ToLower(a.Name)] = def
			prefix = append(prefix, sql)

			continue
		}

		g, gerr := c.expr(f.args[i], sc)
		if gerr != nil {
			return "", gerr
		}

		if !a.AnyType {
			if g, gerr = c.castGX(g, a.Type); gerr != nil {
				return "", gerr
			}
		}

		fr.vars[strings.ToLower(a.Name)] = g
	}

	cq, err := c.tvfBody(r, fr)
	if err != nil {
		return "", err
	}

	alias := f.alias
	if alias == "" {
		alias = r.Ref.Table
	}

	sc.rels = append(sc.rels, relFromCompiled(alias, cq))

	sql := cq.sql
	if len(prefix) > 0 {
		sql = "WITH " + strings.Join(prefix, ", ") + " SELECT * FROM (" + sql + ")"
	}

	return "(" + sql + ") AS " + quoteIdent(alias), nil
}

func (c *compiler) tableArg(a RoutineArg, arg *node, sc *scope) (*cteDef, string, error) {
	var q *Query

	switch arg.kind {
	case nTableArg:
		q = &Query{body: &setExpr{sel: &selectStmt{
			items: []selectItem{{expr: &node{kind: nStar}}},
			from:  &fromItem{kind: fromTable, path: arg.parts},
		}}}
	case nSubquery:
		q = arg.query
	default:
		return nil, "", invalid("Argument %s of a table function must be TABLE name or a subquery", a.Name)
	}

	cq, err := c.compileQuery(q, sc.parent)
	if err != nil {
		return nil, "", err
	}

	for _, want := range a.Table {
		if !slicesContainsCol(cq.cols, want.Name) {
			return nil, "", invalid("Table argument %s is missing column %s", a.Name, want.Name)
		}
	}

	names := make([]string, len(cq.cols))
	for i, col := range cq.cols {
		names[i] = col.Name
	}

	sqlNames := uniqueNames(names)
	def := &cteDef{}

	for i, col := range cq.cols {
		def.cols = append(def.cols, &colInfo{name: names[i], typ: col.Type, sql: quoteIdent(sqlNames[i])})
	}

	return def, quoteIdent(a.Name) + "(" + quoteList(sqlNames) + ") AS (" + cq.sql + ")", nil
}

func slicesContainsCol(cols []Column, name string) bool {
	for _, col := range cols {
		if strings.EqualFold(col.Name, name) {
			return true
		}
	}

	return false
}

func (c *compiler) tvfBody(r *RoutineData, fr *frame) (*compiledQuery, error) {
	st, err := ParseStatement(r.Body)
	if err != nil || st.query == nil {
		return nil, invalid("Table function %s has an invalid body", r.Ref.Table)
	}

	var cq *compiledQuery

	err = c.withRoutine(r, fr, func() error {
		var cerr error

		cq, cerr = c.compileQuery(st.query, nil)

		return cerr
	})
	if err != nil {
		return nil, err
	}

	if len(r.ReturnTable) == 0 {
		return cq, nil
	}

	return c.castTable(cq, r.ReturnTable)
}

func (c *compiler) castTable(cq *compiledQuery, want []Column) (*compiledQuery, error) {
	if len(want) != len(cq.cols) {
		return nil, invalid("Table function returns %d columns, expected %d", len(cq.cols), len(want))
	}

	parts := make([]string, len(want))
	cols := make([]Column, len(want))

	for i, w := range want {
		g, err := c.castGX(&gx{sql: quoteIdent(cq.aliases[i]), typ: cq.cols[i].Type}, w.Type)
		if err != nil {
			return nil, err
		}

		parts[i] = g.sql + " AS " + quoteIdent(w.Name)
		cols[i] = Column{Name: w.Name, Type: w.Type}
	}

	names := make([]string, len(want))
	for i, w := range want {
		names[i] = w.Name
	}

	return &compiledQuery{
		sql:     "SELECT " + strings.Join(parts, ", ") + " FROM (" + cq.sql + ")",
		cols:    cols,
		aliases: uniqueNames(names),
	}, nil
}

// CheckRoutine validates a routine definition the way CREATE FUNCTION does: the body must analyse with its arguments.
func CheckRoutine(in *Input, r *RoutineData) error {
	c := newCompiler(in)

	return c.checkRoutine(r)
}

func (c *compiler) checkRoutine(r *RoutineData) error {
	switch r.Kind {
	case RoutineScalar:
		return c.checkScalar(r)
	case RoutineTable:
		return c.checkTable(r)
	case RoutineProcedure:
		return checkProcedure(r)
	}

	return invalid("Unknown routine type %q", r.Kind)
}

func (c *compiler) checkScalar(r *RoutineData) error {
	if r.Language != langSQL {
		return unsupported("%s user-defined functions", r.Language)
	}

	args := make([]*gx, len(r.Args))
	for i, a := range r.Args {
		args[i] = &gx{sql: litNull, typ: a.Type}
		if a.AnyType {
			args[i].typ = scalar(KindString)
		}
	}

	_, err := c.inlineScalar(r, args)

	return err
}

func (c *compiler) checkTable(r *RoutineData) error {
	if r.Language != langSQL {
		return unsupported("%s table functions", r.Language)
	}

	fr := &frame{vars: map[string]*gx{}, tables: map[string]*cteDef{}}

	for _, a := range r.Args {
		switch {
		case a.IsTable:
			def := &cteDef{}

			for _, col := range a.Table {
				def.cols = append(def.cols, &colInfo{name: col.Name, typ: col.Type, sql: quoteIdent(col.Name)})
			}

			fr.tables[strings.ToLower(a.Name)] = def
		case a.AnyType:
			fr.vars[strings.ToLower(a.Name)] = &gx{sql: litNull, typ: scalar(KindString)}
		default:
			fr.vars[strings.ToLower(a.Name)] = &gx{sql: litNull, typ: a.Type}
		}
	}

	_, err := c.tvfBody(r, fr)

	return err
}

func checkProcedure(r *RoutineData) error {
	if r.Language != langSQL {
		return unsupported("%s procedures", r.Language)
	}

	_, err := ParseScript(procedureScript(r.Body))

	return err
}

func procedureScript(body string) string {
	t := strings.TrimSpace(body)
	if len(t) >= len("BEGIN") && strings.EqualFold(t[:len("BEGIN")], "BEGIN") {
		return t
	}

	return "BEGIN\n" + t + "\nEND"
}

func routineEffectName(st *Statement) string {
	switch st.routine.Kind {
	case RoutineTable:
		return "CREATE_TABLE_FUNCTION"
	case RoutineProcedure:
		return "CREATE_PROCEDURE"
	}

	return "CREATE_FUNCTION"
}

func dropRoutineName(kind string) string {
	switch kind {
	case RoutineTable:
		return "DROP_TABLE_FUNCTION"
	case RoutineProcedure:
		return "DROP_PROCEDURE"
	}

	return "DROP_FUNCTION"
}

func (c *compiler) ddlCreateRoutine(st *Statement) ([]Effect, error) {
	r := st.routine
	r.Description = st.options["description"]

	if st.temp {
		if len(st.target) != 1 {
			return nil, invalid("A temporary routine name must not be qualified")
		}

		r.Ref, r.Temp = TableRef{Table: st.target[0]}, true
	} else {
		ref, err := c.routineRef(st.target)
		if err != nil {
			return nil, err
		}

		if skip, cerr := c.routineConflict(ref, st); skip || cerr != nil {
			return nil, cerr
		}

		r.Ref = ref
	}

	if err := c.checkRoutine(r); err != nil {
		return nil, err
	}

	return []Effect{{Op: OpCreateRoutine, Ref: r.Ref, Routine: r, OrReplace: st.orReplace, IfNotExist: st.ifNot}}, nil
}

// routineConflict reports whether CREATE should be skipped (IF NOT EXISTS) or fail because ref already exists.
func (c *compiler) routineConflict(ref TableRef, st *Statement) (bool, error) {
	if !c.in.Catalog.DatasetExists(ref.Project, ref.Dataset) {
		return false, notFound("Not found: Dataset %s:%s was not found in location US", ref.Project, ref.Dataset)
	}

	rc, ok := c.in.Catalog.(RoutineCatalog)
	if !ok {
		return false, nil
	}

	if _, found := rc.Routine(ref); !found || st.orReplace {
		return false, nil
	}

	if st.ifNot {
		return true, nil
	}

	return false, exists("Already Exists: Routine %s", ref)
}

func (c *compiler) ddlDropRoutine(st *Statement) ([]Effect, error) {
	r, ok, err := c.findRoutine(st.target)
	if err != nil {
		return nil, err
	}

	if !ok || r.Kind != st.dropKind {
		if st.ifExists {
			return nil, nil
		}

		return nil, notFound("Not found: Routine %s", strings.Join(st.target, "."))
	}

	return []Effect{{Op: OpDropRoutine, Ref: r.Ref, Routine: r, IfExists: st.ifExists}}, nil
}

// VarScope resolves script variables for the compiler.
type VarScope interface {
	Lookup(name string) (*Param, bool)
}

func (c *compiler) resolveVar(parts []string) (*gx, bool, error) {
	if f := c.topFrame(); f != nil {
		if g, ok := f.vars[strings.ToLower(parts[0])]; ok {
			g2, err := applyFields(g, parts[1:])

			return g2, true, err
		}

		return nil, false, nil
	}

	if c.in.Vars == nil {
		return nil, false, nil
	}

	p, ok := c.in.Vars.Lookup(parts[0])
	if !ok {
		return nil, false, nil
	}

	typ := p.Field.SQLType()
	sql := c.bind(p)

	if typ.Kind == KindStruct || typ.Kind == KindArray {
		sql = "json(" + sql + ")"
	}

	g, err := applyFields(&gx{sql: sql, typ: typ}, parts[1:])

	return g, true, err
}
