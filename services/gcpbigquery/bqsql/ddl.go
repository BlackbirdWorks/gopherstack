package bqsql

import (
	"context"
)

func (c *compiler) runDDL(ctx context.Context, st *Statement, dryRun bool) (*Result, error) {
	res := &Result{Kind: st.Kind, Statement: ddlName(st)}

	var (
		eff []Effect
		err error
	)

	switch st.Kind {
	case StmtCreateTable:
		eff, err = c.ddlCreateTable(ctx, st, dryRun)
	case StmtCreateView:
		eff, err = c.ddlCreateView(st)
	case StmtCreateSchema:
		eff, err = c.ddlSchema(st, OpCreateSchema)
	case StmtDrop:
		eff, err = c.ddlDrop(st)
	case StmtAlter:
		eff, err = c.ddlAlter(st)
	case StmtQuery, StmtInsert, StmtUpdate, StmtDelete, StmtMerge, StmtTruncate:
		err = unsupported("statement")
	}

	if err != nil {
		return nil, err
	}

	if !dryRun {
		res.Effects = eff
	}

	return res, nil
}

func ddlName(st *Statement) string {
	switch st.Kind {
	case StmtCreateTable:
		if st.query != nil {
			return "CREATE_TABLE_AS_SELECT"
		}

		return "CREATE_TABLE"
	case StmtCreateView:
		return "CREATE_VIEW"
	case StmtCreateSchema:
		return "CREATE_SCHEMA"
	case StmtDrop:
		return "DROP_" + st.dropKind
	case StmtAlter:
		return "ALTER_TABLE"
	case StmtQuery, StmtInsert, StmtUpdate, StmtDelete, StmtMerge, StmtTruncate:
		return litSelect
	}

	return litSelect
}

// existing reports whether creating ref should proceed, be skipped or fail.
func (c *compiler) existing(ref TableRef, st *Statement) (bool, error) {
	if _, ok := c.in.Catalog.Table(ref); !ok {
		if !c.in.Catalog.DatasetExists(ref.Project, ref.Dataset) {
			return false, notFound("Not found: Dataset %s:%s was not found in location US", ref.Project, ref.Dataset)
		}

		return false, nil
	}

	switch {
	case st.orReplace:
		return false, nil
	case st.ifNot:
		return true, nil
	}

	return false, exists("Already Exists: Table %s", ref)
}

func (c *compiler) ddlCreateTable(ctx context.Context, st *Statement, dryRun bool) ([]Effect, error) {
	ref, err := c.resolveRef(st.target)
	if err != nil {
		return nil, err
	}

	skip, err := c.existing(ref, st)
	if err != nil || skip {
		return nil, err
	}

	e := Effect{Op: OpCreateTable, Ref: ref, OrReplace: st.orReplace, Options: st.options}

	if st.query != nil {
		sub := &Statement{Kind: StmtQuery, query: st.query}

		res, qErr := c.runQuery(ctx, sub, dryRun)
		if qErr != nil {
			return nil, qErr
		}

		e.Fields, e.Rows = res.Fields, res.Rows
		if len(st.colDefs) > 0 {
			e.Fields = colDefFields(st.colDefs)
		}

		return []Effect{e}, nil
	}

	if len(st.colDefs) == 0 {
		return nil, invalid("CREATE TABLE requires a column list or AS SELECT")
	}

	e.Fields = colDefFields(st.colDefs)

	return []Effect{e}, nil
}

func colDefFields(defs []colDef) []Field {
	out := make([]Field, len(defs))

	for i, d := range defs {
		out[i] = TypeField(d.name, d.typ)
		if d.required {
			out[i].Mode = ModeRequired
		}
	}

	return out
}

func (c *compiler) ddlCreateView(st *Statement) ([]Effect, error) {
	ref, err := c.resolveRef(st.target)
	if err != nil {
		return nil, err
	}

	skip, err := c.existing(ref, st)
	if err != nil || skip {
		return nil, err
	}

	cq, err := c.compileQuery(st.query, nil)
	if err != nil {
		return nil, err
	}

	fields := ColumnFields(cq.cols)

	if len(st.cols) > 0 {
		if len(st.cols) != len(fields) {
			return nil, invalid(
				"View column list has %d names but the query returns %d columns",
				len(st.cols),
				len(fields),
			)
		}

		for i := range fields {
			fields[i].Name = st.cols[i]
		}
	}

	return []Effect{{
		Op: OpCreateView, Ref: ref, ViewQuery: st.viewSQL, Fields: fields, OrReplace: st.orReplace, Options: st.options,
	}}, nil
}

func (c *compiler) schemaRef(path []string) (TableRef, error) {
	switch len(path) {
	case 1:
		return TableRef{Project: c.project, Dataset: path[0]}, nil
	case 2: //nolint:mnd // project.dataset
		return TableRef{Project: path[0], Dataset: path[1]}, nil
	}

	return TableRef{}, invalid("Invalid dataset name")
}

func (c *compiler) ddlSchema(st *Statement, op string) ([]Effect, error) {
	ref, err := c.schemaRef(st.target)
	if err != nil {
		return nil, err
	}

	return []Effect{
		{Op: op, Ref: ref, IfNotExist: st.ifNot, IfExists: st.ifExists, Cascade: st.cascade, Options: st.options},
	}, nil
}

func (c *compiler) ddlDrop(st *Statement) ([]Effect, error) {
	if st.dropKind == "SCHEMA" {
		return c.ddlSchema(st, OpDropSchema)
	}

	ref, err := c.resolveRef(st.target)
	if err != nil {
		return nil, err
	}

	td, ok := c.in.Catalog.Table(ref)
	if !ok {
		if st.ifExists {
			return nil, nil
		}

		return nil, notFound("Not found: Table %s", ref)
	}

	op := OpDropTable
	if td.ViewQuery != "" {
		op = OpDropView
	}

	if (st.dropKind == "VIEW") != (td.ViewQuery != "") {
		return nil, invalid("Cannot drop %s with DROP %s", ref, st.dropKind)
	}

	return []Effect{{Op: op, Ref: ref}}, nil
}

func (c *compiler) ddlAlter(st *Statement) ([]Effect, error) {
	ref, err := c.resolveRef(st.target)
	if err != nil {
		return nil, err
	}

	td, ok := c.in.Catalog.Table(ref)
	if !ok {
		if st.ifExists {
			return nil, nil
		}

		return nil, notFound("Not found: Table %s", ref)
	}

	e := Effect{Op: OpAlter, Ref: ref, AlterOp: st.alterOp, Options: st.options, IfNotExist: st.ifNot}

	switch st.alterOp {
	case "ADD":
		e.Fields = append(append([]Field{}, td.Fields...), colDefFields(st.alterCols)...)
		e.Rows = padRows(td.Rows, len(e.Fields))
	case "DROP":
		return c.alterDrop(td, st, e)
	}

	return []Effect{e}, nil
}

func padRows(rows [][]any, width int) [][]any {
	out := make([][]any, len(rows))

	for i, r := range rows {
		nr := make([]any, width)
		copy(nr, r)
		out[i] = nr
	}

	return out
}

func (c *compiler) alterDrop(td *TableData, st *Statement, e Effect) ([]Effect, error) {
	idx := -1

	for i, f := range td.Fields {
		if equalFold(f.Name, st.alterCols[0].name) {
			idx = i
		}
	}

	if idx < 0 {
		return nil, invalid("Column %s not found in table %s", st.alterCols[0].name, td.Ref)
	}

	e.Fields = append(append([]Field{}, td.Fields[:idx]...), td.Fields[idx+1:]...)

	for _, r := range td.Rows {
		nr := append(append([]any{}, r[:idx]...), r[idx+1:]...)
		e.Rows = append(e.Rows, nr)
	}

	return []Effect{e}, nil
}
