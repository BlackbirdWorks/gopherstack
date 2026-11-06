package athena

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

const (
	posDeleteFilePathID = 2147483546
	posDeletePosID      = 2147483545
	dmlHidden           = 2
	errMultiMatch       = "MERGE_TARGET_ROW_MULTIPLE_MATCHES: One MERGE target table row matched more than one source row"
	nonIcebergDML       = "NOT_SUPPORTED: Modifying Hive table rows is only supported for transactional tables"
)

func isDMLKind(k writeKind) bool {
	return k == writeDelete || k == writeUpdate || k == writeMerge || k == writeOptimize
}

func andWhere(where string) string {
	if where == "" {
		return ""
	}

	return " WHERE (" + where + ")"
}

func (p *writePlan) lookupDML(ctx context.Context, get S3Getter) (*GlueTableDef, *icebergTable, statementOutcome) {
	def, err := p.cat.GetTableDef(p.db, p.table)
	if err != nil {
		return nil, nil, stmtFail(athenaErrTypeEntityMiss,
			"TABLE_NOT_FOUND: line 1:1: Table '%s' does not exist", p.display())
	}

	if tableTypeOf(def) != tableTypeIceberg {
		if p.stmt.kind == writeOptimize {
			return nil, nil, stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: OPTIMIZE is supported only for Iceberg tables")
		}

		return nil, nil, stmtFail(athenaErrTypeSyntax, "%s", nonIcebergDML)
	}

	l := &tableLoader{get: get, limits: defaultEngineLimits()}

	tbl, err := l.openIceberg(ctx, def, p.db+"."+p.table)
	if err != nil {
		return nil, nil, engineFailure(err)
	}

	return def, tbl, stmtOK()
}

// prepareDML resolves the Iceberg target and builds the SELECT that feeds the write.
func (p *writePlan) prepareDML(get S3Getter) statementOutcome {
	_, tbl, out := p.lookupDML(context.Background(), get)
	if out.failed {
		return out
	}

	if _, ok := p.cat.(tableCommitter); !ok {
		return stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: the catalog cannot commit Iceberg metadata")
	}

	cols, err := icebergColumns(tbl.meta.CurrentSchema(), p.display())
	if err != nil {
		return engineFailure(err)
	}

	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}

	d := p.stmt.dml

	switch p.stmt.kind {
	case writeDelete:
		p.stmt.body = "SELECT " + hiddenCols("") + " FROM " + d.targetText + andWhere(d.where)
	case writeUpdate:
		items, uout := updateItems(names, d.sets, "")
		if uout.failed {
			return uout
		}

		p.stmt.body = "SELECT " + hiddenCols("") + ", " + strings.Join(items, ", ") + " FROM " + d.targetText +
			andWhere(d.where)
	case writeOptimize:
		if out = checkPartitionOnly(d.where, tbl.meta, names); out.failed {
			return out
		}

		pred := "1"
		if d.where != "" {
			pred = "(" + d.where + ")"
		}

		p.stmt.body = "SELECT " + hiddenCols("") + ", " + pred + ", " + quotedList(names, "") + " FROM " + d.targetText
	default:
		body, mout := p.mergeBody(names)
		if mout.failed {
			return mout
		}

		p.stmt.body = body
	}

	return stmtOK()
}

func hiddenCols(alias string) string {
	q := ""
	if alias != "" {
		q = quoteIdent(alias) + "."
	}

	return q + quoteIdent(colRowPath) + ", " + q + quoteIdent(colRowPos)
}

func quotedList(names []string, alias string) string {
	q := ""
	if alias != "" {
		q = quoteIdent(alias) + "."
	}

	out := make([]string, len(names))
	for i, n := range names {
		out[i] = q + quoteIdent(n)
	}

	return strings.Join(out, ", ")
}

func updateItems(names []string, sets []dmlAssign, alias string) ([]string, statementOutcome) {
	byCol := map[string]string{}

	for _, s := range sets {
		known := slices.ContainsFunc(names, func(n string) bool { return strings.EqualFold(n, s.col) })
		if !known {
			return nil, stmtFail(athenaErrTypeEntityMiss,
				"COLUMN_NOT_FOUND: line 1:1: Column '%s' cannot be resolved", s.col)
		}

		byCol[strings.ToLower(s.col)] = s.expr
	}

	q := ""
	if alias != "" {
		q = quoteIdent(alias) + "."
	}

	items := make([]string, len(names))

	for i, n := range names {
		if e, ok := byCol[strings.ToLower(n)]; ok {
			items[i] = "(" + e + ")"
		} else {
			items[i] = q + quoteIdent(n)
		}
	}

	return items, stmtOK()
}

func checkPartitionOnly(where string, meta *iceberg.TableMetadata, names []string) statementOutcome {
	if where == "" {
		return stmtOK()
	}

	toks, err := tokenize(where)
	if err != nil {
		return stmtOK()
	}

	part := map[string]bool{}
	spec := meta.DefaultSpec()
	schema := meta.CurrentSchema()

	for _, pf := range spec.Fields {
		for _, f := range schema.Fields {
			if f.ID == pf.SourceID {
				part[strings.ToLower(f.Name)] = true
			}
		}
	}

	for _, t := range significant(toks) {
		if t.kind != tkWord && t.kind != tkQuoted {
			continue
		}

		n := t.ident()
		if slices.ContainsFunc(names, func(c string) bool { return strings.EqualFold(c, n) }) && !part[n] {
			return stmtFail(athenaErrTypeSyntax,
				"NOT_SUPPORTED: line 1:1: Only partition columns are allowed in the OPTIMIZE WHERE clause: %s", n)
		}
	}

	return stmtOK()
}

func (p *writePlan) mergeBody(names []string) (string, statementOutcome) {
	d := p.stmt.dml

	alias := d.targetAlias
	if alias == "" {
		alias = p.stmt.target[len(p.stmt.target)-1]
	}

	hid := quoteIdent(alias) + "." + quoteIdent(colRowPath)
	cases := make([]string, 0, len(d.clauses))
	blocks := make([]string, 0, len(d.clauses)*len(names))

	for i, mc := range d.clauses {
		cond := hid + " IS NULL"
		if mc.matched {
			cond = hid + " IS NOT NULL"
		}

		if mc.cond != "" {
			cond += " AND (" + mc.cond + ")"
		}

		cases = append(cases, fmt.Sprintf("WHEN %s THEN %d", cond, i+1))

		items, out := mergeBlock(mc, names, alias)
		if out.failed {
			return "", out
		}

		blocks = append(blocks, items...)
	}

	sel := hiddenCols(alias) + ", CASE " + strings.Join(cases, " ") + " ELSE 0 END"
	if len(blocks) > 0 {
		sel += ", " + strings.Join(blocks, ", ")
	}

	return "SELECT " + sel + " FROM " + d.source + " LEFT JOIN " + d.targetText + " AS " + quoteIdent(alias) +
		" ON (" + d.on + ")", stmtOK()
}

func mergeBlock(mc mergeClause, names []string, alias string) ([]string, statementOutcome) {
	switch mc.action {
	case mergeUpdate:
		return updateItems(names, mc.sets, alias)
	case mergeInsert:
		cols := mc.insCols
		if len(cols) == 0 {
			cols = names
		}

		if len(mc.insVals) != len(cols) {
			return nil, stmtFail(athenaErrTypeSyntax,
				"line 1:1: Insert query has mismatched column types: Table: [%d], Query: %d column(s)",
				len(cols), len(mc.insVals))
		}

		vals := map[string]string{}

		for i, c := range cols {
			if !slices.ContainsFunc(names, func(n string) bool { return strings.EqualFold(n, c) }) {
				return nil, stmtFail(athenaErrTypeEntityMiss,
					"COLUMN_NOT_FOUND: line 1:1: Column '%s' cannot be resolved", c)
			}

			vals[strings.ToLower(c)] = mc.insVals[i]
		}

		items := make([]string, len(names))

		for i, n := range names {
			if v, ok := vals[strings.ToLower(n)]; ok {
				items[i] = "(" + v + ")"
			} else {
				items[i] = "NULL"
			}
		}

		return items, stmtOK()
	default:
		return nil, stmtOK()
	}
}

// openForApply re-reads the target and fails when it moved since the query read it.
func (p *writePlan) openForApply(ctx context.Context) (*tableLoader, statementOutcome) {
	_, tbl, out := p.lookupDML(ctx, p.get)
	if out.failed {
		return nil, out
	}

	if p.target != nil && tbl.location != p.target.loadedAt {
		return nil, stmtFail(athenaErrTypeSyntax,
			"ICEBERG_COMMIT_ERROR: Failed to commit Iceberg update to table: %s (concurrent modification)", p.display())
	}

	p.ice = tbl

	return &tableLoader{get: p.get, list: p.list, limits: defaultEngineLimits()}, stmtOK()
}

func parsePos(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)

	return n
}

type rowRef struct {
	path string
	pos  int64
}

// applyIcebergDML dispatches DELETE, UPDATE, MERGE and OPTIMIZE once their feeding SELECT has run.
func (p *writePlan) applyIcebergDML(ctx context.Context, res *sqlResult, limits EngineLimits) statementOutcome {
	l, out := p.openForApply(ctx)
	if out.failed {
		return out
	}

	switch p.stmt.kind {
	case writeOptimize:
		return p.applyOptimize(ctx, l, res, limits)
	case writeDelete:
		return p.commitRowChanges(ctx, l, res, nil, limits, iceberg.OpDelete)
	case writeUpdate:
		keep := make([]int, len(res.rows))
		for i := range keep {
			keep[i] = i
		}

		return p.commitRowChanges(ctx, l, res, p.sliceRows(res, keep, dmlHidden), limits, iceberg.OpOverwrite)
	default:
		return p.applyMerge(ctx, l, res, limits)
	}
}

// sliceRows copies the columns from off onward of the chosen rows into a new result.
func (p *writePlan) sliceRows(res *sqlResult, rows []int, off int) *sqlResult {
	out := &sqlResult{columns: res.columns[off:]}

	for _, r := range rows {
		out.rows = append(out.rows, res.rows[r][off:])

		var nulls []bool
		if r < len(res.nulls) && res.nulls[r] != nil {
			nulls = res.nulls[r][off:]
		}

		out.nulls = append(out.nulls, nulls)
	}

	return out
}

func (p *writePlan) applyMerge(ctx context.Context, l *tableLoader, res *sqlResult, limits EngineLimits) statementOutcome {
	d := p.stmt.dml
	ncols := len(p.ice.meta.CurrentSchema().Fields)

	offsets := make([]int, len(d.clauses))
	next := dmlHidden + 1

	for i, mc := range d.clauses {
		offsets[i] = next
		if mc.action != mergeDelete {
			next += ncols
		}
	}

	seen := map[rowRef]bool{}
	removed := map[rowRef]bool{}

	var fresh []int

	var freshOff []int

	for r, row := range res.rows {
		act := int(parsePos(row[dmlHidden]))
		if act == 0 {
			continue
		}

		mc := d.clauses[act-1]

		if mc.matched {
			ref := rowRef{path: row[0], pos: parsePos(row[1])}
			if seen[ref] {
				return stmtFail(athenaErrTypeSyntax, "%s", errMultiMatch)
			}

			seen[ref], removed[ref] = true, true
		}

		if mc.action != mergeDelete {
			fresh, freshOff = append(fresh, r), append(freshOff, offsets[act-1])
		}
	}

	var added *sqlResult

	if len(fresh) > 0 {
		added = &sqlResult{columns: res.columns[offsets[0]:min(len(res.columns), offsets[0]+ncols)]}

		for i, r := range fresh {
			off := freshOff[i]
			added.rows = append(added.rows, res.rows[r][off:off+ncols])

			var nulls []bool
			if r < len(res.nulls) && res.nulls[r] != nil {
				nulls = res.nulls[r][off : off+ncols]
			}

			added.nulls = append(added.nulls, nulls)
		}
	}

	return p.commitRefs(ctx, l, removed, added, limits, iceberg.OpOverwrite)
}

// commitRowChanges removes the rows named by res's leading path/pos columns and appends added.
func (p *writePlan) commitRowChanges(
	ctx context.Context, l *tableLoader, res *sqlResult, added *sqlResult, limits EngineLimits, op string,
) statementOutcome {
	removed := map[rowRef]bool{}
	for _, row := range res.rows {
		removed[rowRef{path: row[0], pos: parsePos(row[1])}] = true
	}

	return p.commitRefs(ctx, l, removed, added, limits, op)
}

func (p *writePlan) commitRefs(
	ctx context.Context, l *tableLoader, removed map[rowRef]bool, added *sqlResult, limits EngineLimits, op string,
) statementOutcome {
	if len(removed) == 0 && (added == nil || len(added.rows) == 0) {
		return stmtOK()
	}

	ch := iceberg.Change{Operation: op}

	if added != nil && len(added.rows) > 0 {
		src := make([]int, len(added.columns))
		for i := range src {
			src[i] = i
		}

		files, out := p.writeIcebergFiles(ctx, added, src, limits)
		if out.failed {
			return out
		}

		ch.AddData = files
	}

	if len(removed) > 0 {
		dels, out := p.positionDeleteFiles(ctx, l, removed)
		if out.failed {
			return out
		}

		ch.AddDeletes = dels
	}

	if out := p.commitIcebergChange(ctx, ch, false); out.failed {
		return out
	}

	return stmtWrote(int64(len(removed)))
}

func (p *writePlan) dataFileIndex(ctx context.Context, l *tableLoader) (map[string]fileEntry, error) {
	snap, ok := p.ice.meta.CurrentSnapshot()
	if !ok {
		return map[string]fileEntry{}, nil
	}

	files, err := l.snapshotFiles(ctx, snap)
	if err != nil {
		return nil, err
	}

	idx := make(map[string]fileEntry, len(files.data))
	for _, f := range files.data {
		idx[f.entry.DataFile.Path] = f
	}

	return idx, nil
}

type deleteGroup struct {
	partition map[string]any
	rows      []rowRef
	specID    int
}

// positionDeleteFiles writes one position delete file per partition of the affected data files.
func (p *writePlan) positionDeleteFiles(
	ctx context.Context, l *tableLoader, removed map[rowRef]bool,
) ([]iceberg.DataFile, statementOutcome) {
	idx, err := p.dataFileIndex(ctx, l)
	if err != nil {
		return nil, engineFailure(err)
	}

	groups := map[string]*deleteGroup{}

	var keys []string

	for ref := range removed {
		fe, ok := idx[ref.path]
		if !ok {
			return nil, stmtFail(athenaErrTypeSyntax,
				"ICEBERG_COMMIT_ERROR: Failed to commit Iceberg update to table: %s (data file %s no longer exists)",
				p.display(), ref.path)
		}

		key := fmt.Sprint(fe.spec, "|", partitionKey(fe.entry.DataFile.Partition))

		g, ok := groups[key]
		if !ok {
			g = &deleteGroup{partition: fe.entry.DataFile.Partition, specID: fe.spec}
			groups[key] = g
			keys = append(keys, key)
		}

		g.rows = append(g.rows, ref)
	}

	sort.Strings(keys)

	meta := p.ice.meta
	out := make([]iceberg.DataFile, 0, len(keys))

	for seq, k := range keys {
		g := groups[k]

		slices.SortFunc(g.rows, func(a, b rowRef) int {
			return cmp.Or(strings.Compare(a.path, b.path), cmp.Compare(a.pos, b.pos))
		})

		res := &sqlResult{columns: []sqlColumn{{name: "file_path"}, {name: "pos"}}}
		idxs := make([]int, len(g.rows))

		for i, ref := range g.rows {
			res.rows = append(res.rows, []string{ref.path, strconv.FormatInt(ref.pos, 10)})
			idxs[i] = i
		}

		cols := []outCol{
			{name: "file_path", typ: "string", src: 0, id: posDeleteFilePathID},
			{name: "pos", typ: "bigint", src: 1, id: posDeletePosID},
		}

		body, eerr := encodeParquetRows(res, idxs, cols, meta.Properties[propParquetCodec])
		if eerr != nil {
			return nil, engineFailure(eerr)
		}

		spec, _ := meta.SpecByID(g.specID)
		path := fmt.Sprintf("%s/data/%s%s_delete_%0*d.parquet", strings.TrimSuffix(meta.Location, "/"),
			icebergPartDir(spec, g.partition), p.queryID, fileSeqWidth, seq)

		if o := p.putObjectAt(ctx, path, body); o.failed {
			return nil, o
		}

		out = append(out, iceberg.DataFile{
			Content: iceberg.ContentPositionDeletes, Path: path, Format: "PARQUET", Partition: g.partition,
			RecordCount: int64(len(g.rows)), FileSize: int64(len(body)),
		})
	}

	return out, stmtOK()
}

func partitionKey(part map[string]any) string {
	names := make([]string, 0, len(part))
	for n := range part {
		names = append(names, n)
	}

	sort.Strings(names)

	var sb strings.Builder

	for _, n := range names {
		fmt.Fprintf(&sb, "%s=%v;", n, part[n])
	}

	return sb.String()
}

// bindTarget marks the plan's table for the target so its rows carry file path and position.
func (p *writePlan) bindTarget(plan *enginePlan) {
	want := p.db + "." + p.table

	for _, t := range plan.tables {
		if t.display == want && t.travel.kind == "" {
			t.rowIDs, p.target = true, t
		}
	}
}
