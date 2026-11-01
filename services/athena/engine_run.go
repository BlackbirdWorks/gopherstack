package athena

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var (
	errResultRows  = errors.New("result row limit exceeded")
	errResultBytes = errors.New("result size limit exceeded")
	noSuchColumn   = regexp.MustCompile(`no such column: (\S+)`)
	noSuchFunction = regexp.MustCompile(`no such function: (\S+)`)
)

type planTable struct {
	def       *GlueTableDef
	schema    string
	name      string
	display   string
	metaTable string
	travel    travelSpec
	parts     []GluePartitionDef
}

type enginePlan struct {
	get    S3Getter
	list   S3Lister
	write  *writePlan
	sql    string
	tables []*planTable
}

// standalone reports a SELECT that needs no table (no FROM, or only UNNEST),
// which the legacy in-memory evaluator cannot run.
func (p *enginePlan) standalone() bool {
	toks, err := tokenize(p.sql)
	if err != nil {
		return false
	}

	depth := 0

	for _, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isWord(kwFrom):
			return strings.Contains(p.sql, unnestModuleName+"(")
		}
	}

	return true
}

// glueSchema maps a Glue database to the attached SQLite schema name.
func glueSchema(db string) string {
	if db == "main" || db == "temp" {
		return "glue_" + db
	}

	return db
}

func normalizeCatalog(c string) string {
	if strings.EqualFold(c, awsDataCatalog) {
		return awsDataCatalog
	}

	return c
}

// planEngineQuery translates a SELECT over Glue tables with S3 locations, or
// a CTAS/INSERT/UNLOAD whose source is such a SELECT. It returns planned=false
// when the statement should take the legacy in-memory path.
func (b *InMemoryBackend) planEngineQuery(
	query string, qctx QueryExecutionContext,
) (*enginePlan, statementOutcome, bool) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	isRead := hasKeyword(upper, "SELECT") || hasKeyword(upper, "WITH")

	if !isRead && !hasKeyword(upper, "CREATE") && !hasKeyword(upper, "INSERT") && !hasKeyword(upper, "UNLOAD") {
		return nil, stmtOK(), false
	}

	b.mu.RLock("planEngineQuery")
	defer b.mu.RUnlock()

	cat, get, list, ok := b.engineDeps()
	if !ok {
		return nil, stmtOK(), false
	}

	if !isRead {
		return b.planWrite(query, qctx, cat, get, list)
	}

	plan, unresolved, err := b.buildPlan(query, qctx, get, list)
	if err != nil {
		return nil, planFailure(err), true
	}

	if len(plan.tables) == 0 && !plan.standalone() {
		if len(unresolved) > 0 && strings.HasPrefix(strings.ToLower(unresolved[0]), s3TablesCatalogPrefix) {
			return nil, stmtFail(athenaErrTypeEntityMiss,
				"TABLE_NOT_FOUND: line 1:15: Table '%s' does not exist", unresolved[0]), true
		}

		return nil, stmtOK(), false
	}

	if len(unresolved) > 0 {
		return nil, stmtFail(athenaErrTypeEntityMiss,
			"TABLE_NOT_FOUND: line 1:15: Table '%s' does not exist", unresolved[0]), true
	}

	return plan, stmtOK(), true
}

func planFailure(err error) statementOutcome {
	if pe, ok := errors.AsType[*projectionError](err); ok {
		return stmtFail(athenaErrTypeSyntax, "%s", pe.Error())
	}

	return stmtFail(athenaErrTypeSyntax, "SYNTAX_ERROR: %s", err.Error())
}

// buildPlan translates one SELECT and resolves its Glue-backed tables.
// Callers must hold b.mu.
func (b *InMemoryBackend) buildPlan(
	query string, qctx QueryExecutionContext, get S3Getter, list S3Lister,
) (*enginePlan, []string, error) {
	plan := &enginePlan{get: get, list: list}
	seen := map[string]*planTable{}

	resolve := func(parts []string) resolvedRef {
		parts, travel := splitTravel(parts)
		catalog, db, table := resolveTableRef(strings.Join(parts, "."), qctx)
		catalog, db, table = normalizeCatalog(catalog), strings.ToLower(db), strings.ToLower(table)

		display := strings.ToLower(catalog) + "." + db + "." + table
		tcat, tok := b.tableCatalogFor(catalog)

		if db == "information_schema" || !tok {
			return resolvedRef{}
		}

		if _, legacy := b.tableData[catalog+"/"+db+"/"+table]; legacy {
			return resolvedRef{}
		}

		base, metaTable, _ := icebergMetaSuffix(table)
		key := db + "." + table + "|" + travel.kind + travel.value

		if pt, dup := seen[key]; dup {
			return resolvedRef{schema: pt.schema, name: pt.name, found: true}
		}

		def, err := tcat.GetTableDef(db, base)
		if err != nil || def.Location == "" {
			return resolvedRef{display: display}
		}

		pt := &planTable{def: def, schema: glueSchema(db), name: table, display: db + "." + base, metaTable: metaTable}
		pt.travel = travel

		if travel.kind != "" {
			pt.name = fmt.Sprintf("%s@%d", table, len(plan.tables))
		}

		if len(def.PartitionKeys) > 0 && tableTypeOf(def) == "" {
			pt.parts, _ = tcat.GetPartitionDefs(db, table)
		}

		seen[key] = pt
		plan.tables = append(plan.tables, pt)

		return resolvedRef{schema: pt.schema, name: pt.name, found: true, structCols: rowColumns(def)}
	}

	sqlText, unresolved, err := translateSQL(query, resolve)
	if err != nil {
		return nil, nil, err
	}

	plan.sql = sqlText

	if err = applyProjection(plan, query, b.now().UTC()); err != nil {
		return nil, nil, err
	}

	return plan, unresolved, nil
}

func execAll(ctx context.Context, conn *sql.Conn, stmts ...string) error {
	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			return err
		}
	}

	return nil
}

// run executes the plan against a fresh in-memory SQLite database.
func (p *enginePlan) run(ctx context.Context, limits EngineLimits) (*sqlResult, int64, statementOutcome) {
	registerEngineFuncs()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, 0, engineFailure(err)
	}
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, 0, engineFailure(err)
	}
	defer func() { _ = conn.Close() }()

	if p.write != nil {
		out, skip := p.write.precheck(ctx)
		if out.failed || skip {
			return &sqlResult{}, 0, out
		}
	}

	loader := &tableLoader{get: p.get, list: p.list, limits: limits, want: referencedColumns(p.sql)}

	if err = p.load(ctx, conn, loader); err != nil {
		return nil, loader.scanned, engineFailure(err)
	}

	if strings.Contains(p.sql, unnestModuleName+"(") {
		if err = execAll(
			ctx,
			conn,
			"CREATE VIRTUAL TABLE temp."+unnestModuleName+" USING "+unnestModuleName,
		); err != nil {
			return nil, loader.scanned, engineFailure(err)
		}
	}

	if err = execAll(ctx, conn, "PRAGMA case_sensitive_like=ON", "PRAGMA query_only=ON"); err != nil {
		return nil, loader.scanned, engineFailure(err)
	}

	res, err := readResult(ctx, conn, p.sql, limits, p.write == nil)
	if err != nil {
		return nil, loader.scanned, engineFailure(err)
	}

	if p.write != nil {
		out := p.write.apply(ctx, res, limits)

		return &sqlResult{}, loader.scanned, out
	}

	return res, loader.scanned, stmtOK()
}

func (p *enginePlan) load(ctx context.Context, conn *sql.Conn, l *tableLoader) error {
	attached := map[string]bool{}

	for _, t := range p.tables {
		if attached[t.schema] {
			continue
		}

		attached[t.schema] = true

		if err := execAll(ctx, conn, "ATTACH DATABASE ':memory:' AS "+quoteIdent(t.schema)); err != nil {
			return err
		}
	}

	for _, t := range p.tables {
		if err := t.loadInto(ctx, conn, l); err != nil {
			return err
		}
	}

	return nil
}

func rowColumns(def *GlueTableDef) []string {
	var out []string

	for _, c := range def.Columns {
		if decl, ok := complexTypeOf(c.Type); ok && decl == declRow {
			out = append(out, strings.ToLower(c.Name))
		}
	}

	return out
}

func tableColumns(def *GlueTableDef) []Column {
	cols := make([]Column, 0, len(def.Columns)+len(def.PartitionKeys))
	have := map[string]bool{}

	for _, c := range def.Columns {
		have[strings.ToLower(c.Name)] = true
		cols = append(cols, c)
	}

	for _, c := range def.PartitionKeys {
		if !have[strings.ToLower(c.Name)] {
			cols = append(cols, c)
		}
	}

	return cols
}

func (t *planTable) loadInto(ctx context.Context, conn *sql.Conn, l *tableLoader) error {
	switch tableTypeOf(t.def) {
	case tableTypeIceberg:
		return t.loadIceberg(ctx, conn, l)
	case tableTypeDelta:
		return t.loadDelta(ctx, conn, l)
	}

	if t.travel.kind != "" || t.metaTable != "" {
		return fmt.Errorf("%w: this connector does not support versioned tables", errUnsupportedFormat)
	}

	format, err := detectFormat(t.def)
	if err != nil {
		return err
	}

	all := tableColumns(t.def)
	decls := make([]string, len(all))
	marks := make([]string, len(all))

	for i, c := range all {
		decls[i] = quoteIdent(c.Name) + " " + sqliteDecl(c.Type)
		marks[i] = "?"
	}

	target := quoteIdent(t.schema) + "." + quoteIdent(t.name)

	if err = execAll(ctx, conn, "CREATE TABLE "+target+" ("+strings.Join(decls, ", ")+")"); err != nil {
		return err
	}

	insertSQL := "INSERT INTO " + target + " VALUES (" + strings.Join(marks, ",") + ")"

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err = t.insertAll(ctx, tx, insertSQL, l, format); err != nil {
		_ = tx.Rollback()

		return err
	}

	return tx.Commit()
}

func (t *planTable) insertAll(ctx context.Context, tx *sql.Tx, insertSQL string, l *tableLoader, f tableFormat) error {
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	return t.loadLocations(ctx, l, f, stmt)
}

func (t *planTable) loadLocations(ctx context.Context, l *tableLoader, f tableFormat, stmt *sql.Stmt) error {
	kinds := make([]colKind, len(t.def.Columns))
	for i, c := range t.def.Columns {
		kinds[i] = kindOf(c.Type)
	}

	total := len(tableColumns(t.def))

	if len(t.def.PartitionKeys) == 0 {
		sink := &rowSink{stmt: stmt, kinds: kinds, nargs: total}

		return l.loadLocation(ctx, t.def.Location, f, t.def.Columns, sink)
	}

	for _, part := range t.parts {
		sink := &rowSink{stmt: stmt, kinds: kinds, nargs: total, extra: partitionArgs(t.def, part)}
		if err := l.loadLocation(ctx, partitionLocation(t.def, part), f, t.def.Columns, sink); err != nil {
			return err
		}
	}

	return nil
}

func partitionArgs(def *GlueTableDef, part GluePartitionDef) []any {
	have := map[string]bool{}
	for _, c := range def.Columns {
		have[strings.ToLower(c.Name)] = true
	}

	var out []any

	for i, k := range def.PartitionKeys {
		if have[strings.ToLower(k.Name)] {
			continue
		}

		if i < len(part.Values) {
			out = append(out, convertValue(part.Values[i], kindOf(k.Type), false))
		} else {
			out = append(out, nil)
		}
	}

	return out
}

func partitionLocation(def *GlueTableDef, part GluePartitionDef) string {
	if part.Location != "" {
		return part.Location
	}

	var sb strings.Builder

	sb.WriteString(strings.TrimSuffix(def.Location, "/"))

	for i, k := range def.PartitionKeys {
		if i < len(part.Values) {
			sb.WriteString("/" + k.Name + "=" + part.Values[i])
		}
	}

	sb.WriteString("/")

	return sb.String()
}

var simpleColName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func resultColName(name, decl string, idx int) string {
	switch {
	case simpleColName.MatchString(name):
		return strings.ToLower(name)
	case decl == "" && strings.ContainsAny(name, "()+-*/|<>=,"):
		return "_col" + strconv.Itoa(idx)
	default:
		return name
	}
}

func athenaTypeOfDecl(decl string) string {
	d := strings.ToUpper(decl)

	switch base := typeBase(d); base {
	case declVarchar, "":
		return typeVarchar
	case sqlInteger:
		return typeInteger
	case declDateText:
		return typeDate
	case declTimestampText:
		return typeTimestamp
	case "DECIMAL":
		return typeDecimal
	default:
		return strings.ToLower(base)
	}
}

// glueTypeOfDecl maps a SQLite column declaration to the Glue type a CTAS
// table registers; "" means the type is inferred from the first value.
func glueTypeOfDecl(decl string) string {
	d := strings.ToUpper(decl)

	switch base := typeBase(d); base {
	case "":
		return ""
	case declVarchar:
		return columnTypeString
	case sqlInteger:
		return typeInt
	case declDateText:
		return typeDate
	case declTimestampText:
		return typeTimestamp
	case "DECIMAL":
		if strings.Contains(d, "(") {
			return strings.ToLower(strings.ReplaceAll(d, " ", ""))
		}

		return "decimal(10,0)"
	default:
		return strings.ToLower(base)
	}
}

func glueTypeOfAthena(typ string) string {
	if typ == typeVarchar {
		return columnTypeString
	}

	return typ
}

func decimalScale(decl string) int {
	_, rest, ok := strings.Cut(decl, ",")
	if !ok {
		return 0
	}

	n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(rest), ")"))

	return n
}

func formatFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}

	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}

	return s
}

func formatCell(v any, decl string) string {
	d := strings.ToUpper(decl)

	switch t := v.(type) {
	case nil:
		return ""
	case int64:
		if d == "BOOLEAN" {
			return strconv.FormatBool(t != 0)
		}

		return strconv.FormatInt(t, 10)
	case float64:
		if strings.HasPrefix(d, "DECIMAL") {
			return strconv.FormatFloat(t, 'f', decimalScale(d), 64)
		}

		return formatFloat(t)
	case []byte:
		return string(t)
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprint(t)
	}
}

func inferType(v any) string {
	switch v.(type) {
	case int64:
		return typeBigint
	case float64:
		return typeDouble
	default:
		return typeVarchar
	}
}

type resultBuilder struct {
	res      *sqlResult
	decls    []string
	inferred []bool
	limits   EngineLimits
	size     int64
	display  bool
}

// cellText formats one cell; display mode renders ARRAY/MAP/ROW values Trino-style.
func (rb *resultBuilder) cellText(i int, v any) string {
	s := formatCell(v, rb.decls[i])

	if !strings.HasPrefix(s, containerMark) && !isComplexDecl(rb.decls[i]) {
		return s
	}

	if rb.display {
		if out, kind, ok := displayComplex(s, rb.decls[i]); ok {
			rb.res.columns[i].typ = kind
			rb.res.columns[i].glue = columnTypeString
			rb.inferred[i] = false

			return out
		}
	}

	return strings.TrimPrefix(s, containerMark)
}

func newResultBuilder(rows *sql.Rows, limits EngineLimits, display bool) (*resultBuilder, error) {
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	rb := &resultBuilder{
		res:      &sqlResult{columns: make([]sqlColumn, len(names))},
		limits:   limits,
		display:  display,
		decls:    make([]string, len(names)),
		inferred: make([]bool, len(names)),
	}

	for i := range names {
		rb.decls[i] = ctypes[i].DatabaseTypeName()
		if !display && isComplexDecl(rb.decls[i]) {
			rb.decls[i] = declVarchar
		}

		rb.res.columns[i] = sqlColumn{
			name: resultColName(names[i], rb.decls[i], i), typ: athenaTypeOfDecl(rb.decls[i]),
			glue: glueTypeOfDecl(rb.decls[i]),
		}
		rb.inferred[i] = rb.decls[i] == ""
	}

	return rb, nil
}

func (rb *resultBuilder) add(values []any) error {
	if len(rb.res.rows) >= rb.limits.MaxResultRows {
		return fmt.Errorf("%w: more than %d rows", errResultRows, rb.limits.MaxResultRows)
	}

	row := make([]string, len(values))

	var nulls []bool

	for i, v := range values {
		if rb.inferred[i] && v != nil {
			rb.res.columns[i].typ = inferType(v)
			rb.res.columns[i].glue = glueTypeOfAthena(rb.res.columns[i].typ)
			rb.inferred[i] = false
		}

		if v == nil {
			if nulls == nil {
				nulls = make([]bool, len(values))
			}

			nulls[i] = true
		}

		row[i] = rb.cellText(i, v)
		rb.size += int64(len(row[i]))
	}

	if rb.size > rb.limits.MaxResultBytes {
		return fmt.Errorf("%w: more than %d bytes", errResultBytes, rb.limits.MaxResultBytes)
	}

	rb.res.rows = append(rb.res.rows, row)
	rb.res.nulls = append(rb.res.nulls, nulls)

	return nil
}

func (rb *resultBuilder) finish() *sqlResult {
	for i := range rb.res.columns {
		if rb.inferred[i] {
			rb.res.columns[i].typ = typeVarchar
			rb.res.columns[i].glue = columnTypeString
		}
	}

	return rb.res
}

func readResult(
	ctx context.Context, conn *sql.Conn, query string, limits EngineLimits, display bool,
) (*sqlResult, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	rb, err := newResultBuilder(rows, limits, display)
	if err != nil {
		return nil, err
	}

	dest := make([]any, len(rb.decls))
	ptrs := make([]any, len(dest))

	for i := range dest {
		ptrs[i] = &dest[i]
	}

	for rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		if err = rb.add(dest); err != nil {
			return nil, err
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return rb.finish(), nil
}

// engineFailure maps an engine error onto an Athena-style failure.
func engineFailure(err error) statementOutcome {
	msg := err.Error()

	switch {
	case errors.Is(err, errUnsupportedFormat):
		return stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: %s", msg)
	case errors.Is(err, errRowLimit), errors.Is(err, errScanLimit),
		errors.Is(err, errResultRows), errors.Is(err, errResultBytes):
		return stmtFail(athenaErrTypeSyntax, "EXCEEDED_LIMIT: %s", msg)
	}

	if _, tail, ok := strings.Cut(msg, errArgPrefix); ok {
		return stmtFail(athenaErrTypeSyntax, "%s%s", errArgPrefix, strings.TrimSuffix(tail, ")"))
	}

	if m := noSuchColumn.FindStringSubmatch(msg); m != nil {
		return stmtFail(athenaErrTypeEntityMiss,
			"COLUMN_NOT_FOUND: line 1:8: Column '%s' cannot be resolved", strings.Trim(m[1], `"`))
	}

	if m := noSuchFunction.FindStringSubmatch(msg); m != nil {
		return stmtFail(athenaErrTypeSyntax, "SYNTAX_ERROR: line 1:8: Function '%s' not registered", m[1])
	}

	if strings.Contains(msg, "syntax error") {
		return stmtFail(athenaErrTypeSyntax, "SYNTAX_ERROR: line 1:1: mismatched input (%s)", msg)
	}

	return stmtFail(athenaErrTypeSyntax, "%s", msg)
}
