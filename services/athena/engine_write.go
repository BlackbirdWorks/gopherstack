package athena

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	hiveNullToken    = `\N`
	hiveDefaultPart  = "__HIVE_DEFAULT_PARTITION__"
	fmtNameText      = "TEXTFILE"
	fmtNameJSON      = "JSON"
	fmtNameCSV       = "CSV"
	fmtNameParquet   = "PARQUET"
	compressionNone  = "NONE"
	compressionGzip  = "GZIP"
	fileSeqWidth     = 5
	csvDefaultDelim  = ","
	serdeParamFmtKey = "serialization.format"
)

var (
	errNoWriteLocation = errors.New("either external_location or a query result location is required")
)

func isWriteProp(k string) bool {
	switch k {
	case propFormat, "external_location", "partitioned_by", "field_delimiter",
		propWriteCompression, "compression", "parquet_compression", "orc_compression", "is_external":
		return true
	default:
		return false
	}
}

// outFormat is how rows are laid out in the written objects.
type outFormat struct {
	delim string
	codec string
	kind  dataFormat
	gzip  bool
}

// outCol maps a destination column to a source result column; src < 0 writes NULL.
type outCol struct {
	name string
	typ  string
	src  int
	id   int
}

// writePlan is the write side of a CTAS, INSERT or UNLOAD.
type writePlan struct {
	cat            GlueTableCatalog
	put            S3Storer
	list           S3Lister
	get            S3Getter
	now            func() time.Time
	def            *GlueTableDef
	stmt           *writeStmt
	catalog        string
	db             string
	table          string
	ice            *icebergTable
	queryID        string
	outputLocation string
	location       string
	partitionBy    []string
	format         outFormat
	s3tables       bool
}

// bind supplies the query id and effective result location, which are only
// known once the execution record exists.
func (p *writePlan) bind(id, outputLocation string) {
	p.queryID = id
	p.outputLocation = outputLocation
}

func (p *writePlan) display() string {
	if p.table == "" {
		return "unload"
	}

	return strings.ToLower(p.catalog) + "." + p.db + "." + p.table
}

// planWrite plans a CTAS, INSERT or UNLOAD. Callers must hold b.mu.
func (b *InMemoryBackend) planWrite(
	query string, qctx QueryExecutionContext, cat GlueTableCatalog, get S3Getter, list S3Lister,
) (*enginePlan, statementOutcome, bool) {
	w, ok := parseWriteStmt(query)
	if !ok {
		return nil, stmtOK(), false
	}

	wp := &writePlan{stmt: w, cat: cat, put: b.s3, list: list, get: get, now: b.now}

	if w.kind != writeUnload {
		catalog, db, table := refTarget(w.target, qctx)

		tcat, found := b.tableCatalogFor(catalog)
		if !found {
			return nil, stmtOK(), false
		}

		_, wp.s3tables = s3TablesBucket(catalog)
		wp.cat = tcat
		wp.catalog, wp.db, wp.table = catalog, db, table
	}

	plan, unresolved, err := b.buildPlan(w.body, qctx, get, list)
	if err != nil {
		return nil, planFailure(err), true
	}

	if len(plan.tables) == 0 && len(unresolved) > 0 {
		return nil, stmtOK(), false
	}

	if len(unresolved) > 0 {
		return nil, stmtFail(athenaErrTypeEntityMiss,
			"TABLE_NOT_FOUND: line 1:15: Table '%s' does not exist", unresolved[0]), true
	}

	if out := wp.validate(b); out.failed {
		return nil, out, true
	}

	plan.write = wp

	return plan, stmtOK(), true
}

func (p *writePlan) validate(b *InMemoryBackend) statementOutcome {
	switch p.stmt.kind {
	case writeInsert:
		return stmtOK()
	case writeCTAS:
		if p.s3tables {
			return stmtFail(athenaErrTypeSyntax, "%s", errS3TablesDDL.Error())
		}

		if _, err := b.glueSource.GetDatabase(p.db); err != nil {
			return stmtFail(athenaErrTypeEntityMiss, "SCHEMA_NOT_FOUND: Schema '%s' does not exist", p.db)
		}
	case writeUnload:
	}

	return p.validateProps()
}

func (p *writePlan) validateProps() statementOutcome {
	props := p.stmt.props

	for k := range props {
		if !isWriteProp(k) {
			return stmtFail(athenaErrTypeSyntax,
				"INVALID_TABLE_PROPERTY: line 1:1: table property '%s' is not supported", k)
		}
	}

	name := strings.ToUpper(firstProp(props, propFormat))
	if name == "" {
		name = "PARQUET"
	}

	switch name {
	case fmtNameText, fmtNameJSON, fmtNameCSV, fmtNameParquet:
	case "ORC", "AVRO":
		return stmtFail(athenaErrTypeSyntax,
			"NOT_SUPPORTED: format %s is not supported by the embedded SQL engine; use PARQUET, TEXTFILE, CSV or JSON",
			name)
	default:
		return stmtFail(athenaErrTypeSyntax, "INVALID_TABLE_PROPERTY: line 1:1: invalid format '%s'", name)
	}

	switch name {
	case fmtNameJSON:
		p.format.kind = fmtJSON
	case fmtNameParquet:
		p.format.kind = fmtParquet
	case fmtNameCSV:
		p.format.kind, p.format.delim = fmtCSVSerde, csvDefaultDelim
	default:
		p.format.kind, p.format.delim = fmtDelimited, ctrlA
	}

	if d := firstProp(props, "field_delimiter"); d != "" {
		d = unescapeDelim(d)
		if len([]rune(d)) != 1 {
			return stmtFail(athenaErrTypeSyntax,
				"INVALID_TABLE_PROPERTY: line 1:1: field_delimiter must be a single character")
		}

		p.format.delim = d
	}

	return p.validateCompression()
}

func (p *writePlan) validateCompression() statementOutcome {
	props := p.stmt.props
	c := strings.ToUpper(orDefault(firstProp(props, propWriteCompression),
		orDefault(firstProp(props, "compression"), firstProp(props, "parquet_compression"))))

	if out := p.checkCompression(c); out.failed {
		return out
	}

	p.partitionBy = nil
	for _, k := range p.stmt.props["partitioned_by"] {
		p.partitionBy = append(p.partitionBy, strings.ToLower(k))
	}

	return stmtOK()
}

func (p *writePlan) checkCompression(c string) statementOutcome {
	if p.format.kind == fmtParquet {
		switch c {
		case "", "SNAPPY", compressionGzip, "ZSTD", "LZ4", compressionNone:
			p.format.codec = c

			return stmtOK()
		default:
			return stmtFail(athenaErrTypeSyntax,
				"NOT_SUPPORTED: compression %s is not supported for PARQUET; use SNAPPY, GZIP, ZSTD, LZ4 or NONE", c)
		}
	}

	switch c {
	case "", compressionNone:
	case compressionGzip:
		p.format.gzip = true
	default:
		return stmtFail(athenaErrTypeSyntax,
			"NOT_SUPPORTED: compression %s is not supported; use GZIP or NONE", c)
	}

	return stmtOK()
}

func firstProp(props map[string][]string, key string) string {
	if v := props[key]; len(v) > 0 {
		return v[0]
	}

	return ""
}

func withSlash(s string) string {
	if strings.HasSuffix(s, "/") {
		return s
	}

	return s + "/"
}

// precheck validates the target before the SELECT runs. skip=true means the
// statement is a no-op success (CREATE TABLE IF NOT EXISTS on an existing table).
func (p *writePlan) precheck(ctx context.Context) (statementOutcome, bool) {
	switch p.stmt.kind {
	case writeInsert:
		return p.precheckInsert(ctx), false
	case writeUnload:
		p.location = withSlash(p.stmt.location)
	default:
		if _, err := p.cat.GetTableDef(p.db, p.table); err == nil {
			if p.stmt.ifNotExists {
				return stmtOK(), true
			}

			return stmtFail(athenaErrTypeEntityFound,
				"TABLE_ALREADY_EXISTS: line 1:1: Table '%s' already exists", p.display()), false
		}

		loc, err := p.ctasLocation()
		if err != nil {
			return stmtFail(athenaErrTypeSyntax, "line 1:1: Unable to create table: %s", err.Error()), false
		}

		p.location = loc
	}

	return p.checkEmpty(ctx), false
}

func (p *writePlan) ctasLocation() (string, error) {
	loc := firstProp(p.stmt.props, "external_location")

	switch {
	case loc != "":
	case p.outputLocation != "":
		loc = strings.TrimRight(p.outputLocation, "/") + "/tables/" + p.queryID
	default:
		return "", errNoWriteLocation
	}

	return withSlash(loc), nil
}

func (p *writePlan) checkEmpty(ctx context.Context) statementOutcome {
	bucket, prefix, ok := splitS3URI(p.location)
	if !ok {
		return stmtFail(athenaErrTypeSyntax, "line 1:1: invalid S3 location %q", p.location)
	}

	out, err := p.list.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return stmtFail(athenaErrTypeSyntax, "HIVE_METASTORE_ERROR: cannot list %s: %s", p.location, err)
	}

	if len(out.Contents) > 0 {
		return stmtFail(athenaErrTypeSyntax,
			"HIVE_PATH_ALREADY_EXISTS: Target directory for table '%s' already exists: %s. "+
				"You may need to manually clean the data at location '%s' before retrying. "+
				"Athena will not delete data in your account.",
			p.display(), p.location, p.location)
	}

	return stmtOK()
}

func (p *writePlan) precheckInsert(ctx context.Context) statementOutcome {
	def, err := p.cat.GetTableDef(p.db, p.table)
	if err != nil {
		return stmtFail(athenaErrTypeEntityMiss,
			"TABLE_NOT_FOUND: line 1:1: Table '%s' does not exist", p.display())
	}

	switch tableTypeOf(def) {
	case tableTypeIceberg:
		return p.precheckIcebergInsert(ctx, def)
	case tableTypeDelta:
		return stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: Delta Lake tables are read-only in Athena")
	}

	f, err := detectFormat(def)
	if err != nil {
		return engineFailure(err)
	}

	if def.Location == "" {
		return stmtFail(athenaErrTypeSyntax, "line 1:1: Table '%s' has no storage location", p.display())
	}

	p.def = def
	p.format = outFormat{kind: f.kind, delim: f.delim}

	return stmtOK()
}

// apply writes the SELECT result and registers the Glue table or partitions.
func (p *writePlan) apply(ctx context.Context, res *sqlResult, limits EngineLimits) statementOutcome {
	switch p.stmt.kind {
	case writeInsert:
		return p.applyInsert(ctx, res, limits)
	case writeUnload:
		return p.applyUnload(ctx, res, limits)
	default:
		return p.applyCTAS(ctx, res, limits)
	}
}

func resultOutCols(res *sqlResult) []outCol {
	cols := make([]outCol, len(res.columns))
	for i, c := range res.columns {
		cols[i] = outCol{name: c.name, typ: c.glue, src: i}
	}

	return cols
}

func (p *writePlan) applyUnload(ctx context.Context, res *sqlResult, limits EngineLimits) statementOutcome {
	data, parts, out := splitPartitionCols(resultOutCols(res), p.partitionBy)
	if out.failed {
		return out
	}

	_, out = p.writeRows(ctx, res, data, parts, p.location, nil, limits)

	return out
}

func (p *writePlan) applyCTAS(ctx context.Context, res *sqlResult, limits EngineLimits) statementOutcome {
	all := resultOutCols(res)
	seen := map[string]bool{}

	for i := range all {
		all[i].name = strings.ToLower(all[i].name)
		if seen[all[i].name] {
			return stmtFail(athenaErrTypeSyntax, "line 1:1: Duplicate column name '%s'", all[i].name)
		}

		seen[all[i].name] = true
	}

	data, parts, out := splitPartitionCols(all, p.partitionBy)
	if out.failed {
		return out
	}

	if len(data) == 0 {
		return stmtFail(athenaErrTypeSyntax, "line 1:1: Table must have at least one non-partition column")
	}

	if p.stmt.noData {
		res = &sqlResult{columns: res.columns}
	}

	newParts, out := p.writeRows(ctx, res, data, parts, p.location, nil, limits)
	if out.failed {
		return out
	}

	if err := p.cat.CreateTableDef(p.db, p.ctasDef(data, parts)); err != nil {
		return stmtFail(athenaErrTypeSyntax, "line 1:1: Unable to create table: %s", err.Error())
	}

	return p.registerPartitions(newParts)
}

func (p *writePlan) ctasDef(data, parts []outCol) GlueTableDef {
	def := GlueTableDef{
		Name:          p.table,
		TableType:     tableTypeExternal,
		Location:      p.location,
		Parameters:    map[string]string{"EXTERNAL": externalTrue},
		Columns:       outColumns(data),
		PartitionKeys: outColumns(parts),
		InputFormat:   textInputFormat,
		OutputFormat:  textOutputFmt,
	}

	switch p.format.kind {
	case fmtParquet:
		def.SerdeLibrary = parquetSerde
		def.InputFormat = parquetInputFmt
		def.OutputFormat = parquetOutFmt
		def.SerdeParameters = map[string]string{serdeParamFmtKey: "1"}
	case fmtJSON:
		def.SerdeLibrary = jsonSerdeLib
	case fmtCSVSerde:
		def.SerdeLibrary = lazySimpleSerde
		def.SerdeParameters = map[string]string{"field.delim": p.format.delim, serdeParamFmtKey: p.format.delim}
	default:
		def.SerdeLibrary = lazySimpleSerde
		def.SerdeParameters = map[string]string{serdeParamFmtKey: "1"}

		if p.format.delim != ctrlA {
			def.SerdeParameters["field.delim"] = p.format.delim
			def.SerdeParameters[serdeParamFmtKey] = p.format.delim
		}
	}

	return def
}

func outColumns(cols []outCol) []Column {
	out := make([]Column, len(cols))
	for i, c := range cols {
		out[i] = Column{Name: c.name, Type: c.typ}
	}

	return out
}

func splitPartitionCols(all []outCol, partitionBy []string) ([]outCol, []outCol, statementOutcome) {
	isPart := map[string]bool{}
	parts := make([]outCol, 0, len(partitionBy))

	for _, k := range partitionBy {
		idx := -1

		for i, c := range all {
			if c.name == k {
				idx = i
			}
		}

		if idx < 0 {
			return nil, nil, stmtFail(athenaErrTypeEntityMiss,
				"COLUMN_NOT_FOUND: line 1:1: Partition column '%s' is not in the SELECT list", k)
		}

		isPart[k] = true

		parts = append(parts, all[idx])
	}

	data := make([]outCol, 0, len(all))

	for _, c := range all {
		if !isPart[c.name] {
			data = append(data, c)
		}
	}

	return data, parts, stmtOK()
}

func (p *writePlan) registerPartitions(parts []GluePartitionDef) statementOutcome {
	if len(parts) == 0 {
		return stmtOK()
	}

	if err := p.cat.CreatePartitionDefs(p.db, p.table, parts); err != nil &&
		!errors.Is(err, ErrCatalogEntityExists) {
		return stmtFail(athenaErrTypeSyntax, "%s", err.Error())
	}

	return stmtOK()
}

func (p *writePlan) applyInsert(ctx context.Context, res *sqlResult, limits EngineLimits) statementOutcome {
	if p.ice != nil {
		return p.applyIcebergInsert(ctx, res, limits)
	}

	def := p.def
	dest := tableColumns(def)
	keys := insertPartitionKeys(def)

	src, out := p.insertSources(dest, len(res.columns))
	if out.failed {
		return out
	}

	var data, parts []outCol

	for i, c := range dest {
		oc := outCol{name: strings.ToLower(c.Name), typ: c.Type, src: src[i]}
		if keys[oc.name] {
			parts = append(parts, oc)
		} else {
			data = append(data, oc)
		}
	}

	parts = orderPartitionCols(def, parts)

	known, err := p.knownPartitions(def)
	if err != nil {
		return stmtFail(athenaErrTypeSyntax, "%s", err.Error())
	}

	newParts, out := p.writeRows(ctx, res, data, parts, withSlash(def.Location), known, limits)
	if out.failed {
		return out
	}

	return p.registerPartitions(newParts)
}

func insertPartitionKeys(def *GlueTableDef) map[string]bool {
	have := map[string]bool{}
	for _, c := range def.Columns {
		have[strings.ToLower(c.Name)] = true
	}

	keys := map[string]bool{}

	for _, k := range def.PartitionKeys {
		if n := strings.ToLower(k.Name); !have[n] {
			keys[n] = true
		}
	}

	return keys
}

func orderPartitionCols(def *GlueTableDef, parts []outCol) []outCol {
	out := make([]outCol, 0, len(parts))

	for _, k := range def.PartitionKeys {
		for _, c := range parts {
			if c.name == strings.ToLower(k.Name) {
				out = append(out, c)
			}
		}
	}

	return out
}

// insertSources maps each destination column to a result column index.
func (p *writePlan) insertSources(dest []Column, nsrc int) ([]int, statementOutcome) {
	src := make([]int, len(dest))
	cols := p.stmt.insertCols

	if len(cols) == 0 {
		if nsrc != len(dest) {
			return nil, p.mismatch(dest, nsrc)
		}

		for i := range src {
			src[i] = i
		}

		return src, stmtOK()
	}

	if nsrc != len(cols) {
		return nil, p.mismatch(dest, nsrc)
	}

	for i := range src {
		src[i] = -1
	}

	for si, name := range cols {
		found := false

		for di, c := range dest {
			if strings.EqualFold(c.Name, name) {
				src[di], found = si, true
			}
		}

		if !found {
			return nil, stmtFail(athenaErrTypeEntityMiss,
				"COLUMN_NOT_FOUND: line 1:1: Column '%s' cannot be resolved", name)
		}
	}

	return src, stmtOK()
}

func (p *writePlan) mismatch(dest []Column, nsrc int) statementOutcome {
	types := make([]string, len(dest))
	for i, c := range dest {
		types[i] = c.Type
	}

	return stmtFail(athenaErrTypeSyntax,
		"line 1:1: Insert query has mismatched column types: Table: [%s], Query: %d column(s)",
		strings.Join(types, ", "), nsrc)
}

func (p *writePlan) knownPartitions(def *GlueTableDef) (map[string]string, error) {
	if len(def.PartitionKeys) == 0 {
		return map[string]string{}, nil
	}

	existing, err := p.cat.GetPartitionDefs(p.db, p.table)
	if err != nil {
		return nil, err
	}

	known := make(map[string]string, len(existing))
	for _, pd := range existing {
		known[strings.Join(pd.Values, "\x00")] = partitionLocation(def, pd)
	}

	return known, nil
}

type rowGroup struct {
	values []string
	rows   []int
}

// groupRows buckets row indexes by partition values, keeping first-seen order.
func groupRows(res *sqlResult, parts []outCol) []*rowGroup {
	if len(parts) == 0 {
		g := &rowGroup{rows: make([]int, len(res.rows))}
		for i := range g.rows {
			g.rows[i] = i
		}

		if len(g.rows) == 0 {
			return nil
		}

		return []*rowGroup{g}
	}

	byKey := map[string]*rowGroup{}

	var order []*rowGroup

	for r := range res.rows {
		vals := make([]string, len(parts))
		for i, pc := range parts {
			v, null := res.cell(r, pc.src)
			if null {
				v = hiveDefaultPart
			}

			vals[i] = v
		}

		key := strings.Join(vals, "\x00")

		g, ok := byKey[key]
		if !ok {
			g = &rowGroup{values: vals}
			byKey[key] = g
			order = append(order, g)
		}

		g.rows = append(g.rows, r)
	}

	return order
}

func (r *sqlResult) cell(row, col int) (string, bool) {
	if col < 0 || r.isNull(row, col) {
		return "", true
	}

	return r.rows[row][col], false
}

func partDir(parts []outCol, values []string) string {
	var sb strings.Builder

	for i, pc := range parts {
		sb.WriteString(url.PathEscape(pc.name) + "=" + url.PathEscape(values[i]) + "/")
	}

	return sb.String()
}

// writeRows writes the grouped rows under base and returns the partitions
// that are not already in known.
func (p *writePlan) writeRows(
	ctx context.Context, res *sqlResult, data, parts []outCol, base string, known map[string]string,
	limits EngineLimits,
) ([]GluePartitionDef, statementOutcome) {
	if _, _, ok := splitS3URI(base); !ok {
		return nil, stmtFail(athenaErrTypeSyntax, "line 1:1: invalid S3 location %q", base)
	}

	var (
		fresh []GluePartitionDef
		total int64
	)

	for seq, g := range groupRows(res, parts) {
		if err := ctx.Err(); err != nil {
			return nil, engineFailure(err)
		}

		body, err := p.encodeGroup(res, g, data)
		if err != nil {
			return nil, engineFailure(err)
		}

		if total += int64(len(body)); total > limits.MaxResultBytes {
			return nil, engineFailure(fmt.Errorf("%w: more than %d bytes", errResultBytes, limits.MaxResultBytes))
		}

		loc := base + partDir(parts, g.values)
		if existing, has := known[strings.Join(g.values, "\x00")]; has && len(parts) > 0 {
			loc = withSlash(existing)
		} else if len(parts) > 0 {
			fresh = append(fresh, GluePartitionDef{Values: g.values, Location: loc})
		}

		if out := p.putObject(ctx, loc, seq, body); out.failed {
			return nil, out
		}
	}

	return fresh, stmtOK()
}

func (p *writePlan) encodeGroup(res *sqlResult, g *rowGroup, data []outCol) ([]byte, error) {
	var body []byte

	switch p.format.kind {
	case fmtParquet:
		return encodeParquetRows(res, g.rows, data, p.format.codec)
	case fmtJSON:
		body = encodeJSONLines(res, g.rows, data)
	case fmtCSVSerde:
		body = encodeCSVRows(res, g.rows, data, p.format.delim)
	default:
		body = encodeTextRows(res, g.rows, data, p.format.delim)
	}

	if !p.format.gzip {
		return body, nil
	}

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (p *writePlan) putObject(ctx context.Context, loc string, seq int, body []byte) statementOutcome {
	bucket, locPrefix, _ := splitS3URI(loc)

	name := fmt.Sprintf("%s_%0*d", p.queryID, fileSeqWidth, seq)
	switch {
	case p.format.kind == fmtParquet:
		name += ".parquet"
	case p.format.gzip:
		name += ".gz"
	}

	_, err := p.put.PutObject(ctx, &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(locPrefix + name), Body: bytes.NewReader(body),
	})
	if err != nil {
		return stmtFail(athenaErrTypeSyntax, "HIVE_WRITER_CLOSE_ERROR: cannot write s3://%s/%s%s: %s",
			bucket, locPrefix, name, err)
	}

	return stmtOK()
}
