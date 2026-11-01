package athena

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
)

// glueCatalogFor returns the Glue table catalog when catalog is Glue-backed.
func (b *InMemoryBackend) glueCatalogFor(catalog string) (GlueTableCatalog, bool) {
	b.mu.RLock("glueCatalogFor")
	defer b.mu.RUnlock()

	if !b.isGlueBacked(catalog) {
		return nil, false
	}

	cat, ok := b.glueSource.(GlueTableCatalog)

	return cat, ok
}

func refTarget(parts []string, qctx QueryExecutionContext) (string, string, string) {
	catalog, db, table := resolveTableRef(strings.Join(parts, "."), qctx)

	return normalizeCatalog(catalog), strings.ToLower(db), strings.ToLower(table)
}

// execCatalogStatement runs the statements that read or change the Glue
// catalog and S3 layout. handled=false defers to the legacy statement path.
func (b *InMemoryBackend) execCatalogStatement(
	query string, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	upper := strings.ToUpper(strings.TrimSpace(query))

	switch {
	case hasKeyword(upper, "CREATE TABLE"), hasKeyword(upper, "CREATE EXTERNAL TABLE"):
		return b.execGlueCreateTable(query, qctx)
	case hasKeyword(upper, "DROP TABLE"):
		return b.execGlueDropTable(query, qctx)
	case hasKeyword(upper, "MSCK"):
		return b.execMsckRepair(query, qctx)
	case hasKeyword(upper, "ALTER"):
		return b.execAlterAddPartition(query, qctx)
	case hasKeyword(upper, "SHOW"), hasKeyword(upper, "DESCRIBE"), hasKeyword(upper, "DESC"):
		return b.execShowDescribe(query, qctx)
	default:
		return nil, stmtOK(), false
	}
}

func (b *InMemoryBackend) execGlueCreateTable(
	query string, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	spec, err := parseGlueCreateTable(query)
	if err != nil {
		return nil, stmtOK(), false
	}

	catalog, db, table := refTarget(spec.parts, qctx)

	if _, isS3T := s3TablesBucket(catalog); isS3T {
		return nil, stmtFail(athenaErrTypeSyntax, "%s", errS3TablesDDL.Error()), true
	}

	cat, ok := b.glueCatalogFor(catalog)
	if !ok {
		return nil, stmtOK(), false
	}

	spec.def.Name = table

	switch tableTypeOf(&spec.def) {
	case tableTypeIceberg:
		return b.createIcebergTable(spec, db, cat)
	case tableTypeDelta:
		return b.createDeltaTable(spec, db, cat)
	}

	switch err = cat.CreateTableDef(db, spec.def); {
	case err == nil:
		return &sqlResult{}, stmtOK(), true
	case errors.Is(err, ErrCatalogEntityExists):
		if spec.ifNotExists {
			return &sqlResult{}, stmtOK(), true
		}

		return nil, stmtFail(athenaErrTypeEntityFound, "Table %s already exists", table), true
	case errors.Is(err, ErrCatalogEntityNotFound):
		return nil, stmtFail(athenaErrTypeEntityMiss, "Database does not exist: %s", db), true
	default:
		return nil, stmtFail(athenaErrTypeSyntax, "%s", err.Error()), true
	}
}

func (b *InMemoryBackend) execGlueDropTable(
	query string, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	name, _ := parseDropTable(query)
	if name == "" {
		return nil, stmtOK(), false
	}

	parts := strings.Split(strings.ReplaceAll(name, `"`, ""), ".")
	catalog, db, table := refTarget(parts, qctx)

	if _, isS3T := s3TablesBucket(catalog); isS3T {
		return nil, stmtFail(athenaErrTypeSyntax, "%s", errS3TablesDDL.Error()), true
	}

	cat, ok := b.glueCatalogFor(catalog)
	if !ok {
		return nil, stmtOK(), false
	}

	if _, err := cat.GetTableDef(db, table); err != nil {
		return nil, stmtOK(), false
	}

	if err := cat.DeleteTable(db, table); err != nil {
		return nil, stmtFail(athenaErrTypeSyntax, "%s", err.Error()), true
	}

	return &sqlResult{}, stmtOK(), true
}

var hivePartDir = regexp.MustCompile(`^([^=/]+)=(.*)$`)

// discoverPartitions finds Hive-style key=value directories under the table location.
func discoverPartitions(def *GlueTableDef, keys []string) []GluePartitionDef {
	bucket, base, _ := splitS3URI(def.Location)
	base = strings.TrimSuffix(base, "/")

	var parts []GluePartitionDef

	seen := map[string]bool{}

	for _, k := range keys {
		rel := strings.TrimPrefix(strings.TrimPrefix(k, base), "/")
		segs := strings.Split(path.Dir(rel), "/")

		if len(segs) < len(def.PartitionKeys) {
			continue
		}

		values := make([]string, 0, len(def.PartitionKeys))
		var dir strings.Builder

		for i, pk := range def.PartitionKeys {
			m := hivePartDir.FindStringSubmatch(segs[i])
			if m == nil || !strings.EqualFold(m[1], pk.Name) {
				values = nil

				break
			}

			v, err := url.PathUnescape(m[2])
			if err != nil {
				v = m[2]
			}

			values = append(values, v)
			dir.WriteString("/" + segs[i])
		}

		id := strings.Join(values, "\x00")
		if values == nil || seen[id] {
			continue
		}

		seen[id] = true
		loc := "s3://" + bucket + "/" + strings.TrimPrefix(base+dir.String(), "/") + "/"
		parts = append(parts, GluePartitionDef{Values: values, Location: loc})
	}

	return parts
}

func (b *InMemoryBackend) execMsckRepair(
	query string, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	toks, err := tokenize(query)
	if err != nil {
		return nil, stmtOK(), false
	}

	c := &sigCursor{toks: significant(toks)}
	if !c.acceptWords("msck", "repair", "table") {
		return nil, stmtOK(), false
	}

	parts := c.qualifiedName()
	if parts == nil {
		return nil, stmtFail(athenaErrTypeSyntax, "line 1:1: missing table name in MSCK REPAIR TABLE"), true
	}

	catalog, db, table := refTarget(parts, qctx)

	b.mu.RLock("execMsckRepair")
	cat, get, list, ok := b.engineDeps()
	glue := b.isGlueBacked(catalog)
	b.mu.RUnlock()

	if !ok || !glue {
		return nil, stmtOK(), false
	}

	def, err := cat.GetTableDef(db, table)
	if err != nil {
		return nil, stmtFail(athenaErrTypeEntityMiss, "Table does not exist: %s", table), true
	}

	if len(def.PartitionKeys) == 0 {
		return nil, stmtFail(athenaErrTypeSyntax, "Table %s is not partitioned", table), true
	}

	loader := &tableLoader{get: get, list: list, limits: b.limitsSnapshot()}

	_, keys, err := loader.listKeys(context.Background(), def.Location)
	if err != nil {
		return nil, stmtFail(athenaErrTypeSyntax, "HIVE_METASTORE_ERROR: cannot list %s: %s", def.Location, err), true
	}

	existing, _ := cat.GetPartitionDefs(db, table)
	known := map[string]bool{}

	for _, p := range existing {
		known[strings.Join(p.Values, "\x00")] = true
	}

	var fresh []GluePartitionDef

	for _, p := range discoverPartitions(def, keys) {
		if !known[strings.Join(p.Values, "\x00")] {
			fresh = append(fresh, p)
		}
	}

	if len(fresh) > 0 {
		if err = cat.CreatePartitionDefs(db, table, fresh); err != nil {
			return nil, stmtFail(athenaErrTypeSyntax, "%s", err.Error()), true
		}
	}

	return &sqlResult{}, stmtOK(), true
}

func (b *InMemoryBackend) limitsSnapshot() EngineLimits {
	b.mu.RLock("limitsSnapshot")
	defer b.mu.RUnlock()

	return b.limits
}

// execAlterAddPartition handles ALTER TABLE t ADD [IF NOT EXISTS] PARTITION (...) [LOCATION '...'].
func (b *InMemoryBackend) execAlterAddPartition(
	query string, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	toks, err := tokenize(query)
	if err != nil {
		return nil, stmtOK(), false
	}

	c := &sigCursor{toks: significant(toks)}
	if !c.acceptWords("alter", "table") {
		return nil, stmtOK(), false
	}

	parts := c.qualifiedName()
	if parts == nil || !c.acceptWords("add") {
		return nil, stmtOK(), false
	}

	ifNotExists := c.acceptWords("if", "not", "exists")
	catalog, db, table := refTarget(parts, qctx)

	cat, ok := b.glueCatalogFor(catalog)
	if !ok {
		return nil, stmtOK(), false
	}

	def, err := cat.GetTableDef(db, table)
	if err != nil {
		return nil, stmtFail(athenaErrTypeEntityMiss, "Table does not exist: %s", table), true
	}

	var specs []GluePartitionDef

	for c.acceptWords("partition") {
		inner, gok := c.group()
		if !gok {
			return nil, stmtFail(athenaErrTypeSyntax, "line 1:1: expected partition specification"), true
		}

		spec, perr := partitionFromSpec(def, inner)
		if perr != nil {
			return nil, stmtFail(athenaErrTypeSyntax, "%s", perr.Error()), true
		}

		if c.acceptWords("location") {
			spec.Location = c.stringLit()
		}

		specs = append(specs, spec)
	}

	if len(specs) == 0 {
		return nil, stmtOK(), false
	}

	if err = cat.CreatePartitionDefs(db, table, specs); err != nil {
		if errors.Is(err, ErrCatalogEntityExists) && ifNotExists {
			return &sqlResult{}, stmtOK(), true
		}

		return nil, stmtFail(athenaErrTypeEntityFound, "%s", err.Error()), true
	}

	return &sqlResult{}, stmtOK(), true
}

func partitionFromSpec(def *GlueTableDef, inner []token) (GluePartitionDef, error) {
	vals := map[string]string{}

	for _, kv := range splitCommas(inner) {
		sig := significant(kv)
		if len(sig) != 3 || !sig[1].isPunct("=") {
			return GluePartitionDef{}, &syntaxError{fmt.Sprintf("line 1:1: invalid partition spec %q", joinTokens(kv))}
		}

		v := sig[2].text
		if sig[2].kind == tkString {
			v = stringLiteral(sig[2])
		}

		vals[sig[0].ident()] = v
	}

	out := GluePartitionDef{Values: make([]string, 0, len(def.PartitionKeys))}

	for _, pk := range def.PartitionKeys {
		v, ok := vals[strings.ToLower(pk.Name)]
		if !ok {
			return GluePartitionDef{}, &syntaxError{
				fmt.Sprintf("line 1:1: missing value for partition key %q", pk.Name),
			}
		}

		out.Values = append(out.Values, v)
	}

	return out, nil
}

func singleColumn(name string, values []string) *sqlResult {
	res := &sqlResult{columns: []sqlColumn{{name: name, typ: typeVarchar}}}
	for _, v := range values {
		res.rows = append(res.rows, []string{v})
	}

	return res
}

func likeMatches(pattern, name string) bool {
	if pattern == "" {
		return true
	}

	re := "(?i)^" + strings.ReplaceAll(strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, ".*"), `\|`, "|") + "$"
	ok, _ := regexp.MatchString(re, name)

	return ok
}

// execShowDescribe handles SHOW TABLES/DATABASES/PARTITIONS/COLUMNS and DESCRIBE.
func (b *InMemoryBackend) execShowDescribe(
	query string, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	toks, err := tokenize(query)
	if err != nil {
		return nil, stmtOK(), false
	}

	c := &sigCursor{toks: significant(toks)}
	catalog := normalizeCatalog(orDefault(qctx.Catalog, awsDataCatalog))

	switch {
	case c.acceptWords("show", "tables"):
		return b.showTables(c, catalog, qctx)
	case c.acceptWords("show", "databases"), c.acceptWords("show", "schemas"):
		return b.showDatabases(c, catalog)
	case c.acceptWords("show", "partitions"):
		return b.showPartitions(c, qctx)
	case c.acceptWords("show", "columns"):
		return b.describeTable(c, qctx, "field", false)
	case c.acceptWords("describe"), c.acceptWords("desc"):
		c.acceptWords("formatted")
		c.acceptWords("extended")

		return b.describeTable(c, qctx, "col_name", true)
	default:
		return nil, stmtOK(), false
	}
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}

	return s
}

func (c *sigCursor) likePattern() string {
	if c.acceptWords("like") {
		p := c.stringLit()

		return p
	}

	return ""
}

func (b *InMemoryBackend) showTables(
	c *sigCursor, catalog string, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	db := orDefault(qctx.Database, "default")

	if c.acceptWords("in") || c.acceptWords(kwFrom) {
		if parts := c.qualifiedName(); parts != nil {
			db = parts[len(parts)-1]
		}
	}

	pattern := c.likePattern()

	tables, err := b.ListTableMetadata(catalog, strings.ToLower(db), "")
	if err != nil {
		return nil, stmtFail(athenaErrTypeEntityMiss, "Schema '%s' does not exist", db), true
	}

	var names []string

	for _, t := range tables {
		if likeMatches(pattern, t.Name) {
			names = append(names, t.Name)
		}
	}

	return singleColumn("tab_name", names), stmtOK(), true
}

func (b *InMemoryBackend) showDatabases(c *sigCursor, catalog string) (*sqlResult, statementOutcome, bool) {
	pattern := c.likePattern()

	dbs, err := b.ListDatabases(catalog)
	if err != nil {
		return nil, stmtFail(athenaErrTypeEntityMiss, "Catalog does not exist: %s", catalog), true
	}

	var names []string

	for _, d := range dbs {
		if likeMatches(pattern, d.Name) {
			names = append(names, d.Name)
		}
	}

	return singleColumn("database_name", names), stmtOK(), true
}

func (b *InMemoryBackend) showPartitions(
	c *sigCursor, qctx QueryExecutionContext,
) (*sqlResult, statementOutcome, bool) {
	parts := c.qualifiedName()
	if parts == nil {
		return nil, stmtOK(), false
	}

	catalog, db, table := refTarget(parts, qctx)

	cat, ok := b.glueCatalogFor(catalog)
	if !ok {
		return nil, stmtOK(), false
	}

	def, err := cat.GetTableDef(db, table)
	if err != nil {
		return nil, stmtFail(athenaErrTypeEntityMiss, "Table does not exist: %s", table), true
	}

	existing, _ := cat.GetPartitionDefs(db, table)
	rows := make([]string, 0, len(existing))

	for _, p := range existing {
		segs := make([]string, 0, len(p.Values))
		for i, v := range p.Values {
			if i < len(def.PartitionKeys) {
				segs = append(segs, def.PartitionKeys[i].Name+"="+v)
			}
		}

		rows = append(rows, strings.Join(segs, "/"))
	}

	return singleColumn("partition", rows), stmtOK(), true
}

func (b *InMemoryBackend) describeTable(
	c *sigCursor, qctx QueryExecutionContext, colName string, withType bool,
) (*sqlResult, statementOutcome, bool) {
	c.acceptWords("in")
	c.acceptWords(kwFrom)

	parts := c.qualifiedName()
	if parts == nil {
		return nil, stmtOK(), false
	}

	catalog, db, table := refTarget(parts, qctx)

	if c.acceptWords(kwFrom) || c.acceptWords("in") {
		if dbParts := c.qualifiedName(); dbParts != nil {
			db = strings.ToLower(dbParts[len(dbParts)-1])
		}
	}

	tm, err := b.GetTableMetadata(catalog, db, table)
	if err != nil {
		return nil, stmtFail(athenaErrTypeEntityMiss,
			"Table '%s.%s.%s' does not exist", strings.ToLower(catalog), db, table), true
	}

	var rows []string

	add := func(cols []Column) {
		for _, col := range cols {
			if withType {
				rows = append(rows, col.Name+"\t"+col.Type+"\t"+col.Comment)
			} else {
				rows = append(rows, col.Name)
			}
		}
	}

	add(tm.Columns)

	if len(tm.PartitionKeys) > 0 && withType {
		rows = append(rows, "", "# Partition Information", "# col_name            \tdata_type           \tcomment")
	}

	add(tm.PartitionKeys)

	return singleColumn(colName, rows), stmtOK(), true
}
