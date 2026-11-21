package appsync

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	gqlString     = "String"
	gqlBoolean    = "Boolean"
	kindScalar    = "Scalar"
	kindNonNull   = "NonNull"
	dialectPG     = "postgres"
	dialectMy     = "mysql"
	dialectLite   = "sqlite"
	indexRowWidth = 3
	colOrder      = " ORDER BY c.table_name, c.ordinal_position"
)

const (
	maxIntrospectionModels  = 1000
	maxIntrospectionColumns = 20000
)

var errIntrospectionTooLarge = errors.New("the database is too large to introspect")

var errUnsupportedEngine = errors.New("the database engine could not be identified")

type dialect struct {
	tables string
	pk     string
	idx    string
}

func dialects() map[string]dialect {
	const (
		pkTmpl = `SELECT kcu.table_name, tc.constraint_name, kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s
ORDER BY kcu.table_name, kcu.ordinal_position`
		colTmpl = `SELECT c.table_name, c.column_name, c.data_type, c.is_nullable, c.character_maximum_length
FROM information_schema.columns c
JOIN information_schema.tables t
  ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE'
WHERE c.table_schema = %s`
	)

	return map[string]dialect{
		dialectPG: {
			tables: fmt.Sprintf(colTmpl, "current_schema()") + colOrder,
			pk:     fmt.Sprintf(pkTmpl, "current_schema()"),
			idx: `SELECT t.relname, i.relname, a.attname
FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_index ix ON ix.indrelid = t.oid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (ix.indkey::int2[])
WHERE n.nspname = current_schema() AND t.relkind = 'r' AND NOT ix.indisprimary
ORDER BY t.relname, i.relname, array_position(ix.indkey::int2[], a.attnum)`,
		},
		dialectMy: {
			tables: fmt.Sprintf(colTmpl, "DATABASE()") + colOrder,
			pk:     fmt.Sprintf(pkTmpl, "DATABASE()"),
			idx: `SELECT table_name, index_name, column_name
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND index_name <> 'PRIMARY'
ORDER BY table_name, index_name, seq_in_index`,
		},
	}
}

type catalogColumn struct {
	table, name, sqlType string
	notNull              bool
	length               int64
}

type catalogIndex struct{ table, name, column string }

type catalog struct {
	columns []catalogColumn
	pk      []catalogIndex
	indexes []catalogIndex
}

type rdsIntrospector struct {
	exec RDSDataExecutor
	cfg  *RDSDataAPIConfig
}

func (in rdsIntrospector) query(ctx context.Context, sql string) ([][]any, error) {
	out, err := in.exec.ExecuteRDSStatement(ctx, RDSStatement{
		ClusterID: in.cfg.ResourceARN, Database: in.cfg.DatabaseName, SQL: sql,
	})
	if err != nil {
		return nil, fmt.Errorf("rds statement failed: %w", err)
	}

	recs, _ := out["records"].([]any)
	rows := make([][]any, 0, len(recs))

	for _, r := range recs {
		cells, _ := r.([]any)
		row := make([]any, len(cells))

		for i, c := range cells {
			row[i] = fieldValue(c)
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func fieldValue(c any) any {
	m, _ := c.(map[string]any)
	for _, k := range rdsFieldKeys() {
		if v, ok := m[k]; ok {
			return v
		}
	}

	return nil
}

func cellString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	default:
		return fmt.Sprint(t)
	}
}

func cellInt(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)

		return n
	default:
		return 0
	}
}

func cellBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case float64:
		return t != 0
	default:
		return false
	}
}

// introspectRDS reads the tables, columns, primary keys and indexes of the database behind cfg.
func introspectRDS(
	ctx context.Context, exec RDSDataExecutor, cfg *RDSDataAPIConfig,
) ([]*DataSourceIntrospectionModel, error) {
	in := rdsIntrospector{exec: exec, cfg: cfg}

	cat, err := in.readCatalog(ctx)
	if err != nil {
		return nil, err
	}

	models := cat.models()
	if len(models) > maxIntrospectionModels {
		return nil, fmt.Errorf("%w: %d tables (limit %d)",
			errIntrospectionTooLarge, len(models), maxIntrospectionModels)
	}

	return models, nil
}

func (in rdsIntrospector) readCatalog(ctx context.Context) (*catalog, error) {
	kind, err := in.dialect(ctx)
	if err != nil {
		return nil, err
	}

	if kind == dialectLite {
		return in.readSQLite(ctx)
	}

	d := dialects()[kind]
	cat := &catalog{}

	rows, err := in.query(ctx, d.tables)
	if err != nil {
		return nil, err
	}

	for _, r := range rows {
		cat.columns = append(cat.columns, catalogColumn{
			table: cellString(r[0]), name: cellString(r[1]), sqlType: cellString(r[2]),
			notNull: strings.EqualFold(cellString(r[3]), "NO"), length: cellInt(r[4]),
		})
	}

	if len(cat.columns) > maxIntrospectionColumns {
		return nil, fmt.Errorf("%w: %d columns (limit %d)",
			errIntrospectionTooLarge, len(cat.columns), maxIntrospectionColumns)
	}

	if cat.pk, err = in.indexRows(ctx, d.pk); err != nil {
		return nil, err
	}

	if cat.indexes, err = in.indexRows(ctx, d.idx); err != nil {
		return nil, err
	}

	return cat, nil
}

// indexRows reads (table, index name, column) rows.
func (in rdsIntrospector) indexRows(ctx context.Context, sql string) ([]catalogIndex, error) {
	rows, err := in.query(ctx, sql)
	if err != nil {
		return nil, err
	}

	out := make([]catalogIndex, 0, len(rows))

	for _, r := range rows {
		if len(r) >= indexRowWidth {
			out = append(out, catalogIndex{table: cellString(r[0]), name: cellString(r[1]), column: cellString(r[2])})
		}
	}

	return out, nil
}

func (in rdsIntrospector) dialect(ctx context.Context) (string, error) {
	rows, err := in.query(ctx, "SELECT version()")
	if err == nil && len(rows) > 0 && len(rows[0]) > 0 {
		if strings.Contains(strings.ToLower(cellString(rows[0][0])), "postgres") {
			return dialectPG, nil
		}

		return dialectMy, nil
	}

	rows, lerr := in.query(ctx, "SELECT sqlite_version()")
	if lerr != nil {
		return "", lerr
	}

	if len(rows) == 0 {
		return "", errUnsupportedEngine
	}

	return dialectLite, nil
}

func quoteLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func (in rdsIntrospector) readSQLite(ctx context.Context) (*catalog, error) {
	cat := &catalog{}

	tables, err := in.query(ctx,
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, err
	}

	for _, t := range tables {
		table := cellString(t[0])

		cols, cerr := in.query(ctx,
			`SELECT name, type, "notnull", pk FROM pragma_table_info(`+quoteLit(table)+`) ORDER BY cid`)
		if cerr != nil {
			return nil, cerr
		}

		pks := map[int64]string{}

		for _, c := range cols {
			typ := cellString(c[1])
			cat.columns = append(cat.columns, catalogColumn{
				table: table, name: cellString(c[0]), sqlType: sqliteBaseType(typ),
				notNull: cellBool(c[2]), length: sqliteLength(typ),
			})

			if pk := cellInt(c[3]); pk > 0 {
				pks[pk] = cellString(c[0])
			}
		}

		for i := int64(1); i <= int64(len(pks)); i++ {
			cat.pk = append(cat.pk, catalogIndex{table: table, name: "PRIMARY", column: pks[i]})
		}

		if err = in.sqliteIndexes(ctx, cat, table); err != nil {
			return nil, err
		}
	}

	return cat, nil
}

func (in rdsIntrospector) sqliteIndexes(ctx context.Context, cat *catalog, table string) error {
	idxs, err := in.query(ctx,
		"SELECT name FROM pragma_index_list("+quoteLit(table)+") WHERE origin <> 'pk' ORDER BY name")
	if err != nil {
		return err
	}

	for _, ix := range idxs {
		name := cellString(ix[0])

		cols, cerr := in.query(ctx, "SELECT name FROM pragma_index_info("+quoteLit(name)+") ORDER BY seqno")
		if cerr != nil {
			return cerr
		}

		for _, c := range cols {
			cat.indexes = append(cat.indexes, catalogIndex{table: table, name: name, column: cellString(c[0])})
		}
	}

	return nil
}

func sqliteBaseType(t string) string {
	base, _, _ := strings.Cut(t, "(")

	return strings.TrimSpace(base)
}

func sqliteLength(t string) int64 {
	_, rest, ok := strings.Cut(t, "(")
	if !ok {
		return 0
	}

	n, _, _ := strings.Cut(strings.TrimSuffix(rest, ")"), ",")
	v, _ := strconv.ParseInt(strings.TrimSpace(n), 10, 64)

	return v
}

func (c *catalog) models() []*DataSourceIntrospectionModel {
	var (
		out   []*DataSourceIntrospectionModel
		byTbl = map[string]*DataSourceIntrospectionModel{}
	)

	for _, col := range c.columns {
		m, ok := byTbl[col.table]
		if !ok {
			m = &DataSourceIntrospectionModel{Name: col.table}
			byTbl[col.table] = m
			out = append(out, m)
		}

		m.Fields = append(m.Fields, &DataSourceIntrospectionModelField{
			Name: col.name, Length: col.length, Type: fieldType(col),
		})
	}

	for _, p := range c.pk {
		if m := byTbl[p.table]; m != nil {
			m.PrimaryKey = addIndexColumn(m.PrimaryKey, p)
		}
	}

	for _, ix := range c.indexes {
		if m := byTbl[ix.table]; m != nil {
			m.Indexes = appendIndexColumn(m.Indexes, ix)
		}
	}

	return out
}

func addIndexColumn(cur *DataSourceIntrospectionModelIndex, ix catalogIndex) *DataSourceIntrospectionModelIndex {
	if cur == nil {
		cur = &DataSourceIntrospectionModelIndex{Name: ix.name}
	}

	cur.Fields = append(cur.Fields, ix.column)

	return cur
}

func appendIndexColumn(
	list []*DataSourceIntrospectionModelIndex, ix catalogIndex,
) []*DataSourceIntrospectionModelIndex {
	if n := len(list); n > 0 && list[n-1].Name == ix.name {
		list[n-1].Fields = append(list[n-1].Fields, ix.column)

		return list
	}

	return append(list, &DataSourceIntrospectionModelIndex{Name: ix.name, Fields: []string{ix.column}})
}

func fieldType(c catalogColumn) *DataSourceIntrospectionModelFieldType {
	scalar := &DataSourceIntrospectionModelFieldType{Kind: kindScalar, Name: graphQLScalar(c.sqlType)}
	if c.notNull {
		return &DataSourceIntrospectionModelFieldType{Kind: kindNonNull, Type: scalar}
	}

	return scalar
}

func graphQLScalar(sqlType string) string {
	t := strings.ToLower(sqlType)

	switch {
	case strings.Contains(t, "bool"), t == "bit":
		return gqlBoolean
	case strings.Contains(t, "timestamp"), strings.Contains(t, "datetime"):
		return "AWSDateTime"
	case strings.HasPrefix(t, "date"):
		return "AWSDate"
	case strings.HasPrefix(t, "time"):
		return "AWSTime"
	case strings.Contains(t, "json"):
		return "AWSJSON"
	case t == "uuid":
		return "ID"
	case strings.Contains(t, "bigint"), strings.Contains(t, "decimal"), strings.Contains(t, "numeric"),
		strings.Contains(t, "float"), strings.Contains(t, "double"), strings.Contains(t, "real"):
		return "Float"
	case slices.Contains([]string{"int", "integer", "tinyint", "smallint", "mediumint"}, t),
		strings.HasSuffix(t, "serial"):
		return "Int"
	default:
		return gqlString
	}
}

func graphQLName(s string) string {
	var b strings.Builder

	for i, r := range s {
		switch {
		case r == '_', r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', i > 0 && r >= '0' && r <= '9':
			b.WriteRune(r)
		case i == 0 && r >= '0' && r <= '9':
			b.WriteString("_" + string(r))
		default:
			b.WriteRune('_')
		}
	}

	return b.String()
}

func fieldTypeRef(t *DataSourceIntrospectionModelFieldType) string {
	if t.Kind == kindNonNull {
		return fieldTypeRef(t.Type) + "!"
	}

	return t.Name
}

// modelSDL renders the model's type definition: fields only, no directives.
func modelSDL(m *DataSourceIntrospectionModel) string {
	var b strings.Builder

	b.WriteString("type " + graphQLName(m.Name) + " {\n")

	for _, f := range m.Fields {
		b.WriteString("  " + graphQLName(f.Name) + ": " + fieldTypeRef(f.Type) + "\n")
	}

	b.WriteString("}")

	return b.String()
}
