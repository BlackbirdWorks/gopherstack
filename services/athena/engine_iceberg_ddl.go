package athena

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

const (
	partitionFieldIDBase = 1000
	millisPerSecondInt   = 1000
)

const (
	fnTruncate           = "truncate"
	propFormat           = "format"
	propWriteCompression = "write_compression"
)

// icebergTableProps maps the TBLPROPERTIES Athena accepts for Iceberg tables to Iceberg table properties.
func icebergTableProps() map[string]string {
	return map[string]string{
		propFormat:                               "write.format.default",
		propWriteCompression:                     propParquetCodec,
		"optimize_rewrite_data_file_threshold":   "optimize_rewrite_data_file_threshold",
		"optimize_rewrite_delete_file_threshold": "optimize_rewrite_delete_file_threshold",
		"vacuum_min_snapshots_to_keep":           "history.expire.min-snapshots-to-keep",
		"vacuum_max_snapshot_age_seconds":        "history.expire.max-snapshot-age-ms",
		"vacuum_max_metadata_files_to_keep":      "write.metadata.previous-versions-max",
		"write_data_path_enabled":                "write.data.path.enabled",
	}
}

func (b *InMemoryBackend) ddlDeps() (S3Getter, S3Lister, S3Storer, bool) {
	b.mu.RLock("ddlDeps")
	defer b.mu.RUnlock()

	_, get, list, ok := b.engineDeps()

	return get, list, b.s3, ok && b.s3 != nil
}

func icebergProps(raw map[string]string) (map[string]string, error) {
	out := map[string]string{}

	for k, v := range raw {
		lk := strings.ToLower(k)
		if lk == paramTableType {
			continue
		}

		target, ok := icebergTableProps()[lk]
		if !ok {
			return nil, &syntaxError{"Unsupported table property key: " + k}
		}

		switch lk {
		case propFormat:
			if !strings.EqualFold(v, "parquet") {
				return nil, fmt.Errorf(
					"%w: Iceberg format %s; the embedded engine writes PARQUET",
					errUnsupportedFormat,
					v,
				)
			}

			v = strings.ToLower(v)
		case propWriteCompression:
			v = strings.ToLower(v)
		case "vacuum_max_snapshot_age_seconds":
			secs, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid value %q for %s", v, k) //nolint:err113 // user message
			}

			v = strconv.FormatInt(secs*millisPerSecondInt, 10)
		}

		out[target] = v
	}

	return out, nil
}

func (b *InMemoryBackend) createIcebergTable(
	spec *createTableSpec, db string, cat GlueTableCatalog,
) (*sqlResult, statementOutcome, bool) {
	if spec.external {
		return nil, stmtFail(athenaErrTypeSyntax, "External keyword not supported for table type ICEBERG"), true
	}

	_, _, put, ok := b.ddlDeps()
	if !ok {
		return nil, stmtOK(), false
	}

	if _, err := cat.GetTableDef(db, spec.def.Name); err == nil {
		if spec.ifNotExists {
			return &sqlResult{}, stmtOK(), true
		}

		return nil, stmtFail(athenaErrTypeEntityFound, "Table %s already exists", spec.def.Name), true
	}

	meta, err := newIcebergMetadata(spec)
	if err != nil {
		return nil, engineFailure(err), true
	}

	body, err := iceberg.EncodeMetadata(meta)
	if err != nil {
		return nil, engineFailure(err), true
	}

	loc := iceberg.InitialMetadataLocation(meta.Location)
	wp := &writePlan{put: put}

	if out := wp.putObjectAt(context.Background(), loc, body); out.failed {
		return nil, out, true
	}

	spec.def.Location = meta.Location
	spec.def.Parameters = map[string]string{paramTableType: tableTypeIceberg, paramMetadataLoc: loc}
	spec.def.SerdeParameters = nil

	switch err = cat.CreateTableDef(db, spec.def); {
	case err == nil:
		return &sqlResult{}, stmtOK(), true
	case errors.Is(err, ErrCatalogEntityExists):
		return nil, stmtFail(athenaErrTypeEntityFound, "Table %s already exists", spec.def.Name), true
	case errors.Is(err, ErrCatalogEntityNotFound):
		return nil, stmtFail(athenaErrTypeEntityMiss, "Database does not exist: %s", db), true
	default:
		return nil, stmtFail(athenaErrTypeSyntax, "%s", err.Error()), true
	}
}

func newIcebergMetadata(spec *createTableSpec) (*iceberg.TableMetadata, error) {
	cols := make([]iceberg.ColumnSpec, len(spec.def.Columns))
	for i, c := range spec.def.Columns {
		cols[i] = iceberg.ColumnSpec{Name: c.Name, Type: c.Type, Comment: c.Comment}
	}

	schema, err := iceberg.SchemaFromColumns(cols)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errUnsupportedFormat, err)
	}

	pspec, err := parsePartitionSpec(schema, spec.partTokens)
	if err != nil {
		return nil, err
	}

	props, err := icebergProps(spec.def.Parameters)
	if err != nil {
		return nil, err
	}

	if _, _, ok := splitS3URI(spec.def.Location); !ok {
		return nil, fmt.Errorf("%w %q", errBadLocation, spec.def.Location)
	}

	return iceberg.NewTableMetadata(iceberg.NewParams{
		Location: strings.TrimSuffix(spec.def.Location, "/"), Schema: schema, Spec: pspec,
		Properties: props, Now: time.Now(),
	}), nil
}

// parsePartitionSpec reads PARTITIONED BY (col, bucket(16, col), day(ts), ...).
func parsePartitionSpec(schema iceberg.Schema, toks []token) (iceberg.PartitionSpec, error) {
	spec := iceberg.PartitionSpec{Fields: []iceberg.PartitionField{}}

	for _, part := range splitCommas(toks) {
		sig := significant(part)
		if len(sig) == 0 {
			continue
		}

		pf, err := partitionField(schema, sig)
		if err != nil {
			return spec, err
		}

		pf.FieldID = partitionFieldIDBase + len(spec.Fields)
		spec.Fields = append(spec.Fields, pf)
	}

	return spec, nil
}

func fieldByName(schema iceberg.Schema, name string) (iceberg.Field, error) {
	for _, f := range schema.Fields {
		if strings.EqualFold(f.Name, name) {
			return f, nil
		}
	}

	return iceberg.Field{}, &syntaxError{
		fmt.Sprintf("COLUMN_NOT_FOUND: line 1:1: Partition column '%s' not found in table", name),
	}
}

type transformCall struct {
	fn    string
	width string
	col   string
}

func ddlSyntax(msg string) error { return &syntaxError{"SYNTAX_ERROR: line 1:1: " + msg} }

// parseTransformCall reads fn(col) or fn(width, col).
func parseTransformCall(sig []token) (transformCall, error) {
	if len(sig) < argsThree || !sig[1].isPunct("(") || !sig[len(sig)-1].isPunct(")") {
		return transformCall{}, ddlSyntax("invalid partition transform")
	}

	c := transformCall{fn: strings.ToLower(sig[0].text)}
	args := splitCommas(sig[2 : len(sig)-1])
	colArg := 0

	if c.fn == "bucket" || c.fn == fnTruncate {
		if len(args) != argsTwo {
			return c, ddlSyntax(c.fn + " takes a width and a column")
		}

		c.width, colArg = strings.TrimSpace(joinTokens(args[0])), 1
	} else if len(args) != 1 {
		return c, ddlSyntax(c.fn + " takes one column")
	}

	colToks := significant(args[colArg])
	if len(colToks) != 1 {
		return c, ddlSyntax("invalid partition column")
	}

	c.col = colToks[0].ident()

	return c, nil
}

func partitionField(schema iceberg.Schema, sig []token) (iceberg.PartitionField, error) {
	if len(sig) == 1 {
		f, err := fieldByName(schema, sig[0].ident())

		return iceberg.PartitionField{Name: f.Name, Transform: "identity", SourceID: f.ID}, err
	}

	c, err := parseTransformCall(sig)
	if err != nil {
		return iceberg.PartitionField{}, err
	}

	f, err := fieldByName(schema, c.col)
	if err != nil {
		return iceberg.PartitionField{}, err
	}

	transform, suffix := c.fn, "_"+c.fn

	switch c.fn {
	case "bucket", fnTruncate:
		transform = c.fn + "[" + c.width + "]"
		if c.fn == fnTruncate {
			suffix = "_trunc"
		}
	case "year", "month", "day", "hour":
	default:
		return iceberg.PartitionField{}, ddlSyntax("unknown partition transform '" + c.fn + "'")
	}

	if _, perr := iceberg.ParseTransform(transform); perr != nil {
		return iceberg.PartitionField{}, ddlSyntax("invalid partition transform '" + transform + "'")
	}

	return iceberg.PartitionField{Name: f.Name + suffix, Transform: transform, SourceID: f.ID}, nil
}

func (b *InMemoryBackend) createDeltaTable(
	spec *createTableSpec, db string, cat GlueTableCatalog,
) (*sqlResult, statementOutcome, bool) {
	get, list, _, ok := b.ddlDeps()
	if !ok {
		return nil, stmtOK(), false
	}

	if len(spec.def.Columns) > 0 {
		return nil, stmtFail(athenaErrTypeSyntax,
			"NOT_SUPPORTED: CREATE TABLE for Delta Lake tables accepts only LOCATION and the table_type property"), true
	}

	if _, err := cat.GetTableDef(db, spec.def.Name); err == nil {
		if spec.ifNotExists {
			return &sqlResult{}, stmtOK(), true
		}

		return nil, stmtFail(athenaErrTypeEntityFound, "Table %s already exists", spec.def.Name), true
	}

	l := &tableLoader{get: get, list: list, limits: b.limitsSnapshot()}

	snap, err := l.loadDeltaSnapshot(context.Background(), spec.def.Location, spec.def.Name)
	if err != nil {
		return nil, engineFailure(err), true
	}

	part := map[string]bool{}
	for _, p := range snap.PartitionColumns {
		part[p] = true
	}

	def := spec.def
	def.Columns, def.PartitionKeys = nil, nil

	for _, c := range snap.Columns {
		if part[c.Name] {
			def.PartitionKeys = append(def.PartitionKeys, Column{Name: c.Name, Type: c.Type})
		} else {
			def.Columns = append(def.Columns, Column{Name: c.Name, Type: c.Type})
		}
	}

	def.Parameters = map[string]string{paramTableType: tableTypeDelta, "EXTERNAL": externalTrue}
	def.SerdeLibrary, def.InputFormat, def.OutputFormat = parquetSerde, parquetInputFmt, parquetOutFmt

	switch err = cat.CreateTableDef(db, def); {
	case err == nil:
		return &sqlResult{}, stmtOK(), true
	case errors.Is(err, ErrCatalogEntityNotFound):
		return nil, stmtFail(athenaErrTypeEntityMiss, "Database does not exist: %s", db), true
	default:
		return nil, stmtFail(athenaErrTypeSyntax, "%s", err.Error()), true
	}
}
