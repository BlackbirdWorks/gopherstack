package athena

import (
	"context"
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

const (
	propTableType    = "table_type"
	propLocation     = "location"
	propPartitioning = "partitioning"
)

func isIcebergCTASProp(k string) bool {
	if _, ok := icebergTableProps()[k]; ok {
		return true
	}

	switch k {
	case propTableType, propLocation, "is_external", propPartitioning,
		"optimize_rewrite_min_data_file_size_bytes", "optimize_rewrite_max_data_file_size_bytes",
		"parquet_compression", "compression":
		return true
	default:
		return false
	}
}

func (p *writePlan) isIcebergCTAS() bool {
	return p.stmt.kind == writeCTAS && strings.EqualFold(firstProp(p.stmt.props, propTableType), tableTypeIceberg)
}

func (p *writePlan) validateIcebergCTAS() statementOutcome {
	props := p.stmt.props

	for k := range props {
		if k == "external_location" {
			return stmtFail(athenaErrTypeSyntax,
				"INVALID_TABLE_PROPERTY: line 1:1: external_location is not supported for table type ICEBERG; use location")
		}

		if !isIcebergCTASProp(k) {
			return stmtFail(athenaErrTypeSyntax,
				"INVALID_TABLE_PROPERTY: line 1:1: table property '%s' is not supported", k)
		}
	}

	if strings.EqualFold(firstProp(props, "is_external"), strTrue) {
		return stmtFail(athenaErrTypeSyntax, "INVALID_TABLE_PROPERTY: line 1:1: is_external must be false for ICEBERG tables")
	}

	if f := strings.ToUpper(firstProp(props, propFormat)); f != "" && f != fmtNameParquet && f != fmtNameAvro {
		return stmtFail(athenaErrTypeSyntax,
			"NOT_SUPPORTED: format %s is not supported for Iceberg tables by the embedded SQL engine; use PARQUET or AVRO", f)
	}

	p.iceCTAS = true

	return stmtOK()
}

// partitioningSpec turns partitioning = ARRAY['month(ts)', 'bucket(id, 8)', 'country'] into a spec.
func partitioningSpec(schema iceberg.Schema, entries []string) (iceberg.PartitionSpec, error) {
	spec := iceberg.PartitionSpec{Fields: []iceberg.PartitionField{}}

	for _, e := range entries {
		toks, err := tokenize(e)
		if err != nil {
			return spec, err
		}

		sig := significant(toks)
		if len(sig) > 1 {
			sig = normalizeTransform(sig)
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

// normalizeTransform reorders bucket(col, N) / truncate(col, N) to the DDL form (N, col).
func normalizeTransform(sig []token) []token {
	if len(sig) != 6 || !sig[1].isPunct("(") || !sig[3].isPunct(",") {
		return sig
	}

	fn := strings.ToLower(sig[0].text)
	if (fn == "bucket" || fn == fnTruncate) && sig[2].kind != tkNumber && sig[4].kind == tkNumber {
		return []token{sig[0], sig[1], sig[4], sig[3], sig[2], sig[5]}
	}

	return sig
}

func (p *writePlan) applyIcebergCTAS(ctx context.Context, res *sqlResult, limits EngineLimits) statementOutcome {
	props := p.stmt.props

	raw := map[string]string{}

	for k, v := range props {
		switch k {
		case propTableType, propLocation, "is_external", propPartitioning,
			"optimize_rewrite_min_data_file_size_bytes", "optimize_rewrite_max_data_file_size_bytes":
		case "parquet_compression", "compression":
			raw[propWriteCompression] = v[0]
		default:
			raw[k] = v[0]
		}
	}

	icProps, err := icebergProps(raw)
	if err != nil {
		return engineFailure(err)
	}

	specs := make([]iceberg.ColumnSpec, len(res.columns))
	seen := map[string]bool{}

	for i, c := range res.columns {
		name := strings.ToLower(c.name)
		if seen[name] {
			return stmtFail(athenaErrTypeSyntax, "line 1:1: Duplicate column name '%s'", name)
		}

		seen[name] = true
		specs[i] = iceberg.ColumnSpec{Name: name, Type: c.glue}
	}

	schema, err := iceberg.SchemaFromColumns(specs)
	if err != nil {
		return stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: %s", err.Error())
	}

	pspec, err := partitioningSpec(schema, props[propPartitioning])
	if err != nil {
		return engineFailure(err)
	}

	meta := iceberg.NewTableMetadata(iceberg.NewParams{
		Location: strings.TrimSuffix(p.location, "/"), Schema: schema, Spec: pspec,
		Properties: icProps, Now: p.now(),
	})

	body, err := iceberg.EncodeMetadata(meta)
	if err != nil {
		return engineFailure(err)
	}

	loc := iceberg.InitialMetadataLocation(meta.Location)
	if out := p.putObjectAt(ctx, loc, body); out.failed {
		return out
	}

	p.ice = &icebergTable{meta: meta, location: loc, display: p.db + "." + p.table}
	current := loc

	if !p.stmt.noData && len(res.rows) > 0 {
		src := make([]int, len(specs))
		for i := range src {
			src[i] = i
		}

		files, out := p.writeIcebergFiles(ctx, res, src, limits)
		if out.failed {
			return out
		}

		plan, perr := meta.PlanAppend(loc, nil, files, p.now().UTC())
		if perr != nil {
			return engineFailure(perr)
		}

		for _, o := range plan.Objects {
			if out = p.putObjectAt(ctx, o.Location, o.Body); out.failed {
				return out
			}
		}

		current = plan.MetadataLocation
	}

	cols := make([]Column, len(specs))
	for i, s := range specs {
		cols[i] = Column{Name: s.Name, Type: res.columns[i].glue}
	}

	def := GlueTableDef{
		Name: p.table, TableType: tableTypeExternal, Location: meta.Location, Columns: cols,
		Parameters: map[string]string{paramTableType: tableTypeIceberg, paramMetadataLoc: current},
	}

	if err = p.cat.CreateTableDef(p.db, def); err != nil {
		if errors.Is(err, ErrCatalogEntityExists) {
			return stmtFail(athenaErrTypeEntityFound, "TABLE_ALREADY_EXISTS: line 1:1: Table '%s' already exists", p.display())
		}

		return stmtFail(athenaErrTypeSyntax, "line 1:1: Unable to create table: %s", err.Error())
	}

	return stmtOK()
}
