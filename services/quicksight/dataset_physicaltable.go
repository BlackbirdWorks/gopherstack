package quicksight

// keyInputColumns is the wire key shared by RelationalTable/CustomSql/
// S3Source/FileSource/SaaSTable's InputColumns member (CustomSql calls its
// own version "Columns" instead, see parseCustomSQL/customSQLToWire).
const keyInputColumns = "InputColumns"

// Parsing, wire-serialization, and deep-clone helpers for DataSet's
// PhysicalTableMap/LogicalTableMap fields (quicksight@v1.123.1
// api_op_CreateDataSet.go:55, api_op_UpdateDataSet.go:55 -- both required).
// See types.go for the modeled shapes and why DataTransforms/join-key
// properties are left inert.

// ---- request parsing ----

// physicalTableMapFromBody parses the required PhysicalTableMap field. AWS
// rejects a CreateDataSet/UpdateDataSet call with no physical tables, so an
// absent or empty map is a validation error rather than a legal "no tables"
// dataset.
func physicalTableMapFromBody(body map[string]any) (map[string]PhysicalTable, error) {
	raw, ok := body["PhysicalTableMap"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil, ErrValidation
	}

	out := make(map[string]PhysicalTable, len(raw))
	for id, v := range raw {
		entry, entryOK := v.(map[string]any)
		if !entryOK {
			return nil, ErrValidation
		}

		pt, err := parsePhysicalTable(entry)
		if err != nil {
			return nil, err
		}
		out[id] = pt
	}

	return out, nil
}

// logicalTableMapFromBody parses the optional LogicalTableMap field. A
// missing field is not an error -- LogicalTableMap is optional -- so the
// nil-map/nil-error return here is intentional, not an oversight.
//
//nolint:nilnil // absent LogicalTableMap is "none provided", not a failure
func logicalTableMapFromBody(body map[string]any) (map[string]LogicalTable, error) {
	raw, ok := body["LogicalTableMap"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil, nil
	}

	out := make(map[string]LogicalTable, len(raw))
	for id, v := range raw {
		entry, entryOK := v.(map[string]any)
		if !entryOK {
			return nil, ErrValidation
		}
		out[id] = parseLogicalTable(entry)
	}

	return out, nil
}

// parsePhysicalTable requires exactly one of the five wire union members to
// be present, matching real AWS's "only one of the attributes can be
// non-null" union contract on PhysicalTable.
func parsePhysicalTable(entry map[string]any) (PhysicalTable, error) {
	var pt PhysicalTable

	n := 0
	if v, ok := entry["RelationalTable"].(map[string]any); ok {
		pt.RelationalTable = parseRelationalTable(v)
		n++
	}
	if v, ok := entry["CustomSql"].(map[string]any); ok {
		pt.CustomSQL = parseCustomSQL(v)
		n++
	}
	if v, ok := entry["S3Source"].(map[string]any); ok {
		pt.S3Source = parseS3Source(v)
		n++
	}
	if v, ok := entry["FileSource"].(map[string]any); ok {
		pt.FileSource = parseFileSource(v)
		n++
	}
	if v, ok := entry["SaaSTable"].(map[string]any); ok {
		pt.SaaSTable = parseSaaSTable(v)
		n++
	}

	if n != 1 {
		return PhysicalTable{}, ErrValidation
	}

	return pt, nil
}

func parseRelationalTable(v map[string]any) *RelationalTable {
	return &RelationalTable{
		DataSourceArn: strField(v, keyDataSourceArn),
		Name:          strField(v, keyName),
		Catalog:       strField(v, "Catalog"),
		Schema:        strField(v, "Schema"),
		InputColumns:  parseInputColumns(v[keyInputColumns]),
	}
}

func parseCustomSQL(v map[string]any) *CustomSQL {
	return &CustomSQL{
		DataSourceArn: strField(v, keyDataSourceArn),
		Name:          strField(v, keyName),
		SQLQuery:      strField(v, "SqlQuery"),
		Columns:       parseInputColumns(v["Columns"]),
	}
}

func parseS3Source(v map[string]any) *S3Source {
	return &S3Source{
		DataSourceArn:  strField(v, keyDataSourceArn),
		InputColumns:   parseInputColumns(v[keyInputColumns]),
		UploadSettings: parseUploadSettings(v["UploadSettings"]),
	}
}

func parseFileSource(v map[string]any) *FileSource {
	return &FileSource{
		DataSourceArn:  strField(v, keyDataSourceArn),
		InputColumns:   parseInputColumns(v[keyInputColumns]),
		UploadSettings: parseUploadSettings(v["UploadSettings"]),
		SheetIndex:     intField(v, "SheetIndex"),
	}
}

func parseSaaSTable(v map[string]any) *SaaSTable {
	return &SaaSTable{
		DataSourceArn: strField(v, keyDataSourceArn),
		InputColumns:  parseInputColumns(v[keyInputColumns]),
		TablePath:     parseTablePath(v["TablePath"]),
	}
}

func parseInputColumns(raw any) []InputColumn {
	list, ok := raw.([]any)
	if len(list) == 0 || !ok {
		return nil
	}

	out := make([]InputColumn, 0, len(list))
	for _, item := range list {
		m, itemOK := item.(map[string]any)
		if !itemOK {
			continue
		}
		out = append(out, InputColumn{
			ID:      strField(m, "Id"),
			Name:    strField(m, keyName),
			Type:    strField(m, keyConnectorType),
			SubType: strField(m, "SubType"),
		})
	}

	return out
}

func parseUploadSettings(raw any) *UploadSettings {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}

	us := &UploadSettings{
		CustomCellAddressRange: strField(m, "CustomCellAddressRange"),
		Delimiter:              strField(m, "Delimiter"),
		Format:                 strField(m, "Format"),
		TextQualifier:          strField(m, "TextQualifier"),
	}
	if b, headerOK := m["ContainsHeader"].(bool); headerOK {
		us.ContainsHeader = &b
	}
	if _, rowOK := m["StartFromRow"]; rowOK {
		v := intField(m, "StartFromRow")
		us.StartFromRow = &v
	}

	return us
}

func parseTablePath(raw any) []TablePathElement {
	list, ok := raw.([]any)
	if len(list) == 0 || !ok {
		return nil
	}

	out := make([]TablePathElement, 0, len(list))
	for _, item := range list {
		m, itemOK := item.(map[string]any)
		if !itemOK {
			continue
		}
		out = append(out, TablePathElement{ID: strField(m, "Id"), Name: strField(m, keyName)})
	}

	return out
}

func parseLogicalTable(entry map[string]any) LogicalTable {
	lt := LogicalTable{Alias: strField(entry, "Alias")}

	if v, ok := entry["Source"].(map[string]any); ok {
		lt.Source = parseLogicalTableSource(v)
	}
	if v, ok := entry["DataTransforms"].([]any); ok {
		lt.DataTransforms = v
	}

	return lt
}

func parseLogicalTableSource(v map[string]any) *LogicalTableSource {
	src := &LogicalTableSource{
		DataSetArn:      strField(v, "DataSetArn"),
		PhysicalTableID: strField(v, "PhysicalTableId"),
	}
	if j, ok := v["JoinInstruction"].(map[string]any); ok {
		src.JoinInstruction = &JoinInstruction{
			LeftOperand:  strField(j, "LeftOperand"),
			OnClause:     strField(j, "OnClause"),
			RightOperand: strField(j, "RightOperand"),
			Type:         strField(j, "Type"),
		}
	}

	return src
}

// ---- response wire serialization ----

func physicalTableMapToWire(m map[string]PhysicalTable) map[string]any {
	out := make(map[string]any, len(m))
	for id, pt := range m {
		out[id] = physicalTableToWire(pt)
	}

	return out
}

func physicalTableToWire(pt PhysicalTable) map[string]any {
	switch {
	case pt.RelationalTable != nil:
		return map[string]any{"RelationalTable": relationalTableToWire(pt.RelationalTable)}
	case pt.CustomSQL != nil:
		return map[string]any{"CustomSql": customSQLToWire(pt.CustomSQL)}
	case pt.S3Source != nil:
		return map[string]any{"S3Source": s3SourceToWire(pt.S3Source)}
	case pt.FileSource != nil:
		return map[string]any{"FileSource": fileSourceToWire(pt.FileSource)}
	case pt.SaaSTable != nil:
		return map[string]any{"SaaSTable": saaSTableToWire(pt.SaaSTable)}
	default:
		return map[string]any{}
	}
}

func relationalTableToWire(rt *RelationalTable) map[string]any {
	w := map[string]any{
		keyDataSourceArn: rt.DataSourceArn,
		keyName:          rt.Name,
		keyInputColumns:  inputColumnsToWire(rt.InputColumns),
	}
	if rt.Catalog != "" {
		w["Catalog"] = rt.Catalog
	}
	if rt.Schema != "" {
		w["Schema"] = rt.Schema
	}

	return w
}

func customSQLToWire(cs *CustomSQL) map[string]any {
	return map[string]any{
		keyDataSourceArn: cs.DataSourceArn,
		keyName:          cs.Name,
		"SqlQuery":       cs.SQLQuery,
		"Columns":        inputColumnsToWire(cs.Columns),
	}
}

func s3SourceToWire(s *S3Source) map[string]any {
	w := map[string]any{
		keyDataSourceArn: s.DataSourceArn,
		keyInputColumns:  inputColumnsToWire(s.InputColumns),
	}
	if s.UploadSettings != nil {
		w["UploadSettings"] = uploadSettingsToWire(s.UploadSettings)
	}

	return w
}

func fileSourceToWire(f *FileSource) map[string]any {
	w := map[string]any{
		keyDataSourceArn: f.DataSourceArn,
		keyInputColumns:  inputColumnsToWire(f.InputColumns),
		"SheetIndex":     f.SheetIndex,
	}
	if f.UploadSettings != nil {
		w["UploadSettings"] = uploadSettingsToWire(f.UploadSettings)
	}

	return w
}

func saaSTableToWire(s *SaaSTable) map[string]any {
	return map[string]any{
		keyDataSourceArn: s.DataSourceArn,
		keyInputColumns:  inputColumnsToWire(s.InputColumns),
		"TablePath":      tablePathToWire(s.TablePath),
	}
}

func inputColumnsToWire(cols []InputColumn) []any {
	out := make([]any, 0, len(cols))
	for _, c := range cols {
		w := map[string]any{keyName: c.Name, keyConnectorType: c.Type}
		if c.ID != "" {
			w["Id"] = c.ID
		}
		if c.SubType != "" {
			w["SubType"] = c.SubType
		}
		out = append(out, w)
	}

	return out
}

func uploadSettingsToWire(us *UploadSettings) map[string]any {
	w := map[string]any{}
	if us.ContainsHeader != nil {
		w["ContainsHeader"] = *us.ContainsHeader
	}
	if us.CustomCellAddressRange != "" {
		w["CustomCellAddressRange"] = us.CustomCellAddressRange
	}
	if us.Delimiter != "" {
		w["Delimiter"] = us.Delimiter
	}
	if us.Format != "" {
		w["Format"] = us.Format
	}
	if us.StartFromRow != nil {
		w["StartFromRow"] = *us.StartFromRow
	}
	if us.TextQualifier != "" {
		w["TextQualifier"] = us.TextQualifier
	}

	return w
}

func tablePathToWire(path []TablePathElement) []any {
	out := make([]any, 0, len(path))
	for _, p := range path {
		w := map[string]any{}
		if p.ID != "" {
			w["Id"] = p.ID
		}
		if p.Name != "" {
			w[keyName] = p.Name
		}
		out = append(out, w)
	}

	return out
}

func logicalTableMapToWire(m map[string]LogicalTable) map[string]any {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]any, len(m))
	for id, lt := range m {
		out[id] = logicalTableToWire(lt)
	}

	return out
}

func logicalTableToWire(lt LogicalTable) map[string]any {
	w := map[string]any{"Alias": lt.Alias}
	if lt.Source != nil {
		w["Source"] = logicalTableSourceToWire(lt.Source)
	}
	if len(lt.DataTransforms) > 0 {
		w["DataTransforms"] = lt.DataTransforms
	}

	return w
}

func logicalTableSourceToWire(src *LogicalTableSource) map[string]any {
	w := map[string]any{}
	if src.DataSetArn != "" {
		w["DataSetArn"] = src.DataSetArn
	}
	if src.PhysicalTableID != "" {
		w["PhysicalTableId"] = src.PhysicalTableID
	}
	if src.JoinInstruction != nil {
		w["JoinInstruction"] = map[string]any{
			"LeftOperand":  src.JoinInstruction.LeftOperand,
			"OnClause":     src.JoinInstruction.OnClause,
			"RightOperand": src.JoinInstruction.RightOperand,
			"Type":         src.JoinInstruction.Type,
		}
	}

	return w
}

// ---- deep clones (stored state must never alias a caller's or response's slices/maps) ----

func clonePhysicalTableMap(m map[string]PhysicalTable) map[string]PhysicalTable {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]PhysicalTable, len(m))
	for id, pt := range m {
		out[id] = clonePhysicalTable(pt)
	}

	return out
}

func clonePhysicalTable(pt PhysicalTable) PhysicalTable {
	var out PhysicalTable
	switch {
	case pt.RelationalTable != nil:
		rt := *pt.RelationalTable
		rt.InputColumns = cloneInputColumns(rt.InputColumns)
		out.RelationalTable = &rt
	case pt.CustomSQL != nil:
		cs := *pt.CustomSQL
		cs.Columns = cloneInputColumns(cs.Columns)
		out.CustomSQL = &cs
	case pt.S3Source != nil:
		s := *pt.S3Source
		s.InputColumns = cloneInputColumns(s.InputColumns)
		s.UploadSettings = cloneUploadSettings(s.UploadSettings)
		out.S3Source = &s
	case pt.FileSource != nil:
		f := *pt.FileSource
		f.InputColumns = cloneInputColumns(f.InputColumns)
		f.UploadSettings = cloneUploadSettings(f.UploadSettings)
		out.FileSource = &f
	case pt.SaaSTable != nil:
		s := *pt.SaaSTable
		s.InputColumns = cloneInputColumns(s.InputColumns)
		s.TablePath = append([]TablePathElement(nil), s.TablePath...)
		out.SaaSTable = &s
	}

	return out
}

func cloneInputColumns(cols []InputColumn) []InputColumn {
	if len(cols) == 0 {
		return nil
	}

	return append([]InputColumn(nil), cols...)
}

func cloneUploadSettings(us *UploadSettings) *UploadSettings {
	if us == nil {
		return nil
	}

	out := *us
	if us.ContainsHeader != nil {
		v := *us.ContainsHeader
		out.ContainsHeader = &v
	}
	if us.StartFromRow != nil {
		v := *us.StartFromRow
		out.StartFromRow = &v
	}

	return &out
}

func cloneLogicalTableMap(m map[string]LogicalTable) map[string]LogicalTable {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]LogicalTable, len(m))
	for id, lt := range m {
		out[id] = cloneLogicalTable(lt)
	}

	return out
}

func cloneLogicalTable(lt LogicalTable) LogicalTable {
	out := lt
	if lt.Source != nil {
		src := *lt.Source
		if lt.Source.JoinInstruction != nil {
			ji := *lt.Source.JoinInstruction
			src.JoinInstruction = &ji
		}
		out.Source = &src
	}
	// DataTransforms is opaque decoded JSON (see types.go doc comment); a
	// shallow copy matches Dashboard/Analysis.Definition's existing
	// treatment of raw map[string]any blobs elsewhere in this package.
	out.DataTransforms = lt.DataTransforms

	return out
}
