package redshiftdata

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const (
	pgInt4            = "int4"
	pgTimestamp       = "timestamp"
	unloadPartName    = "0000_part_00"
	pgTimestamptz     = "timestamptz"
	pgTimestamptzFmt  = "2006-01-02 15:04:05.999999-07"
	parquetTimeLayout = "2006-01-02 15:04:05.999999999"
)

type unloadFile struct {
	key     string
	body    []byte
	records int
}

func (r *s3Runner) unload(ctx context.Context, q querier, s *unloadSpec) error {
	if err := r.requireStore(); err != nil {
		return err
	}

	rs, err := runUnloadQuery(ctx, q, s.query)
	if err != nil {
		return err
	}

	existing, err := r.store.ListKeys(ctx, s.bucket, s.prefix)
	if err != nil {
		return stmtErrorf("S3ServiceException:%v,Status 404,Error NoSuchBucket,Rid 0,CanRetry 1", err)
	}

	if err = r.clearDestination(ctx, s, existing); err != nil {
		return err
	}

	files, err := buildUnloadFiles(s, rs)
	if err != nil {
		return err
	}

	for _, f := range files {
		if err = r.store.PutObject(ctx, s.bucket, f.key, f.body); err != nil {
			return stmtErrorf("S3ServiceException:%v,Status 403,Error AccessDenied,Rid 0,CanRetry 1", err)
		}
	}

	if !s.manifest {
		return nil
	}

	if err = r.store.PutObject(ctx, s.bucket, s.prefix+"manifest", unloadManifest(s, rs, files)); err != nil {
		return stmtErrorf("S3ServiceException:%v,Status 403,Error AccessDenied,Rid 0,CanRetry 1", err)
	}

	return nil
}

func runUnloadQuery(ctx context.Context, q querier, query string) (*ResultSet, error) {
	rs, err := runStatement(ctx, q, nil, query, nil)
	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, stmtErrorf("UNLOAD requires a query that returns rows")
	}

	return rs, nil
}

func (r *s3Runner) clearDestination(ctx context.Context, s *unloadSpec, existing []string) error {
	if len(existing) == 0 || s.allowOverride && !s.cleanPath {
		return nil
	}

	if !s.cleanPath {
		return stmtErrorf("Specified unload destination on S3 is not empty. Consider using a different bucket / " +
			"prefix, manually removing the target files in S3, or using the ALLOWOVERWRITE option.")
	}

	for _, k := range existing {
		if err := r.store.DeleteObject(ctx, s.bucket, k); err != nil {
			return stmtErrorf("S3ServiceException:%v,Status 403,Error AccessDenied,Rid 0,CanRetry 1", err)
		}
	}

	return nil
}

func (s *unloadSpec) fileExtension() string {
	ext := s.extension

	if ext == "" {
		switch s.format {
		case fmtCSV, fmtLines, fmtParquet:
			ext = s.format
		}
	}

	if ext != "" {
		ext = "." + ext
	}

	if s.gzip {
		ext += ".gz"
	}

	return ext
}

func buildUnloadFiles(s *unloadSpec, rs *ResultSet) ([]unloadFile, error) {
	groups, err := partitionRows(s, rs)
	if err != nil {
		return nil, err
	}

	files := make([]unloadFile, 0, len(groups))

	for _, g := range groups {
		body, berr := encodeUnload(s, g.cols, g.rows)
		if berr != nil {
			return nil, berr
		}

		files = append(files, unloadFile{
			key: s.prefix + g.dir + unloadPartName + s.fileExtension(), body: body, records: len(g.rows),
		})
	}

	return files, nil
}

type rowGroup struct {
	dir  string
	cols []ColumnMetadata
	rows [][]Field
}

func partitionRows(s *unloadSpec, rs *ResultSet) ([]rowGroup, error) {
	if len(s.partitionBy) == 0 {
		return []rowGroup{{cols: rs.Columns, rows: rs.Rows}}, nil
	}

	partIdx := make([]int, len(s.partitionBy))

	for i, name := range s.partitionBy {
		partIdx[i] = -1

		for j, c := range rs.Columns {
			if strings.EqualFold(c.Name, name) {
				partIdx[i] = j
			}
		}

		if partIdx[i] < 0 {
			return nil, stmtErrorf("column %q specified in PARTITION BY does not exist in the query", name)
		}
	}

	keep := keptColumns(s, rs, partIdx)
	cols := make([]ColumnMetadata, len(keep))

	for i, k := range keep {
		cols[i] = rs.Columns[k]
	}

	var groups []rowGroup

	pos := map[string]int{}

	for _, row := range rs.Rows {
		var dir strings.Builder

		for i, pi := range partIdx {
			dir.WriteString(s.partitionBy[i] + "=" + url.PathEscape(fieldText(row[pi])) + "/")
		}

		gi, ok := pos[dir.String()]
		if !ok {
			gi = len(groups)
			pos[dir.String()] = gi
			groups = append(groups, rowGroup{dir: dir.String(), cols: cols})
		}

		kept := make([]Field, len(keep))
		for i, k := range keep {
			kept[i] = row[k]
		}

		groups[gi].rows = append(groups[gi].rows, kept)
	}

	if len(groups) == 0 {
		return nil, nil
	}

	return groups, nil
}

func keptColumns(s *unloadSpec, rs *ResultSet, partIdx []int) []int {
	var keep []int

	for j := range rs.Columns {
		isPart := false

		for _, pi := range partIdx {
			isPart = isPart || pi == j
		}

		if !isPart || s.includePart {
			keep = append(keep, j)
		}
	}

	return keep
}

func encodeUnload(s *unloadSpec, cols []ColumnMetadata, rows [][]Field) ([]byte, error) {
	var (
		body []byte
		err  error
	)

	switch s.format {
	case fmtParquet:
		return encodeParquet(cols, rows)
	case fmtCSV:
		body, err = encodeCSV(s, cols, rows)
	case fmtLines:
		body, err = encodeJSONLines(cols, rows)
	default:
		body = encodeDelimited(s, cols, rows)
	}

	if err != nil {
		return nil, err
	}

	if !s.gzip {
		return body, nil
	}

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	if _, err = zw.Write(body); err != nil {
		return nil, stmtErrorf("could not compress the unloaded data: %v", err)
	}

	if err = zw.Close(); err != nil {
		return nil, stmtErrorf("could not compress the unloaded data: %v", err)
	}

	return buf.Bytes(), nil
}

func fieldText(f Field) string {
	switch {
	case f.IsNull:
		return ""
	case f.StringValue != nil:
		return *f.StringValue
	case f.LongValue != nil:
		return strconv.FormatInt(*f.LongValue, 10)
	case f.DoubleValue != nil:
		return strconv.FormatFloat(*f.DoubleValue, 'g', -1, 64)
	case f.BooleanValue != nil:
		if *f.BooleanValue {
			return "t"
		}

		return "f"
	case f.BlobValue != nil:
		return hexOfBase64(*f.BlobValue)
	default:
		return ""
	}
}

func hexOfBase64(b64 string) string {
	raw, err := decodeBase64(b64)
	if err != nil {
		return b64
	}

	return hex.EncodeToString(raw)
}

func (s *unloadSpec) cellText(f Field) string {
	if f.IsNull {
		return s.nullAs
	}

	return fieldText(f)
}

func encodeCSV(s *unloadSpec, cols []ColumnMetadata, rows [][]Field) ([]byte, error) {
	var buf bytes.Buffer

	w := csv.NewWriter(&buf)
	w.Comma = ','

	if s.delimiter != "" {
		w.Comma = []rune(s.delimiter)[0]
	}

	if s.header {
		names := make([]string, len(cols))
		for i, c := range cols {
			names[i] = c.Name
		}

		if err := w.Write(names); err != nil {
			return nil, stmtErrorf("could not write CSV: %v", err)
		}
	}

	rec := make([]string, len(cols))

	for _, row := range rows {
		for i, f := range row {
			rec[i] = s.cellText(f)
		}

		if err := w.Write(rec); err != nil {
			return nil, stmtErrorf("could not write CSV: %v", err)
		}
	}

	w.Flush()

	return buf.Bytes(), nil
}

func encodeDelimited(s *unloadSpec, cols []ColumnMetadata, rows [][]Field) []byte {
	delim := copyPipe
	if s.delimiter != "" {
		delim = s.delimiter
	}

	var buf bytes.Buffer

	if s.header {
		names := make([]string, len(cols))
		for i, c := range cols {
			names[i] = c.Name
		}

		buf.WriteString(strings.Join(names, delim) + "\n")
	}

	for _, row := range rows {
		for i, f := range row {
			if i > 0 {
				buf.WriteString(delim)
			}

			buf.WriteString(s.delimitedCell(f, delim))
		}

		buf.WriteByte('\n')
	}

	return buf.Bytes()
}

func (s *unloadSpec) delimitedCell(f Field, delim string) string {
	v := s.cellText(f)

	if s.escape {
		v = strings.NewReplacer(`\`, `\\`, "\n", `\n`, "\r", `\r`, delim, `\`+delim, `"`, `\"`).Replace(v)
	}

	if s.addQuotes && !f.IsNull {
		return `"` + v + `"`
	}

	return v
}

func encodeJSONLines(cols []ColumnMetadata, rows [][]Field) ([]byte, error) {
	var buf bytes.Buffer

	for _, row := range rows {
		buf.WriteByte('{')

		for i, f := range row {
			name, err := json.Marshal(cols[i].Name)
			if err != nil {
				return nil, stmtErrorf("could not write JSON: %v", err)
			}

			val, err := json.Marshal(jsonValue(f))
			if err != nil {
				return nil, stmtErrorf("could not write JSON: %v", err)
			}

			if i > 0 {
				buf.WriteByte(',')
			}

			buf.Write(name)
			buf.WriteByte(':')
			buf.Write(val)
		}

		buf.WriteString("}\n")
	}

	return buf.Bytes(), nil
}

func jsonValue(f Field) any {
	switch {
	case f.IsNull:
		return nil
	case f.StringValue != nil:
		return *f.StringValue
	case f.LongValue != nil:
		return *f.LongValue
	case f.DoubleValue != nil:
		return *f.DoubleValue
	case f.BooleanValue != nil:
		return *f.BooleanValue
	case f.BlobValue != nil:
		return hexOfBase64(*f.BlobValue)
	default:
		return nil
	}
}

func parquetColumnType(c ColumnMetadata) string {
	switch c.TypeName {
	case "bool":
		return "boolean"
	case "int2":
		return "smallint"
	case pgInt4:
		return "int"
	case "int8":
		return "bigint"
	case "float4":
		return "float"
	case "float8":
		return "double"
	case "date":
		return "date"
	case pgTimestamp, pgTimestamptz:
		return pgTimestamp
	case "bytea":
		return "binary"
	case "numeric":
		if c.Precision > 0 {
			return "decimal(" + strconv.Itoa(int(c.Precision)) + "," + strconv.Itoa(int(c.Scale)) + ")"
		}

		return "string"
	default:
		return "string"
	}
}

func parquetCell(c ColumnMetadata, f Field) any {
	switch {
	case f.IsNull:
		return nil
	case f.LongValue != nil:
		return *f.LongValue
	case f.DoubleValue != nil:
		return *f.DoubleValue
	case f.BooleanValue != nil:
		return *f.BooleanValue
	case f.BlobValue != nil:
		raw, err := decodeBase64(*f.BlobValue)
		if err != nil {
			return *f.BlobValue
		}

		return string(raw)
	case f.StringValue != nil && c.TypeName == pgTimestamptz:
		if t, err := time.Parse(pgTimestamptzFmt, *f.StringValue); err == nil {
			return t.UTC().Format(parquetTimeLayout)
		}
	}

	return fieldText(f)
}

func encodeParquet(cols []ColumnMetadata, rows [][]Field) ([]byte, error) {
	pcols := make([]parquetio.Column, len(cols))
	for i, c := range cols {
		pcols[i] = parquetio.Column{Name: c.Name, Type: parquetColumnType(c)}
	}

	cells := make([][]any, len(rows))

	for i, row := range rows {
		cells[i] = make([]any, len(row))
		for j, f := range row {
			cells[i][j] = parquetCell(cols[j], f)
		}
	}

	var buf bytes.Buffer
	if err := parquetio.Write(&buf, pcols, cells, ""); err != nil {
		return nil, stmtErrorf("could not write Parquet: %v", err)
	}

	return buf.Bytes(), nil
}

func unloadManifest(s *unloadSpec, rs *ResultSet, files []unloadFile) []byte {
	type meta struct {
		ContentLength int `json:"content_length"`
		RecordCount   int `json:"record_count,omitempty"`
	}

	type entry struct {
		Meta *meta  `json:"meta,omitempty"`
		URL  string `json:"url"`
	}

	entries := make([]entry, len(files))

	for i, f := range files {
		m := &meta{ContentLength: len(f.body)}
		if s.verbose {
			m.RecordCount = f.records
		}

		entries[i] = entry{URL: "s3://" + s.bucket + "/" + f.key, Meta: m}
	}

	doc := map[string]any{"entries": entries}

	if s.verbose {
		elems := make([]map[string]any, len(rs.Columns))
		for i, c := range rs.Columns {
			elems[i] = map[string]any{"name": c.Name, "type": map[string]any{"base": c.TypeName}}
		}

		doc["schema"] = map[string]any{"elements": elems}
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		return []byte("{}")
	}

	return raw
}

func decodeBase64(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }
