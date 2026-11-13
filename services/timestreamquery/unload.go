package timestreamquery

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json/v2"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const (
	unloadResultVersion = "Amazon Timestream version 1.0.0"
	unloadAuthor        = "Amazon Timestream"
	bytesPerMB          = 1 << 20
	bytesPerGB          = 1 << 30
	minUnloadFileSize   = 16 * bytesPerMB
	maxUnloadFileSize   = 78 * bytesPerGB
	formatCSV           = "CSV"
	formatParquet       = "PARQUET"
	compGzip            = "GZIP"
	encSSES3            = "SSE_S3"
	encSSEKMS           = "SSE_KMS"
	stmtUnload          = "UNLOAD"
)

var (
	reUnloadHead = regexp.MustCompile(`(?is)^\s*unload\s*\(`)
	reUnloadTo   = regexp.MustCompile(`(?is)^\s*to\s+'s3://([^/']+)(?:/([^']*))?'\s*`)
	reUnloadWith = regexp.MustCompile(`(?is)^with\s*\(`)
	reMaxSize    = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*(MB|GB)$`)
	reOptionKV   = regexp.MustCompile(`(?s)^\s*([A-Za-z_]+)\s*=\s*(.*?)\s*$`)
	reStringLit  = regexp.MustCompile(`(?s)^'(.*)'$`)
)

// unloadOptions are the validated WITH options of an UNLOAD statement.
type unloadOptions struct {
	format        string
	compression   string
	encryption    string
	kmsKey        string
	partitionedBy []string
	maxFileSize   int64
	delimiter     rune
	escape        rune
	includeHeader bool
}

type unloadStmt struct {
	selectSQL string
	bucket    string
	folder    string
	opts      unloadOptions
}

func unloadErr(format string, args ...any) error {
	return validationf("line 1:1: "+format, args...)
}

func parseUnload(q string) (*unloadStmt, error) {
	head := reUnloadHead.FindStringIndex(q)
	if head == nil {
		return nil, unloadErr("mismatched input: expecting 'UNLOAD (' ")
	}

	open := head[1] - 1
	closeIdx := closeParen(q, open)

	if closeIdx < 0 {
		return nil, unloadErr("mismatched input '<EOF>'. Expecting: ')'")
	}

	st := &unloadStmt{selectSQL: strings.TrimSpace(q[open+1 : closeIdx])}
	rest := q[closeIdx+1:]

	to := reUnloadTo.FindStringSubmatch(rest)
	if to == nil {
		return nil, unloadErr("mismatched input. Expecting: 'TO' followed by an s3:// location")
	}

	st.bucket, st.folder = to[1], strings.Trim(to[2], "/")
	rest = rest[len(to[0]):]

	opts, err := parseUnloadOptions(rest)
	if err != nil {
		return nil, err
	}

	st.opts = opts

	return st, nil
}

func parseUnloadOptions(rest string) (unloadOptions, error) {
	opts := unloadOptions{
		format: formatCSV, compression: compGzip, encryption: encSSES3,
		delimiter: ',', escape: '\\', maxFileSize: maxUnloadFileSize,
	}

	rest = strings.TrimSpace(rest)
	if rest == "" {
		return opts, nil
	}

	loc := reUnloadWith.FindStringIndex(rest)
	if loc == nil {
		return opts, unloadErr("mismatched input '%s'. Expecting: 'WITH'", firstToken(rest))
	}

	closeIdx := closeParen(rest, loc[1]-1)
	if closeIdx < 0 || strings.TrimSpace(rest[closeIdx+1:]) != "" {
		return opts, unloadErr("mismatched input in WITH clause")
	}

	for _, part := range splitTop(rest[loc[1]:closeIdx]) {
		if strings.TrimSpace(part) == "" {
			continue
		}

		m := reOptionKV.FindStringSubmatch(part)
		if m == nil {
			return opts, unloadErr("invalid UNLOAD option '%s'", strings.TrimSpace(part))
		}

		if err := opts.set(strings.ToLower(m[1]), m[2]); err != nil {
			return opts, err
		}
	}

	return opts, opts.validate()
}

func firstToken(s string) string {
	if f := strings.Fields(s); len(f) > 0 {
		return f[0]
	}

	return ""
}

func stringLiteral(name, v string) (string, error) {
	m := reStringLit.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return "", unloadErr("option '%s' expects a string literal", name)
	}

	return strings.ReplaceAll(m[1], "''", "'"), nil
}

func singleChar(name, v string) (rune, error) {
	s, err := stringLiteral(name, v)
	if err != nil {
		return 0, err
	}

	r := []rune(s)
	if len(r) != 1 || r[0] > 127 {
		return 0, unloadErr("option '%s' must be a single ASCII character", name)
	}

	return r[0], nil
}

func (o *unloadOptions) set(name, v string) error {
	var (
		s   string
		err error
	)

	switch name {
	case "partitioned_by":
		return o.setPartitions(v)
	case "field_delimiter":
		o.delimiter, err = singleChar(name, v)

		return err
	case "escaped_by":
		o.escape, err = singleChar(name, v)

		return err
	case "format", "compression", "encryption", "kms_key", "include_header", "max_file_size":
		if s, err = stringLiteral(name, v); err != nil {
			return err
		}
	default:
		return unloadErr("unknown UNLOAD option '%s'", name)
	}

	return o.setString(name, s)
}

func (o *unloadOptions) setString(name, s string) error {
	switch name {
	case "format":
		o.format = strings.ToUpper(s)
	case "compression":
		o.compression = strings.ToUpper(s)
	case "encryption":
		o.encryption = strings.ToUpper(s)
	case "kms_key":
		o.kmsKey = s
	case "include_header":
		switch strings.ToLower(s) {
		case "true":
			o.includeHeader = true
		case "false":
			o.includeHeader = false
		default:
			return unloadErr("option 'include_header' must be 'true' or 'false'")
		}
	default:
		return o.setMaxSize(s)
	}

	return nil
}

func (o *unloadOptions) setMaxSize(s string) error {
	m := reMaxSize.FindStringSubmatch(strings.ToUpper(strings.TrimSpace(s)))
	if m == nil {
		return unloadErr("option 'max_file_size' must look like '24.7MB' or '12GB'")
	}

	n, _ := strconv.ParseFloat(m[1], 64)

	unit := float64(bytesPerMB)
	if m[2] == "GB" {
		unit = bytesPerGB
	}

	size := n * unit
	if size < minUnloadFileSize || size > maxUnloadFileSize {
		return unloadErr("option 'max_file_size' must be between 16MB and 78GB")
	}

	o.maxFileSize = int64(math.Round(size))

	return nil
}

var reArrayLit = regexp.MustCompile(`(?is)^array\s*\[(.*)\]$`)

func (o *unloadOptions) setPartitions(v string) error {
	m := reArrayLit.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return unloadErr("option 'partitioned_by' expects ARRAY['column', ...]")
	}

	for _, p := range splitTop(m[1]) {
		name, err := stringLiteral("partitioned_by", p)
		if err != nil {
			return err
		}

		o.partitionedBy = append(o.partitionedBy, name)
	}

	return nil
}

func (o *unloadOptions) validate() error {
	switch {
	case o.format != formatCSV && o.format != formatParquet:
		return unloadErr("option 'format' must be CSV or PARQUET")
	case o.compression != compGzip && o.compression != "NONE":
		return unloadErr("option 'compression' must be GZIP or NONE")
	case o.encryption != encSSES3 && o.encryption != encSSEKMS:
		return unloadErr("option 'encryption' must be SSE_S3 or SSE_KMS")
	case o.encryption == encSSEKMS && o.kmsKey == "":
		return unloadErr("option 'kms_key' is required when encryption is SSE_KMS")
	case o.encryption == "SSE_S3" && o.kmsKey != "":
		return unloadErr("option 'kms_key' requires encryption = 'SSE_KMS'")
	}

	return nil
}

// unload runs UNLOAD (SELECT ...) TO 's3://...' and writes data, manifest and metadata files.
func (b *InMemoryBackend) unload(ctx context.Context, query, queryID string) (*resultSet, error) {
	st, err := parseUnload(query)
	if err != nil {
		return nil, err
	}

	if firstWord(st.selectSQL) == stmtUnload {
		return nil, unloadErr("UNLOAD cannot be nested")
	}

	rs, err := b.execute(ctx, st.selectSQL, nil, "")
	if err != nil {
		return nil, err
	}

	_, objects := b.deps()
	if objects == nil {
		return nil, unloadErr("S3 is not available to UNLOAD")
	}

	if !billingexport.BucketExists(ctx, objects, st.bucket) {
		return nil, unloadErr("The S3 bucket '%s' does not exist or is not accessible", st.bucket)
	}

	if err = checkPartitionColumns(rs, st.opts.partitionedBy); err != nil {
		return nil, err
	}

	if queryID == "" {
		queryID = newQueryID()
	}

	w := &unloadWriter{ctx: ctx, objects: objects, st: st, rs: rs, queryID: queryID}

	return w.run()
}

func checkPartitionColumns(rs *resultSet, parts []string) error {
	have := map[string]bool{}
	for _, c := range rs.cols {
		have[c.Name] = true
	}

	for _, p := range parts {
		if !have[p] {
			return unloadErr("Partition column '%s' is not a column of the UNLOAD query", p)
		}
	}

	if len(parts) >= len(rs.cols) {
		return unloadErr("UNLOAD requires at least one column that is not a partition column")
	}

	return nil
}

type unloadFile struct {
	URL    string `json:"url"`
	Meta   unloadFileMeta
	Rows   int64
	Length int64
}

type unloadFileMeta struct {
	ContentLength int64 `json:"content_length_in_bytes"`
	RowCount      int64 `json:"row_count"`
}

type unloadWriter struct {
	ctx     context.Context
	objects billingexport.ObjectStore
	st      *unloadStmt
	rs      *resultSet
	queryID string
	files   []unloadFile
}

func (w *unloadWriter) run() (*resultSet, error) {
	dataIdx, partIdx := w.split()
	groups, order := w.groupRows(partIdx)

	var total int64

	for _, key := range order {
		for _, chunk := range w.chunks(groups[key], dataIdx) {
			if err := w.writeData(key, chunk, dataIdx); err != nil {
				return nil, err
			}

			total += int64(len(chunk))
		}
	}

	suffix := w.queryID + "_" + uuid.NewString()
	base := w.st.folder
	meta := w.key(base, suffix+"_metadata.json")
	manifest := w.key(base, suffix+"_manifest.json")

	if err := w.writeMetadata(meta); err != nil {
		return nil, err
	}

	if err := w.writeManifest(manifest, total); err != nil {
		return nil, err
	}

	out := &resultSet{scanned: w.rs.scanned, tables: w.rs.tables}
	out.cols = []ColumnInfo{
		scalarColumnInfo("rows", ScalarTypeBigint),
		scalarColumnInfo("metadataFile", ScalarTypeVarchar),
		scalarColumnInfo("manifestFile", ScalarTypeVarchar),
	}
	out.rows = []Row{{Data: []Datum{
		ScalarDatum(strconv.FormatInt(total, 10)), ScalarDatum(w.uri(meta)), ScalarDatum(w.uri(manifest)),
	}}}

	return out, nil
}

func (w *unloadWriter) uri(key string) string { return "s3://" + w.st.bucket + "/" + key }

func (w *unloadWriter) key(parts ...string) string {
	var kept []string

	for _, p := range parts {
		if p = strings.Trim(p, "/"); p != "" {
			kept = append(kept, p)
		}
	}

	return strings.Join(kept, "/")
}

func (w *unloadWriter) split() ([]int, []int) {
	parts := map[string]int{}
	for i, p := range w.st.opts.partitionedBy {
		parts[p] = i
	}

	var data []int

	partIdx := make([]int, len(w.st.opts.partitionedBy))

	for i, c := range w.rs.cols {
		if pi, ok := parts[c.Name]; ok {
			partIdx[pi] = i

			continue
		}

		data = append(data, i)
	}

	return data, partIdx
}

func cellText(d Datum) string {
	switch {
	case d.ScalarValue != nil:
		return *d.ScalarValue
	case len(d.TimeSeriesValue) > 0:
		return timeSeriesJSON(d.TimeSeriesValue)
	case d.ArrayValue != nil:
		return arrayJSON(d.ArrayValue)
	default:
		return ""
	}
}

func jsonCell(d Datum) any {
	if d.ScalarValue == nil {
		return nil
	}

	if _, err := strconv.ParseFloat(*d.ScalarValue, 64); err == nil && !strings.ContainsAny(*d.ScalarValue, " :") {
		return jsonx.Number(*d.ScalarValue)
	}

	return *d.ScalarValue
}

func arrayJSON(vals []Datum) string {
	out := make([]any, len(vals))
	for i, v := range vals {
		out[i] = jsonCell(v)
	}

	b, _ := json.Marshal(out, jsonx.V1())

	return string(b)
}

func timeSeriesJSON(pts []TimeSeriesDataPoint) string {
	out := make([]map[string]any, len(pts))
	for i, p := range pts {
		out[i] = map[string]any{"time": p.Time + "Z", "value": jsonCell(p.Value)}
	}

	b, _ := json.Marshal(out, jsonx.V1())

	return string(b)
}

func (w *unloadWriter) groupRows(partIdx []int) (map[string][]Row, []string) {
	groups := map[string][]Row{}

	var order []string

	for _, r := range w.rs.rows {
		segs := make([]string, len(partIdx))
		for i, pi := range partIdx {
			segs[i] = w.st.opts.partitionedBy[i] + "=" + cellText(r.Data[pi])
		}

		k := strings.Join(segs, "/")
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}

		groups[k] = append(groups[k], r)
	}

	sort.Strings(order)

	return groups, order
}

// chunks splits rows so each file stays near max_file_size (estimated from cell text).
func (w *unloadWriter) chunks(rows []Row, dataIdx []int) [][]Row {
	var (
		out   [][]Row
		start int
		size  int64
	)

	for i, r := range rows {
		var n int64
		for _, di := range dataIdx {
			n += int64(len(cellText(r.Data[di]))) + 1
		}

		if i > start && size+n > w.st.opts.maxFileSize {
			out, start, size = append(out, rows[start:i]), i, 0
		}

		size += n
	}

	if start < len(rows) {
		out = append(out, rows[start:])
	}

	return out
}

func (w *unloadWriter) put(key string, body []byte, contentType string) error {
	in := &sdk_s3.PutObjectInput{
		Bucket: &w.st.bucket, Key: &key, Body: bytes.NewReader(body), ContentType: &contentType,
	}

	if w.st.opts.encryption == encSSEKMS {
		in.ServerSideEncryption = s3types.ServerSideEncryptionAwsKms
		in.SSEKMSKeyId = &w.st.opts.kmsKey
	} else {
		in.ServerSideEncryption = s3types.ServerSideEncryptionAes256
	}

	if _, err := w.objects.PutObject(w.ctx, in); err != nil {
		return unloadErr("Failed to write s3://%s/%s: %v", w.st.bucket, key, err)
	}

	return nil
}

func (w *unloadWriter) writeData(partition string, rows []Row, dataIdx []int) error {
	var (
		body []byte
		ext  string
		err  error
	)

	if w.st.opts.format == formatParquet {
		body, err = w.parquetBody(rows, dataIdx)
		ext = ".parquet"
	} else {
		body, ext, err = w.csvBody(rows, dataIdx)
	}

	if err != nil {
		return unloadErr("Failed to encode UNLOAD output: %v", err)
	}

	key := w.key(w.st.folder, "results", partition, w.queryID+"_"+uuid.NewString()+ext)
	if err = w.put(key, body, "application/octet-stream"); err != nil {
		return err
	}

	w.files = append(w.files, unloadFile{
		URL: w.uri(key), Meta: unloadFileMeta{ContentLength: int64(len(body)), RowCount: int64(len(rows))},
	})

	return nil
}

func (w *unloadWriter) csvBody(rows []Row, dataIdx []int) ([]byte, string, error) {
	var buf bytes.Buffer

	var gz *gzip.Writer

	ext := ".csv"

	if w.st.opts.compression == compGzip {
		gz = gzip.NewWriter(&buf)
		ext = ".gz"
	}

	o := w.st.opts
	write := func(fields []string, nulls []bool) {
		line := make([]string, len(fields))

		for i, f := range fields {
			if nulls != nil && nulls[i] {
				continue
			}

			line[i] = csvField(f, o)
		}

		text := strings.Join(line, string(o.delimiter)) + "\n"
		if gz != nil {
			_, _ = gz.Write([]byte(text))

			return
		}

		buf.WriteString(text)
	}

	if o.includeHeader {
		names := make([]string, len(dataIdx))
		for i, di := range dataIdx {
			names[i] = w.rs.cols[di].Name
		}

		write(names, nil)
	}

	for _, r := range rows {
		fields := make([]string, len(dataIdx))
		nulls := make([]bool, len(dataIdx))

		for i, di := range dataIdx {
			fields[i] = cellText(r.Data[di])
			nulls[i] = r.Data[di].ScalarValue == nil && len(r.Data[di].ArrayValue) == 0 &&
				len(r.Data[di].TimeSeriesValue) == 0
		}

		write(fields, nulls)
	}

	if gz != nil {
		if err := gz.Close(); err != nil {
			return nil, "", err
		}
	}

	return buf.Bytes(), ext, nil
}

// csvField quotes values holding the delimiter, quote, escape or a line break, escaping quote and escape characters.
func csvField(v string, o unloadOptions) string {
	if !strings.ContainsAny(v, string([]rune{o.delimiter, '"', o.escape, '\n', '\r'})) {
		return v
	}

	esc := string(o.escape)
	v = strings.ReplaceAll(v, esc, esc+esc)
	v = strings.ReplaceAll(v, `"`, esc+`"`)

	return `"` + v + `"`
}

func parquetType(c ColumnInfo) string {
	switch c.Type.ScalarType {
	case ScalarTypeBigint:
		return "bigint"
	case ScalarTypeDouble:
		return "double"
	case ScalarTypeBoolean:
		return "boolean"
	case ScalarTypeTimestamp:
		return "timestamp"
	case ScalarTypeDate:
		return "date"
	default:
		return "varchar"
	}
}

func (w *unloadWriter) parquetBody(rows []Row, dataIdx []int) ([]byte, error) {
	cols := make([]parquetio.Column, len(dataIdx))
	for i, di := range dataIdx {
		cols[i] = parquetio.Column{Name: w.rs.cols[di].Name, Type: parquetType(w.rs.cols[di])}
	}

	cells := make([][]any, 0, len(rows))

	for _, r := range rows {
		row := make([]any, len(dataIdx))

		for i, di := range dataIdx {
			d := r.Data[di]
			if d.ScalarValue == nil && len(d.ArrayValue) == 0 && len(d.TimeSeriesValue) == 0 {
				continue
			}

			row[i] = parquetCell(cols[i].Type, cellText(d))
		}

		cells = append(cells, row)
	}

	comp := "NONE"
	if w.st.opts.compression == compGzip {
		comp = compGzip
	}

	var buf bytes.Buffer
	if err := parquetio.Write(&buf, cols, cells, comp); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func parquetCell(typ, text string) any {
	switch typ {
	case "bigint":
		n, _ := strconv.ParseInt(text, 10, 64)

		return n
	case "double":
		f, _ := strconv.ParseFloat(text, 64)

		return f
	case "boolean":
		return strings.EqualFold(text, "true")
	default:
		return text
	}
}

func (w *unloadWriter) writeMetadata(key string) error {
	body, err := json.Marshal(map[string]any{
		"ColumnInfo": w.rs.cols,
		"Author":     map[string]string{"Name": unloadAuthor, "MetadataFileVersion": "1.0"},
	}, jsonx.V1())
	if err != nil {
		return unloadErr("Failed to encode UNLOAD metadata: %v", err)
	}

	return w.put(key, body, "application/json")
}

func (w *unloadWriter) writeManifest(key string, total int64) error {
	type resultFile struct {
		URL  string         `json:"url"`
		Meta unloadFileMeta `json:"file_metadata"`
	}

	files := make([]resultFile, 0, len(w.files))

	var length int64

	for _, f := range w.files {
		files = append(files, resultFile{URL: f.URL, Meta: f.Meta})
		length += f.Meta.ContentLength
	}

	body, err := json.Marshal(map[string]any{
		"result_files": files,
		"query_metadata": map[string]any{
			"content_length_in_bytes": length, "total_row_count": total,
			"result_format": w.st.opts.format, "result_version": unloadResultVersion,
		},
		"author": map[string]string{"name": unloadAuthor, "manifest_file_version": "1.0"},
	}, jsonx.V1())
	if err != nil {
		return unloadErr("Failed to encode UNLOAD manifest: %v", err)
	}

	return w.put(key, body, "application/json")
}
