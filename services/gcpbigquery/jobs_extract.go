package gcpbigquery

import (
	"bytes"
	"compress/gzip"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const defaultExtractShardBytes int64 = 1 << 30

type extractConfig struct {
	Format      string
	Delimiter   string
	Compression string
	URIs        []string
	Header      bool
}

func decodeExtract(cfg map[string]any) extractConfig {
	ec := extractConfig{Format: formatCSV, Delimiter: ",", Header: true}

	if v, ok := cfg["destinationFormat"].(string); ok && v != "" {
		ec.Format = strings.ToUpper(v)
	}

	if v, ok := cfg["fieldDelimiter"].(string); ok && v != "" {
		ec.Delimiter = v
	}

	if v, ok := cfg["printHeader"].(bool); ok {
		ec.Header = v
	}

	ec.Compression, _ = cfg["compression"].(string)
	ec.URIs = anyStrings(cfg["destinationUris"])

	if one, ok := cfg["destinationUri"].(string); ok && one != "" {
		ec.URIs = append(ec.URIs, one)
	}

	return ec
}

// maxExtractURIs is the documented 500 wildcard URIs per extract job, applied to every destination URI.
const maxExtractURIs = 500

type extractFile struct {
	uri   string
	ctype string
	data  []byte
	uriIx int
	seq   int
}

// execExtract validates an extract job without writing anything (dryRun).
func (b *InMemoryBackend) execExtract(cfg map[string]any) (*outcome, error) {
	out, _, err := b.execExtractPlan(cfg, false)

	return out, err
}

// execExtractPlan validates the job and encodes the files under the write lock; the returned func writes them to
// Cloud Storage and must run after the lock is released.
func (b *InMemoryBackend) execExtractPlan(cfg map[string]any, plan bool) (*outcome, func() *outcome, error) {
	ids := refIDs(cfg, "sourceTable")
	ec := decodeExtract(cfg)

	if ids["tableId"] == "" || len(ec.URIs) == 0 {
		return nil, nil, errInvalid("Required parameter is missing: extract sourceTable and destinationUris")
	}

	if len(ec.URIs) > maxExtractURIs {
		return nil, nil, errInvalid("Too many destination URIs: an extract job can have at most %d", maxExtractURIs)
	}

	_, t, err := b.tableLocked(ids[litProjectid], ids["datasetId"], ids["tableId"])
	if err != nil {
		return nil, nil, err
	}

	if t.ViewQuery != "" {
		return nil, nil, errInvalid("Cannot extract table %s of type VIEW", ids["tableId"])
	}

	if t.isExternal() {
		return nil, nil, errInvalid("Cannot extract table %s of type EXTERNAL", ids["tableId"])
	}

	counts := make([]any, len(ec.URIs))
	for i := range counts {
		counts[i] = "0"
	}

	out := &outcome{
		Stats: map[string]any{"destinationUriFileCounts": counts, "inputBytes": strconv.FormatInt(tableBytes(t), 10)},
	}

	if msg := badExtractURI(ec.URIs); msg != "" {
		out.Err = &jobError{Reason: litInvalid, Message: msg}

		return out, nil, nil
	}

	if !plan {
		return out, nil, nil
	}

	if b.objects == nil {
		out.Err = &jobError{Reason: litInvalid, Message: "Cloud Storage is not available to this BigQuery emulator"}

		return out, nil, nil
	}

	files, jerr := b.planShards(ec, t)
	if jerr != nil {
		out.Err = jerr

		return out, nil, nil
	}

	objs := b.objects

	return out, func() *outcome { return writeShards(objs, ec, files, out) }, nil
}

// splitShards cuts the rows into shards of at most limit logical bytes (a row never splits), and into at least
// minShards shards when there are enough rows; an empty table is one empty shard.
func splitShards(t *Table, limit int64, minShards int) [][][]any {
	var (
		shards [][][]any
		size   int64
	)

	cur := make([][]any, 0, len(t.Rows))

	for _, row := range t.Rows {
		var n int64
		for i, f := range t.Schema {
			n += bqsql.LogicalBytes(f, row[i])
		}

		if len(cur) > 0 && size+n > limit {
			shards, cur, size = append(shards, cur), nil, 0
		}

		cur, size = append(cur, row), size+n
	}

	if len(cur) > 0 || len(shards) == 0 {
		shards = append(shards, cur)
	}

	if len(shards) >= minShards || len(t.Rows) < minShards {
		return shards
	}

	shards = make([][][]any, minShards)
	for i := range shards {
		shards[i] = t.Rows[i*len(t.Rows)/minShards : (i+1)*len(t.Rows)/minShards]
	}

	return shards
}

// planShards encodes shard k for URI k mod n, numbering files per URI from 000000000000. Without a wildcard a URI
// holds one file only.
func (b *InMemoryBackend) planShards(ec extractConfig, t *Table) ([]extractFile, *jobError) {
	limit := b.extractShardBytes
	if limit <= 0 {
		limit = defaultExtractShardBytes
	}

	shards := splitShards(t, limit, len(ec.URIs))
	files := make([]extractFile, 0, len(shards))

	for k, rows := range shards {
		uri := ec.URIs[k%len(ec.URIs)]
		seq := k / len(ec.URIs)

		if seq > 0 && !strings.Contains(uri, "*") {
			return nil, &jobError{
				Reason: litInvalid,
				Message: "Table too large to be exported to a single file. " +
					"Specify a uri including a * to shard export. URI: " + uri,
			}
		}

		view := *t
		view.Rows = rows

		data, ctype, err := encodeExtract(ec, &view)
		if err != nil {
			return nil, &jobError{Reason: litInvalid, Message: err.Error()}
		}

		data, ctype = compressExtract(ec, data, ctype)
		files = append(files, extractFile{uri: uri, uriIx: k % len(ec.URIs), seq: seq, data: data, ctype: ctype})
	}

	return files, nil
}

// writeShards stores the planned files and fills the per-URI file counts of out.
func writeShards(objs ObjectStore, ec extractConfig, files []extractFile, out *outcome) *outcome {
	written := make([]int, len(ec.URIs))

	defer func() {
		counts, _ := out.Stats["destinationUriFileCounts"].([]any)
		for i, n := range written {
			counts[i] = strconv.Itoa(n)
		}
	}()

	for _, f := range files {
		if jerr := writeExtract(objs, f); jerr != nil {
			out.Err = jerr

			return out
		}

		written[f.uriIx]++
	}

	return out
}

// badExtractURI applies the documented URI rules: at most one wildcard per URI and only in the file name.
func badExtractURI(uris []string) string {
	for _, uri := range uris {
		u, ok := parseGCSURI(uri)
		if !ok || u.object == "" {
			return "Invalid Cloud Storage URI: " + uri
		}

		dir := ""
		if i := strings.LastIndex(u.object, "/"); i >= 0 {
			dir = u.object[:i]
		}

		if strings.Count(u.object, "*") > 1 || strings.Contains(dir, "*") {
			return "Invalid Cloud Storage URI " + uri + ": a single wildcard is allowed, only in the file name."
		}
	}

	return ""
}

func compressExtract(ec extractConfig, data []byte, ctype string) ([]byte, string) {
	if !strings.EqualFold(ec.Compression, "GZIP") || ec.Format == formatParquet || ec.Format == formatAvro {
		return data, ctype
	}

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(data)
	_ = zw.Close()

	return buf.Bytes(), "application/gzip"
}

func writeExtract(objs ObjectStore, f extractFile) *jobError {
	u, ok := parseGCSURI(f.uri)
	if !ok || u.object == "" {
		return &jobError{Reason: litInvalid, Message: "Invalid Cloud Storage URI: " + f.uri}
	}

	name := strings.Replace(u.object, "*", fmt.Sprintf("%012d", f.seq), 1)

	if err := objs.WriteObject(u.bucket, name, f.data, f.ctype); err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "No such") {
			return &jobError{Reason: litNotfound, Message: "Not found: Bucket " + u.bucket}
		}

		return &jobError{Reason: "backendError", Message: "Cannot write " + f.uri + ": " + err.Error()}
	}

	return nil
}

func encodeExtract(ec extractConfig, t *Table) ([]byte, string, error) {
	switch ec.Format {
	case formatCSV:
		data, err := encodeCSV(ec, t)

		return data, "text/csv", err
	case formatJSON:
		return encodeNDJSON(t), "application/json", nil
	case formatParquet:
		data, err := encodeParquet(ec, t)

		return data, "application/octet-stream", err
	case formatAvro:
		data, err := encodeAvro(ec, t)

		return data, "application/avro", err
	}

	return nil, "", fmt.Errorf("%w: destinationFormat %s is not supported", errLoad, ec.Format)
}

func csvEscape(s, delim string) string {
	if strings.ContainsAny(s, "\"\r\n") || strings.Contains(s, delim) {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}

	return s
}

func csvText(f bqsql.Field, v any) string {
	s, _ := v.(string)
	k, _ := bqsql.KindOf(f.Type)

	switch k {
	case bqsql.KindTimestamp:
		if t, err := time.Parse(bqsql.TimestampLayout, s); err == nil {
			return t.Format("2006-01-02 15:04:05.999999") + " UTC"
		}
	case bqsql.KindDatetime, bqsql.KindTime:
		return strings.TrimSuffix(strings.TrimRight(s, "0"), ".")
	case bqsql.KindFloat:
		return strings.TrimSuffix(s, ".0")
	case bqsql.KindUnknown, bqsql.KindInt, bqsql.KindBool, bqsql.KindString, bqsql.KindBytes, bqsql.KindDate,
		bqsql.KindNumeric, bqsql.KindBigNumeric, bqsql.KindJSON, bqsql.KindGeography, bqsql.KindInterval,
		bqsql.KindStruct, bqsql.KindArray:
	}

	return s
}

func encodeCSV(ec extractConfig, t *Table) ([]byte, error) {
	for _, f := range t.Schema {
		if f.Type == litRecord || f.Mode == bqsql.ModeRepeated {
			return nil, fmt.Errorf("%w: operation cannot be performed on a nested schema (field %s)", errLoad, f.Name)
		}
	}

	delim := ec.Delimiter
	if strings.EqualFold(delim, "tab") || delim == `\t` {
		delim = "\t"
	}

	var buf bytes.Buffer

	if ec.Header {
		names := make([]string, len(t.Schema))
		for i, f := range t.Schema {
			names[i] = csvEscape(f.Name, delim)
		}

		buf.WriteString(strings.Join(names, delim) + "\n")
	}

	for _, row := range t.Rows {
		cells := make([]string, len(t.Schema))

		for i, f := range t.Schema {
			if row[i] != nil {
				cells[i] = csvEscape(csvText(f, row[i]), delim)
			}
		}

		buf.WriteString(strings.Join(cells, delim) + "\n")
	}

	return buf.Bytes(), nil
}

func jsonCell(f bqsql.Field, v any) jsontext.Value {
	if v == nil && f.Mode != bqsql.ModeRepeated {
		return jsontext.Value("null")
	}

	k, _ := bqsql.KindOf(f.Type)
	if k == bqsql.KindStruct || f.Mode == bqsql.ModeRepeated {
		text, _ := bqsql.ToSQLite(f, v).(string)

		return jsontext.Value(text)
	}

	s, _ := v.(string)

	switch k {
	case bqsql.KindInt, bqsql.KindBool, bqsql.KindNumeric, bqsql.KindBigNumeric:
		return jsontext.Value(s)
	case bqsql.KindFloat:
		if f, err := strconv.ParseFloat(
			s,
			64,
		); err == nil && !math.IsNaN(f) &&
			!math.IsInf(f, 0) {
			return jsontext.Value(strconv.FormatFloat(f, 'g', -1, 64))
		}
	case bqsql.KindTimestamp:
		if t, err := time.Parse(bqsql.TimestampLayout, s); err == nil {
			s = t.Format("2006-01-02T15:04:05.999999Z")
		}
	case bqsql.KindDatetime, bqsql.KindTime:
		s = strings.TrimSuffix(strings.TrimRight(s, "0"), ".")
	case bqsql.KindUnknown, bqsql.KindString, bqsql.KindBytes, bqsql.KindDate, bqsql.KindJSON, bqsql.KindGeography,
		bqsql.KindInterval, bqsql.KindStruct, bqsql.KindArray:
	}

	q, _ := json.Marshal(s, jsonx.V1())

	return q
}

func encodeNDJSON(t *Table) []byte {
	var buf bytes.Buffer

	for _, row := range t.Rows {
		buf.WriteByte('{')

		for i, f := range t.Schema {
			if i > 0 {
				buf.WriteByte(',')
			}

			name, _ := json.Marshal(f.Name, jsonx.V1())
			buf.Write(name)
			buf.WriteByte(':')
			buf.Write(jsonCell(f, row[i]))
		}

		buf.WriteString("}\n")
	}

	return buf.Bytes()
}

func parquetType(f bqsql.Field) (string, error) {
	if f.Type == litRecord || f.Mode == bqsql.ModeRepeated {
		return "", fmt.Errorf("%w: Parquet export of nested field %s is not supported", errLoad, f.Name)
	}

	switch f.Type {
	case litInteger:
		return "bigint", nil
	case litFloat:
		return "double", nil
	case litBoolean:
		return "boolean", nil
	case litBytes:
		return "binary", nil
	case litNumeric:
		return "decimal(38,9)", nil
	case litBignumeric:
		return litStringType, nil
	case litTimestamp:
		return "timestamp", nil
	case litDate:
		return "date", nil
	}

	return litStringType, nil
}

func parquetCell(f bqsql.Field, v any) any {
	if v == nil {
		return nil
	}

	s, _ := v.(string)

	switch f.Type {
	case litInteger:
		n, _ := strconv.ParseInt(s, 10, 64)

		return n
	case litFloat:
		x, _ := strconv.ParseFloat(s, 64)

		return x
	case litBoolean:
		return s == "true"
	case litBytes:
		raw, _ := bqsqlDecodeBase64(s)

		return string(raw)
	case litTimestamp:
		if t, err := time.Parse(bqsql.TimestampLayout, s); err == nil {
			return t.Format("2006-01-02 15:04:05.000000")
		}
	}

	return s
}

func encodeParquet(ec extractConfig, t *Table) ([]byte, error) {
	cols := make([]parquetio.Column, len(t.Schema))

	for i, f := range t.Schema {
		ty, err := parquetType(f)
		if err != nil {
			return nil, err
		}

		cols[i] = parquetio.Column{Name: f.Name, Type: ty}
	}

	rows := make([][]any, len(t.Rows))

	for i, r := range t.Rows {
		row := make([]any, len(t.Schema))
		for j, f := range t.Schema {
			row[j] = parquetCell(f, r[j])
		}

		rows[i] = row
	}

	var buf bytes.Buffer

	comp := ec.Compression
	if strings.EqualFold(comp, "NONE") {
		comp = "UNCOMPRESSED"
	}

	if err := parquetio.Write(&buf, cols, rows, comp); err != nil {
		return nil, fmt.Errorf("parquet export: %w", err)
	}

	return buf.Bytes(), nil
}
