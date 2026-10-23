package gcpbigquery

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const firstShard = "000000000000"

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

// execExtract runs an extract job; the caller holds the write lock.
func (b *InMemoryBackend) execExtract(_ string, cfg map[string]any, dryRun bool) (*outcome, error) {
	ids := refIDs(cfg, "sourceTable")
	ec := decodeExtract(cfg)

	if ids["tableId"] == "" || len(ec.URIs) == 0 {
		return nil, errInvalid("Required parameter is missing: extract sourceTable and destinationUris")
	}

	_, t, err := b.tableLocked(ids[litProjectid], ids["datasetId"], ids["tableId"])
	if err != nil {
		return nil, err
	}

	if t.ViewQuery != "" {
		return nil, errInvalid("Cannot extract table %s of type VIEW", ids["tableId"])
	}

	counts := make([]any, len(ec.URIs))
	for i := range counts {
		counts[i] = "0"
	}

	out := &outcome{
		Stats: map[string]any{"destinationUriFileCounts": counts, "inputBytes": strconv.FormatInt(tableBytes(t), 10)},
	}
	if dryRun {
		return out, nil
	}

	if b.objects == nil {
		out.Err = &jobError{Reason: litInvalid, Message: "Cloud Storage is not available to this BigQuery emulator"}

		return out, nil
	}

	data, ctype, err := encodeExtract(ec, t)
	if err != nil {
		return out.fail(litInvalid, err.Error())
	}

	if jerr := b.writeExtract(ec, data, ctype); jerr != nil {
		out.Err = jerr

		return out, nil
	}

	counts[0] = "1"

	return out, nil
}

func (b *InMemoryBackend) writeExtract(ec extractConfig, data []byte, ctype string) *jobError {
	uri := ec.URIs[0]

	u, ok := parseGCSURI(uri)
	if !ok || u.object == "" {
		return &jobError{Reason: litInvalid, Message: "Invalid Cloud Storage URI: " + uri}
	}

	name := strings.Replace(u.object, "*", firstShard, 1)

	if strings.EqualFold(ec.Compression, "GZIP") && ec.Format != formatParquet {
		var buf bytes.Buffer

		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(data)
		_ = zw.Close()
		data, ctype = buf.Bytes(), "application/gzip"
	}

	if err := b.objects.WriteObject(u.bucket, name, data, ctype); err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "No such") {
			return &jobError{Reason: litNotfound, Message: "Not found: Bucket " + u.bucket}
		}

		return &jobError{Reason: "backendError", Message: "Cannot write " + uri + ": " + err.Error()}
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
		bqsql.KindNumeric, bqsql.KindBigNumeric, bqsql.KindJSON, bqsql.KindGeography, bqsql.KindStruct, bqsql.KindArray:
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

func jsonCell(f bqsql.Field, v any) json.RawMessage {
	if v == nil && f.Mode != bqsql.ModeRepeated {
		return json.RawMessage("null")
	}

	k, _ := bqsql.KindOf(f.Type)
	if k == bqsql.KindStruct || f.Mode == bqsql.ModeRepeated {
		text, _ := bqsql.ToSQLite(f, v).(string)

		return json.RawMessage(text)
	}

	s, _ := v.(string)

	switch k {
	case bqsql.KindInt, bqsql.KindBool, bqsql.KindNumeric, bqsql.KindBigNumeric:
		return json.RawMessage(s)
	case bqsql.KindFloat:
		if f, err := strconv.ParseFloat(
			s,
			64,
		); err == nil && !math.IsNaN(f) &&
			!math.IsInf(f, 0) {
			return json.RawMessage(strconv.FormatFloat(f, 'g', -1, 64))
		}
	case bqsql.KindTimestamp:
		if t, err := time.Parse(bqsql.TimestampLayout, s); err == nil {
			s = t.Format("2006-01-02T15:04:05.999999Z")
		}
	case bqsql.KindDatetime, bqsql.KindTime:
		s = strings.TrimSuffix(strings.TrimRight(s, "0"), ".")
	case bqsql.KindUnknown, bqsql.KindString, bqsql.KindBytes, bqsql.KindDate, bqsql.KindJSON, bqsql.KindGeography,
		bqsql.KindStruct, bqsql.KindArray:
	}

	q, _ := json.Marshal(s)

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

			name, _ := json.Marshal(f.Name)
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
	case "BYTES":
		return "binary", nil
	case "NUMERIC":
		return "decimal(38,9)", nil
	case "BIGNUMERIC":
		return "string", nil
	case "TIMESTAMP":
		return "timestamp", nil
	case "DATE":
		return "date", nil
	}

	return "string", nil
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
	case "BYTES":
		raw, _ := bqsqlDecodeBase64(s)

		return string(raw)
	case "TIMESTAMP":
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

// fail records a job-level error and returns the outcome; the job still completes with state DONE.
func (o *outcome) fail(reason, msg string) (*outcome, error) {
	o.Err = &jobError{Reason: reason, Message: msg}

	return o, nil
}
