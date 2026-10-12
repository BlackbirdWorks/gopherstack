package athena

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type dataFormat int

const (
	fmtDelimited dataFormat = iota
	fmtCSVSerde
	fmtJSON
)

const (
	serdeOpenCSV      = "org.apache.hadoop.hive.serde2.opencsvserde"
	maxLineBytes      = 16 << 20
	decompressRatio   = 10
	gzipMagic0        = 0x1f
	gzipMagic1        = 0x8b
	ctrlA             = "\x01"
	paramSkipHeader   = "skip.header.line.count"
	paramIgnoreMalfmd = "ignore.malformed.json"
)

var (
	errUnsupportedFormat = errors.New("unsupported data format")
	errRowLimit          = errors.New("row limit exceeded")
	errBadLocation       = errors.New("invalid S3 location")
	errScanLimit         = errors.New("scan limit exceeded")
)

// tableFormat is the decoded SerDe configuration of a Glue table.
type tableFormat struct {
	delim      string
	kind       dataFormat
	skipHeader int
	ignoreBad  bool
}

func lowerParam(m map[string]string, key string) string {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v
		}
	}

	return ""
}

func unescapeDelim(s string) string {
	switch s {
	case `\t`:
		return "\t"
	case `\n`:
		return "\n"
	case `\u0001`, `\001`:
		return ctrlA
	default:
		return s
	}
}

func lazyDelimiter(params map[string]string) string {
	if d := lowerParam(params, "field.delim"); d != "" {
		return unescapeDelim(d)
	}

	sf := lowerParam(params, "serialization.format")

	switch {
	case sf == "":
		return ctrlA
	case len(sf) == 1 && !isDigit(sf[0]):
		return sf
	default:
		n, err := strconv.Atoi(sf)
		if err != nil || n <= 0 {
			return unescapeDelim(sf)
		}

		return fmt.Sprintf("%c", n)
	}
}

// detectFormat maps the table's SerDe and input format onto a reader, or
// returns an unsupported-format error for columnar and binary formats.
func detectFormat(def *GlueTableDef) (tableFormat, error) {
	lib := strings.ToLower(def.SerdeLibrary)
	in := strings.ToLower(def.InputFormat)
	f := tableFormat{}

	skip := lowerParam(def.Parameters, paramSkipHeader)
	if skip == "" {
		skip = lowerParam(def.SerdeParameters, paramSkipHeader)
	}

	f.skipHeader, _ = strconv.Atoi(skip)
	f.ignoreBad = strings.EqualFold(lowerParam(def.SerdeParameters, paramIgnoreMalfmd), "true")

	switch {
	case strings.Contains(lib, "parquet") || strings.Contains(in, "parquet"):
		return f, fmt.Errorf("%w: Parquet (SerDe %q); the embedded SQL engine reads CSV, TSV and JSON lines only",
			errUnsupportedFormat, def.SerdeLibrary)
	case strings.Contains(lib, "orc") || strings.Contains(in, "orc"):
		return f, fmt.Errorf("%w: ORC (SerDe %q); the embedded SQL engine reads CSV, TSV and JSON lines only",
			errUnsupportedFormat, def.SerdeLibrary)
	case strings.Contains(lib, "avro") || strings.Contains(in, "avro"):
		return f, fmt.Errorf("%w: Avro (SerDe %q); the embedded SQL engine reads CSV, TSV and JSON lines only",
			errUnsupportedFormat, def.SerdeLibrary)
	case strings.Contains(lib, "jsonserde"):
		f.kind = fmtJSON

		return f, nil
	case lib == serdeOpenCSV:
		f.kind = fmtCSVSerde
		f.delim = ","

		if sep := lowerParam(def.SerdeParameters, "separatorChar"); sep != "" {
			f.delim = unescapeDelim(sep)
		}

		return f, nil
	case lib == "" || strings.Contains(lib, "lazysimpleserde") || strings.Contains(in, "textinputformat"):
		f.kind = fmtDelimited
		f.delim = lazyDelimiter(def.SerdeParameters)

		return f, nil
	default:
		return f, fmt.Errorf("%w: SerDe %q", errUnsupportedFormat, def.SerdeLibrary)
	}
}

type colKind int

const (
	kindString colKind = iota
	kindInt
	kindFloat
	kindBool
	kindText
)

func kindOf(glueType string) colKind {
	switch typeBase(strings.ToLower(strings.TrimSpace(glueType))) {
	case typeTinyint, typeSmallint, typeInt, typeInteger, typeBigint:
		return kindInt
	case typeFloat, typeDouble, typeReal, typeDecimal, typeNumeric:
		return kindFloat
	case typeBoolean:
		return kindBool
	case typeDate, typeTimestamp:
		return kindText
	default:
		return kindString
	}
}

// sqliteDecl returns the SQLite column declaration that round-trips the Glue type.
func sqliteDecl(glueType string) string {
	t := strings.ToLower(strings.TrimSpace(glueType))

	switch base := typeBase(t); base {
	case typeTinyint, typeSmallint, typeBigint, typeBoolean, typeDouble:
		return strings.ToUpper(base)
	case typeInt, typeInteger:
		return sqlInteger
	case typeFloat, typeReal:
		return "REAL"
	case typeDecimal, typeNumeric:
		if strings.Contains(t, "(") {
			return strings.ToUpper(strings.ReplaceAll(t, " ", ""))
		}

		return "DECIMAL(10,0)"
	case typeDate:
		return "DATE_TEXT"
	case typeTimestamp:
		return "TIMESTAMP_TEXT"
	default:
		return "VARCHAR"
	}
}

func convertValue(raw string, k colKind, nullToken bool) any {
	if nullToken && raw == `\N` {
		return nil
	}

	switch k {
	case kindInt:
		if n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
			return n
		}

		return nil
	case kindFloat:
		if f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil {
			return f
		}

		return nil
	case kindBool:
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "true":
			return int64(1)
		case "false":
			return int64(0)
		default:
			return nil
		}
	case kindText:
		if raw == "" {
			return nil
		}

		return raw
	default:
		return raw
	}
}

type rowSink struct {
	stmt  *sql.Stmt
	kinds []colKind
	extra []any
	nargs int
}

type tableLoader struct {
	get     S3Getter
	list    S3Lister
	limits  EngineLimits
	scanned int64
	rows    int
}

type countingReader struct {
	r io.Reader
	n *int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	*c.n += int64(n)

	return n, err
}

// splitS3URI splits s3://bucket/prefix into bucket and prefix.
func splitS3URI(u string) (string, string, bool) {
	const scheme = "s3://"

	if !strings.HasPrefix(strings.ToLower(u), scheme) {
		return "", "", false
	}

	bucket, prefix, _ := strings.Cut(u[len(scheme):], "/")
	if bucket == "" {
		return "", "", false
	}

	return bucket, prefix, true
}

func (l *tableLoader) listKeys(ctx context.Context, location string) (string, []string, error) {
	bucket, prefix, ok := splitS3URI(location)
	if !ok {
		return "", nil, fmt.Errorf("%w %q", errBadLocation, location)
	}

	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var keys []string

	var token *string

	for {
		out, err := l.list.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return "", nil, err
		}

		for _, o := range out.Contents {
			k := aws.ToString(o.Key)
			base := path.Base(k)

			if strings.HasSuffix(k, "/") || strings.HasPrefix(base, "_") || strings.HasPrefix(base, ".") {
				continue
			}

			keys = append(keys, k)
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return bucket, keys, nil
		}

		token = out.NextContinuationToken
	}
}

// loadLocation streams every data object under location into sink.
func (l *tableLoader) loadLocation(
	ctx context.Context, location string, f tableFormat, cols []Column, sink *rowSink,
) error {
	bucket, keys, err := l.listKeys(ctx, location)
	if err != nil {
		return fmt.Errorf("HIVE_METASTORE_ERROR: cannot list %s: %w", location, err)
	}

	for _, key := range keys {
		if err = l.loadObject(ctx, bucket, key, f, cols, sink); err != nil {
			return err
		}
	}

	return nil
}

func (l *tableLoader) loadObject(
	ctx context.Context, bucket, key string, f tableFormat, cols []Column, sink *rowSink,
) error {
	out, err := l.get.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("HIVE_CANNOT_OPEN_SPLIT: Error opening Hive split s3://%s/%s: %w", bucket, key, err)
	}
	defer func() { _ = out.Body.Close() }()

	counted := &countingReader{r: out.Body, n: &l.scanned}
	br := bufio.NewReaderSize(counted, readBufBytes)

	var rd io.Reader = br

	if magic, perr := br.Peek(gzipMagicLen); perr == nil && magic[0] == gzipMagic0 && magic[1] == gzipMagic1 {
		gz, gerr := gzip.NewReader(br)
		if gerr != nil {
			return fmt.Errorf("HIVE_CURSOR_ERROR: s3://%s/%s: %w", bucket, key, gerr)
		}

		defer func() { _ = gz.Close() }()

		rd = io.LimitReader(gz, l.limits.MaxScanBytes*decompressRatio)
	}

	if err = l.decode(ctx, rd, f, cols, sink); err != nil {
		return err
	}

	if l.scanned > l.limits.MaxScanBytes {
		return fmt.Errorf("%w: more than %d bytes", errScanLimit, l.limits.MaxScanBytes)
	}

	return nil
}

func (l *tableLoader) decode(ctx context.Context, rd io.Reader, f tableFormat, cols []Column, sink *rowSink) error {
	switch f.kind {
	case fmtJSON:
		return l.decodeJSON(ctx, rd, f, cols, sink)
	case fmtCSVSerde:
		return l.decodeCSV(ctx, rd, f, sink)
	default:
		return l.decodeDelimited(ctx, rd, f, sink)
	}
}

func (l *tableLoader) decodeCSV(ctx context.Context, rd io.Reader, f tableFormat, sink *rowSink) error {
	cr := csv.NewReader(rd)
	cr.Comma = []rune(f.delim)[0]
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1
	cr.ReuseRecord = true

	for n := 0; ; n++ {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("HIVE_CURSOR_ERROR: %w", err)
		}

		if n < f.skipHeader {
			continue
		}

		if err = l.insert(ctx, sink, rec, false); err != nil {
			return err
		}
	}
}

func (l *tableLoader) decodeDelimited(ctx context.Context, rd io.Reader, f tableFormat, sink *rowSink) error {
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 0, readBufBytes), maxLineBytes)

	for n := 0; sc.Scan(); n++ {
		if n < f.skipHeader {
			continue
		}

		line := strings.TrimSuffix(sc.Text(), "\r")
		if line == "" {
			continue
		}

		if err := l.insert(ctx, sink, strings.Split(line, f.delim), true); err != nil {
			return err
		}
	}

	if err := sc.Err(); err != nil {
		return fmt.Errorf("HIVE_CURSOR_ERROR: %w", err)
	}

	return nil
}

func jsonRaw(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	default:
		b, _ := json.Marshal(t)

		return string(b)
	}
}

func (l *tableLoader) decodeJSON(ctx context.Context, rd io.Reader, f tableFormat, cols []Column, sink *rowSink) error {
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 0, readBufBytes), maxLineBytes)

	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}

		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()

		var obj map[string]any

		if err := dec.Decode(&obj); err != nil {
			if f.ignoreBad {
				continue
			}

			return fmt.Errorf("HIVE_CURSOR_ERROR: Failed to read JSON record: %w", err)
		}

		lower := make(map[string]any, len(obj))
		for k, v := range obj {
			lower[strings.ToLower(k)] = v
		}

		rec := make([]string, len(cols))
		nulls := make([]bool, len(cols))

		for i, c := range cols {
			v, ok := lower[strings.ToLower(c.Name)]
			rec[i] = jsonRaw(v)
			nulls[i] = !ok || v == nil
		}

		if err := l.insertJSON(ctx, sink, rec, nulls); err != nil {
			return err
		}
	}

	if err := sc.Err(); err != nil {
		return fmt.Errorf("HIVE_CURSOR_ERROR: %w", err)
	}

	return nil
}

func (l *tableLoader) checkRow(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	l.rows++
	if l.rows > l.limits.MaxLoadedRows {
		return fmt.Errorf("%w: more than %d rows", errRowLimit, l.limits.MaxLoadedRows)
	}

	return nil
}

func (l *tableLoader) insert(ctx context.Context, sink *rowSink, rec []string, nullToken bool) error {
	if err := l.checkRow(ctx); err != nil {
		return err
	}

	args := make([]any, 0, sink.nargs)

	for i, k := range sink.kinds {
		if i < len(rec) {
			args = append(args, convertValue(rec[i], k, nullToken))
		} else {
			args = append(args, nil)
		}
	}

	args = append(args, sink.extra...)
	_, err := sink.stmt.ExecContext(ctx, args...)

	return err
}

func (l *tableLoader) insertJSON(ctx context.Context, sink *rowSink, rec []string, nulls []bool) error {
	if err := l.checkRow(ctx); err != nil {
		return err
	}

	args := make([]any, 0, sink.nargs)

	for i, k := range sink.kinds {
		if nulls[i] {
			args = append(args, nil)
		} else {
			args = append(args, convertValue(rec[i], k, false))
		}
	}

	args = append(args, sink.extra...)
	_, err := sink.stmt.ExecContext(ctx, args...)

	return err
}
