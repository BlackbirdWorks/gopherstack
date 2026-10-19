package redshiftdata

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

var errNoPgConn = errors.New("the connection does not expose a Postgres wire connection")

const (
	copyNullLiteral = `\N`
	copyTab         = "\t"
	copyPipe        = "|"
	copyComma       = ","
)

// s3Runner runs the COPY FROM and UNLOAD TO statements that the Postgres engine cannot execute itself.
type s3Runner struct {
	conn  *sql.Conn
	store ObjectStore
}

func leadingWord(stmt string) string {
	s := strings.TrimLeft(stmt, " \t\r\n(")
	end := 0

	for end < len(s) && isWordByte(s[end]) {
		end++
	}

	return strings.ToLower(s[:end])
}

// run executes stmt when it is COPY FROM s3 or UNLOAD; handled is false for everything else.
func (r *s3Runner) run(ctx context.Context, q querier, stmt string) (bool, error) {
	if r == nil {
		return false, nil
	}

	switch leadingWord(stmt) {
	case "copy":
		spec, handled, err := parseCopy(stmt)
		if !handled {
			return false, nil
		}

		if err != nil {
			return true, err
		}

		return true, r.copyFrom(ctx, q, spec)
	case "unload":
		spec, err := parseUnload(stmt)
		if err != nil {
			return true, err
		}

		return true, r.unload(ctx, q, spec)
	default:
		return false, nil
	}
}

func (r *s3Runner) requireStore() error {
	if r.store == nil {
		return stmtErrorf("S3 access is not available to this engine")
	}

	return nil
}

func (r *s3Runner) copyFrom(ctx context.Context, q querier, s *copySpec) error {
	if err := r.requireStore(); err != nil {
		return err
	}

	bucket, key, _ := s3URI(s.source)

	keys, err := r.copyObjects(ctx, s, bucket, key)
	if err != nil {
		return err
	}

	cols, err := r.targetColumns(ctx, q, s)
	if err != nil {
		return err
	}

	var body bytes.Buffer

	for _, obj := range keys {
		raw, gerr := r.store.GetObject(ctx, obj.bucket, obj.key)
		if gerr != nil {
			return stmtErrorf("S3ServiceException:%s,Status 404,Error NoSuchKey,Rid 0,CanRetry 1: %v", obj.key, gerr)
		}

		if err = appendCopyObject(&body, s, cols, raw, func(uri string) ([]byte, error) {
			b, k, ok := s3URI(uri)
			if !ok {
				return nil, stmtErrorf("invalid S3 URI %q", uri)
			}

			return r.store.GetObject(ctx, b, k)
		}); err != nil {
			return err
		}
	}

	if s.noLoad {
		return nil
	}

	return r.pgCopy(ctx, copyStatement(s), body.Bytes())
}

type s3Object struct{ bucket, key string }

func (r *s3Runner) copyObjects(ctx context.Context, s *copySpec, bucket, key string) ([]s3Object, error) {
	if s.manifest {
		return r.manifestObjects(ctx, bucket, key)
	}

	keys, err := r.store.ListKeys(ctx, bucket, key)
	if err != nil {
		return nil, stmtErrorf("S3ServiceException:%v,Status 404,Error NoSuchBucket,Rid 0,CanRetry 1", err)
	}

	if len(keys) == 0 {
		return nil, stmtErrorf("The specified S3 prefix '%s' does not exist", key)
	}

	sort.Strings(keys)

	objs := make([]s3Object, len(keys))
	for i, k := range keys {
		objs[i] = s3Object{bucket: bucket, key: k}
	}

	return objs, nil
}

func (r *s3Runner) manifestObjects(ctx context.Context, bucket, key string) ([]s3Object, error) {
	raw, err := r.store.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, stmtErrorf("S3ServiceException:%s,Status 404,Error NoSuchKey,Rid 0,CanRetry 1: %v", key, err)
	}

	var m struct {
		Entries []struct {
			URL       string `json:"url"`
			Mandatory bool   `json:"mandatory"`
		} `json:"entries"`
	}

	if err = json.Unmarshal(raw, &m); err != nil {
		return nil, stmtErrorf("Manifest file is not in correct json format")
	}

	objs := make([]s3Object, 0, len(m.Entries))

	for _, e := range m.Entries {
		b, k, ok := s3URI(e.URL)
		if !ok {
			return nil, stmtErrorf("Manifest entry url %q is not an S3 URL", e.URL)
		}

		if _, gerr := r.store.GetObject(ctx, b, k); gerr != nil {
			if e.Mandatory {
				return nil, stmtErrorf("Mandatory file %s does not exist", e.URL)
			}

			continue
		}

		objs = append(objs, s3Object{bucket: b, key: k})
	}

	return objs, nil
}

// targetColumns lists the columns a COPY loads, in order; the file formats without delimiters need them.
func (r *s3Runner) targetColumns(ctx context.Context, q querier, s *copySpec) ([]string, error) {
	if len(s.columns) > 0 || s.format != fmtLines && s.format != fmtParquet {
		return s.columns, nil
	}

	schema, name := "", s.tableParts[len(s.tableParts)-1]
	if len(s.tableParts) > 1 {
		schema = s.tableParts[len(s.tableParts)-2]
	}

	rows, err := q.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns `+
			`WHERE table_name = $1 AND table_schema = coalesce(nullif($2, ''), current_schema()) `+
			`ORDER BY ordinal_position`, name, schema)
	if err != nil {
		return nil, formatPGError(err)
	}

	defer func() { _ = rows.Close() }()

	var cols []string

	for rows.Next() {
		var c string
		if err = rows.Scan(&c); err != nil {
			return nil, formatPGError(err)
		}

		cols = append(cols, strings.ToLower(c))
	}

	if err = rows.Err(); err != nil {
		return nil, formatPGError(err)
	}

	if len(cols) == 0 {
		return nil, stmtErrorf("relation %q does not exist", strings.Join(s.tableParts, "."))
	}

	return cols, nil
}

func decompress(s *copySpec, raw []byte) ([]byte, error) {
	switch {
	case s.gzip:
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, stmtErrorf("Unknown zlib error code. zlib error code: -3: %v", err)
		}

		return io.ReadAll(zr)
	case s.bzip2:
		return io.ReadAll(bzip2.NewReader(bytes.NewReader(raw)))
	default:
		return raw, nil
	}
}

func appendCopyObject(
	body *bytes.Buffer, s *copySpec, cols []string, raw []byte, fetch func(string) ([]byte, error),
) error {
	if s.format == fmtParquet {
		return appendParquet(body, s, cols, raw)
	}

	data, err := decompress(s, raw)
	if err != nil {
		return stmtErrorf("could not decompress the S3 object: %v", err)
	}

	if s.format == fmtLines {
		return appendJSON(body, s, cols, data, fetch)
	}

	body.Write(delimitedData(s, data))

	return nil
}

func delimitedData(s *copySpec, data []byte) []byte {
	if s.fixInvalidOn {
		data = []byte(strings.ToValidUTF8(string(data), s.fixInvalid))
	}

	lines := bytes.SplitAfter(data, []byte("\n"))
	out := make([]byte, 0, len(data)+1)

	for i, ln := range lines {
		if i < s.ignoreHeader {
			continue
		}

		if s.skipBlank && len(bytes.TrimSpace(ln)) == 0 {
			continue
		}

		out = append(out, ln...)
	}

	if len(out) > 0 && out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}

	return out
}

func appendParquet(body *bytes.Buffer, _ *copySpec, cols []string, raw []byte) error {
	f, err := parquetio.Open(raw)
	if err != nil {
		return stmtErrorf("Spectrum Scan Error: the S3 object is not a valid Parquet file: %v", err)
	}

	fileCols := f.Columns()
	if len(cols) > 0 && len(fileCols) != len(cols) {
		return stmtErrorf("Spectrum Scan Error: the Parquet file has %d columns but the COPY loads %d",
			len(fileCols), len(cols))
	}

	return f.ForEach(fileCols, func(vals []any) error {
		cells := make([]*string, len(vals))
		for i, v := range vals {
			cells[i] = textCell(v)
		}

		writeTextRow(body, cells)

		return nil
	})
}

func appendJSON(
	body *bytes.Buffer, s *copySpec, cols []string, data []byte, fetch func(string) ([]byte, error),
) error {
	paths, err := jsonPathsFor(s, cols, fetch)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	for {
		var rec any
		if err = dec.Decode(&rec); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return stmtErrorf("Invalid JSON record in S3 object: %v", err)
		}

		obj, ok := rec.(map[string]any)
		if !ok {
			return stmtErrorf("Invalid JSON record in S3 object: a record must be a JSON object")
		}

		cells := make([]*string, len(cols))

		for i := range cols {
			cells[i] = textCell(paths[i](obj))
		}

		writeTextRow(body, cells)
	}
}

type jsonExtract func(obj map[string]any) any

func jsonPathsFor(s *copySpec, cols []string, fetch func(string) ([]byte, error)) ([]jsonExtract, error) {
	out := make([]jsonExtract, len(cols))

	if strings.EqualFold(s.jsonPaths, "auto") || strings.EqualFold(s.jsonPaths, "auto ignorecase") {
		for i, c := range cols {
			out[i] = func(obj map[string]any) any { return lookupFold(obj, c) }
		}

		return out, nil
	}

	raw, err := fetch(s.jsonPaths)
	if err != nil {
		return nil, stmtErrorf("could not read the JSONPaths file %s: %v", s.jsonPaths, err)
	}

	var doc struct {
		JSONPaths []string `json:"jsonpaths"`
	}

	if err = json.Unmarshal(raw, &doc); err != nil {
		return nil, stmtErrorf("The JSONPaths file is not valid JSON: %v", err)
	}

	if len(doc.JSONPaths) != len(cols) {
		return nil, stmtErrorf("Number of jsonpaths and the number of columns should match. jsonpaths: %d, columns: %d",
			len(doc.JSONPaths), len(cols))
	}

	for i, p := range doc.JSONPaths {
		steps, perr := parseJSONPath(p)
		if perr != nil {
			return nil, perr
		}

		out[i] = func(obj map[string]any) any { return walkJSONPath(obj, steps) }
	}

	return out, nil
}

func lookupFold(obj map[string]any, name string) any {
	if v, ok := obj[name]; ok {
		return v
	}

	for k, v := range obj {
		if strings.EqualFold(k, name) {
			return v
		}
	}

	return nil
}

type pathStep struct {
	key   string
	index int
	isIdx bool
}

// parseJSONPath understands $, .key, ['key'], ["key"] and [n].
func parseJSONPath(p string) ([]pathStep, error) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(p), "$")
	if !ok {
		return nil, stmtErrorf("Invalid JSONPath expression %q", p)
	}

	var steps []pathStep

	for rest != "" {
		switch rest[0] {
		case '.':
			end := strings.IndexAny(rest[1:], ".[")
			if end < 0 {
				end = len(rest) - 1
			}

			steps = append(steps, pathStep{key: rest[1 : 1+end]})
			rest = rest[1+end:]
		case '[':
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return nil, stmtErrorf("Invalid JSONPath expression %q", p)
			}

			step, err := bracketStep(rest[1:end], p)
			if err != nil {
				return nil, err
			}

			steps, rest = append(steps, step), rest[end+1:]
		default:
			return nil, stmtErrorf("Invalid JSONPath expression %q", p)
		}
	}

	return steps, nil
}

func bracketStep(inner, whole string) (pathStep, error) {
	if n := len(inner); n >= 2 && (inner[0] == '\'' || inner[0] == '"') && inner[n-1] == inner[0] {
		return pathStep{key: inner[1 : n-1]}, nil
	}

	var idx int
	if _, err := fmt.Sscanf(inner, "%d", &idx); err != nil || idx < 0 {
		return pathStep{}, stmtErrorf("Invalid JSONPath expression %q", whole)
	}

	return pathStep{index: idx, isIdx: true}, nil
}

func walkJSONPath(v any, steps []pathStep) any {
	for _, st := range steps {
		switch cur := v.(type) {
		case map[string]any:
			if st.isIdx {
				return nil
			}

			v = cur[st.key]
		case []any:
			if !st.isIdx || st.index >= len(cur) {
				return nil
			}

			v = cur[st.index]
		default:
			return nil
		}
	}

	return v
}

// textCell renders a decoded value as a COPY text cell; nil means NULL.
func textCell(v any) *string {
	var s string

	switch x := v.(type) {
	case nil:
		return nil
	case string:
		s = x
	case json.Number:
		s = x.String()
	case bool:
		s = strconv.FormatBool(x)
	case int64, float64, int32, int:
		s = fmt.Sprint(x)
	default:
		raw, err := json.Marshal(x)
		if err != nil {
			s = fmt.Sprint(x)
		} else {
			s = string(raw)
		}
	}

	return &s
}

func writeTextRow(w *bytes.Buffer, cells []*string) {
	for i, c := range cells {
		if i > 0 {
			w.WriteString(copyTab)
		}

		if c == nil {
			w.WriteString(copyNullLiteral)

			continue
		}

		w.WriteString(escapeCopyText(*c))
	}

	w.WriteByte('\n')
}

func copyStatement(s *copySpec) string {
	opts := "FORMAT text, DELIMITER " + quoteLiteral(copyTab) + ", NULL " + quoteLiteral(copyNullLiteral)

	if s.format == fmtText || s.format == fmtCSV {
		opts = delimitedCopyOptions(s)
	}

	stmt := "COPY " + s.table
	if s.columnsRaw != "" {
		stmt += " " + s.columnsRaw
	}

	return stmt + " FROM STDIN WITH (" + opts + ")"
}

func delimitedCopyOptions(s *copySpec) string {
	delim := copyPipe
	if s.format == fmtCSV {
		delim = copyComma
	}

	if s.delimiter != "" {
		delim = s.delimiter
	}

	if delim == `\t` {
		delim = copyTab
	}

	null := copyNullLiteral

	switch {
	case s.nullSet:
		null = s.nullAs
	case s.emptyAsNull || s.format == fmtCSV:
		null = ""
	}

	opts := "FORMAT " + s.format + ", DELIMITER " + quoteLiteral(delim) + ", NULL " + quoteLiteral(null)
	if s.format == fmtCSV && s.quote != "" {
		opts += ", QUOTE " + quoteLiteral(s.quote)
	}

	return opts
}

func (r *s3Runner) pgCopy(ctx context.Context, stmt string, data []byte) error {
	return r.conn.Raw(func(dc any) error {
		c, ok := dc.(*stdlib.Conn)
		if !ok {
			return errNoPgConn
		}

		if _, err := c.Conn().PgConn().CopyFrom(ctx, bytes.NewReader(data), stmt); err != nil {
			return formatPGError(err)
		}

		return nil
	})
}

func escapeCopyText(s string) string {
	if !strings.ContainsAny(s, "\\\t\n\r") {
		return s
	}

	var b strings.Builder

	for i := range len(s) {
		switch s[i] {
		case '\\':
			b.WriteString(`\\`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteByte(s[i])
		}
	}

	return b.String()
}
