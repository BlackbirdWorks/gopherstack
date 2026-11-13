package dmsengine

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// Errors reported by the S3 source.
var (
	ErrS3Source        = errors.New("invalid S3 source")
	errS3SourceBucket  = fmt.Errorf("%w: S3Settings.BucketName is required", ErrS3Source)
	errS3SourceDef     = fmt.Errorf("%w: an ExternalTableDefinition is required", ErrS3Source)
	errS3SourceCDCPath = fmt.Errorf("%w: S3Settings.CdcPath is required to capture changes", ErrS3Source)
	errS3SourceParquet = fmt.Errorf("%w: Parquet source files are not supported", ErrS3Source)
	errS3SourceDelim   = fmt.Errorf("%w: CsvDelimiter must be a single character", ErrS3Source)
	errS3SourceNoStore = fmt.Errorf("%w: %w: S3", ErrS3Source, ErrNotConfigured)
)

const (
	s3SourceLoadPrefix  = "load"
	s3SourceCSVSuffix   = ".csv"
	s3SourceMinCDCCells = 3
)

// jsonScalar reads a JSON string, number or boolean as its text; the external table definition quotes everything.
type jsonScalar string

func (s *jsonScalar) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v, jsonx.V1()); err != nil {
		return err
	}

	switch x := v.(type) {
	case nil:
		*s = ""
	case string:
		*s = jsonScalar(x)
	default:
		*s = jsonScalar(strings.Trim(string(b), `"`))
	}

	return nil
}

func (s jsonScalar) bool() bool { return strings.EqualFold(strings.TrimSpace(string(s)), "true") }

func (s jsonScalar) int() int {
	n, _ := strconv.Atoi(strings.TrimSpace(string(s)))

	return n
}

type s3ColumnDef struct {
	Name       string     `json:"ColumnName"`
	Type       string     `json:"ColumnType"`
	Length     jsonScalar `json:"ColumnLength"`
	Precision  jsonScalar `json:"ColumnPrecision"`
	Scale      jsonScalar `json:"ColumnScale"`
	Nullable   jsonScalar `json:"ColumnNullable"`
	IsPK       jsonScalar `json:"ColumnIsPk"`
	DateFormat string     `json:"ColumnDateFormat"`
}

type s3TableDef struct {
	Name         string        `json:"TableName"`
	Path         string        `json:"TablePath"`
	Owner        string        `json:"TableOwner"`
	ColumnsTotal jsonScalar    `json:"TableColumnsTotal"`
	Columns      []s3ColumnDef `json:"TableColumns"`
}

type s3SourceOptions struct {
	Rfc4180          *bool  `json:"Rfc4180"`
	BucketName       string `json:"BucketName"`
	BucketFolder     string `json:"BucketFolder"`
	CdcPath          string `json:"CdcPath"`
	CsvDelimiter     string `json:"CsvDelimiter"`
	CsvRowDelimiter  string `json:"CsvRowDelimiter"`
	CsvNullValue     string `json:"CsvNullValue"`
	DataFormat       string `json:"DataFormat"`
	ExternalTableDef string `json:"ExternalTableDefinition"`
	IgnoreHeaderRows int    `json:"IgnoreHeaderRows"`
}

type s3Source struct {
	store  ObjectStore
	paths  map[string]string
	dates  map[string][]string
	opts   s3SourceOptions
	tables []Table
	poll   time.Duration
	delim  rune
}

func newS3Source(ep EndpointConfig, store ObjectStore) (Source, error) {
	if store == nil {
		return nil, errS3SourceNoStore
	}

	var o s3SourceOptions
	if ep.S3Settings != "" {
		if err := json.Unmarshal([]byte(ep.S3Settings), &o, jsonx.V1()); err != nil {
			return nil, fmt.Errorf("%w: S3Settings: %w", ErrS3Source, err)
		}
	}

	if o.BucketName == "" {
		return nil, errS3SourceBucket
	}

	if strings.EqualFold(o.DataFormat, "parquet") {
		return nil, errS3SourceParquet
	}

	s := &s3Source{store: store, opts: o, poll: ep.PollInterval, delim: ',', paths: map[string]string{},
		dates: map[string][]string{}}

	if o.CsvDelimiter != "" {
		rs := []rune(o.CsvDelimiter)
		if len(rs) != 1 {
			return nil, errS3SourceDelim
		}

		s.delim = rs[0]
	}

	def := ep.ExternalTableDefinition
	if def == "" {
		def = o.ExternalTableDef
	}

	if err := s.loadDefinition(def); err != nil {
		return nil, err
	}

	if s.poll <= 0 {
		s.poll = defaultPGPoll
	}

	return s, nil
}

func (s *s3Source) loadDefinition(def string) error {
	if strings.TrimSpace(def) == "" {
		return errS3SourceDef
	}

	var doc struct {
		Tables []s3TableDef `json:"Tables"`
	}

	if err := json.Unmarshal([]byte(def), &doc, jsonx.V1()); err != nil {
		return fmt.Errorf("%w: ExternalTableDefinition: %w", ErrS3Source, err)
	}

	for _, td := range doc.Tables {
		t, err := td.table()
		if err != nil {
			return err
		}

		s.tables = append(s.tables, t)
		s.paths[t.Key()] = td.Path

		fm := make([]string, len(td.Columns))
		for i, c := range td.Columns {
			fm[i] = c.DateFormat
		}

		s.dates[t.Key()] = fm
	}

	return nil
}

func (td s3TableDef) table() (Table, error) {
	if td.Name == "" || len(td.Columns) == 0 {
		return Table{}, fmt.Errorf("%w: a table needs TableName and TableColumns", ErrS3Source)
	}

	if td.ColumnsTotal != "" && td.ColumnsTotal.int() != len(td.Columns) {
		return Table{}, fmt.Errorf("%w: %s: TableColumnsTotal %s does not match its %d columns",
			ErrS3Source, td.Name, td.ColumnsTotal, len(td.Columns))
	}

	t := Table{Schema: td.Owner, Name: td.Name}

	for _, c := range td.Columns {
		kind := strings.ToLower(c.Type)
		if kind == "byte" {
			kind = tBytes
		}

		dt, err := ParseDMSType(kind)
		if err != nil {
			return t, fmt.Errorf("%w: %s.%s: %w", ErrS3Source, td.Name, c.Name, err)
		}

		dt.Length, dt.Precision, dt.Scale = c.Length.int(), c.Precision.int(), c.Scale.int()
		col := dt.Column(c.Name)
		col.Nullable = c.Nullable.bool()
		t.Columns = append(t.Columns, col)

		if c.IsPK.bool() {
			t.PK = append(t.PK, c.Name)
		}
	}

	return t, nil
}

func (s *s3Source) Ping(ctx context.Context) error {
	return s.store.CheckBucket(ctx, s.opts.BucketName)
}

func (s *s3Source) Close() error { return nil }

func (s *s3Source) Release(context.Context) error { return nil }

func (s *s3Source) Tables(context.Context) ([]Table, error) { return s.tables, nil }

func (s *s3Source) Position(context.Context) (string, error) { return "", nil }

func (s *s3Source) key(parts ...string) string {
	all := []string{}
	if f := strings.Trim(s.opts.BucketFolder, "/"); f != "" {
		all = append(all, f)
	}

	for _, p := range parts {
		if p = strings.Trim(p, "/"); p != "" {
			all = append(all, p)
		}
	}

	return strings.Join(all, "/")
}

func (s *s3Source) tablePath(t Table) string {
	if p := s.paths[t.Key()]; p != "" {
		return p
	}

	return t.Schema + "/" + t.Name
}

func (s *s3Source) Scan(ctx context.Context, t Table, fn func(Row) error) error {
	dir := s.key(s.tablePath(t)) + "/"

	keys, err := s.store.ListObjects(ctx, s.opts.BucketName, dir)
	if err != nil {
		return err
	}

	for _, k := range keys {
		base := strings.ToLower(strings.TrimPrefix(k, dir))
		if strings.Contains(base, "/") || !strings.HasPrefix(base, s3SourceLoadPrefix) ||
			!strings.HasSuffix(base, s3SourceCSVSuffix) {
			continue
		}

		recs, rerr := s.records(ctx, k)
		if rerr != nil {
			return rerr
		}

		for i, rec := range recs {
			row, rowErr := s.fullLoadRow(t, rec)
			if rowErr != nil {
				return fmt.Errorf("%s record %d: %w", k, i+1, rowErr)
			}

			if err = fn(row); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *s3Source) records(ctx context.Context, key string) ([][]string, error) {
	body, err := s.store.GetObject(ctx, s.opts.BucketName, key)
	if err != nil {
		return nil, err
	}

	if rd := s.opts.CsvRowDelimiter; rd != "" && rd != "\n" && rd != "\r\n" {
		body = bytes.ReplaceAll(body, []byte(rd), []byte("\n"))
	}

	r := csv.NewReader(bytes.NewReader(body))
	r.Comma, r.FieldsPerRecord, r.LazyQuotes = s.delim, -1, s.opts.Rfc4180 != nil && !*s.opts.Rfc4180

	var out [][]string

	for {
		rec, rerr := r.Read()
		if errors.Is(rerr, io.EOF) {
			break
		}

		if rerr != nil {
			return nil, fmt.Errorf("%s: %w", key, rerr)
		}

		out = append(out, rec)
	}

	return out[min(s.opts.IgnoreHeaderRows, len(out)):], nil
}

func isInsertOp(s string) bool {
	return strings.EqualFold(s, "I") || strings.EqualFold(s, opWordInsert)
}

func (s *s3Source) fullLoadRow(t Table, rec []string) (Row, error) {
	if len(rec) == len(t.Columns)+1 && isInsertOp(rec[0]) {
		rec = rec[1:]
	}

	return s.row(t, rec)
}

func (s *s3Source) row(t Table, rec []string) (Row, error) {
	if len(rec) != len(t.Columns) {
		return nil, fmt.Errorf(
			"%w: %s has %d columns, the record has %d",
			ErrS3Source,
			t.Key(),
			len(t.Columns),
			len(rec),
		)
	}

	fm := s.dates[t.Key()]
	row := make(Row, len(rec))

	for i, f := range rec {
		switch {
		case f == s.opts.CsvNullValue:
			row[i] = nil
		case t.Columns[i].Kind == KindBytes:
			row[i] = []byte(f)
		default:
			row[i] = normalizeDate(f, fm[i], t.Columns[i].DMS.Name)
		}
	}

	return row, nil
}

func dateTokens() *strings.Replacer {
	return strings.NewReplacer(
		"YYYY",
		"2006",
		"MM",
		"01",
		"dd",
		"02",
		"HH",
		"15",
		"mm",
		"04",
		"ss",
		"05",
		".F",
		".999999999",
	)
}

// normalizeDate rewrites a ColumnDateFormat value to the ISO form databases accept.
func normalizeDate(v, format, dmsType string) string {
	if format == "" {
		return v
	}

	t, err := time.Parse(dateTokens().Replace(format), v)
	if err != nil {
		return v
	}

	switch dmsType {
	case tDate:
		return t.Format("2006-01-02")
	case tTime:
		return t.Format("15:04:05")
	}

	return t.Format("2006-01-02 15:04:05.999999999")
}

func (s *s3Source) Stream(
	ctx context.Context, start StreamStart, tables func(string, string) (Table, bool), fn func(Change) error,
) error {
	if s.opts.CdcPath == "" {
		return errS3SourceCDCPath
	}

	last := start.Position
	tick := time.NewTicker(s.poll)

	defer tick.Stop()

	for {
		var err error
		if last, err = s.drain(ctx, last, tables, fn); err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// drain applies the CDC files that sort after last, in order, and returns the last key applied.
func (s *s3Source) drain(
	ctx context.Context, last string, tables func(string, string) (Table, bool), fn func(Change) error,
) (string, error) {
	dir := s.key(s.opts.CdcPath) + "/"

	keys, err := s.store.ListObjects(ctx, s.opts.BucketName, dir)
	if err != nil {
		return last, err
	}

	for _, k := range keys {
		base := strings.ToLower(path.Base(k))
		if k <= last || strings.Contains(strings.TrimPrefix(k, dir), "/") ||
			!strings.HasSuffix(base, s3SourceCSVSuffix) {
			continue
		}

		if err = s.applyFile(ctx, k, tables, fn); err != nil {
			return last, err
		}

		last = k
	}

	return last, nil
}

func (s *s3Source) applyFile(
	ctx context.Context, key string, tables func(string, string) (Table, bool), fn func(Change) error,
) error {
	recs, err := s.records(ctx, key)
	if err != nil {
		return err
	}

	for i, rec := range recs {
		c, ok, cerr := s.change(rec, tables)
		if cerr != nil {
			return fmt.Errorf("%s record %d: %w", key, i+1, cerr)
		}

		if !ok {
			continue
		}

		c.Position, c.Time = key, time.Now().UTC()
		if err = fn(c); err != nil {
			return err
		}
	}

	return fn(Change{Commit: true, Position: key, Time: time.Now().UTC()})
}

// change reads one CDC record: operation, table name, schema name, then the row.
func (s *s3Source) change(rec []string, tables func(string, string) (Table, bool)) (Change, bool, error) {
	if len(rec) < s3SourceMinCDCCells {
		return Change{}, false, fmt.Errorf("%w: a CDC record needs operation, table, schema and data", ErrS3Source)
	}

	t, ok := tables(rec[2], rec[1])
	if !ok {
		return Change{}, false, nil
	}

	row, err := s.row(t, rec[s3SourceMinCDCCells:])
	if err != nil {
		return Change{}, false, err
	}

	c := Change{Schema: t.Schema, Table: t.Name}

	switch strings.ToUpper(rec[0]) {
	case "I", opWordInsert:
		c.Op, c.After = OpInsert, row
	case "U", "UPDATE":
		c.Op, c.After = OpUpdate, row
	case "D", "DELETE":
		c.Op, c.Before = OpDelete, row
	default:
		return Change{}, false, fmt.Errorf("%w: unknown operation %q", ErrS3Source, rec[0])
	}

	return c, true, nil
}
