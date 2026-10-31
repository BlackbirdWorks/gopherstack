package dmsengine

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// s3Options mirrors the S3Settings members the writer honours (types.S3Settings).
type s3Options struct {
	BucketName           string `json:"BucketName"`
	BucketFolder         string `json:"BucketFolder"`
	CsvDelimiter         string `json:"CsvDelimiter"`
	CsvRowDelimiter      string `json:"CsvRowDelimiter"`
	CsvNullValue         string `json:"CsvNullValue"`
	DataFormat           string `json:"DataFormat"`
	AddColumnName        bool   `json:"AddColumnName"`
	IncludeOpForFullLoad bool   `json:"IncludeOpForFullLoad"`
	CdcInsertsOnly       bool   `json:"CdcInsertsOnly"`
	CdcInsertsAndUpdates bool   `json:"CdcInsertsAndUpdates"`
	TimestampColumnName  string `json:"TimestampColumnName"`
	MaxFileSize          int    `json:"MaxFileSize"`
	Rfc4180              *bool  `json:"Rfc4180"`
	quote                bool
	delimiter, rowDelim  string
	nullValue            string
	maxBytes             int
}

type s3Target struct {
	store   ObjectStore
	opts    s3Options
	now     func() time.Time
	load    map[string]*loadFile
	cdc     map[string]*bytes.Buffer
	pqLoad  map[string]*pqBuf
	pqCDC   map[string]*pqBuf
	parquet bool
	seq     int
	mu      sync.Mutex
}

type loadFile struct {
	buf  bytes.Buffer
	next int
}

func newS3Target(ep EndpointConfig, store ObjectStore) (Target, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: S3", ErrNotConfigured)
	}

	var o s3Options
	if ep.S3Settings != "" {
		if err := json.Unmarshal([]byte(ep.S3Settings), &o); err != nil {
			return nil, fmt.Errorf("invalid S3Settings: %w", err)
		}
	}

	if o.BucketName == "" {
		return nil, errors.New("S3Settings.BucketName is required")
	}

	o.delimiter, o.rowDelim, o.nullValue = o.CsvDelimiter, o.CsvRowDelimiter, o.CsvNullValue
	if o.delimiter == "" {
		o.delimiter = ","
	}

	if o.rowDelim == "" {
		o.rowDelim = "\n"
	}

	if o.nullValue == "" {
		o.nullValue = "NULL"
	}

	o.quote = o.Rfc4180 == nil || *o.Rfc4180
	o.maxBytes = 1048576 * 1024

	if o.MaxFileSize > 0 {
		o.maxBytes = o.MaxFileSize * 1024
	}

	return &s3Target{
		store: store, opts: o, now: time.Now, load: map[string]*loadFile{}, cdc: map[string]*bytes.Buffer{},
		pqLoad: map[string]*pqBuf{}, pqCDC: map[string]*pqBuf{}, parquet: strings.EqualFold(o.DataFormat, "parquet"),
	}, nil
}

func (t *s3Target) Ping(ctx context.Context) error {
	return t.store.CheckBucket(ctx, t.opts.BucketName)
}

func (t *s3Target) Close() error { return nil }

func (t *s3Target) keyFor(tt TargetTable, file string) string {
	parts := []string{}
	if f := strings.Trim(t.opts.BucketFolder, "/"); f != "" {
		parts = append(parts, f)
	}

	return strings.Join(append(parts, tt.Schema, tt.Name, file), "/")
}

func (t *s3Target) Prepare(_ context.Context, tt TargetTable, _ PrepMode) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.load[tt.Key()] = &loadFile{next: 1}
	if t.parquet {
		t.pqLoad[tt.Key()] = newPQBuf(tt, t.opts.IncludeOpForFullLoad, t.opts.TimestampColumnName)
	}

	return nil
}

func (t *s3Target) field(v any) string {
	var s string

	switch x := v.(type) {
	case nil:
		return t.opts.nullValue
	case []byte:
		s = hex.EncodeToString(x)
	default:
		s = fmt.Sprint(x)
	}

	if t.opts.quote && (strings.Contains(s, t.opts.delimiter) || strings.ContainsAny(s, "\"\r\n")) {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}

	return s
}

func (t *s3Target) line(opCode string, row Row, stamp string) string {
	fields := make([]string, 0, len(row)+2)
	if opCode != "" {
		fields = append(fields, opCode)
	}

	if stamp != "" {
		fields = append(fields, stamp)
	}

	for _, v := range row {
		fields = append(fields, t.field(v))
	}

	return strings.Join(fields, t.opts.delimiter) + t.opts.rowDelim
}

func (t *s3Target) header(tt TargetTable, withOp bool) string {
	if !t.opts.AddColumnName {
		return ""
	}

	names := make([]string, 0, len(tt.Columns)+2)
	if withOp {
		names = append(names, "Op")
	}

	if t.opts.TimestampColumnName != "" {
		names = append(names, t.opts.TimestampColumnName)
	}

	names = append(names, columnNames(tt)...)

	return strings.Join(names, t.opts.delimiter) + t.opts.rowDelim
}

func (t *s3Target) Load(ctx context.Context, tt TargetTable, rows []Row) error {
	t.mu.Lock()
	lf := t.load[tt.Key()]
	pq := t.pqLoad[tt.Key()]
	t.mu.Unlock()

	if t.parquet && pq != nil {
		return t.loadParquet(pq, rows)
	}

	if lf == nil {
		return fmt.Errorf("table %s was not prepared", tt.Key())
	}

	stamp := ""
	if t.opts.TimestampColumnName != "" {
		stamp = t.now().UTC().Format("2006-01-02 15:04:05.000000")
	}

	for _, r := range rows {
		if lf.buf.Len() == 0 {
			lf.buf.WriteString(t.header(tt, t.opts.IncludeOpForFullLoad))
		}

		op := ""
		if t.opts.IncludeOpForFullLoad {
			op = "I"
		}

		lf.buf.WriteString(t.line(op, r, stamp))

		if lf.buf.Len() >= t.opts.maxBytes {
			if err := t.flushLoad(ctx, tt, lf); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *s3Target) flushLoad(ctx context.Context, tt TargetTable, lf *loadFile) error {
	if lf.buf.Len() == 0 {
		return nil
	}

	key := t.keyFor(tt, fmt.Sprintf("LOAD%08d.csv", lf.next))
	if err := t.store.PutObject(ctx, t.opts.BucketName, key, lf.buf.Bytes()); err != nil {
		return err
	}

	lf.next++
	lf.buf.Reset()

	return nil
}

func (t *s3Target) EndLoad(ctx context.Context, tt TargetTable) error {
	t.mu.Lock()
	lf := t.load[tt.Key()]
	pq := t.pqLoad[tt.Key()]
	t.mu.Unlock()

	if t.parquet {
		if pq == nil {
			return nil
		}

		return t.writeParquet(ctx, pq, "LOAD00000001.parquet")
	}

	if lf == nil {
		return nil
	}

	if lf.next == 1 && lf.buf.Len() == 0 {
		lf.buf.WriteString(t.header(tt, t.opts.IncludeOpForFullLoad))
	}

	return t.flushLoad(ctx, tt, lf)
}

func (t *s3Target) Apply(_ context.Context, tt TargetTable, c Change) error {
	if t.opts.CdcInsertsOnly && c.Op != OpInsert {
		return nil
	}

	if t.opts.CdcInsertsAndUpdates && c.Op == OpDelete {
		return nil
	}

	row, code := c.After, "I"

	switch c.Op {
	case OpUpdate:
		code = "U"
	case OpDelete:
		row, code = c.Before, "D"
	}

	if t.opts.CdcInsertsOnly {
		code = ""
	}

	stamp := ""
	if t.opts.TimestampColumnName != "" {
		stamp = c.Time.UTC().Format("2006-01-02 15:04:05.000000")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.parquet {
		return t.applyParquet(tt, c, row, code, stamp)
	}

	buf := t.cdc[tt.Key()]
	if buf == nil {
		buf = &bytes.Buffer{}
		t.cdc[tt.Key()] = buf
		buf.WriteString(t.header(tt, code != ""))
	}

	buf.WriteString(t.line(code, row, stamp))

	return nil
}

func (t *s3Target) tableOf(key string) TargetTable {
	schema, name, _ := strings.Cut(key, ".")

	return TargetTable{Schema: schema, Name: name}
}

func (t *s3Target) Commit(ctx context.Context) error {
	t.mu.Lock()
	pending := t.cdc
	t.cdc = map[string]*bytes.Buffer{}
	pendingPQ := t.pqCDC
	t.pqCDC = map[string]*pqBuf{}
	t.seq++
	seq := t.seq
	t.mu.Unlock()

	if t.parquet {
		name := t.now().UTC().Format("20060102-150405") + fmt.Sprintf("%03d", seq%1000) + ".parquet"

		for _, b := range pendingPQ {
			if err := t.writeParquet(ctx, b, name); err != nil {
				return err
			}
		}

		return nil
	}

	name := t.now().UTC().Format("20060102-150405") + fmt.Sprintf("%03d", seq%1000) + ".csv"

	for key, buf := range pending {
		if buf.Len() == 0 {
			continue
		}

		if err := t.store.PutObject(ctx, t.opts.BucketName, t.keyFor(t.tableOf(key), name), buf.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func (t *s3Target) loadParquet(pq *pqBuf, rows []Row) error {
	stamp := ""
	if t.opts.TimestampColumnName != "" {
		stamp = t.now().UTC().Format("2006-01-02 15:04:05.000000")
	}

	for _, r := range rows {
		if err := pq.add("I", stamp, r, t.opts.IncludeOpForFullLoad, t.opts.TimestampColumnName); err != nil {
			return err
		}
	}

	return nil
}

func (t *s3Target) applyParquet(tt TargetTable, _ Change, row Row, code, stamp string) error {
	withOp := code != ""

	b := t.pqCDC[tt.Key()]
	if b == nil {
		b = newPQBuf(tt, withOp, t.opts.TimestampColumnName)
		t.pqCDC[tt.Key()] = b
	}

	return b.add(code, stamp, row, withOp, t.opts.TimestampColumnName)
}
