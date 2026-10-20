package gcpbigquery

import (
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	insertIDWindow   = time.Minute
	defaultDataPage  = 100000
	insertErrReason  = litInvalid
	insertErrStopped = "stopped"
)

// InsertRow is one row of a streaming insert.
type InsertRow struct {
	JSON     map[string]any `json:"json"`
	InsertID string         `json:"insertId"`
}

// InsertRequest is the tabledata.insertAll request body.
type InsertRequest struct {
	Rows                []InsertRow `json:"rows"`
	SkipInvalidRows     bool        `json:"skipInvalidRows"`
	IgnoreUnknownValues bool        `json:"ignoreUnknownValues"`
}

func insertError(index int, reason, location, msg string) map[string]any {
	return map[string]any{"index": index, "errors": []map[string]any{{
		"reason": reason, litLocation: location, "message": msg, "debugInfo": "",
	}}}
}

// InsertAll implements tabledata.insertAll.
func (b *InMemoryBackend) InsertAll(project, ds, id string, req InsertRequest) (map[string]any, error) {
	b.mu.Lock("InsertAll")
	defer b.mu.Unlock()

	d, t, err := b.tableLocked(project, ds, id)
	if err != nil {
		return nil, err
	}

	_ = d

	if t.ViewQuery != "" {
		return nil, errInvalid("Cannot insert into view %s:%s.%s", project, ds, id)
	}

	if len(req.Rows) == 0 {
		return nil, errInvalid("No rows present in the request.")
	}

	opts := bqsql.ParseOptions{IgnoreUnknown: req.IgnoreUnknownValues}
	parsed := make([][]any, len(req.Rows))

	var errs []map[string]any

	bad := map[int]bool{}

	for i, r := range req.Rows {
		row, perr := bqsql.ParseRecord(t.Schema, r.JSON, opts)
		if perr != nil {
			errs = append(errs, insertError(i, insertErrReason, "", perr.Error()))
			bad[i] = true

			continue
		}

		parsed[i] = row
	}

	if len(errs) > 0 && !req.SkipInvalidRows {
		return insertResponse(stopRest(errs, bad, parsed)), nil
	}

	t.prune(time.Now())
	t.appendRows(req.Rows, parsed)
	t.Modified = nowMs()
	t.Seq = b.nextSeq()

	return insertResponse(errs), nil
}

func stopRest(errs []map[string]any, bad map[int]bool, parsed [][]any) []map[string]any {
	for i, row := range parsed {
		if row != nil && !bad[i] {
			errs = append(errs, insertError(i, insertErrStopped, "", ""))
		}
	}

	return errs
}

func insertResponse(errs []map[string]any) map[string]any {
	out := map[string]any{litKind: "bigquery#tableDataInsertAllResponse"}
	if len(errs) > 0 {
		out["insertErrors"] = errs
	}

	return out
}

func (t *Table) prune(now time.Time) {
	for k, at := range t.Seen {
		if now.Sub(time.UnixMilli(at)) > insertIDWindow {
			delete(t.Seen, k)
		}
	}
}

func (t *Table) appendRows(in []InsertRow, parsed [][]any) {
	if t.Seen == nil {
		t.Seen = map[string]int64{}
	}

	now := nowMs()

	for i, row := range parsed {
		if row == nil {
			continue
		}

		if id := in[i].InsertID; id != "" {
			if _, dup := t.Seen[id]; dup {
				continue
			}

			t.Seen[id] = now
		}

		t.Rows = append(t.Rows, row)
	}
}

// DataOptions are the tabledata.list parameters.
type DataOptions struct {
	PageToken      string
	SelectedFields string
	StartIndex     int
	Max            int
	Int64Timestamp bool
}

type fieldSel struct {
	sub map[string]*fieldSel
}

func parseSelection(spec string) *fieldSel {
	root := &fieldSel{sub: map[string]*fieldSel{}}

	for path := range strings.SplitSeq(spec, ",") {
		cur := root

		for p := range strings.SplitSeq(strings.TrimSpace(path), ".") {
			key := strings.ToLower(p)

			next, ok := cur.sub[key]
			if !ok {
				next = &fieldSel{sub: map[string]*fieldSel{}}
				cur.sub[key] = next
			}

			cur = next
		}
	}

	return root
}

// project returns the selected schema and, for each kept field, its source index.
func (s *fieldSel) project(fields []bqsql.Field) []bqsql.Field {
	var out []bqsql.Field

	for _, f := range fields {
		sub, ok := s.sub[strings.ToLower(f.Name)]
		if !ok {
			continue
		}

		if len(sub.sub) > 0 && len(f.Fields) > 0 {
			f.Fields = sub.project(f.Fields)
		}

		out = append(out, f)
	}

	return out
}

func (s *fieldSel) projectRow(fields []bqsql.Field, row []any) []any {
	var out []any

	for i, f := range fields {
		sub, ok := s.sub[strings.ToLower(f.Name)]
		if !ok {
			continue
		}

		v := row[i]
		if len(sub.sub) > 0 && len(f.Fields) > 0 {
			v = sub.projectValue(f, v)
		}

		out = append(out, v)
	}

	return out
}

func (s *fieldSel) projectValue(f bqsql.Field, v any) any {
	if f.Mode == bqsql.ModeRepeated {
		arr, _ := v.([]any)
		out := make([]any, len(arr))

		for i, e := range arr {
			rec, _ := e.([]any)
			out[i] = sub(s, f, rec)
		}

		return out
	}

	rec, ok := v.([]any)
	if !ok {
		return v
	}

	return sub(s, f, rec)
}

func sub(s *fieldSel, f bqsql.Field, rec []any) []any {
	return s.projectRow(f.Fields, rec)
}

// ListData implements tabledata.list.
func (b *InMemoryBackend) ListData(project, ds, id string, o DataOptions) (map[string]any, error) {
	b.mu.RLock("ListData")
	defer b.mu.RUnlock()

	_, t, err := b.tableLocked(project, ds, id)
	if err != nil {
		return nil, err
	}

	if t.ViewQuery != "" {
		return nil, errInvalid("Cannot list a table of type VIEW.")
	}

	schema, rows := t.Schema, t.Rows

	if o.SelectedFields != "" {
		sel := parseSelection(o.SelectedFields)
		schema = sel.project(t.Schema)

		projected := make([][]any, len(rows))
		for i, r := range rows {
			projected[i] = sel.projectRow(t.Schema, r)
		}

		rows = projected
	}

	return dataPage(
		schema,
		rows,
		o,
		etagOf("data", project, ds, id, strconv.FormatInt(t.Seq, 10)),
	), nil
}

// dataPage renders one page of rows; the continuation token carries the absolute row offset.
func dataPage(schema []bqsql.Field, rows [][]any, o DataOptions, etag string) map[string]any {
	start := o.StartIndex
	if o.PageToken != "" {
		start = page.DecodeToken(o.PageToken)
	}

	start = min(max(start, 0), len(rows))
	limit := o.Max

	if limit <= 0 {
		limit = defaultDataPage
	}

	end := min(start+limit, len(rows))

	cells := make([]map[string]any, 0, end-start)
	for _, r := range rows[start:end] {
		cells = append(cells, bqsql.RowFor(schema, r, bqsql.CellOptions{Int64Timestamp: o.Int64Timestamp}))
	}

	out := map[string]any{
		litKind: "bigquery#tableDataList", litEtag: etag, "totalRows": strconv.Itoa(len(rows)), "rows": cells,
	}

	if end < len(rows) {
		out["pageToken"] = page.EncodeToken(end)
	}

	return out
}
