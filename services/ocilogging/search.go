package ocilogging

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultSearchLimit = 1000
	maxSearchLimit     = 1000
	tenancyPlaceholder = "ocid1.tenancy.oc1..emulator"
	timeMillis         = "2006-01-02T15:04:05.000Z07:00"
)

// SearchIn is the SearchLogs request body.
type SearchIn struct {
	TimeStart       *time.Time `json:"timeStart"`
	TimeEnd         *time.Time `json:"timeEnd"`
	ReturnFieldInfo *bool      `json:"isReturnFieldInfo"`
	SearchQuery     string     `json:"searchQuery"`
}

type searchResultWire struct {
	Data any `json:"data"`
}

type fieldInfoWire struct {
	FieldName string `json:"fieldName"`
}

type searchSummaryWire struct {
	ResultCount int `json:"resultCount"`
	FieldCount  int `json:"fieldCount"`
}

// SearchOut is the SearchLogs response body.
type SearchOut struct {
	Results []searchResultWire `json:"results"`
	Fields  []fieldInfoWire    `json:"fields,omitempty"`
	Summary searchSummaryWire  `json:"summary"`
}

const maxScopeParts = 3

// resolveStreams maps query scopes to the ids of the logs they cover; the caller holds the lock.
func (b *InMemoryBackend) resolveStreams(streams []string) ([]string, *APIError) {
	var ids []string

	for _, s := range streams {
		parts := strings.Split(s, "/")
		if len(parts) > maxScopeParts || parts[0] == "" {
			return nil, errParam(`a scope must look like "<compartment>[/<logGroup>[/<log>]]"`)
		}

		found, aerr := b.scopeLogs(parts)
		if aerr != nil {
			return nil, aerr
		}

		ids = append(ids, found...)
	}

	return ids, nil
}

func logSelected(parts []string, l *Log) bool {
	return len(parts) < maxScopeParts || l.ID == parts[2] || l.Name == parts[2]
}

// scopeLogs returns the ids of the logs one scope (compartment, group, log) selects.
func (b *InMemoryBackend) scopeLogs(parts []string) ([]string, *APIError) {
	var groups []string

	for _, g := range b.groups {
		if g.CompartmentID == parts[0] && (len(parts) < 2 || g.ID == parts[1] || g.Name == parts[1]) {
			groups = append(groups, g.ID)
		}
	}

	if len(parts) > 1 && len(groups) == 0 {
		return nil, errParam("log group " + parts[1] + " was not found in compartment " + parts[0])
	}

	var ids []string

	for _, l := range b.logs {
		if l.State != stateDeleting && slices.Contains(groups, l.GroupID) && logSelected(parts, l) {
			ids = append(ids, l.ID)
		}
	}

	if len(parts) == maxScopeParts && len(ids) == 0 {
		return nil, errParam("log " + parts[2] + " was not found in log group " + parts[1])
	}

	return ids, nil
}

func (b *InMemoryBackend) buildRow(l *Log, e *Entry) *row {
	var data any = e.Data

	var parsed any
	if json.Unmarshal([]byte(e.Data), &parsed) == nil {
		if _, isMap := parsed.(map[string]any); isMap {
			data = parsed
		}
	}

	comp := ""
	if g, ok := b.groups[l.GroupID]; ok {
		comp = g.CompartmentID
	}

	ce := map[string]any{
		"data": data, "id": e.ID, "source": e.Source, "specversion": specVersion, "type": e.Type,
		"time": e.Time.UTC().Format(timeMillis), "subject": e.Subject,
		"oracle": map[string]any{
			"loggroupid": l.GroupID, "logid": l.ID, "compartmentid": comp, "tenantid": tenancyPlaceholder,
			"ingestedtime": e.Ingested.UTC().Format(timeMillis),
		},
	}

	return &row{ce: ce, raw: e.Data, ms: float64(e.Time.UnixMilli()), seq: e.Seq}
}

// SearchLogs runs a query over the ingested entries within the time range.
func (b *InMemoryBackend) SearchLogs(in SearchIn, token string, limit int) (SearchOut, string, error) {
	if limit < 0 || limit > maxSearchLimit {
		return SearchOut{}, "", errParam("limit must be between 1 and 1000")
	}

	if in.TimeStart == nil || in.TimeEnd == nil {
		return SearchOut{}, "", errMissing("timeStart and timeEnd are required")
	}

	if !in.TimeStart.Before(*in.TimeEnd) {
		return SearchOut{}, "", errParam("timeStart must be before timeEnd")
	}

	q, err := ParseQuery(in.SearchQuery)
	if err != nil {
		return SearchOut{}, "", err
	}

	b.mu.Lock("SearchLogs")
	defer b.mu.Unlock()

	b.settleAll()

	ids, aerr := b.resolveStreams(q.Streams)
	if aerr != nil {
		return SearchOut{}, "", aerr
	}

	var rows []*row

	for _, id := range slices.Compact(slices.Sorted(slices.Values(ids))) {
		for _, e := range b.entries[id] {
			if !e.Time.Before(*in.TimeStart) && e.Time.Before(*in.TimeEnd) {
				rows = append(rows, b.buildRow(b.logs[id], e))
			}
		}
	}

	slices.SortStableFunc(rows, func(a, c *row) int {
		return compareRows(c, a, []sortKey{{path: []string{fieldTime}}}) // newest first
	})

	rows, counted := q.apply(rows)

	return b.finishSearch(rows, counted, in, token, limit)
}

func (b *InMemoryBackend) finishSearch(
	rows []*row, counted *int, in SearchIn, token string, limit int,
) (SearchOut, string, error) {
	out := SearchOut{Results: []searchResultWire{}}

	fields := []string{fieldTime, fieldRaw, fieldRegion}

	if counted != nil {
		out.Results = append(out.Results, searchResultWire{Data: map[string]any{kindCount: *counted}})
		out.Summary = searchSummaryWire{ResultCount: 1, FieldCount: 1}
		fields = []string{kindCount}
	} else {
		pg := page.New(rows, token, limit, defaultSearchLimit)
		for _, r := range pg.Data {
			out.Results = append(out.Results, searchResultWire{Data: map[string]any{
				fieldTime: int64(r.ms), fieldRaw: r.ce, fieldRegion: Region,
			}})
		}

		out.Summary = searchSummaryWire{ResultCount: len(pg.Data), FieldCount: len(fields)}

		if in.ReturnFieldInfo != nil && *in.ReturnFieldInfo {
			out.Fields = fieldList(fields)
		}

		return out, pg.Next, nil
	}

	if in.ReturnFieldInfo != nil && *in.ReturnFieldInfo {
		out.Fields = fieldList(fields)
	}

	return out, "", nil
}

func fieldList(names []string) []fieldInfoWire {
	out := make([]fieldInfoWire, 0, len(names))
	for _, n := range names {
		out = append(out, fieldInfoWire{FieldName: n})
	}

	return out
}

func (h *Handler) searchLogs(q *req) error {
	var in SearchIn
	if err := q.decode(&in); err != nil {
		return err
	}

	limit := 0
	if raw := q.query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return errParam("limit must be an integer")
		}

		limit = n
	}

	out, next, err := h.Backend.SearchLogs(in, q.query("page"), limit)
	if err != nil {
		return err
	}

	q.nextPage(next)

	return q.json(http.StatusOK, out, "")
}
