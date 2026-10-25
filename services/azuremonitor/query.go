package azuremonitor

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/azuremonitor/kql"
)

// QueryRequest is the body of a query call.
type QueryRequest struct {
	Query      string   `json:"query"`
	Timespan   string   `json:"timespan"`
	Workspaces []string `json:"workspaces"`
}

type resultColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type resultTable struct {
	Name    string         `json:"name"`
	Columns []resultColumn `json:"columns"`
	Rows    [][]any        `json:"rows"`
}

// QueryResponse is the documented tables/columns/rows result.
type QueryResponse struct {
	Statistics map[string]any `json:"statistics,omitempty"`
	Tables     []resultTable  `json:"tables"`
}

var isoDurationRe = regexp.MustCompile(
	`^P(?:(\d+(?:\.\d+)?)Y)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)W)?(?:(\d+(?:\.\d+)?)D)?` +
		`(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

var isoUnits = []time.Duration{ //nolint:gochecknoglobals // static lookup
	365 * 24 * time.Hour, 30 * 24 * time.Hour, 7 * 24 * time.Hour, 24 * time.Hour, time.Hour, time.Minute, time.Second,
}

func parseISODuration(s string) (time.Duration, bool) {
	m := isoDurationRe.FindStringSubmatch(strings.ToUpper(s))
	if m == nil || s == "P" || strings.HasSuffix(strings.ToUpper(s), "T") {
		return 0, false
	}

	var d time.Duration

	for i, u := range isoUnits {
		if m[i+1] == "" {
			continue
		}

		f, err := strconv.ParseFloat(m[i+1], 64)
		if err != nil {
			return 0, false
		}

		d += time.Duration(f * float64(u))
	}

	return d, true
}

// parseTimespanParam resolves the API timespan parameter into a [from, to] window; zero values are unbounded.
func parseTimespanParam(s string, now time.Time) (time.Time, time.Time, bool) {
	if s == "" {
		return time.Time{}, time.Time{}, true
	}

	left, right, interval := strings.Cut(s, "/")
	if !interval {
		d, ok := parseISODuration(left)

		return now.Add(-d), now, ok
	}

	parse := func(part string) (time.Time, time.Duration, bool, bool) {
		if t, err := time.Parse(time.RFC3339Nano, part); err == nil {
			return t.UTC(), 0, true, true
		}

		d, ok := parseISODuration(part)

		return time.Time{}, d, false, ok
	}

	lt, ld, lIsTime, lok := parse(left)
	rt, rd, rIsTime, rok := parse(right)

	switch {
	case !lok || !rok:
		return time.Time{}, time.Time{}, false
	case lIsTime && rIsTime:
		return lt, rt, true
	case lIsTime:
		return lt, lt.Add(rd), true
	case rIsTime:
		return rt.Add(-ld), rt, true
	}

	return time.Time{}, time.Time{}, false
}

type queryResolver struct {
	b    *Backend
	home *Workspace
	// resource, when set, evaluates in a resource context: tables exist but hold no rows for it.
	resource string
	from, to time.Time
}

func (r queryResolver) Table(ws, name string) (*kql.Rel, bool) {
	if r.resource != "" {
		return r.resourceTable(name)
	}

	w := r.home

	if ws != "" {
		found, ok := r.b.findWorkspaceLocked(ws)
		if !ok {
			return nil, false
		}

		w = found
	}

	t, ok := w.table(name)
	if !ok {
		return nil, false
	}

	return t.relOf(w.CustomerID, r.from, r.to), true
}

func (r queryResolver) resourceTable(name string) (*kql.Rel, bool) {
	for _, k := range sortedKeys(r.b.workspaces) {
		w := r.b.workspaces[k]
		if t, ok := w.table(name); ok {
			rel := (&Table{Name: t.Name, Columns: t.Columns}).relOf(w.CustomerID, time.Time{}, time.Time{})

			return rel, true
		}
	}

	return nil, false
}

func kqlAPIError(err error) *apiError {
	var ke *kql.Error
	if !errors.As(err, &ke) {
		ke = &kql.Error{Kind: "SemanticError", Code: "SEM0100", Message: err.Error()}
	}

	msg := "A semantic error occurred."
	if ke.Kind == "SyntaxError" {
		msg = "Syntax error"
	}

	return &apiError{
		status: http.StatusBadRequest, code: "BadArgumentError", message: "The request had some invalid properties",
		inner: &innerError{Code: ke.Kind, Message: msg, Inner: &innerError{Code: ke.Code, Message: ke.Message}},
	}
}

func workspaceNotFound(id string) *apiError {
	return newErr(http.StatusNotFound, "WorkspaceNotFoundError", "The workspace '%s' could not be found.", id)
}

// QueryWorkspace runs a query against one workspace.
func (b *Backend) QueryWorkspace(idOrName string, req QueryRequest, withStats bool) (*QueryResponse, *apiError) {
	b.mu.RLock("QueryWorkspace")
	defer b.mu.RUnlock()

	w, ok := b.findWorkspaceLocked(idOrName)
	if !ok {
		return nil, workspaceNotFound(idOrName)
	}

	return b.run(queryResolver{b: b, home: w}, req, withStats)
}

// QueryResource runs a resource-centric query.
func (b *Backend) QueryResource(resourceID string, req QueryRequest, withStats bool) (*QueryResponse, *apiError) {
	b.mu.RLock("QueryResource")
	defer b.mu.RUnlock()

	if w, ok := b.findWorkspaceLocked(resourceID); ok {
		return b.run(queryResolver{b: b, home: w}, req, withStats)
	}

	return b.run(queryResolver{b: b, resource: resourceID}, req, withStats)
}

func (b *Backend) run(res queryResolver, req QueryRequest, withStats bool) (*QueryResponse, *apiError) {
	if strings.TrimSpace(req.Query) == "" {
		return nil, newErr(http.StatusBadRequest, "BadArgumentError", "The 'query' property must be set.")
	}

	now := b.now()

	from, to, ok := parseTimespanParam(req.Timespan, now)
	if !ok {
		return nil, newErr(http.StatusBadRequest, "BadArgumentError", "The 'timespan' value '%s' is not valid.", req.Timespan)
	}

	res.from, res.to = from, to
	start := time.Now()

	rel, err := kql.Execute(req.Query, kql.Options{Now: now, Resolver: res})
	if err != nil {
		return nil, kqlAPIError(err)
	}

	out := resultTable{
		Name: "PrimaryResult", Columns: make([]resultColumn, len(rel.Cols)), Rows: make([][]any, len(rel.Rows)),
	}

	for i, c := range rel.Cols {
		out.Columns[i] = resultColumn{Name: c.Name, Type: string(c.Type)}
	}

	for i, row := range rel.Rows {
		cells := make([]any, len(row))
		for j, v := range row {
			cells[j] = kql.ResultValue(rel.Cols[j].Type, v)
		}

		out.Rows[i] = cells
	}

	resp := &QueryResponse{Tables: []resultTable{out}}

	if withStats {
		resp.Statistics = map[string]any{"query": map[string]any{
			"executionTime": time.Since(start).Seconds(),
		}}
	}

	return resp, nil
}
