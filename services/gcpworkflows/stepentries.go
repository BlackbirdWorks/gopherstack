package gcpworkflows

import (
	"cmp"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

const (
	stepEntriesSegment = "stepEntries"
	maxStepEntries     = 10_000
	maxStepVarsBytes   = 64 << 10
	maxStepVarsTotal   = 4 << 20
	maxStepPage        = 1000
	maxStepSkip        = maxStepEntries
	viewDetailed       = "EXECUTION_ENTRY_VIEW_DETAILED"
	stepEntryPathLen   = 10
)

// stepEntry is one executed step of an execution; its JSON form is the persisted one.
type stepEntry struct {
	Updated   time.Time      `json:"updated"`
	Created   time.Time      `json:"created"`
	State     string         `json:"state"`
	Exception string         `json:"exception,omitempty"`
	Routine   string         `json:"routine"`
	Step      string         `json:"step"`
	Type      string         `json:"type,omitempty"`
	Children  []int64        `json:"children,omitempty"`
	Vars      jsontext.Value `json:"vars,omitempty"`
	ID        int64          `json:"id"`
	Parent    int64          `json:"parent,omitempty"`
	Prev      int64          `json:"prev,omitempty"`
	Next      int64          `json:"next,omitempty"`
	Thread    int            `json:"thread,omitempty"`
}

// stepLog holds the entries of one execution under the backend lock.
type stepLog struct {
	byID     map[int64]*stepEntry
	order    []*stepEntry
	varBytes int
}

func newStepLog() *stepLog { return &stepLog{byID: map[int64]*stepEntry{}} }

func stepLogFrom(entries []stepEntry) *stepLog {
	l := newStepLog()

	for i := range entries {
		e := entries[i]
		l.byID[e.ID] = &e
		l.order = append(l.order, &e)
		l.varBytes += len(e.Vars)
	}

	return l
}

// recordStep applies one interpreter step event to the execution's step log.
func (b *InMemoryBackend) recordStep(execName string, ev wfinterp.StepEvent) {
	b.mu.Lock("recordStep")
	defer b.mu.Unlock()

	rec, ok := b.execs[execName]
	if !ok {
		return
	}

	if rec.steps == nil {
		rec.steps = newStepLog()
	}

	rec.steps.apply(ev)
}

// stepBudget reports whether the execution can still record entries and variable snapshots.
func (b *InMemoryBackend) stepBudget(execName string) (bool, bool) {
	b.mu.RLock("stepBudget")
	defer b.mu.RUnlock()

	rec, ok := b.execs[execName]
	if !ok {
		return false, false
	}

	if rec.steps == nil {
		return true, true
	}

	return len(rec.steps.order) < maxStepEntries, rec.steps.varBytes < maxStepVarsTotal
}

func (l *stepLog) apply(ev wfinterp.StepEvent) {
	if ev.State == wfinterp.StepInProgress {
		l.begin(ev)

		return
	}

	e, ok := l.byID[ev.ID]
	if !ok {
		return
	}

	e.State, e.Updated, e.Exception = ev.State, ev.Time, ev.Exception

	if ev.Vars == nil {
		return
	}

	raw, err := wfinterp.ToJSON(ev.Vars)
	if err != nil || len(raw) > maxStepVarsBytes || l.varBytes+len(raw) > maxStepVarsTotal {
		return
	}

	e.Vars = raw
	l.varBytes += len(raw)
}

func (l *stepLog) begin(ev wfinterp.StepEvent) {
	if len(l.order) >= maxStepEntries {
		return
	}

	e := &stepEntry{
		ID: ev.ID, Parent: ev.Parent, Prev: ev.Prev, Routine: ev.Routine, Step: ev.Step, Type: ev.Type,
		State: ev.State, Thread: ev.Thread, Created: ev.Time, Updated: ev.Time,
	}
	l.byID[e.ID] = e
	l.order = append(l.order, e)

	if p, ok := l.byID[ev.Prev]; ok && ev.Prev != 0 {
		p.Next = ev.ID
	}

	if p, ok := l.byID[ev.Parent]; ok && ev.Parent != 0 {
		p.Children = append(p.Children, ev.ID)
	}
}

// failOpenEntries ends entries still in progress after the runner that owned them is gone.
func (l *stepLog) failOpenEntries(at time.Time, payload string) {
	for _, e := range l.order {
		if e.State == wfinterp.StepInProgress {
			e.State, e.Updated, e.Exception = wfinterp.StepFailed, at, payload
		}
	}
}

func stepFilterField(field string) (fieldAccess[*stepEntry], bool) {
	str := func(get func(*stepEntry) string) fieldAccess[*stepEntry] {
		return fieldAccess[*stepEntry]{kind: kindString, get: func(e *stepEntry) []string { return one(get(e)) }}
	}

	num := func(get func(*stepEntry) int64) fieldAccess[*stepEntry] {
		return fieldAccess[*stepEntry]{kind: kindNumber, get: func(e *stepEntry) []string {
			if v := get(e); v != 0 {
				return []string{strconv.FormatInt(v, 10)}
			}

			return nil
		}}
	}

	ts := func(get func(*stepEntry) time.Time) fieldAccess[*stepEntry] {
		return fieldAccess[*stepEntry]{kind: kindTime, get: func(e *stepEntry) []string {
			return []string{get(e).UTC().Format(time.RFC3339Nano)}
		}}
	}

	switch field {
	case "entryId":
		return num(func(e *stepEntry) int64 { return e.ID }), true
	case "parent":
		return num(func(e *stepEntry) int64 { return e.Parent }), true
	case "routine":
		return str(func(e *stepEntry) string { return e.Routine }), true
	case keyStep:
		return str(func(e *stepEntry) string { return e.Step }), true
	case "stepType":
		return fieldAccess[*stepEntry]{kind: kindEnum, get: func(e *stepEntry) []string { return one(e.Type) }}, true
	case fieldState:
		return fieldAccess[*stepEntry]{
			kind: kindEnum,
			get: func(e *stepEntry) []string {
				return []string{e.State, strings.TrimPrefix(e.State, "STATE_")}
			},
		}, true
	case fieldCreateTime:
		return ts(func(e *stepEntry) time.Time { return e.Created }), true
	case fieldUpdateTime:
		return ts(func(e *stepEntry) time.Time { return e.Updated }), true
	default:
		return fieldAccess[*stepEntry]{}, false
	}
}

func orderSteps(all []*stepEntry, orderBy string) error {
	type term struct {
		cmp  func(a, b *stepEntry) int
		desc bool
	}

	var terms []term

	for part := range strings.SplitSeq(orderBy, ",") {
		f := strings.Fields(part)
		if len(f) == 0 {
			continue
		}

		if len(f) > 2 || (len(f) == 2 && f[1] != "asc" && f[1] != dirDescending) {
			return errInvalid("order_by %q is malformed.", orderBy)
		}

		c, ok := stepComparer(f[0])
		if !ok {
			return errInvalid("order_by field %q is not supported.", f[0])
		}

		terms = append(terms, term{cmp: c, desc: len(f) == 2 && f[1] == dirDescending})
	}

	slices.SortStableFunc(all, func(a, b *stepEntry) int {
		for _, t := range terms {
			c := t.cmp(a, b)
			if t.desc {
				c = -c
			}

			if c != 0 {
				return c
			}
		}

		return cmp.Compare(a.ID, b.ID)
	})

	return nil
}

func stepComparer(field string) (func(a, b *stepEntry) int, bool) {
	switch field {
	case "entryId":
		return func(a, b *stepEntry) int { return cmp.Compare(a.ID, b.ID) }, true
	case fieldCreateTime:
		return func(a, b *stepEntry) int { return a.Created.Compare(b.Created) }, true
	case fieldUpdateTime:
		return func(a, b *stepEntry) int { return a.Updated.Compare(b.Updated) }, true
	case "routine":
		return func(a, b *stepEntry) int { return strings.Compare(a.Routine, b.Routine) }, true
	case keyStep:
		return func(a, b *stepEntry) int { return strings.Compare(a.Step, b.Step) }, true
	case "stepType":
		return func(a, b *stepEntry) int { return strings.Compare(a.Type, b.Type) }, true
	case fieldState:
		return func(a, b *stepEntry) int { return strings.Compare(a.State, b.State) }, true
	default:
		return nil, false
	}
}

type stepListQuery struct {
	filter    string
	orderBy   string
	pageToken string
	view      string
	pageSize  int
	skip      int
}

type exceptionJSON struct {
	Payload string `json:"payload"`
}

type navigationJSON struct {
	Next     string   `json:"next,omitempty"`
	Parent   string   `json:"parent,omitempty"`
	Previous string   `json:"previous,omitempty"`
	Children []string `json:"children,omitempty"`
}

type stepMetadataJSON struct {
	ProgressType string `json:"progressType,omitempty"`
	ThreadID     string `json:"threadId,omitempty"`
}

type variableDataJSON struct {
	Variables jsontext.Value `json:"variables"`
}

type stepEntryJSON struct {
	Exception         *exceptionJSON    `json:"exception,omitempty"`
	NavigationInfo    *navigationJSON   `json:"navigationInfo,omitempty"`
	StepEntryMetadata *stepMetadataJSON `json:"stepEntryMetadata,omitempty"`
	VariableData      *variableDataJSON `json:"variableData,omitempty"`
	CreateTime        string            `json:"createTime"`
	EntryID           string            `json:"entryId"`
	Name              string            `json:"name"`
	Routine           string            `json:"routine"`
	State             string            `json:"state"`
	Step              string            `json:"step"`
	StepType          string            `json:"stepType,omitempty"`
	UpdateTime        string            `json:"updateTime"`
}

type listStepEntriesJSON struct {
	NextPageToken string           `json:"nextPageToken,omitempty"`
	StepEntries   []*stepEntryJSON `json:"stepEntries"`
	TotalSize     int              `json:"totalSize"`
}

func progressType(stepType string) string {
	switch stepType {
	case wfinterp.StepTypeFor:
		return "PROGRESS_TYPE_FOR"
	case wfinterp.StepTypeParallelFor:
		return "PROGRESS_TYPE_PARALLEL_FOR"
	case wfinterp.StepTypeParallel:
		return "PROGRESS_TYPE_PARALLEL_BRANCH"
	case wfinterp.StepTypeSwitch:
		return "PROGRESS_TYPE_SWITCH"
	default:
		return ""
	}
}

func (e *stepEntry) render(exec, view string) *stepEntryJSON {
	out := &stepEntryJSON{
		CreateTime: e.Created.UTC().Format(time.RFC3339Nano), UpdateTime: e.Updated.UTC().Format(time.RFC3339Nano),
		EntryID: strconv.FormatInt(e.ID, 10), Name: exec + "/" + stepEntriesSegment + "/" + strconv.FormatInt(e.ID, 10),
		Routine: e.Routine, Step: e.Step, State: e.State, StepType: e.Type,
	}

	if e.Exception != "" {
		out.Exception = &exceptionJSON{Payload: e.Exception}
	}

	if nav := e.navigation(); nav != nil {
		out.NavigationInfo = nav
	}

	meta := &stepMetadataJSON{ProgressType: progressType(e.Type)}
	if e.Thread != 0 {
		meta.ThreadID = strconv.Itoa(e.Thread)
	}

	if *meta != (stepMetadataJSON{}) {
		out.StepEntryMetadata = meta
	}

	if view == viewDetailed && len(e.Vars) > 0 {
		out.VariableData = &variableDataJSON{Variables: slices.Clone(e.Vars)}
	}

	return out
}

func (e *stepEntry) navigation() *navigationJSON {
	if e.Parent == 0 && e.Prev == 0 && e.Next == 0 && len(e.Children) == 0 {
		return nil
	}

	nav := &navigationJSON{}

	for _, c := range e.Children {
		nav.Children = append(nav.Children, strconv.FormatInt(c, 10))
	}

	if e.Next != 0 {
		nav.Next = strconv.FormatInt(e.Next, 10)
	}

	if e.Parent != 0 {
		nav.Parent = strconv.FormatInt(e.Parent, 10)
	}

	if e.Prev != 0 {
		nav.Previous = strconv.FormatInt(e.Prev, 10)
	}

	return nav
}

func validEntryView(view string) error {
	switch view {
	case "", "EXECUTION_ENTRY_VIEW_UNSPECIFIED", "EXECUTION_ENTRY_VIEW_BASIC", viewDetailed:
		return nil
	default:
		return errInvalid("view %q is not a valid EXECUTION_ENTRY_VIEW.", view)
	}
}

func (b *InMemoryBackend) stepsOf(execName string) ([]*stepEntry, error) {
	if err := validateExecutionName(execName); err != nil {
		return nil, err
	}

	b.mu.RLock("stepsOf")
	defer b.mu.RUnlock()

	rec, ok := b.execs[execName]
	if !ok {
		return nil, errNotFound("Execution %q not found.", execName)
	}

	if rec.steps == nil {
		return nil, nil
	}

	out := make([]*stepEntry, len(rec.steps.order))
	for i, e := range rec.steps.order {
		c := *e
		c.Children = slices.Clone(e.Children)
		out[i] = &c
	}

	return out, nil
}

// listStepEntries lists the step entries of an execution, ascending by entry id unless ordered otherwise.
func (b *InMemoryBackend) listStepEntries(execName string, q stepListQuery) (*listStepEntriesJSON, error) {
	if q.pageSize < 0 || q.skip < 0 {
		return nil, errInvalid("page_size and skip must not be negative.")
	}

	if err := validEntryView(q.view); err != nil {
		return nil, err
	}

	if q.pageToken != "" {
		if err := page.ValidateToken(q.pageToken); err != nil {
			return nil, errInvalid("Invalid page token.")
		}
	}

	pred, err := parseFilter(q.filter, stepFilterField)
	if err != nil {
		return nil, err
	}

	all, err := b.stepsOf(execName)
	if err != nil {
		return nil, err
	}

	all = slices.DeleteFunc(all, func(e *stepEntry) bool { return !pred(e) })

	if err = orderSteps(all, q.orderBy); err != nil {
		return nil, err
	}

	token := q.pageToken
	if q.skip > 0 {
		token = page.EncodeToken(page.DecodeToken(token) + min(q.skip, maxStepSkip))
	}

	pg := page.New(all, token, min(q.pageSize, maxStepPage), maxStepPage)

	out := make([]*stepEntryJSON, len(pg.Data))
	for i, e := range pg.Data {
		out[i] = e.render(execName, q.view)
	}

	return &listStepEntriesJSON{StepEntries: out, NextPageToken: pg.Next, TotalSize: len(all)}, nil
}

// getStepEntry returns one step entry of an execution.
func (b *InMemoryBackend) getStepEntry(name, view string) (*stepEntryJSON, error) {
	if err := validEntryView(view); err != nil {
		return nil, err
	}

	execName, id, ok := splitStepEntryName(name)
	if !ok {
		return nil, errInvalid("Invalid step entry name %q.", name)
	}

	all, err := b.stepsOf(execName)
	if err != nil {
		return nil, err
	}

	for _, e := range all {
		if e.ID == id {
			return e.render(execName, view), nil
		}
	}

	return nil, errNotFound("Step entry %q not found.", name)
}

func splitStepEntryName(name string) (string, int64, bool) {
	exec, last, ok := strings.CutLast(name, "/")
	if !ok {
		return "", 0, false
	}

	exec, seg, ok := strings.CutLast(exec, "/")
	if !ok || seg != stepEntriesSegment {
		return "", 0, false
	}

	id, err := strconv.ParseInt(last, 10, 64)
	if err != nil || id <= 0 {
		return "", 0, false
	}

	return exec, id, true
}

// stepEntriesTarget splits a /v1/... path into the execution and the optional entry id segment.
func stepEntriesTarget(path string) (string, string, bool) {
	rest, ok := strings.CutPrefix(path, apiPrefix)
	if !ok {
		return "", "", false
	}

	p := strings.Split(rest, "/")
	if len(p) != stepEntryPathLen-1 && len(p) != stepEntryPathLen {
		return "", "", false
	}

	if p[0] != "projects" || p[2] != "locations" || p[4] != kindWorkflows || p[6] != kindExecutions ||
		p[8] != stepEntriesSegment {
		return "", "", false
	}

	exec := strings.Join(p[:8], "/")
	if len(p) == stepEntryPathLen {
		return exec, p[9], true
	}

	return exec, "", true
}

// IsStepEntriesPath reports whether path addresses the stepEntries collection or one entry.
func IsStepEntriesPath(path string) bool {
	_, _, ok := stepEntriesTarget(path)

	return ok
}

func httpStatusOf(err error) (int, string, string) {
	st, _ := status.FromError(err)

	switch st.Code() {
	case codes.InvalidArgument:
		return http.StatusBadRequest, "INVALID_ARGUMENT", st.Message()
	case codes.NotFound:
		return http.StatusNotFound, "NOT_FOUND", st.Message()
	default:
		return http.StatusInternalServerError, "INTERNAL", st.Message()
	}
}

// StepEntriesHandler serves the REST-only executions.stepEntries list and get methods.
func (b *InMemoryBackend) StepEntriesHandler() http.Handler {
	return http.HandlerFunc(b.serveStepEntries)
}

func (b *InMemoryBackend) serveStepEntries(w http.ResponseWriter, r *http.Request) {
	execName, id, _ := stepEntriesTarget(r.URL.Path)

	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only GET is supported.")

		return
	}

	q := r.URL.Query()

	var (
		out any
		err error
	)

	if id != "" {
		out, err = b.getStepEntry(execName+"/"+stepEntriesSegment+"/"+id, q.Get("view"))
	} else {
		var lq stepListQuery

		lq, err = parseStepListQuery(q.Get)
		if err == nil {
			out, err = b.listStepEntries(execName, lq)
		}
	}

	if err != nil {
		code, st, msg := httpStatusOf(err)
		writeJSONError(w, code, st, msg)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.MarshalWrite(w, out, jsonx.V1())
}

func parseStepListQuery(get func(string) string) (stepListQuery, error) {
	q := stepListQuery{
		filter: get("filter"), orderBy: get("orderBy"), pageToken: get("pageToken"), view: get("view"),
	}

	for key, dst := range map[string]*int{"pageSize": &q.pageSize, "skip": &q.skip} {
		v := get(key)
		if v == "" {
			continue
		}

		n, err := strconv.Atoi(v)
		if err != nil {
			return q, errInvalid("%s must be an integer.", key)
		}

		*dst = n
	}

	return q, nil
}

func detailedHistory(wf *workflowspb.Workflow) bool {
	return wf.GetExecutionHistoryLevel() == workflowspb.ExecutionHistoryLevel_EXECUTION_HISTORY_DETAILED
}
