package neptune

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

const cypherStatusPath = "/opencypher/status"

var errQueryCancelled = errors.New("query cancelled by user")

type runningQuery struct {
	started time.Time
	cancel  context.CancelCauseFunc
	id      string
	query   string
}

// queryTracker lists in-flight openCypher queries for the neptunedata status and cancel operations.
type queryTracker struct {
	running *safemap.Map[string, *runningQuery]
}

func newQueryTracker() *queryTracker {
	return &queryTracker{running: safemap.New[string, *runningQuery]("neptune-opencypher")}
}

// begin registers a query and returns its cancellable, time-limited context and the finish callback.
func (t *queryTracker) begin(parent context.Context, query string) (context.Context, func()) {
	ctx, cancelCause := context.WithCancelCause(parent)
	ctx, cancelTimeout := context.WithTimeout(ctx, cypherQueryTimeout)

	q := &runningQuery{id: uuid.NewString(), query: query, started: time.Now(), cancel: cancelCause}
	t.running.Set(q.id, q)

	return ctx, func() {
		t.running.Delete(q.id)
		cancelTimeout()
		cancelCause(nil)
	}
}

type queryEvalStats struct {
	Waited    int64 `json:"waited"`
	Elapsed   int64 `json:"elapsed"`
	Cancelled bool  `json:"cancelled"`
}

type queryStatus struct {
	QueryID     string         `json:"queryId"`
	QueryString string         `json:"queryString"`
	EvalStats   queryEvalStats `json:"queryEvalStats"`
}

func (q *runningQuery) status() queryStatus {
	return queryStatus{
		QueryID: q.id, QueryString: q.query,
		EvalStats: queryEvalStats{Elapsed: time.Since(q.started).Milliseconds()},
	}
}

// serveQueryStatus answers the neptunedata Get/List/CancelOpenCypherQuery routes; false when r is not one.
func (f *cypherFront) serveQueryStatus(w http.ResponseWriter, r *http.Request) bool {
	path := strings.TrimRight(strings.ToLower(r.URL.Path), "/")
	if path != cypherStatusPath && !strings.HasPrefix(path, cypherStatusPath+"/") {
		return false
	}

	id := strings.TrimPrefix(r.URL.Path[len(cypherStatusPath):], "/")
	id = strings.TrimRight(id, "/")

	switch {
	case id == "" && r.Method == http.MethodGet:
		f.listQueries(w)
	case id != "" && r.Method == http.MethodGet:
		f.getQuery(w, id)
	case id != "" && r.Method == http.MethodDelete:
		f.cancelQuery(w, id)
	default:
		w.Header().Set("Allow", "GET, DELETE")
		writeCypherError(w, http.StatusMethodNotAllowed, "BadRequestException", "Unsupported method for this path")
	}

	return true
}

func (f *cypherFront) listQueries(w http.ResponseWriter) {
	qs := f.queries.running.Values()
	out := make([]queryStatus, 0, len(qs))

	for _, q := range qs {
		out = append(out, q.status())
	}

	writeCypherJSON(w, http.StatusOK, map[string]any{
		"acceptedQueryCount": len(out), "runningQueryCount": len(out), "queries": out,
	})
}

func (f *cypherFront) getQuery(w http.ResponseWriter, id string) {
	q, ok := f.queries.running.Get(id)
	if !ok {
		writeCypherError(w, http.StatusBadRequest, opencypher.CodeInvalidParameter,
			"Query "+strconv.Quote(id)+" does not exist or has already finished")

		return
	}

	writeCypherJSON(w, http.StatusOK, q.status())
}

func (f *cypherFront) cancelQuery(w http.ResponseWriter, id string) {
	q, ok := f.queries.running.Get(id)
	if !ok {
		writeCypherError(w, http.StatusBadRequest, opencypher.CodeInvalidParameter,
			"Query "+strconv.Quote(id)+" does not exist or has already finished")

		return
	}

	q.cancel(errQueryCancelled)
	writeCypherJSON(w, http.StatusOK, map[string]any{"status": "200 OK", "payload": true})
}
