package neptune

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	rebuildsPath       = "/storage/indexes/rebuilds"
	rebuildPreparing   = "PREPARING"
	rebuildRebuilding  = "REBUILDING"
	rebuildCompleted   = "COMPLETED"
	rebuildCancelled   = "CANCELLED"
	rebuildCancelling  = "CANCELLING"
	rebuildHalfway     = 50
	scopeAll           = "ALL"
	scopeQuads         = "QUADS"
	scopeDictionaries  = "DICTIONARIES"
	bufferLow          = "LOW"
	bufferMedium       = "MEDIUM"
	bufferHigh         = "HIGH"
	rebuildTimeLayout  = "2006-01-02T15:04:05.000Z"
	codeNotFound       = "ResourceNotFoundException"
	actionStartRebuild = "neptune-db:StartIndexRebuild"
	actionCancelRebld  = "neptune-db:CancelIndexRebuild"
	actionRebuildState = "neptune-db:GetIndexRebuildStatus"
	actionListRebuilds = "neptune-db:ListIndexRebuilds"
)

type rebuildJob struct {
	Start       time.Time `json:"startTime"`
	End         time.Time `json:"endTime"`
	ID          string    `json:"id"`
	Status      string    `json:"status"`
	Scope       string    `json:"scope"`
	Buffer      string    `json:"writeBufferSize"`
	ClientToken string    `json:"clientToken,omitempty"`
	Concurrency int       `json:"concurrency"`
	cancel      bool
}

type rebuildState struct {
	mu    *lockmetrics.RWMutex
	jobs  map[string]*rebuildJob
	gate  chan struct{}
	order []string
}

func newRebuildState() *rebuildState {
	return &rebuildState{mu: lockmetrics.New("neptune-index-rebuild"), jobs: map[string]*rebuildJob{}}
}

func (s *rebuildState) export() []byte {
	s.mu.RLock("rebuildExport")
	defer s.mu.RUnlock()

	if len(s.order) == 0 {
		return nil
	}

	out := make([]rebuildJob, 0, len(s.order))

	for _, id := range s.order {
		if j := s.jobs[id]; j.Status == rebuildCompleted || j.Status == rebuildCancelled {
			out = append(out, *j)
		}
	}

	if len(out) == 0 {
		return nil
	}

	b, err := json.Marshal(out, jsonx.V1())
	if err != nil {
		return nil
	}

	return b
}

func (s *rebuildState) restore(data []byte) {
	var jobs []rebuildJob
	if err := json.Unmarshal(data, &jobs, jsonx.V1()); err != nil {
		return
	}

	s.mu.Lock("rebuildRestore")
	defer s.mu.Unlock()

	for i := range jobs {
		j := jobs[i]
		s.jobs[j.ID] = &j
		s.order = append(s.order, j.ID)
	}
}

type rebuildRequest struct {
	ClientToken string `json:"clientToken"`
	Scope       string `json:"scope"`
	Buffer      string `json:"writeBufferSize"`
	Concurrency int    `json:"concurrency"`
}

func (f *cypherFront) serveRebuilds(w http.ResponseWriter, r *http.Request, path string) {
	rest := strings.Trim(strings.TrimPrefix(path, rebuildsPath), "/")
	id, tail, _ := strings.Cut(rest, "/")

	switch {
	case rest == "" && r.Method == http.MethodPost:
		f.startRebuild(w, r)
	case rest == "" && r.Method == http.MethodGet:
		f.listRebuilds(w, r)
	case tail == "" && r.Method == http.MethodGet:
		f.rebuildStatus(w, r, id)
	case tail == "cancel" && r.Method == http.MethodPost:
		f.cancelRebuild(w, r, id)
	default:
		writeCypherError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "Method Not Allowed.")
	}
}

func parseRebuildRequest(r *http.Request) (rebuildRequest, string) {
	req := rebuildRequest{Scope: scopeAll, Buffer: bufferLow, Concurrency: 1}
	q := r.URL.Query()

	if body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, cypherMaxBody)); err == nil && len(body) > 0 {
		var in struct {
			ClientToken string `json:"clientToken"`
		}

		if json.Unmarshal(body, &in, jsonx.V1()) == nil {
			req.ClientToken = in.ClientToken
		}
	}

	if v := q.Get("scope"); v != "" {
		req.Scope = v
	}

	if v := q.Get("writeBufferSize"); v != "" {
		req.Buffer = v
	}

	if v := q.Get("concurrency"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return req, "concurrency must be a positive integer"
		}

		req.Concurrency = n
	}

	switch req.Scope {
	case scopeAll, scopeQuads, scopeDictionaries:
	default:
		return req, "scope must be one of ALL, QUADS, DICTIONARIES"
	}

	switch req.Buffer {
	case bufferLow, bufferMedium, bufferHigh:
	default:
		return req, "writeBufferSize must be one of LOW, MEDIUM, HIGH"
	}

	return req, ""
}

func (f *cypherFront) startRebuild(w http.ResponseWriter, r *http.Request) {
	if !authorizeActions(w, r, langSparql, actionStartRebuild) {
		return
	}

	req, bad := parseRebuildRequest(r)
	if bad != "" {
		writeCypherError(w, http.StatusBadRequest, codeInvalidParam, bad)

		return
	}

	st := f.rdf.rebuilds
	st.mu.Lock("startRebuild")
	defer st.mu.Unlock()

	if req.ClientToken != "" {
		for _, j := range st.jobs {
			if j.ClientToken == req.ClientToken {
				writeCypherJSON(w, http.StatusOK, rebuildBody(j, false))

				return
			}
		}
	}

	job := &rebuildJob{
		ID: uuid.NewString(), Status: rebuildPreparing, Scope: req.Scope, Buffer: req.Buffer,
		ClientToken: req.ClientToken, Concurrency: min(req.Concurrency, f.rdf.cpuCount()), Start: time.Now().UTC(),
	}
	st.jobs[job.ID] = job
	st.order = append(st.order, job.ID)

	body := rebuildBody(job, false)
	gate := st.gate

	f.rdf.loader.wg.Go(func() { f.runRebuild(job, gate) })

	writeCypherJSON(w, http.StatusOK, body)
}

func (f *cypherFront) runRebuild(job *rebuildJob, gate <-chan struct{}) {
	if gate != nil {
		select {
		case <-gate:
		case <-f.ctx.Done():
			return
		}
	}

	st := f.rdf.rebuilds

	step := func(status string, work func()) bool {
		st.mu.Lock("rebuildStep")
		if job.cancel {
			job.Status, job.End = rebuildCancelled, time.Now().UTC()
			st.mu.Unlock()

			return false
		}

		job.Status = status
		st.mu.Unlock()

		if work != nil {
			work()
		}

		return true
	}

	if !step(rebuildRebuilding, nil) {
		return
	}

	if job.Scope != scopeDictionaries {
		if !step(rebuildRebuilding, f.rdf.store.RebuildIndexes) || !step(rebuildRebuilding, f.graph.RebuildIndexes) {
			return
		}
	}

	st.mu.Lock("rebuildDone")
	defer st.mu.Unlock()

	if job.cancel {
		job.Status, job.End = rebuildCancelled, time.Now().UTC()

		return
	}

	job.Status, job.End = rebuildCompleted, time.Now().UTC()
}

func rebuildBody(j *rebuildJob, full bool) map[string]any {
	body := map[string]any{
		keyStatus: j.Status, "scope": j.Scope, "concurrency": j.Concurrency, "writeBufferSize": j.Buffer,
	}

	if !full {
		body["rebuildId"] = j.ID

		return body
	}

	body["startTime"] = j.Start.UTC().Format(rebuildTimeLayout)

	switch j.Status {
	case rebuildCompleted:
		body["progressPercentage"] = 100
		body["endTime"] = j.End.UTC().Format(rebuildTimeLayout)
	case rebuildCancelled:
		body["endTime"] = j.End.UTC().Format(rebuildTimeLayout)
	case rebuildRebuilding:
		body["progressPercentage"] = rebuildHalfway
	}

	return body
}

func (f *cypherFront) listRebuilds(w http.ResponseWriter, r *http.Request) {
	if !authorizeActions(w, r, langSparql, actionListRebuilds) {
		return
	}

	st := f.rdf.rebuilds
	st.mu.RLock("listRebuilds")
	defer st.mu.RUnlock()

	ids := append([]string{}, st.order...)
	writeCypherJSON(w, http.StatusOK, map[string]any{"rebuildIds": ids})
}

func (f *cypherFront) rebuildStatus(w http.ResponseWriter, r *http.Request, id string) {
	if !authorizeActions(w, r, langSparql, actionRebuildState) {
		return
	}

	st := f.rdf.rebuilds
	st.mu.RLock("rebuildStatus")
	defer st.mu.RUnlock()

	j, ok := st.jobs[id]
	if !ok {
		writeCypherError(w, http.StatusNotFound, codeNotFound, "Index rebuild '"+id+"' not found")

		return
	}

	writeCypherJSON(w, http.StatusOK, rebuildBody(j, true))
}

func (f *cypherFront) cancelRebuild(w http.ResponseWriter, r *http.Request, id string) {
	if !authorizeActions(w, r, langSparql, actionCancelRebld) {
		return
	}

	st := f.rdf.rebuilds
	st.mu.Lock("cancelRebuild")
	defer st.mu.Unlock()

	j, ok := st.jobs[id]
	if !ok {
		writeCypherError(w, http.StatusNotFound, codeNotFound, "Index rebuild '"+id+"' not found")

		return
	}

	if j.Status != rebuildPreparing && j.Status != rebuildRebuilding {
		writeCypherError(w, http.StatusBadRequest, codeInvalidParam,
			"Index rebuild '"+id+"' is "+j.Status+" and can no longer be cancelled")

		return
	}

	j.cancel, j.Status = true, rebuildCancelling

	writeCypherJSON(w, http.StatusOK, map[string]any{
		keyStatus: j.Status, "concurrency": j.Concurrency, "writeBufferSize": j.Buffer,
	})
}

func (b *InMemoryBackend) exportRebuildsLocked() map[string]string {
	out := map[string]string{}

	for key, r := range b.rdf {
		if data := r.rebuilds.export(); data != nil {
			out[key] = string(data)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (b *InMemoryBackend) importRebuildsLocked(dumps map[string]string) {
	for key, data := range dumps {
		b.rdfLocked(key).rebuilds.restore([]byte(data))
	}
}
