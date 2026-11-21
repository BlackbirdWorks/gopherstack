package gcpbigquery

import (
	"context"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	stateDone   = "DONE"
	maxJobs     = 2000
	userEmail   = "gopherstack@localhost"
	jobQuery    = "query"
	jobLoad     = "load"
	jobCopy     = "copy"
	jobExtract  = "extract"
	queryPageSz = 100000
	maxIDLen    = 1024

	maxRequestIDLen = 36
	requestIDWindow = 15 * time.Minute
)

func jobIDPattern() *regexp.Regexp { return regexp.MustCompile(`^[A-Za-z0-9_-]+$`) }

// outcome is what a job produced.
type outcome struct {
	Err      *jobError
	Warnings []*jobError
	Dest     *tableRef
	Stats    map[string]any
	Extra    map[string]any
	Fields   []bqsql.Field
	Rows     [][]any
	NumDML   int64
	IsQuery  bool
}

func jobKind(cfg map[string]any) string {
	for _, k := range []string{jobQuery, jobLoad, jobCopy, jobExtract} {
		if _, ok := cfg[k].(map[string]any); ok {
			return k
		}
	}

	return ""
}

func jobKey(project, id string) string { return project + "/" + id }

func (b *InMemoryBackend) newJob(project string, ref map[string]string, kind string, cfg map[string]any) (*Job, error) {
	id := ref[litJobID]
	if id == "" {
		id = awsid.NewN("job_", 24) //nolint:mnd // job id length
	}

	if len(id) > maxIDLen || !jobIDPattern().MatchString(id) {
		return nil, errInvalid(
			"Invalid job ID %q. Job IDs must be alphanumeric (plus underscores and dashes) and at most 1024 characters long.",
			id,
		)
	}

	loc := ref[litLocation]
	if loc == "" {
		loc = defaultLocation
	}

	if _, ok := b.jobs[jobKey(project, id)]; ok {
		return nil, errDuplicate("Already Exists: Job %s:%s.%s", project, loc, id)
	}

	cfg = cloneMap(cfg)
	cfg["jobType"] = strings.ToUpper(kind)
	now := nowMs()

	return &Job{
		Config: cfg, Project: project, ID: id, Location: loc, Type: strings.ToUpper(kind), State: statePending,
		User: userEmail, Created: now, Started: now,
	}, nil
}

// InsertJob implements jobs.insert; media carries the body of a multipart or resumable upload for load jobs.
func (b *InMemoryBackend) InsertJob(
	ctx context.Context,
	project string,
	body map[string]any,
	media []byte,
) (map[string]any, error) {
	out, err := b.insertJob(ctx, project, body, media)
	if err == nil {
		id, _ := out["id"].(string)
		_, jobID, _ := strings.Cut(id, ".")
		b.auditCall("google.cloud.bigquery.v2.JobService.InsertJob", "projects/"+project+"/jobs/"+jobID, project)
	}

	return out, err
}

func (b *InMemoryBackend) insertJob(
	ctx context.Context,
	project string,
	body map[string]any,
	media []byte,
) (map[string]any, error) {
	cfg, _ := body["configuration"].(map[string]any)

	kind := jobKind(cfg)
	if kind == "" {
		return nil, errInvalid(
			"Required parameter is missing: configuration must contain a query, load, copy or extract job",
		)
	}

	b.mu.Lock("InsertJob")
	defer b.mu.Unlock()

	job, err := b.newJob(project, refIDs(body, litJobRef), kind, cfg)
	if err != nil {
		return nil, err
	}

	job.DryRun, _ = cfg["dryRun"].(bool)

	if job.DryRun {
		out, derr := b.execute(ctx, job, kind, cfg, true)
		if derr != nil {
			return nil, derr
		}

		b.finishJob(job, kind, out)

		return jobWire(job), nil
	}

	if _, err = b.execute(ctx, job, kind, cfg, true); err != nil {
		return nil, err
	}

	if err = b.startJob(ctx, job, kind, cfg, media); err != nil {
		return nil, err
	}

	return jobWire(job), nil
}

// execute analyses (dryRun) or runs a job; the caller holds the write lock. Load and extract jobs only validate
// here: their Cloud Storage traffic runs outside the lock.
func (b *InMemoryBackend) execute(
	ctx context.Context,
	job *Job,
	kind string,
	cfg map[string]any,
	dryRun bool,
) (*outcome, error) {
	switch kind {
	case jobQuery:
		qc, err := decodeQueryConfig(cfg[jobQuery])
		if err != nil {
			return nil, err
		}

		return b.execQuery(ctx, job.Project, job.ID, qc, dryRun)
	case jobLoad:
		return b.execLoad(objectOf(cfg, jobLoad))
	case jobCopy:
		return b.execCopy(job.Project, objectOf(cfg, jobCopy), dryRun)
	}

	return b.execExtract(objectOf(cfg, jobExtract))
}

func (b *InMemoryBackend) finishJob(job *Job, kind string, out *outcome) {
	job.State = stateDone
	job.Ended = nowMs()
	job.ErrorResult = out.Err
	job.Errors = out.Warnings
	job.Dest = out.Dest
	job.NumDML = out.NumDML
	job.HasResult = out.IsQuery
	job.Schema, job.Rows = out.Fields, out.Rows

	stats := map[string]any{
		litCreationtime: msString(job.Created), "startTime": msString(job.Started), "endTime": msString(job.Ended),
		litTotalbytesprocessed: "0",
	}

	if out.Stats != nil {
		stats[kind] = out.Stats
	}

	maps.Copy(stats, out.Extra)

	job.Stats = stats
}

func (b *InMemoryBackend) storeJob(job *Job) {
	b.jobs[jobKey(job.Project, job.ID)] = job

	if len(b.jobs) <= maxJobs {
		return
	}

	oldest := ""

	for k, j := range b.jobs {
		if j.State != stateDone && j.State != "" {
			continue
		}

		if oldest == "" || j.Created < b.jobs[oldest].Created {
			oldest = k
		}
	}

	if oldest != "" {
		delete(b.jobs, oldest)
	}
}

func (b *InMemoryBackend) jobLocked(project, id string) (*Job, error) {
	j, ok := b.jobs[jobKey(project, id)]
	if !ok {
		return nil, errNotFound("Not found: Job %s:%s", project, id)
	}

	return j, nil
}

// GetJob implements jobs.get.
func (b *InMemoryBackend) GetJob(project, id string) (map[string]any, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	j, err := b.jobLocked(project, id)
	if err != nil {
		return nil, err
	}

	return jobWire(j), nil
}

// CancelJob implements jobs.cancel: a PENDING or RUNNING job is asked to stop and finishes with errorResult
// stopped; a finished job is returned as is.
func (b *InMemoryBackend) CancelJob(project, id string) (map[string]any, error) {
	b.mu.RLock("CancelJob")

	j, err := b.jobLocked(project, id)
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	wire := jobWire(j)
	b.mu.RUnlock()

	if run, ok := b.runs.Get(jobKey(project, id)); ok {
		run.cancelled.Store(true)
		run.cancel()
	}

	return map[string]any{litKind: "bigquery#jobCancelResponse", "job": wire}, nil
}

// DeleteJob implements jobs.delete.
func (b *InMemoryBackend) DeleteJob(project, id string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	j, err := b.jobLocked(project, id)
	if err != nil {
		return err
	}

	if j.State != stateDone {
		return errInvalid("Job %s:%s is not done and cannot be deleted", project, id)
	}

	delete(b.jobs, jobKey(project, id))

	return nil
}

// JobListOptions are the jobs.list parameters.
type JobListOptions struct {
	PageToken   string
	Parent      string
	StateFilter []string
	MinCreated  int64
	MaxCreated  int64
	Max         int
}

// ListJobs implements jobs.list, newest first.
func (b *InMemoryBackend) ListJobs(project string, o JobListOptions) page.Page[map[string]any] {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	var jobs []*Job

	for _, j := range b.jobs {
		parent, _ := j.Stats[litParentJobID].(string)

		if j.Project != project || !stateWanted(o.StateFilter, j.State) || j.Created < o.MinCreated ||
			o.MaxCreated > 0 && j.Created > o.MaxCreated || parent != o.Parent {
			continue
		}

		jobs = append(jobs, j)
	}

	sort.Slice(jobs, func(i, k int) bool {
		if jobs[i].Created != jobs[k].Created {
			return jobs[i].Created > jobs[k].Created
		}

		return jobs[i].ID < jobs[k].ID
	})

	all := make([]map[string]any, len(jobs))
	for i, j := range jobs {
		w := jobWire(j)
		w["state"] = j.State
		all[i] = w
	}

	return page.New(all, o.PageToken, o.Max, defaultListMax)
}

// stateWanted reports whether state passes the stateFilter list (empty means all states).
func stateWanted(filter []string, state string) bool {
	if len(filter) == 0 {
		return true
	}

	for _, s := range filter {
		if strings.EqualFold(s, state) {
			return true
		}
	}

	return false
}

func jobWire(j *Job) map[string]any {
	status := map[string]any{"state": j.State}

	switch {
	case j.ErrorResult != nil:
		status["errorResult"] = j.ErrorResult
		status["errors"] = append(append([]*jobError{}, j.Errors...), j.ErrorResult)
	case len(j.Errors) > 0:
		status["errors"] = j.Errors
	}

	stats := cloneMap(j.Stats)
	if q, ok := stats["query"].(map[string]any); ok && j.HasResult {
		q["schema"] = schemaWire(j.Schema)
	}

	cfg := cloneMap(j.Config)
	if j.Dest != nil {
		if sub, ok := cfg[strings.ToLower(j.Type)].(map[string]any); ok {
			sub["destinationTable"] = j.Dest
		}
	}

	return map[string]any{
		litKind: "bigquery#job", litEtag: etagOf("job", j.Project, j.ID),
		"id":         j.Project + ":" + j.Location + "." + j.ID,
		litSelflink:  selfLinkBase + "projects/" + j.Project + "/jobs/" + j.ID + "?location=" + j.Location,
		"user_email": j.User, "principal_subject": "user:" + j.User,
		litJobRef:       map[string]any{litProjectid: j.Project, litJobID: j.ID, litLocation: j.Location},
		"configuration": cfg, "status": status, "statistics": stats,
	}
}

// QueryResultOptions are the getQueryResults parameters.
type QueryResultOptions struct {
	TimeoutMs      *int
	PageToken      string
	StartIndex     int
	Max            int
	Int64Timestamp bool
}

// GetQueryResults implements jobs.getQueryResults; it waits up to timeoutMs (default 10 s) for a job that is not
// done and then answers jobComplete=false.
func (b *InMemoryBackend) GetQueryResults(project, id string, o QueryResultOptions) (map[string]any, error) {
	b.mu.RLock("GetQueryResults")

	j, err := b.jobLocked(project, id)
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	if j.Type != "QUERY" {
		b.mu.RUnlock()

		return nil, errInvalid("Job %s:%s is not a query job", project, id)
	}

	run, _ := b.runs.Get(jobKey(project, id))
	b.mu.RUnlock()

	b.waitFor(waitTimeout(o.TimeoutMs), run)

	b.mu.RLock("GetQueryResults")
	defer b.mu.RUnlock()

	if j, err = b.jobLocked(project, id); err != nil {
		return nil, err
	}

	if j.State != stateDone && j.State != "" {
		return incompleteWire(j, "bigquery#getQueryResultsResponse"), nil
	}

	if j.ErrorResult != nil {
		return nil, jobFailure(j.ErrorResult)
	}

	return queryResultsWire(
		j,
		DataOptions{PageToken: o.PageToken, StartIndex: o.StartIndex, Max: o.Max, Int64Timestamp: o.Int64Timestamp},
		"bigquery#getQueryResultsResponse",
	), nil
}

func incompleteWire(j *Job, kind string) map[string]any {
	return map[string]any{
		litKind: kind, "jobComplete": false,
		litJobRef: map[string]any{litProjectid: j.Project, litJobID: j.ID, litLocation: j.Location},
	}
}

func queryResultsWire(j *Job, o DataOptions, kind string) map[string]any {
	out := map[string]any{
		litKind: kind, "jobComplete": true, "cacheHit": false, litTotalbytesprocessed: "0",
		litJobRef: map[string]any{litProjectid: j.Project, litJobID: j.ID, litLocation: j.Location},
	}

	if j.HasResult {
		out["schema"] = schemaWire(j.Schema)

		pg := dataPage(j.Schema, j.Rows, o, etagOf("qr", j.Project, j.ID))
		out["totalRows"], out["rows"] = pg["totalRows"], pg["rows"]

		if t, ok := pg["pageToken"]; ok {
			out["pageToken"] = t
		}

		out[litEtag] = pg[litEtag]
	} else {
		out["totalRows"] = "0"
		out[litEtag] = etagOf("qr", j.Project, j.ID)
		out["numDmlAffectedRows"] = strconv.FormatInt(j.NumDML, 10)
	}

	return out
}

// RunQuery implements jobs.query: it creates a query job, runs it and returns the first page of results.
func (b *InMemoryBackend) RunQuery(ctx context.Context, project string, req map[string]any) (map[string]any, error) {
	qcfg := map[string]any{}

	for _, k := range []string{"query", litUselegacysql, "queryParameters", "parameterMode", "defaultDataset"} {
		if v, ok := req[k]; ok {
			qcfg[k] = v
		}
	}

	if _, ok := qcfg[litUselegacysql]; !ok {
		qcfg[litUselegacysql] = false
	}

	cfg := map[string]any{jobQuery: qcfg}
	if dr, ok := req["dryRun"].(bool); ok {
		cfg["dryRun"] = dr
	}

	reqID, _ := req["requestId"].(string)
	if len(reqID) > maxRequestIDLen {
		return nil, errInvalid("requestId must be at most %d characters long.", maxRequestIDLen)
	}

	run, job, resp, err := b.prepareQuery(ctx, project, req, cfg, reqID)
	if err != nil || resp != nil {
		return resp, err
	}

	var timeout *int
	if _, ok := req["timeoutMs"]; ok {
		ms, _ := intOf(req["timeoutMs"])
		timeout = &ms
	}

	b.waitFor(waitTimeout(timeout), run)

	b.mu.RLock("RunQuery")
	defer b.mu.RUnlock()

	if job.State != stateDone && job.State != "" {
		return incompleteWire(job, "bigquery#queryResponse"), nil
	}

	if job.ErrorResult != nil {
		return nil, jobFailure(job.ErrorResult)
	}

	return queryResponse(job, req), nil
}

// prepareQuery creates and starts the job of a jobs.query call; a replayed requestId or a dry run answers at once.
func (b *InMemoryBackend) prepareQuery(
	ctx context.Context, project string, req, cfg map[string]any, reqID string,
) (*jobRun, *Job, map[string]any, error) {
	b.mu.Lock("RunQuery")
	defer b.mu.Unlock()

	if prev := b.jobByRequestID(project, reqID); prev != nil {
		return nil, nil, queryResponse(prev, req), nil
	}

	loc, _ := req[litLocation].(string)

	job, err := b.newJob(project, map[string]string{litLocation: loc}, jobQuery, cfg)
	if err != nil {
		return nil, nil, nil, err
	}

	job.DryRun, _ = req["dryRun"].(bool)
	job.RequestID = reqID

	out, err := b.execute(ctx, job, jobQuery, cfg, true)
	if err != nil {
		return nil, nil, nil, err
	}

	if job.DryRun {
		b.finishJob(job, jobQuery, out)

		return nil, nil, queryResponse(job, req), nil
	}

	if err = b.startJob(ctx, job, jobQuery, cfg, nil); err != nil {
		return nil, nil, nil, err
	}

	run, _ := b.runs.Get(jobKey(project, job.ID))

	return run, job, nil, nil
}

// jobByRequestID returns the query job a requestId created within the idempotency window.
func (b *InMemoryBackend) jobByRequestID(project, reqID string) *Job {
	if reqID == "" {
		return nil
	}

	cutoff := nowMs() - requestIDWindow.Milliseconds()

	for _, j := range b.jobs {
		if j.Project == project && j.RequestID == reqID && j.Created >= cutoff {
			return j
		}
	}

	return nil
}

func queryResponse(job *Job, req map[string]any) map[string]any {
	limit := queryPageSz
	if m, ok := intOf(req["maxResults"]); ok && m > 0 {
		limit = m
	}

	fo, _ := req["formatOptions"].(map[string]any)
	useInt, _ := fo["useInt64Timestamp"].(bool)
	resp := queryResultsWire(job, DataOptions{Max: limit, Int64Timestamp: useInt}, "bigquery#queryResponse")
	if job.DryRun {
		delete(resp, litJobRef)
	}

	return resp
}

// jobFailure maps a failed job's error to the HTTP error getQueryResults and jobs.query return.
func jobFailure(e *jobError) *APIError {
	status := http.StatusBadRequest

	switch e.Reason {
	case litDuplicate:
		status = http.StatusConflict
	case litNotfound:
		status = http.StatusNotFound
	}

	return newErr(status, e.Reason, e.Message)
}

func objectOf(m map[string]any, key string) map[string]any {
	o, _ := m[key].(map[string]any)

	return o
}
