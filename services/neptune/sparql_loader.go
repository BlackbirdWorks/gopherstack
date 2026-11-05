package neptune

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

const (
	loaderPath        = "/loader"
	loaderQueueMax    = 64
	loaderListMax     = 100
	loaderKeepJobs    = 1024
	loaderErrorsKept  = 10000
	loaderErrPerPage  = 10
	loadNotStarted    = "LOAD_NOT_STARTED"
	loadInQueue       = "LOAD_IN_QUEUE"
	loadInProgress    = "LOAD_IN_PROGRESS"
	loadCompleted     = "LOAD_COMPLETED"
	loadFailed        = "LOAD_FAILED"
	loadCancelledUser = "LOAD_CANCELLED_BY_USER"
	loadDepFailed     = "LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED"
	loadS3ReadError   = "LOAD_S3_READ_ERROR"
	actionStartLoad   = "neptune-db:StartLoaderJob"
	actionGetLoad     = "neptune-db:GetLoaderJobStatus"
	actionListLoad    = "neptune-db:ListLoaderJobs"
	actionCancelLoad  = "neptune-db:CancelLoaderJob"
	formTrueUpper     = "TRUE"
)

type loadFeed struct {
	URI       string `json:"fullUri"`
	Status    string `json:"status"`
	Run       int    `json:"runNumber"`
	Retry     int    `json:"retryNumber"`
	Spent     int64  `json:"totalTimeSpent"`
	Start     int64  `json:"startTime"`
	Records   int    `json:"totalRecords"`
	Dups      int    `json:"totalDuplicates"`
	Parsing   int    `json:"parsingErrors"`
	Datatype  int    `json:"datatypeMismatchErrors"`
	Insert    int    `json:"insertErrors"`
	LoadError string `json:"-"`
}

type loadError struct {
	Code    string `json:"errorCode"`
	Message string `json:"errorMessage"`
	File    string `json:"fileName"`
	Record  int    `json:"recordNum"`
}

type loadJob struct {
	cancel     context.CancelFunc
	id         string
	source     string
	bucket     string
	status     string
	graph      string
	base       string
	format     sparql.Format
	startTime  time.Time
	endTime    time.Time
	deps       []string
	keys       []string
	feeds      []*loadFeed
	errors     []loadError
	failOnErr  bool
	queued     bool
	finished   bool
	cancelUser bool
}

type loaderState struct {
	mu      *lockmetrics.RWMutex
	jobs    map[string]*loadJob
	order   []string
	running string
	queue   []string
	wg      sync.WaitGroup
}

func newLoaderState() *loaderState {
	return &loaderState{mu: lockmetrics.New("neptune-loader"), jobs: map[string]*loadJob{}}
}

type loadRequest struct {
	Source      string `json:"source"`
	Format      string `json:"format"`
	IAMRoleArn  string `json:"iamRoleArn"`
	Mode        string `json:"mode"`
	Region      string `json:"region"`
	FailOnError string `json:"failOnError"`
	Queue       string `json:"queueRequest"`
	Parser      struct {
		NamedGraph string `json:"namedGraphUri"`
		BaseURI    string `json:"baseUri"`
	} `json:"parserConfiguration"`
	Deps []string `json:"dependencies"`
}

func loaderFormat(s string) (sparql.Format, bool) {
	switch strings.ToLower(s) {
	case "ntriples":
		return sparql.FormatNTriples, true
	case "nquads":
		return sparql.FormatNQuads, true
	case "turtle":
		return sparql.FormatTurtle, true
	case "rdfxml":
		return sparql.FormatRDFXML, true
	default:
		return 0, false
	}
}

func envelope(payload any) map[string]any {
	return map[string]any{"status": "200 OK", "payload": payload}
}

func (f *cypherFront) serveLoader(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimRight(r.URL.Path, "/")
	id := strings.TrimPrefix(strings.TrimPrefix(path[len(loaderPath):], "/"), "/")

	if id == "" {
		id = r.URL.Query().Get("loadId")
	}

	switch {
	case r.Method == http.MethodPost && id == "":
		f.startLoad(w, r)
	case r.Method == http.MethodGet && id == "":
		f.listLoads(w, r)
	case r.Method == http.MethodGet:
		f.getLoad(w, r, id)
	case r.Method == http.MethodDelete && id != "":
		f.cancelLoad(w, r, id)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		writeCypherError(w, http.StatusMethodNotAllowed, "BadRequestException", "Unsupported method for this path")
	}
}

func readLoadRequest(w http.ResponseWriter, r *http.Request) (loadRequest, error) {
	var req loadRequest

	r.Body = http.MaxBytesReader(w, r.Body, cypherMaxBody)

	dec := json.NewDecoder(r.Body)
	// the loader documents boolean-ish flags as strings but clients also send JSON booleans
	var raw map[string]json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return req, err
	}

	for k, v := range raw {
		if string(v) == "true" || string(v) == "false" {
			raw[k] = json.RawMessage(strconv.Quote(strings.ToUpper(string(v))))
		}
	}

	buf, _ := json.Marshal(raw)

	return req, json.Unmarshal(buf, &req)
}

func (f *cypherFront) startLoad(w http.ResponseWriter, r *http.Request) {
	if !authorizeActions(w, r, langSparql, actionStartLoad) {
		return
	}

	req, err := readLoadRequest(w, r)
	if err != nil {
		writeCypherError(w, http.StatusBadRequest, "BadRequestException", "Invalid loader request: "+err.Error())

		return
	}

	job, apiErr := f.rdf.newLoadJob(r.Context(), req)
	if apiErr != "" {
		code := "BadRequestException"
		if apiErr == codeMissingParam {
			code = codeMissingParam
		}

		writeCypherError(w, http.StatusBadRequest, code, apiErr)

		return
	}

	if msg := f.rdf.loader.admit(job, strings.EqualFold(req.Queue, formTrueUpper)); msg != "" {
		writeCypherError(w, http.StatusBadRequest, "BadRequestException", msg)

		return
	}

	f.rdf.loader.dispatch(f.ctx, f.rdf)
	writeCypherJSON(w, http.StatusOK, envelope(map[string]string{"loadId": job.id}))
}

// newLoadJob validates req and lists the feeds; the returned string is an error detail.
func (rs *rdfService) newLoadJob(ctx context.Context, req loadRequest) (*loadJob, string) {
	if req.Source == "" {
		return nil, codeMissingParam
	}

	format, ok := loaderFormat(req.Format)
	if !ok {
		if slices.Contains([]string{"csv", "opencypher"}, strings.ToLower(req.Format)) {
			return nil, "Loader format " + strconv.Quote(req.Format) +
				" (property-graph CSV) is not supported by this emulator; use ntriples, nquads, rdfxml or turtle"
		}

		return nil, "Invalid format " + strconv.Quote(req.Format) + "; allowed values are csv, opencypher, ntriples, nquads, rdfxml, turtle"
	}

	bucket, prefix, err := parseS3Source(req.Source)
	if err != nil {
		return nil, err.Error()
	}

	keys, err := rs.listFeeds(ctx, bucket, prefix)
	if err != nil {
		return nil, "Failed to start new load for the source " + strconv.Quote(req.Source) + ": " + err.Error()
	}

	job := &loadJob{
		id: uuid.NewString(), source: req.Source, bucket: bucket, format: format, keys: keys,
		status: loadNotStarted, failOnErr: !strings.EqualFold(req.FailOnError, "FALSE"),
		graph: req.Parser.NamedGraph, base: req.Parser.BaseURI, deps: req.Deps,
	}

	for _, k := range keys {
		job.feeds = append(job.feeds, &loadFeed{URI: "s3://" + bucket + "/" + k, Status: loadNotStarted, Run: 1})
	}

	return job, ""
}

func (rs *rdfService) listFeeds(ctx context.Context, bucket, prefix string) ([]string, error) {
	var s3 S3Reader
	if rs.s3 != nil {
		s3 = rs.s3()
	}

	if s3 == nil {
		return nil, errNoS3
	}

	var keys []string

	var token *string

	for {
		out, err := s3.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, o := range out.Contents {
			if k := aws.ToString(o.Key); !strings.HasSuffix(k, "/") {
				keys = append(keys, k)
			}
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			break
		}

		token = out.NextContinuationToken
	}

	if len(keys) == 0 {
		return nil, errors.New("no files found at the source")
	}

	slices.Sort(keys)

	return keys, nil
}

// admit records the job and either marks it runnable or queues it; the returned string is a rejection reason.
func (ls *loaderState) admit(job *loadJob, queueOK bool) string {
	ls.mu.Lock("admit")
	defer ls.mu.Unlock()

	switch {
	case ls.running == "" && len(ls.queue) == 0:
		job.status = loadNotStarted
	case !queueOK:
		return "Failed to start new load for the source " + strconv.Quote(job.source) +
			". Max concurrent load limit breached. Limit is 1"
	case len(ls.queue) >= loaderQueueMax:
		return "Failed to queue the load request: the load queue is full (limit " + strconv.Itoa(loaderQueueMax) + ")"
	default:
		job.status, job.queued = loadInQueue, true
	}

	ls.jobs[job.id] = job
	ls.order = append(ls.order, job.id)
	ls.queue = append(ls.queue, job.id)

	for len(ls.order) > loaderKeepJobs {
		delete(ls.jobs, ls.order[0])
		ls.order = ls.order[1:]
	}

	return ""
}

// dispatch starts the next queued job when nothing is running.
func (ls *loaderState) dispatch(ctx context.Context, rs *rdfService) {
	ls.mu.Lock("dispatch")
	defer ls.mu.Unlock()

	for ls.running == "" && len(ls.queue) > 0 {
		id := ls.queue[0]
		ls.queue = ls.queue[1:]

		job := ls.jobs[id]
		if job == nil || job.finished {
			continue
		}

		if msg := ls.dependencyFailure(job); msg {
			job.status, job.finished, job.endTime = loadDepFailed, true, time.Now()

			continue
		}

		jctx, cancel := context.WithCancel(ctx)
		job.cancel = cancel
		job.status, job.queued, job.startTime = loadInProgress, false, time.Now()
		ls.running = id

		ls.wg.Go(func() {
			rs.runLoad(jctx, job)
			cancel()
			ls.finish(ctx, rs, id)
		})
	}
}

func (ls *loaderState) dependencyFailure(job *loadJob) bool {
	for _, d := range job.deps {
		if dep := ls.jobs[d]; dep == nil || dep.status != loadCompleted {
			return true
		}
	}

	return false
}

func (ls *loaderState) finish(ctx context.Context, rs *rdfService, id string) {
	ls.mu.Lock("finish")
	if ls.running == id {
		ls.running = ""
	}
	ls.mu.Unlock()

	ls.dispatch(ctx, rs)
}

func (rs *rdfService) runLoad(ctx context.Context, job *loadJob) {
	status := loadCompleted

	for i, key := range job.keys {
		feed := job.feeds[i]
		st := rs.loadFeed(ctx, job, feed, key)

		if st != loadCompleted {
			status = loadFailed

			if st == loadCancelledUser || st == loadS3ReadError {
				status = st
			}

			if job.failOnErr {
				break
			}
		}
	}

	rs.loader.mu.Lock("runLoad")
	defer rs.loader.mu.Unlock()

	if job.cancelUser {
		status = loadCancelledUser
	}

	job.status, job.finished, job.endTime = status, true, time.Now()
}

type feedResult struct {
	status  string
	errs    []loadError
	records int
	dups    int
	parsing int
}

func (rs *rdfService) loadFeed(ctx context.Context, job *loadJob, feed *loadFeed, key string) string {
	ls := rs.loader
	start := time.Now()

	ls.mu.Lock("feedStart")
	feed.Start, feed.Status = start.Unix(), loadInProgress
	ls.mu.Unlock()

	res := rs.readFeed(ctx, job, feed.URI, key)

	ls.mu.Lock("feedDone")
	defer ls.mu.Unlock()

	feed.Status, feed.Records, feed.Dups, feed.Parsing = res.status, res.records, res.dups, res.parsing
	feed.Spent = int64(time.Since(start).Seconds())
	job.errors = append(job.errors, res.errs[:min(len(res.errs), loaderErrorsKept-len(job.errors))]...)

	return res.status
}

func (rs *rdfService) readFeed(ctx context.Context, job *loadJob, uri, key string) feedResult {
	if ctx.Err() != nil {
		return feedResult{status: loadCancelledUser}
	}

	data, err := rs.readObject(ctx, job.bucket, key)
	if err != nil {
		return feedResult{
			status: loadS3ReadError,
			errs:   []loadError{{Code: "S3_ACCESS_DENIED_ERROR", Message: err.Error(), File: uri}},
		}
	}

	opts := sparql.ParseOptions{Base: job.base}
	if job.graph != "" {
		opts.Graph = sparql.IRI(job.graph)
	}

	quads, perrs := sparql.ParseRDFLenient(job.format, data, opts, !job.failOnErr)
	added := rs.store.Add(quads)

	res := feedResult{
		status: loadCompleted, records: len(quads) + len(perrs), parsing: len(perrs), dups: len(quads) - added,
	}

	for _, pe := range perrs {
		res.errs = append(res.errs, loadError{Code: "PARSING_ERROR", Message: pe.Msg, File: uri, Record: pe.Record})
	}

	if len(perrs) > 0 && job.failOnErr {
		res.status = loadFailed
	}

	return res
}

func (ls *loaderState) snapshotJob(id string) (*loadJob, bool) {
	job, ok := ls.jobs[id]
	if !ok {
		return nil, false
	}

	cp := *job
	cp.feeds = make([]*loadFeed, len(job.feeds))

	for i, f := range job.feeds {
		fc := *f
		cp.feeds[i] = &fc
	}

	cp.errors = slices.Clone(job.errors)

	return &cp, true
}

func (f *cypherFront) getLoad(w http.ResponseWriter, r *http.Request, id string) {
	if !authorizeActions(w, r, langSparql, actionGetLoad) {
		return
	}

	ls := f.rdf.loader
	ls.mu.RLock("getLoad")
	job, ok := ls.snapshotJob(id)
	ls.mu.RUnlock()

	if !ok {
		writeCypherError(w, http.StatusBadRequest, "InvalidParameterException",
			"Load "+strconv.Quote(id)+" does not exist")

		return
	}

	q := r.URL.Query()
	payload := map[string]any{"feedCount": feedCount(job), "overallStatus": overallStatus(job)}

	if strings.EqualFold(q.Get("details"), formTrueUpper) || strings.EqualFold(q.Get("errors"), formTrueUpper) {
		if failed := failedFeeds(job); len(failed) > 0 {
			payload["failedFeeds"] = failed
		}
	}

	if strings.EqualFold(q.Get("errors"), formTrueUpper) {
		payload["errors"] = errorPage(job, q.Get("page"), q.Get("errorsPerPage"))
	}

	writeCypherJSON(w, http.StatusOK, envelope(payload))
}

func feedCount(job *loadJob) []map[string]int {
	counts := map[string]int{}

	var order []string

	for _, f := range job.feeds {
		if counts[f.Status] == 0 {
			order = append(order, f.Status)
		}

		counts[f.Status]++
	}

	out := make([]map[string]int, 0, len(order))
	for _, s := range order {
		out = append(out, map[string]int{s: counts[s]})
	}

	return out
}

func overallStatus(job *loadJob) loadFeed {
	o := loadFeed{URI: job.source, Status: job.status, Run: 1, Start: job.startTime.Unix()}
	if job.startTime.IsZero() {
		o.Start = 0
	}

	if !job.endTime.IsZero() {
		o.Spent = int64(job.endTime.Sub(job.startTime).Seconds())
	}

	for _, f := range job.feeds {
		o.Records += f.Records
		o.Dups += f.Dups
		o.Parsing += f.Parsing
		o.Datatype += f.Datatype
		o.Insert += f.Insert
	}

	return o
}

func failedFeeds(job *loadJob) []*loadFeed {
	var out []*loadFeed

	for _, f := range job.feeds {
		if f.Status != loadCompleted && f.Status != loadNotStarted && f.Status != loadInProgress {
			out = append(out, f)
		}
	}

	return out
}

func errorPage(job *loadJob, page, per string) map[string]any {
	pageN, _ := strconv.Atoi(page)
	perN, _ := strconv.Atoi(per)

	if pageN < 1 {
		pageN = 1
	}

	if perN < 1 {
		perN = loaderErrPerPage
	}

	from := min((pageN-1)*perN, len(job.errors))
	to := min(from+perN, len(job.errors))
	logs := job.errors[from:to]

	if logs == nil {
		logs = []loadError{}
	}

	return map[string]any{"startIndex": from + 1, "endIndex": to, "loadId": job.id, "errorLogs": logs}
}

func (f *cypherFront) listLoads(w http.ResponseWriter, r *http.Request) {
	if !authorizeActions(w, r, langSparql, actionListLoad) {
		return
	}

	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit < 1 || limit > loaderListMax {
		limit = loaderListMax
	}

	includeQueued := !strings.EqualFold(r.URL.Query().Get("includeQueuedLoads"), "FALSE")

	ls := f.rdf.loader
	ls.mu.RLock("listLoads")
	defer ls.mu.RUnlock()

	var queued, others []string

	for i := len(ls.order) - 1; i >= 0; i-- {
		job := ls.jobs[ls.order[i]]
		if job == nil {
			continue
		}

		if job.status == loadInQueue {
			if includeQueued {
				queued = append(queued, job.id)
			}

			continue
		}

		others = append(others, job.id)
	}

	ids := append(queued, others...)
	if len(ids) > limit {
		ids = ids[:limit]
	}

	if ids == nil {
		ids = []string{}
	}

	writeCypherJSON(w, http.StatusOK, envelope(map[string]any{"loadIds": ids}))
}

func (f *cypherFront) cancelLoad(w http.ResponseWriter, r *http.Request, id string) {
	if !authorizeActions(w, r, langSparql, actionCancelLoad) {
		return
	}

	ls := f.rdf.loader
	ls.mu.Lock("cancelLoad")

	job, ok := ls.jobs[id]

	switch {
	case !ok:
		ls.mu.Unlock()
		writeCypherJSON(w, http.StatusNotFound, map[string]any{
			"status": "404 Not Found", "payload": map[string]string{"loadId": id, "error": "Load not found"},
		})

		return
	case job.finished:
		ls.mu.Unlock()
		writeCypherError(w, http.StatusBadRequest, "BadRequestException",
			"Load "+strconv.Quote(id)+" has already finished and cannot be cancelled")

		return
	}

	job.cancelUser = true

	if job.queued {
		job.status, job.finished, job.endTime, job.queued = loadCancelledUser, true, time.Now(), false
	}

	cancel := job.cancel
	ls.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	writeCypherJSON(w, http.StatusOK, map[string]string{"status": "200 OK"})
}
