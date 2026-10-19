package app

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	replicateBasePath   = "/_gopherstack/replicate"
	replicateErrKey     = "error"
	replicateMaxJobs    = 50
	replicateJobTimeout = 2 * time.Hour

	replicateRunning = "running"
	replicateDone    = "completed"
	replicateFailed  = "failed"
)

type replicateFunc func(context.Context, replicator.Config) (*replicator.Report, error)

// replicateRequest is the POST body. Credentials are used for the job only and never stored.
type replicateRequest struct {
	Source         replicator.Endpoint `json:"source"`
	Target         replicator.Endpoint `json:"target"`
	Identifier     string              `json:"identifier,omitempty"`
	ResourceTypes  []string            `json:"resource_types,omitempty"`
	MaxObjectBytes int64               `json:"max_object_bytes,omitempty"`
	DryRun         bool                `json:"dry_run,omitempty"`
	IncludeData    bool                `json:"include_data,omitempty"`
	WithDecryption bool                `json:"with_decryption,omitempty"`
}

type replicateJob struct {
	Started  time.Time           `json:"started_at"`
	Finished *time.Time          `json:"finished_at,omitempty"`
	Counts   map[string]int      `json:"counts"`
	ID       string              `json:"id"`
	Status   string              `json:"status"`
	Error    string              `json:"error,omitempty"`
	Results  []replicator.Result `json:"results"`
	DryRun   bool                `json:"dry_run"`
}

type replicateAPI struct {
	run  replicateFunc
	ctx  context.Context //nolint:containedctx // job lifetime is bound to the server context
	jobs map[string]*replicateJob
	mu   *lockmetrics.RWMutex
	ids  []string
}

func registerReplicateEndpoints(ctx context.Context, e *echo.Echo) {
	a := newReplicateAPI(ctx, replicator.Run)

	e.POST(replicateBasePath, a.start)
	e.GET(replicateBasePath+"/:id", a.status)
}

func newReplicateAPI(ctx context.Context, run replicateFunc) *replicateAPI {
	return &replicateAPI{
		ctx: ctx, run: run, jobs: map[string]*replicateJob{}, mu: lockmetrics.New("replicate"),
	}
}

func (a *replicateAPI) start(c *echo.Context) error {
	var req replicateRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{replicateErrKey: "invalid request body"})
	}

	if _, err := replicator.OrderTypes(req.ResourceTypes); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{replicateErrKey: err.Error()})
	}

	if req.Target.URL == "" {
		req.Target.URL = c.Scheme() + "://" + c.Request().Host
	}

	job := &replicateJob{
		ID: awsid.New("repl-"), Status: replicateRunning, Started: time.Now().UTC(),
		Counts: map[string]int{}, Results: []replicator.Result{}, DryRun: req.DryRun,
	}
	a.register(job)

	cfg := replicator.Config{
		Source: req.Source, Target: req.Target, Types: req.ResourceTypes, Identifier: req.Identifier,
		DryRun: req.DryRun, IncludeData: req.IncludeData, WithDecryption: req.WithDecryption,
		MaxObjectBytes: req.MaxObjectBytes,
		Progress: func(r replicator.Result) {
			a.mu.Lock("Progress")
			defer a.mu.Unlock()

			job.Results = append(job.Results, r)
			job.Counts[r.Action]++
		},
	}

	id := job.ID

	go a.execute(job, cfg)

	return c.JSON(http.StatusAccepted, map[string]string{"id": id, "status": replicateRunning})
}

func (a *replicateAPI) execute(job *replicateJob, cfg replicator.Config) {
	ctx, cancel := context.WithTimeout(a.ctx, replicateJobTimeout)
	defer cancel()

	_, err := a.run(ctx, cfg)

	a.mu.Lock("Finish")
	defer a.mu.Unlock()

	now := time.Now().UTC()
	job.Finished = &now
	job.Status = replicateDone

	if err != nil {
		job.Status = replicateFailed
		job.Error = err.Error()
		logger.Load(a.ctx).WarnContext(a.ctx, "replicate job failed", "job", job.ID)
	}
}

func (a *replicateAPI) register(job *replicateJob) {
	a.mu.Lock("Register")
	defer a.mu.Unlock()

	a.jobs[job.ID] = job
	a.ids = append(a.ids, job.ID)

	for len(a.ids) > replicateMaxJobs {
		i := slices.IndexFunc(a.ids, func(id string) bool { return a.jobs[id].Status != replicateRunning })
		if i < 0 {
			return
		}

		delete(a.jobs, a.ids[i])
		a.ids = slices.Delete(a.ids, i, i+1)
	}
}

func (a *replicateAPI) status(c *echo.Context) error {
	a.mu.RLock("Status")
	defer a.mu.RUnlock()

	job, ok := a.jobs[c.Param("id")]
	if !ok {
		return c.JSON(http.StatusNotFound, map[string]string{replicateErrKey: "replicate job not found"})
	}

	cp := *job
	cp.Results = slices.Clone(job.Results)
	cp.Counts = make(map[string]int, len(job.Counts))

	maps.Copy(cp.Counts, job.Counts)

	return c.JSON(http.StatusOK, cp)
}
