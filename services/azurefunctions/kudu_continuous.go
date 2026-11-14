package azurefunctions

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	kuduContinuous = "/api/continuouswebjobs"
	jobLogCap      = 64 << 10

	jobStatusRunning = "Running"
	jobStatusPending = "PendingRestart"
	jobStatusStopped = "Stopped"
	jobStatusInit    = "Initializing"

	actionStart = "start"
	actionStop  = "stop"
)

// jobRestartDelay is how long a continuous job that exited waits before it is started again.
const jobRestartDelay = 5 * time.Second

// contJob is the live state of one continuous WebJob; guarded by the backend lock.
type contJob struct {
	cancel context.CancelFunc
	status string
	log    []byte
}

type jobLogWriter struct {
	b    *InMemoryBackend
	name string
	job  string
}

func (w jobLogWriter) Write(p []byte) (int, error) {
	w.b.mu.Lock("jobLog")
	defer w.b.mu.Unlock()

	if a, ok := w.b.apps[strs.Fold(w.name)]; ok && a.cont[w.job] != nil {
		j := a.cont[w.job]
		j.log = append(j.log, p...)

		if len(j.log) > jobLogCap {
			j.log = j.log[len(j.log)-jobLogCap:]
		}
	}

	return len(p), nil
}

// startLoopsLocked creates the per-host context and starts what runs alongside a host: the health monitor and the
// continuous WebJobs. takeBoxLocked cancels them.
func (b *InMemoryBackend) startLoopsLocked(a *app) {
	a.loops, a.stopLoops = context.WithCancel(b.runCtx)
	a.cont = map[string]*contJob{}

	b.watchHealthLocked(a)

	for _, d := range continuousJobs(a.Zip) {
		if !slices.Contains(a.StoppedJobs, d.name) {
			b.startContinuousLocked(a, d)
		}
	}
}

func (b *InMemoryBackend) startContinuousLocked(a *app, d webJobDef) {
	if a.loops == nil || a.cont[d.name] != nil && a.cont[d.name].status != jobStatusStopped {
		return
	}

	ctx, cancel := context.WithCancel(a.loops)
	j := &contJob{cancel: cancel, status: jobStatusInit}
	a.cont[d.name] = j

	name, id := a.key(), a.containerID
	dir := homeDir + "/site/wwwroot/" + continuousJobDir + d.name
	cmd := "cd " + shQuote(dir) + " && " + d.cmd

	b.wg.Go(func() { b.superviseJob(ctx, name, id, d.name, cmd, j) })
}

func (b *InMemoryBackend) setJobStatus(name, job string, j *contJob, status string) {
	b.mu.Lock("jobStatus")
	defer b.mu.Unlock()

	if a, ok := b.apps[strs.Fold(name)]; ok && a.cont[job] == j {
		j.status = status
	}
}

func (b *InMemoryBackend) superviseJob(ctx context.Context, name, id, job, cmd string, j *contJob) {
	defer b.setJobStatus(name, job, j, jobStatusStopped)

	runner, err := b.runner()
	if err != nil {
		return
	}

	se, ok := runner.(container.StreamExecer)
	if !ok {
		logger.Load(ctx).WarnContext(ctx, "azurefunctions: runtime cannot stream exec; continuous webjob not run",
			"app", name, "job", job)

		return
	}

	out := jobLogWriter{b: b, name: name, job: job}

	for ctx.Err() == nil {
		b.setJobStatus(name, job, j, jobStatusRunning)

		_, _ = se.ExecStream(ctx, id, []string{"sh", "-c", cmd}, io.Writer(out))

		b.setJobStatus(name, job, j, jobStatusPending)

		select {
		case <-ctx.Done():
		case <-time.After(jobRestartDelay):
		}
	}
}

func (b *InMemoryBackend) continuousStatus(name, job string) string {
	b.mu.RLock("continuousStatus")
	defer b.mu.RUnlock()

	a, err := b.appLocked(name)
	if err != nil {
		return jobStatusStopped
	}

	if j := a.cont[job]; j != nil {
		return j.status
	}

	return jobStatusStopped
}

// continuousLogs returns the captured output of each continuous job, by job name.
func (b *InMemoryBackend) continuousLogs(name string) map[string][]byte {
	b.mu.RLock("continuousLogs")
	defer b.mu.RUnlock()

	out := map[string][]byte{}

	if a, err := b.appLocked(name); err == nil {
		for k, j := range a.cont {
			out[k] = slices.Clone(j.log)
		}
	}

	return out
}

type continuousJobJSON struct {
	Settings     map[string]any `json:"settings"`
	Name         string         `json:"name"`
	Status       string         `json:"status"`
	RunCommand   string         `json:"runCommand"`
	LogURL       string         `json:"log_url"`
	ExtraInfoURL string         `json:"extra_info_url"`
	URL          string         `json:"url"`
	Type         string         `json:"type"`
	UsingSDK     bool           `json:"using_sdk"`
}

func (b *InMemoryBackend) continuousOf(r *http.Request, name string, d webJobDef) continuousJobJSON {
	return continuousJobJSON{
		Name: d.name, Status: b.continuousStatus(name, d.name), RunCommand: d.run, Type: "continuous",
		URL:          "http://" + r.Host + kuduContinuous + "/" + d.name,
		LogURL:       "http://" + r.Host + kuduVFS + "data/jobs/continuous/" + d.name + "/job_log.txt",
		ExtraInfoURL: "http://" + r.Host + "/azurejobs/#/jobs/continuous/" + d.name,
		Settings:     map[string]any{},
	}
}

func (b *InMemoryBackend) kuduContinuous(w http.ResponseWriter, r *http.Request, name, p string) {
	rest := strings.Trim(strings.TrimPrefix(p, kuduContinuous), "/")
	jobs := continuousJobs(b.Package(name))

	if rest == "" {
		out := make([]continuousJobJSON, 0, len(jobs))
		for _, d := range jobs {
			out = append(out, b.continuousOf(r, name, d))
		}

		writeJSON(w, out)

		return
	}

	job, action, _ := strings.Cut(rest, "/")

	idx := slices.IndexFunc(jobs, func(d webJobDef) bool { return d.name == job })
	if idx < 0 {
		http.NotFound(w, r)

		return
	}

	switch {
	case action == "":
		writeJSON(w, b.continuousOf(r, name, jobs[idx]))
	case (action == actionStart || action == actionStop) && r.Method == http.MethodPost:
		if err := b.setContinuous(r.Context(), name, jobs[idx], action == actionStart); err != nil {
			http.Error(w, err.Error(), execErrorCode(err))

			return
		}

		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

func (b *InMemoryBackend) setContinuous(ctx context.Context, name string, d webJobDef, start bool) error {
	if start {
		if _, err := b.ensureStarted(ctx, name); err != nil {
			return err
		}
	}

	b.mu.Lock("setContinuous")
	defer b.mu.Unlock()

	a, err := b.appLocked(name)
	if err != nil {
		return err
	}

	a.StoppedJobs = slices.DeleteFunc(a.StoppedJobs, func(s string) bool { return s == d.name })

	if start {
		b.startContinuousLocked(a, d)

		return nil
	}

	a.StoppedJobs = append(a.StoppedJobs, d.name)

	if j := a.cont[d.name]; j != nil {
		j.cancel()
		j.status = jobStatusStopped
	}

	return nil
}
