package azurefunctions

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	kuduTriggered    = "/api/triggeredwebjobs"
	kuduWebJobs      = "/api/webjobs"
	triggeredJobDir  = "App_Data/jobs/triggered/"
	continuousJobDir = "App_Data/jobs/continuous/"
	maxWebJobRuns    = 100
	runStatusOK      = "Success"
	runStatusFail    = "Failed"
	runStatusBusy    = "Running"
	runIDLayout      = "20060102150405"
)

// jobRunners maps a job script to the command that runs it (Kudu picks run.<ext> by extension).
func jobRunners() []struct{ file, cmd string } {
	return []struct{ file, cmd string }{
		{"run.sh", "sh run.sh"}, {"run.py", "python run.py"}, {"run.js", "node run.js"}, {"run.php", "php run.php"},
	}
}

type webJobDef struct {
	name string
	run  string
	cmd  string
}

func triggeredJobs(pkg []byte) []webJobDef { return jobsIn(pkg, triggeredJobDir) }

func continuousJobs(pkg []byte) []webJobDef { return jobsIn(pkg, continuousJobDir) }

func jobsIn(pkg []byte, dir string) []webJobDef {
	zr, err := zip.NewReader(bytes.NewReader(pkg), int64(len(pkg)))
	if err != nil {
		return nil
	}

	have := map[string]bool{}
	names := map[string]bool{}

	for _, f := range zr.File {
		rest, ok := strings.CutPrefix(f.Name, dir)
		if !ok || strings.HasSuffix(f.Name, "/") {
			continue
		}

		job, file, nested := strings.Cut(rest, "/")
		if nested && !strings.Contains(file, "/") && job != "" {
			have[job+"/"+file], names[job] = true, true
		}
	}

	var out []webJobDef

	for job := range names {
		for _, r := range jobRunners() {
			if have[job+"/"+r.file] {
				out = append(out, webJobDef{name: job, run: r.file, cmd: r.cmd})

				break
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return strs.Fold(out[i].name) < strs.Fold(out[j].name) })

	return out
}

type webJobRunJSON struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time,omitempty"`
	Duration  string `json:"duration"`
	OutputURL string `json:"output_url"`
	ErrorURL  string `json:"error_url"`
	URL       string `json:"url"`
	JobName   string `json:"job_name"`
	Trigger   string `json:"trigger"`
}

type webJobJSON struct {
	LatestRun    *webJobRunJSON `json:"latest_run,omitempty"`
	Settings     map[string]any `json:"settings"`
	Error        *string        `json:"error"`
	Name         string         `json:"name"`
	RunCommand   string         `json:"runCommand"`
	URL          string         `json:"url"`
	ExtraInfoURL string         `json:"extra_info_url"`
	HistoryURL   string         `json:"history_url"`
	SchedulerURL string         `json:"scheduler_logs_url"`
	Type         string         `json:"type"`
	UsingSDK     bool           `json:"using_sdk"`
}

func runJSON(r *http.Request, run WebJobRun) webJobRunJSON {
	base := "http://" + r.Host + kuduTriggered + "/" + run.Job + "/history/" + run.ID
	vfs := "http://" + r.Host + kuduVFS + "data/jobs/triggered/" + run.Job + "/" + run.ID + "/"
	out := webJobRunJSON{
		ID: run.ID, Status: run.Status, StartTime: run.Start.UTC().Format(time.RFC3339Nano),
		Duration: "00:00:00", URL: base, JobName: run.Job, Trigger: "Manually triggered",
		OutputURL: vfs + "output_" + run.ID + ".log", ErrorURL: vfs + "error_" + run.ID + ".log",
	}

	if run.Status != runStatusBusy {
		out.EndTime = run.End.UTC().Format(time.RFC3339Nano)
		out.Duration = run.End.Sub(run.Start).Truncate(time.Second).String()
	}

	return out
}

func (b *InMemoryBackend) webJobRuns(name, job string) []WebJobRun {
	b.mu.RLock("webJobRuns")
	defer b.mu.RUnlock()

	a, err := b.appLocked(name)
	if err != nil {
		return nil
	}

	var out []WebJobRun

	for _, run := range slices.Backward(a.WebJobRuns) {
		if job == "" || run.Job == job {
			out = append(out, run)
		}
	}

	return out
}

func webJobOf(r *http.Request, d webJobDef, runs []WebJobRun) webJobJSON {
	base := "http://" + r.Host + kuduTriggered + "/" + d.name
	j := webJobJSON{
		Name: d.name, RunCommand: d.run, URL: base, HistoryURL: base + "/history",
		SchedulerURL: "http://" + r.Host + kuduVFS + "data/jobs/triggered/" + d.name + "/job_log.txt",
		ExtraInfoURL: "http://" + r.Host + "/azurejobs/#/jobs/triggered/" + d.name,
		Type:         "triggered", Settings: map[string]any{},
	}

	if len(runs) > 0 {
		lr := runJSON(r, runs[0])
		j.LatestRun = &lr
	}

	return j
}

func (b *InMemoryBackend) kuduWebJobs(w http.ResponseWriter, r *http.Request, name, p string) {
	rest := strings.TrimPrefix(strings.TrimPrefix(p, kuduWebJobs), kuduTriggered)
	rest = strings.Trim(rest, "/")
	jobs := triggeredJobs(b.Package(name))

	if rest == "" {
		out := make([]any, 0, len(jobs))
		for _, d := range jobs {
			out = append(out, webJobOf(r, d, b.webJobRuns(name, d.name)))
		}

		if p == kuduWebJobs {
			for _, d := range continuousJobs(b.Package(name)) {
				out = append(out, b.continuousOf(r, name, d))
			}
		}

		writeJSON(w, out)

		return
	}

	job, sub, _ := strings.Cut(rest, "/")

	idx := slices.IndexFunc(jobs, func(d webJobDef) bool { return d.name == job })
	if idx < 0 {
		http.NotFound(w, r)

		return
	}

	runs := b.webJobRuns(name, job)

	switch {
	case sub == "":
		writeJSON(w, webJobOf(r, jobs[idx], runs))
	case sub == "run" && r.Method == http.MethodPost:
		b.startWebJobRun(w, r, name, jobs[idx])
	case sub == "history":
		out := make([]webJobRunJSON, 0, len(runs))
		for _, run := range runs {
			out = append(out, runJSON(r, run))
		}

		writeJSON(w, map[string]any{"runs": out})
	case strings.HasPrefix(sub, "history/"):
		id := strings.TrimPrefix(sub, "history/")
		if i := slices.IndexFunc(runs, func(x WebJobRun) bool { return x.ID == id }); i >= 0 {
			writeJSON(w, runJSON(r, runs[i]))

			return
		}

		http.NotFound(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (b *InMemoryBackend) startWebJobRun(w http.ResponseWriter, r *http.Request, name string, d webJobDef) {
	ctx := b.loopContext(r.Context(), name)
	if ctx == nil {
		http.Error(w, errNoHost.Error(), http.StatusServiceUnavailable)

		return
	}

	run, err := b.beginWebJobRun(name, d.name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	dir := homeDir + "/" + path.Join("site/wwwroot", triggeredJobDir, d.name)
	args := r.URL.Query().Get("arguments")

	b.wg.Go(func() { b.finishWebJobRun(ctx, name, run, d.cmd+" "+args, dir) })

	w.Header().Set("Location", "http://"+r.Host+kuduTriggered+"/"+d.name+"/history/"+run.ID)
	w.WriteHeader(http.StatusAccepted)
}

func (b *InMemoryBackend) beginWebJobRun(name, job string) (WebJobRun, error) {
	b.mu.Lock("beginWebJobRun")
	defer b.mu.Unlock()

	a, err := b.appLocked(name)
	if err != nil {
		return WebJobRun{}, err
	}

	now := b.now().UTC()
	run := WebJobRun{
		Job:    job,
		ID:     now.Format(runIDLayout) + "_" + strconv.Itoa(len(a.WebJobRuns)),
		Start:  now,
		Status: runStatusBusy,
	}
	a.WebJobRuns = append(a.WebJobRuns, run)

	if len(a.WebJobRuns) > maxWebJobRuns {
		a.WebJobRuns = a.WebJobRuns[len(a.WebJobRuns)-maxWebJobRuns:]
	}

	return run, nil
}

func (b *InMemoryBackend) finishWebJobRun(ctx context.Context, name string, run WebJobRun, cmd, dir string) {
	res, err := b.RunCommand(ctx, name, strings.TrimSpace(cmd), dir)

	run.Status, run.Output, run.Error = runStatusOK, res.Stdout, res.Stderr
	if err != nil {
		run.Status, run.Error = runStatusFail, err.Error()
		logger.Load(ctx).
			WarnContext(ctx, "azurefunctions: webjob did not run", "app", name, "job", run.Job, "error", err)
	} else if res.ExitCode != 0 {
		run.Status = runStatusFail
	}

	b.mu.Lock("finishWebJobRun")
	defer b.mu.Unlock()

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		return
	}

	run.End = b.now().UTC()

	for i := range a.WebJobRuns {
		if a.WebJobRuns[i].ID == run.ID && a.WebJobRuns[i].Job == run.Job {
			a.WebJobRuns[i] = run

			return
		}
	}
}

// loopContext starts the app's host if needed and returns the context that ends when the host is taken down.
func (b *InMemoryBackend) loopContext(ctx context.Context, name string) context.Context {
	if _, err := b.ensureStarted(ctx, name); err != nil {
		return nil
	}

	b.mu.RLock("loopContext")
	defer b.mu.RUnlock()

	if a, err := b.appLocked(name); err == nil {
		return a.loops
	}

	return nil
}
