package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type lifecycleState struct {
	StartedAt   time.Time `json:"started_at"`
	Mode        string    `json:"mode"`
	Name        string    `json:"name"`
	ContainerID string    `json:"container_id,omitempty"`
	Image       string    `json:"image,omitempty"`
	Endpoint    string    `json:"endpoint"`
	LogFile     string    `json:"log_file,omitempty"`
	PID         int       `json:"pid,omitempty"`
}

func (d *lifecycleDeps) statePath(t LifecycleTarget) string {
	return filepath.Join(t.stateDir(d.Getenv, d.Home), t.name()+".json")
}

func (d *lifecycleDeps) loadState(t LifecycleTarget) (lifecycleState, bool) {
	b, err := os.ReadFile(d.statePath(t))
	if err != nil {
		return lifecycleState{}, false
	}

	var st lifecycleState
	if json.Unmarshal(b, &st) != nil {
		return lifecycleState{}, false
	}

	return st, true
}

func (d *lifecycleDeps) saveState(t LifecycleTarget, st lifecycleState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}

	if err = os.WriteFile(d.statePath(t), b, lifecycleStateFilePerm); err != nil {
		return fmt.Errorf("write state: %w", err)
	}

	return nil
}

func (d *lifecycleDeps) dropState(t LifecycleTarget) { _ = os.Remove(d.statePath(t)) }

func (d *lifecycleDeps) request(ctx context.Context, method, target string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}

	resp, err := d.HTTP.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %w", errLifecycleRequest, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	return body, resp.StatusCode, nil
}

func (d *lifecycleDeps) health(ctx context.Context, endpoint string) (healthResponse, error) {
	var h healthResponse

	body, code, err := d.request(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/_gopherstack/health")
	if err != nil {
		return h, err
	}

	if code != http.StatusOK {
		return h, fmt.Errorf("%w: health status %d", errLifecycleRequest, code)
	}

	if err = json.Unmarshal(body, &h); err != nil {
		return h, fmt.Errorf("decode health: %w", err)
	}

	return h, nil
}

func (d *lifecycleDeps) waitReady(
	ctx context.Context,
	endpoint string,
	timeout time.Duration,
	alive func() bool,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tick := time.NewTicker(lifecycleReadyPoll)
	defer tick.Stop()

	for {
		if _, err := d.health(ctx, endpoint); err == nil {
			return nil
		}

		if alive != nil && !alive() {
			return fmt.Errorf("%w: process exited", errLifecycleStartFail)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s not healthy after %s", errLifecycleStartFail, endpoint, timeout)
		case <-tick.C:
		}
	}
}

func (d *lifecycleDeps) envList(c *StartCmd) ([]string, error) {
	var out []string

	for _, e := range c.Env {
		switch {
		case e == "" || strings.HasPrefix(e, "="):
			return nil, fmt.Errorf("%w: %q", errLifecycleBadEnv, e)
		case strings.Contains(e, "="):
			out = append(out, e)
		default:
			if v := d.Getenv(e); v != "" {
				out = append(out, e+"="+v)
			}
		}
	}

	return out, nil
}

func (c *StartCmd) publishPorts() []string {
	ports := []string{c.Bind + ":" + c.Port + ":" + lifecycleContainerPort}

	for _, p := range c.Publish {
		if strings.Count(p, ":") >= 1 {
			ports = append(ports, c.Bind+":"+p)
		} else {
			ports = append(ports, c.Bind+":"+p+":"+p)
		}
	}

	return ports
}

// Run executes `start`.
func (c *StartCmd) Run(d *lifecycleDeps) error {
	ctx, cancel := d.NewContext()
	defer cancel()

	return c.run(ctx, d)
}

func (c *StartCmd) run(ctx context.Context, d *lifecycleDeps) error {
	endpoint := endpointFor(c.Bind, c.Port)

	if err := c.validate(); err != nil {
		return err
	}

	if _, err := d.health(ctx, endpoint); err == nil {
		fmt.Fprintf(d.Out, "gopherstack is already running at %s\n", endpoint)

		return nil
	}

	extra, err := d.envList(c)
	if err != nil {
		return err
	}

	dir := c.stateDir(d.Getenv, d.Home)
	if err = os.MkdirAll(dir, lifecycleStateDirPerm); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	dataDir := c.DataDir
	if dataDir == "" && c.Persist {
		dataDir = filepath.Join(dir, "data-"+c.name())
	}

	if dataDir != "" {
		if dataDir, err = filepath.Abs(dataDir); err != nil {
			return fmt.Errorf("resolve data dir: %w", err)
		}

		if err = os.MkdirAll(dataDir, lifecycleStateDirPerm); err != nil {
			return fmt.Errorf("create data dir: %w", err)
		}
	}

	if c.Host {
		return c.runHost(ctx, d, endpoint, dir, dataDir, extra)
	}

	return c.runContainer(ctx, d, endpoint, dataDir, extra)
}

func (c *StartCmd) runContainer(ctx context.Context, d *lifecycleDeps, endpoint, dataDir string, extra []string) error {
	rt, err := d.Runtime()
	if err != nil {
		return err
	}

	var waiter container.ExitWaiter

	if c.Foreground {
		var ok bool
		if waiter, ok = rt.(container.ExitWaiter); !ok {
			return fmt.Errorf("%w: cannot wait for exit", errLifecycleNoRuntime)
		}
	}

	_ = rt.StopAndRemove(ctx, c.name())

	env := append([]string{"PORT=" + lifecycleContainerPort}, extra...)
	spec := container.Spec{
		Image:     c.Image + ":" + c.Tag,
		Name:      c.name(),
		Ports:     c.publishPorts(),
		Unmanaged: true,
	}

	if c.Persist {
		env = append(env, "PERSIST=true")
	}

	if dataDir != "" {
		env = append(env, "GOPHERSTACK_DATA_DIR="+lifecycleContainerData)
		spec.Mounts = []string{dataDir + ":" + lifecycleContainerData}
	}

	if c.DockerSocket {
		sock := c.dockerSocketPath(d.Getenv)
		spec.Mounts = append(spec.Mounts, sock+":"+lifecycleSocketInside)
		env = append(env, "DOCKER_HOST=unix://"+lifecycleSocketInside)
		fmt.Fprintf(d.Err, "warning: mounting %s gives the server root-equivalent control of the host Docker daemon\n",
			sock)
	}

	spec.Env = env

	id, err := rt.CreateAndStart(ctx, spec)
	if err != nil {
		return fmt.Errorf("start container: %w", err)
	}

	st := lifecycleState{
		Mode: lifecycleModeContainer, Name: c.name(), ContainerID: id, Image: spec.Image,
		Endpoint: endpoint, StartedAt: time.Now().UTC(),
	}
	if err = d.saveState(c.LifecycleTarget, st); err != nil {
		return err
	}

	if err = d.waitReady(ctx, endpoint, c.Timeout, nil); err != nil {
		var tail bytes.Buffer

		_ = rt.ReadLogs(context.WithoutCancel(ctx), id, lifecycleLogTailOnError, false, &tail, &tail)
		_ = rt.StopAndRemove(context.WithoutCancel(ctx), id)
		d.dropState(c.LifecycleTarget)

		return fmt.Errorf("%w\nlast container output:\n%s", err, strings.TrimSpace(tail.String()))
	}

	fmt.Fprintf(d.Out, "gopherstack started (container %s, %s) at %s\n", shortID(id), spec.Image, endpoint)

	if c.Foreground {
		return d.attach(ctx, rt, waiter, c.LifecycleTarget, id)
	}

	return nil
}

func (c *StartCmd) runHost(ctx context.Context, d *lifecycleDeps, endpoint, dir, dataDir string, extra []string) error {
	logFile := filepath.Join(dir, c.name()+".log")
	env := append([]string{"PORT=" + c.Port, "GOPHERSTACK_HOST=" + c.Bind}, extra...)

	if c.Persist {
		env = append(env, "PERSIST=true")
	}

	if dataDir != "" {
		env = append(env, "GOPHERSTACK_DATA_DIR="+dataDir)
	}

	pid, err := d.Spawn(hostSpawn{LogFile: logFile, Dir: dir, Env: env})
	if err != nil {
		return err
	}

	st := lifecycleState{
		Mode: lifecycleModeHost, Name: c.name(), PID: pid, Endpoint: endpoint,
		LogFile: logFile, StartedAt: time.Now().UTC(),
	}
	if err = d.saveState(c.LifecycleTarget, st); err != nil {
		return err
	}

	if err = d.waitReady(ctx, endpoint, c.Timeout, func() bool { return d.Alive(pid) }); err != nil {
		_ = d.Terminate(pid, true)
		d.dropState(c.LifecycleTarget)

		return fmt.Errorf("%w\nlast output (%s):\n%s", err, logFile, tailFile(logFile, lifecycleLogTail()))
	}

	fmt.Fprintf(d.Out, "gopherstack started (pid %d, log %s) at %s\n", pid, logFile, endpoint)

	return nil
}

func lifecycleLogTail() int {
	n, _ := strconv.Atoi(lifecycleLogTailOnError)

	return n
}

func shortID(id string) string {
	const n = 12
	if len(id) > n {
		return id[:n]
	}

	return id
}

func notFoundErr(err error) bool {
	m := strings.ToLower(err.Error())

	return strings.Contains(m, "no such container") || strings.Contains(m, "not found")
}

// Run executes `stop`.
func (c *StopCmd) Run(d *lifecycleDeps) error {
	ctx, cancel := d.NewContext()
	defer cancel()

	return c.run(ctx, d)
}

func (c *StopCmd) run(ctx context.Context, d *lifecycleDeps) error {
	st, ok := d.loadState(c.LifecycleTarget)
	defer d.dropState(c.LifecycleTarget)

	if ok && st.Mode == lifecycleModeHost {
		return d.stopHost(ctx, st)
	}

	rt, err := d.Runtime()
	if err != nil {
		if ok {
			return err
		}

		fmt.Fprintln(d.Out, "gopherstack is not running")

		return nil
	}

	target := c.name()
	if ok && st.ContainerID != "" {
		target = st.ContainerID
	}

	if err = rt.StopAndRemove(ctx, target); err != nil {
		if notFoundErr(err) {
			fmt.Fprintln(d.Out, "gopherstack is not running")

			return nil
		}

		return fmt.Errorf("stop container: %w", err)
	}

	fmt.Fprintf(d.Out, "gopherstack stopped (container %s)\n", c.name())

	return nil
}

func (d *lifecycleDeps) stopHost(ctx context.Context, st lifecycleState) error {
	if !d.Alive(st.PID) {
		fmt.Fprintln(d.Out, "gopherstack is not running")

		return nil
	}

	if err := d.Terminate(st.PID, false); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, lifecycleStopTimeout)
	defer cancel()

	tick := time.NewTicker(lifecycleReadyPoll / 5) //nolint:mnd // poll faster than readiness
	defer tick.Stop()

	for d.Alive(st.PID) {
		select {
		case <-ctx.Done():
			if err := d.Terminate(st.PID, true); err != nil {
				return err
			}

			fmt.Fprintf(d.Out, "gopherstack killed (pid %d)\n", st.PID)

			return nil
		case <-tick.C:
		}
	}

	fmt.Fprintf(d.Out, "gopherstack stopped (pid %d)\n", st.PID)

	return nil
}

type statusReport struct {
	Clouds        map[string][]serviceState `json:"clouds,omitempty"`
	Managed       *lifecycleState           `json:"managed,omitempty"`
	Endpoint      string                    `json:"endpoint"`
	Version       string                    `json:"version,omitempty"`
	Uptime        string                    `json:"uptime,omitempty"`
	UptimeSecs    int64                     `json:"uptime_seconds,omitempty"`
	EnabledCount  int                       `json:"enabled_services"`
	DisabledCount int                       `json:"disabled_services"`
	Running       bool                      `json:"running"`
	Goroutines    int                       `json:"goroutines,omitempty"`
	HeapAllocB    uint64                    `json:"heap_alloc_bytes,omitempty"`
}

type serviceState struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Port   string `json:"port,omitempty"`
	Health string `json:"health"`
}

const (
	bytesPerMiB       = 1 << 20
	healthHealthy     = "healthy"
	healthUnavailable = "unavailable"
	healthDisabled    = "disabled"
)

func endpointPort(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}

	if p := u.Port(); p != "" {
		return p
	}

	if u.Scheme == lifecycleSchemeHTTPS {
		return "443"
	}

	return "80"
}

// serviceHealth returns the per-service availability from the LocalStack-compatible health route,
// or nil when the server does not serve it.
func (d *lifecycleDeps) serviceHealth(ctx context.Context, endpoint string) map[string]string {
	body, code, err := d.request(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/_localstack/health")
	if err != nil || code != http.StatusOK {
		return nil
	}

	var r localstackHealthResponse
	if json.Unmarshal(body, &r) != nil {
		return nil
	}

	return r.Services
}

func cloudOf(name string) string {
	switch {
	case strings.HasPrefix(name, "Azure"):
		return "azure"
	case strings.HasPrefix(name, "GCP"):
		return "gcp"
	default:
		return "aws"
	}
}

func buildStatus(
	endpoint string, h healthResponse, st *lifecycleState, svcHealth map[string]string,
) statusReport {
	r := statusReport{
		Running: true, Endpoint: endpoint, Version: h.Version, UptimeSecs: h.UptimeSecs,
		Uptime: (time.Duration(h.UptimeSecs) * time.Second).String(), Managed: st,
		Clouds: map[string][]serviceState{}, Goroutines: h.Goroutines, HeapAllocB: h.HeapAllocB,
		EnabledCount: len(h.Services), DisabledCount: len(h.Disabled),
	}

	port := endpointPort(endpoint)

	add := func(names []string, state string) {
		for _, n := range names {
			c := cloudOf(n)
			ss := serviceState{Name: n, State: state, Health: healthDisabled}

			if state == "enabled" {
				ss.Port = port
				ss.Health = healthHealthy

				if svcHealth != nil && svcHealth[strings.ToLower(n)] != localstackAvailable {
					ss.Health = healthUnavailable
				}
			}

			r.Clouds[c] = append(r.Clouds[c], ss)
		}
	}

	add(h.Services, "enabled")
	add(h.Disabled, "disabled")

	for _, list := range r.Clouds {
		sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	}

	return r
}

// Run executes `status`.
func (c *StatusCmd) Run(d *lifecycleDeps) error {
	ctx, cancel := d.NewContext()
	defer cancel()

	return c.run(ctx, d)
}

func (c *StatusCmd) run(ctx context.Context, d *lifecycleDeps) error {
	endpoint := c.Endpoint

	var managed *lifecycleState

	if st, ok := d.loadState(c.LifecycleTarget); ok {
		managed = &st
		if endpoint == podDefaultEndpoint {
			endpoint = st.Endpoint
		}
	}

	h, err := d.health(ctx, endpoint)
	if err != nil {
		if c.JSON {
			_ = json.NewEncoder(d.Out).Encode(statusReport{Endpoint: endpoint, Managed: managed})
		} else {
			fmt.Fprintf(d.Out, "gopherstack is not running (%s)\n", endpoint)
		}

		return errLifecycleNotRunning
	}

	rep := buildStatus(endpoint, h, managed, d.serviceHealth(ctx, endpoint))
	if c.JSON {
		enc := json.NewEncoder(d.Out)
		enc.SetIndent("", "  ")

		return enc.Encode(rep)
	}

	rep.print(d.Out)

	return nil
}

func (r statusReport) print(w io.Writer) {
	fmt.Fprintf(w, "Status:   running\nEndpoint: %s\nVersion:  %s\nUptime:   %s\n", r.Endpoint, r.Version, r.Uptime)

	if m := r.Managed; m != nil {
		if m.Mode == lifecycleModeHost {
			fmt.Fprintf(w, "Managed:  host process pid %d (log %s)\n", m.PID, m.LogFile)
		} else {
			fmt.Fprintf(w, "Managed:  container %s (%s)\n", m.Name, m.Image)
		}
	}

	fmt.Fprintf(w, "Memory:   %.1f MiB heap, %d goroutines\n", float64(r.HeapAllocB)/bytesPerMiB, r.Goroutines)
	fmt.Fprintf(w, "Services: %d enabled, %d disabled (one process: shared port and uptime)\n\n",
		r.EnabledCount, r.DisabledCount)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0) //nolint:mnd // tabwriter padding
	fmt.Fprintln(tw, "CLOUD\tSERVICE\tSTATE\tPORT\tHEALTH")

	for _, cloud := range []string{"aws", "azure", "gcp"} {
		for _, s := range r.Clouds[cloud] {
			port := s.Port
			if port == "" {
				port = "-"
			}

			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", cloud, s.Name, s.State, port, s.Health)
		}
	}

	_ = tw.Flush()
}

// Run executes `reset`.
func (c *ResetCmd) Run(d *lifecycleDeps) error {
	ctx, cancel := d.NewContext()
	defer cancel()

	return c.run(ctx, d)
}

func (c *ResetCmd) run(ctx context.Context, d *lifecycleDeps) error {
	base := strings.TrimRight(c.Endpoint, "/") + "/_gopherstack/reset"
	targets := []string{base}

	if len(c.Services) > 0 {
		targets = targets[:0]
		for _, s := range c.Services {
			targets = append(targets, base+"?"+url.Values{"service": {s}}.Encode())
		}
	}

	for _, t := range targets {
		body, code, err := d.request(ctx, http.MethodPost, t)
		if err != nil {
			return err
		}

		if code >= http.StatusBadRequest {
			return fmt.Errorf("%w: %d: %s", errLifecycleRequest, code, strings.TrimSpace(string(body)))
		}

		var resp struct {
			Message string `json:"message"`
		}

		_ = json.Unmarshal(body, &resp)
		fmt.Fprintln(d.Out, resp.Message)
	}

	return nil
}

// Run executes `logs`.
func (c *LogsCmd) Run(d *lifecycleDeps) error {
	ctx, cancel := d.NewContext()
	defer cancel()

	return c.run(ctx, d)
}

func (c *LogsCmd) run(ctx context.Context, d *lifecycleDeps) error {
	st, ok := d.loadState(c.LifecycleTarget)
	if ok && st.Mode == lifecycleModeHost {
		return d.hostLogs(ctx, st.LogFile, c.Tail, c.Follow)
	}

	rt, err := d.Runtime()
	if err != nil {
		return err
	}

	target := c.name()
	if ok && st.ContainerID != "" {
		target = st.ContainerID
	}

	if err = rt.ReadLogs(ctx, target, strconv.Itoa(c.Tail), c.Follow, d.Out, d.Err); err != nil {
		if notFoundErr(err) {
			return errLifecycleNotRunning
		}

		return fmt.Errorf("read logs: %w", err)
	}

	return nil
}

func (d *lifecycleDeps) hostLogs(ctx context.Context, path string, tail int, follow bool) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()

	fmt.Fprint(d.Out, tailFile(path, tail))

	if !follow {
		return nil
	}

	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek log file: %w", err)
	}

	tick := time.NewTicker(lifecycleReadyPoll)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			_, _ = io.Copy(d.Out, f)
		}
	}
}

func tailFile(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	if len(b) > lifecycleLogTailMaxByte {
		b = b[len(b)-lifecycleLogTailMaxByte:]
	}

	lines := strings.SplitAfter(string(b), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if n > 0 && len(lines) > n {
		lines = lines[len(lines)-n:]
	}

	return strings.Join(lines, "")
}
