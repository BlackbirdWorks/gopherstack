package inithooks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

// Stage is an init lifecycle stage.
type Stage string

// Lifecycle stages, matching LocalStack's init hook directories (<stage>.d).
const (
	StageBoot     Stage = "BOOT"
	StageStart    Stage = "START"
	StageReady    Stage = "READY"
	StageShutdown Stage = "SHUTDOWN"
)

// State is the per-script outcome reported at /_localstack/init.
type State string

// Script states.
const (
	StateUnknown    State = "UNKNOWN"
	StateRunning    State = "RUNNING"
	StateSuccessful State = "SUCCESSFUL"
	StateError      State = "ERROR"
)

// Stages lists every stage in lifecycle order.
func Stages() []Stage { return []Stage{StageBoot, StageStart, StageReady, StageShutdown} }

// DefaultRoots are the stage-directory roots scanned when present.
func DefaultRoots() []string { return []string{"/etc/localstack/init", "/etc/gopherstack/init"} }

// DefaultLegacyReadyDirs are LocalStack's deprecated flat READY-stage script directories.
func DefaultLegacyReadyDirs() []string { return []string{"/docker-entrypoint-initaws.d"} }

// ScriptStatus is the status of one discovered script.
type ScriptStatus struct {
	Stage Stage
	Name  string
	State State
	Error string
}

// ManagerConfig configures a [Manager].
type ManagerConfig struct {
	Log *slog.Logger
	// Env is appended to the process environment for every script.
	Env   []string
	Roots []string
	// LegacyReadyDirs are flat script dirs run in READY after the ready.d scripts.
	LegacyReadyDirs []string
	Timeout         time.Duration
}

type managerState struct {
	completed map[Stage]bool
	scripts   []ScriptStatus
}

// Manager discovers and runs <root>/<stage>.d scripts and tracks their status.
type Manager struct {
	state   atomic.Pointer[managerState]
	log     *slog.Logger
	env     []string
	roots   []string
	legacy  []string
	timeout time.Duration
}

// NewManager creates a Manager; a zero Timeout defaults to 30 s.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}

	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}

	m := &Manager{log: cfg.Log, env: cfg.Env, roots: cfg.Roots, legacy: cfg.LegacyReadyDirs, timeout: cfg.Timeout}
	m.state.Store(&managerState{completed: make(map[Stage]bool)})

	return m
}

// Status returns every script seen so far, in stage then execution order.
func (m *Manager) Status() []ScriptStatus {
	return slices.Clone(m.state.Load().scripts)
}

// Completed reports whether the stage has finished running.
func (m *Manager) Completed(stage Stage) bool {
	return m.state.Load().completed[stage]
}

func (m *Manager) update(fn func(s *managerState)) {
	for {
		old := m.state.Load()
		next := &managerState{scripts: slices.Clone(old.scripts), completed: make(map[Stage]bool, len(old.completed))}

		maps.Copy(next.completed, old.completed)

		fn(next)

		if m.state.CompareAndSwap(old, next) {
			return
		}
	}
}

func (m *Manager) setScript(stage Stage, idx int, st State, errMsg string) {
	m.update(func(s *managerState) {
		n := 0

		for i := range s.scripts {
			if s.scripts[i].Stage != stage {
				continue
			}

			if n == idx {
				s.scripts[i].State, s.scripts[i].Error = st, errMsg

				return
			}

			n++
		}
	})
}

// listScripts returns the sorted .sh/.py script paths directly inside dir.
func listScripts(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	names := make([]string, 0, len(entries))

	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		if !e.IsDir() && (ext == ".sh" || ext == ".py") {
			names = append(names, e.Name())
		}
	}

	slices.Sort(names)

	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, filepath.Join(dir, n))
	}

	return out
}

// discover returns the sorted script paths of the stage, one sorted batch per root.
func (m *Manager) discover(stage Stage) []string {
	var out []string

	for _, root := range m.roots {
		out = append(out, listScripts(filepath.Join(root, strings.ToLower(string(stage))+".d"))...)
	}

	if stage != StageReady {
		return out
	}

	for _, dir := range m.legacy {
		if legacy := listScripts(dir); len(legacy) > 0 {
			m.log.Warn("init hook: legacy directory is deprecated; move scripts to <root>/ready.d", "dir", dir)

			out = append(out, legacy...)
		}
	}

	return out
}

// Run executes the stage's scripts sequentially, then marks the stage completed.
// Script failures are recorded and logged but never abort the stage.
func (m *Manager) Run(ctx context.Context, stage Stage) {
	paths := m.discover(stage)

	m.update(func(s *managerState) {
		for _, p := range paths {
			s.scripts = append(s.scripts, ScriptStatus{Stage: stage, Name: filepath.Base(p), State: StateUnknown})
		}
	})

	for i, p := range paths {
		m.setScript(stage, i, StateRunning, "")

		if err := m.runScript(ctx, stage, p, nil); err != nil {
			m.log.ErrorContext(ctx, "init hook: script failed", "stage", stage, "script", p, "error", err)
			m.setScript(stage, i, StateError, err.Error())

			continue
		}

		m.setScript(stage, i, StateSuccessful, "")
	}

	m.update(func(s *managerState) { s.completed[stage] = true })
}

var errNoPython = errors.New("python interpreter not found (python3/python)")

func interpreter(path string) (string, error) {
	if filepath.Ext(path) == ".sh" {
		return "sh", nil
	}

	for _, py := range []string{"python3", "python"} {
		if p, err := exec.LookPath(py); err == nil {
			return p, nil
		}
	}

	return "", errNoPython
}

func (m *Manager) runScript(ctx context.Context, stage Stage, path string, capture io.Writer) error {
	interp, err := interpreter(path)
	if err != nil {
		return err
	}

	sctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	cmd := exec.CommandContext(sctx, interp, path)
	cmd.Env = append(os.Environ(), m.env...)
	cmd.Dir = filepath.Dir(path)
	cmd.WaitDelay = m.timeout
	stdout := &lineLogger{ctx: ctx, log: m.log, stage: stage, script: path, stream: "stdout"}
	stderr := &lineLogger{ctx: ctx, log: m.log, stage: stage, script: path, stream: "stderr"}
	cmd.Stdout, cmd.Stderr = stdout, stderr

	if capture != nil {
		cmd.Stdout, cmd.Stderr = io.MultiWriter(stdout, capture), io.MultiWriter(stderr, capture)
	}

	m.log.InfoContext(ctx, "init hook: running", "stage", stage, "script", path)

	err = cmd.Run()

	stdout.emit(stdout.buf.String())
	stderr.emit(stderr.buf.String())

	if err != nil {
		if sctx.Err() != nil {
			return fmt.Errorf("script did not finish: %w", sctx.Err())
		}

		return err
	}

	m.log.InfoContext(ctx, "init hook: script succeeded", "stage", stage, "script", path)

	return nil
}

// lineLogger forwards complete output lines to the logger.
type lineLogger struct {
	ctx    context.Context //nolint:containedctx // io.Writer cannot take a context
	log    *slog.Logger
	stage  Stage
	script string
	stream string
	buf    bytes.Buffer
}

func (w *lineLogger) Write(p []byte) (int, error) {
	w.buf.Write(p)

	for {
		i := bytes.IndexByte(w.buf.Bytes(), '\n')
		if i < 0 {
			return len(p), nil
		}

		w.emit(string(w.buf.Next(i + 1)))
	}
}

func (w *lineLogger) emit(line string) {
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return
	}

	w.log.InfoContext(
		w.ctx, "init hook: output",
		"stage", w.stage, "script", w.script, "stream", w.stream, "line", line,
	)
}
