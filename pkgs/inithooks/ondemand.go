package inithooks

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// MaxCapturedOutput bounds the output retained per on-demand script run.
const MaxCapturedOutput = 16 << 10

var (
	// ErrUnknownStage means the stage is not one of [Stages].
	ErrUnknownStage = errors.New("unknown init stage")
	// ErrUnknownScript means no script of that name is registered in the stage.
	ErrUnknownScript = errors.New("init script not registered in stage")
)

// ScriptRun is the outcome of one on-demand script execution.
type ScriptRun struct {
	Stage     Stage
	Name      string
	Output    string
	Error     string
	ExitCode  int
	Truncated bool
}

type boundedBuffer struct {
	buf       []byte
	mu        sync.Mutex
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	room := MaxCapturedOutput - len(b.buf)
	if room > 0 {
		b.buf = append(b.buf, p[:min(room, len(p))]...)
	}

	if len(p) > room {
		b.truncated = true
	}

	return len(p), nil
}

// RunNamed runs the discovered scripts of stage, or only the one whose file
// name equals name, and returns their exit status and bounded output. Only
// scripts found in the configured roots can run; name is matched against
// discovered base names, never used as a path.
func (m *Manager) RunNamed(ctx context.Context, stage Stage, name string) ([]ScriptRun, error) {
	stage = Stage(strings.ToUpper(string(stage)))

	known := false

	for _, s := range Stages() {
		known = known || s == stage
	}

	if !known {
		return nil, fmt.Errorf("%w: %q", ErrUnknownStage, stage)
	}

	paths := m.discover(stage)

	if name != "" {
		var sel []string

		for _, p := range paths {
			if filepath.Base(p) == name {
				sel = append(sel, p)
			}
		}

		if len(sel) == 0 {
			return nil, fmt.Errorf("%w: %q in %s", ErrUnknownScript, name, stage)
		}

		paths = sel
	}

	runs := make([]ScriptRun, 0, len(paths))

	for _, p := range paths {
		out := &boundedBuffer{}
		err := m.runScript(ctx, stage, p, out)
		run := ScriptRun{Stage: stage, Name: filepath.Base(p), Output: string(out.buf), Truncated: out.truncated}

		if err != nil {
			run.Error = err.Error()
			run.ExitCode = -1

			if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
				run.ExitCode = ee.ExitCode()
			}
		}

		runs = append(runs, run)
	}

	return runs, nil
}
