package sagemaker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var errNoExitWaiter = errors.New("the container runtime cannot report container exit status")

// logTarget streams container output into one CloudWatch Logs stream.
type logTarget struct {
	sink    LogSink
	ctx     context.Context
	mu      *lockmetrics.RWMutex
	region  string
	group   string
	stream  string
	partial []byte
}

// openLog creates the log group and stream; it returns nil when no sink is wired.
func (e *engineState) openLog(ctx context.Context, region, group, stream string) *logTarget {
	sink := e.svc().Logs
	if sink == nil {
		return nil
	}

	if err := sink.EnsureLogGroupAndStream(ctx, region, group, stream); err != nil {
		return nil
	}

	return &logTarget{
		sink: sink, mu: lockmetrics.New("sagemaker.logs"),
		region: region, group: group, stream: stream, ctx: context.WithoutCancel(ctx),
	}
}

// Write buffers p and ships every complete line.
func (t *logTarget) Write(p []byte) (int, error) {
	if t == nil {
		return len(p), nil
	}

	t.mu.Lock("Write")
	defer t.mu.Unlock()

	t.partial = append(t.partial, p...)

	var lines []string

	for {
		i := bytes.IndexByte(t.partial, '\n')
		if i < 0 {
			break
		}

		if line := strings.TrimRight(string(t.partial[:i]), "\r"); line != "" {
			lines = append(lines, line)
		}

		t.partial = t.partial[i+1:]
	}

	t.ship(lines)

	return len(p), nil
}

func (t *logTarget) ship(lines []string) {
	if len(lines) > 0 {
		_ = t.sink.PutLogLines(t.ctx, t.region, t.group, t.stream, lines)
	}
}

// flush ships a trailing line that had no newline.
func (t *logTarget) flush() {
	if t == nil {
		return
	}

	t.mu.Lock("flush")
	defer t.mu.Unlock()

	if len(t.partial) > 0 {
		t.ship([]string{string(t.partial)})
		t.partial = nil
	}
}

// logf writes one engine-authored line.
func (t *logTarget) logf(format string, args ...any) {
	if t != nil {
		t.ship([]string{fmt.Sprintf(format, args...)})
	}
}

// followLogs streams the container's output into lt; the channel closes when the stream ends.
func followLogs(ctx context.Context, rt EngineRuntime, id string, lt *logTarget) (<-chan struct{}, context.CancelFunc) {
	done := make(chan struct{})
	lctx, cancel := context.WithCancel(ctx)

	ls, ok := rt.(container.LogStreamer)
	if !ok || lt == nil {
		close(done)

		return done, cancel
	}

	go func() {
		defer close(done)

		_ = ls.StreamLogs(lctx, id, lt, lt)
		lt.flush()
	}()

	return done, cancel
}

func drainLogs(done <-chan struct{}) {
	t := time.NewTimer(logDrainLimit)
	defer t.Stop()

	select {
	case <-done:
	case <-t.C:
	}
}

// runToExit starts spec, streams its logs and waits for it to exit. A cancelled ctx stops the
// container and returns ctx.Err().
func (e *engineState) runToExit(ctx context.Context, spec container.Spec, lt *logTarget) (int, error) {
	rt := e.cfg.Runtime

	waiter, ok := rt.(container.ExitWaiter)
	if !ok {
		return 0, errNoExitWaiter
	}

	id, err := rt.CreateAndStart(ctx, spec)
	if err != nil {
		return 0, fmt.Errorf("start container: %w", err)
	}

	defer func() {
		cctx, cancel := cleanupContext()
		defer cancel()

		_ = rt.StopAndRemove(cctx, id)
	}()

	logsDone, stopLogs := followLogs(ctx, rt, id, lt)
	defer stopLogs()

	code, werr := waiter.WaitExit(ctx, id)
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	if werr != nil {
		return 0, fmt.Errorf("wait for container: %w", werr)
	}

	drainLogs(logsDone)

	return code, nil
}
