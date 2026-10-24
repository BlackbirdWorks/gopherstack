package rds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	failureLogTimeout = 3 * time.Second
	failureLogMax     = 2048
)

// EngineFailure returns why the docker engine marked the named instance or cluster failed, or "".
func (b *InMemoryBackend) EngineFailure(identifier string) string {
	b.mu.RLock("EngineFailure")
	defer b.mu.RUnlock()

	if b.engine == nil {
		return ""
	}

	return b.engine.failures[normalizeID(identifier)]
}

// failureReason describes a failed unit: stage, cause, and the container's last log output.
func (b *InMemoryBackend) failureReason(lb *liveDB, stage string, cause error) string {
	reason := fmt.Sprintf("%s: %v", stage, cause)

	b.mu.RLock("rdsEngineFailLogs")
	id := lb.containerID
	b.mu.RUnlock()

	if tail := b.engine.containerLogTail(id); tail != "" {
		reason += "; container logs: " + tail
	}

	return reason
}

func (e *dbEngine) containerLogTail(id string) string {
	ls, ok := e.cfg.Runtime.(container.LogStreamer)
	if !ok || id == "" {
		return ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), failureLogTimeout)
	defer cancel()

	var out, errOut tailBuffer

	if err := ls.StreamLogs(ctx, id, &out, &errOut); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf("(log read failed: %v)", err)
	}

	return string(bytes.TrimSpace(append(out.buf, errOut.buf...)))
}

type tailBuffer struct{ buf []byte }

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > failureLogMax {
		t.buf = t.buf[len(t.buf)-failureLogMax:]
	}

	return len(p), nil
}
