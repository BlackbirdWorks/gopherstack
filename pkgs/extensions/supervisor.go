package extensions

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

var (
	errStartTimeout = errors.New("extension did not register in time")
	errUnhealthy    = errors.New("extension failed health checks")

	errRestartRequested = errors.New("restart requested")
)

// runOnce starts one incarnation and blocks until it exits, turns unhealthy or ctx ends.
func (m *Manager) runOnce(ctx context.Context, e *extension) error {
	token, port, err := m.prepare(e)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	r := m.newRunner(e, port, token)
	if err = r.Start(runCtx, m.env(e, token, port)); err != nil {
		return err
	}

	var waitErr error

	exited := make(chan struct{})

	go func() {
		waitErr = r.Wait()
		close(exited)
	}()

	defer func() {
		r.Stop()
		cancel()
		<-exited
	}()

	m.mu.RLock("registered")
	registered := e.registered
	m.mu.RUnlock()

	timer := time.NewTimer(e.spec.StartTimeout)
	defer timer.Stop()

	select {
	case <-registered:
	case <-exited:
		return errOrExited(waitErr)
	case <-e.restartReq:
		return errRestartRequested
	case <-timer.C:
		return errStartTimeout
	case <-ctx.Done():
		return nil
	}

	m.lifecycle(ctx, e, "on_startup")

	if m.ready.Load() {
		m.lifecycle(ctx, e, "on_ready")
	}

	return m.monitor(ctx, e, port, exited, &waitErr)
}

func errOrExited(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w: %w", errExited, err)
}

func (m *Manager) monitor(ctx context.Context, e *extension, port int, exited <-chan struct{}, waitErr *error) error {
	tick := time.NewTicker(e.spec.HealthInterval)
	defer tick.Stop()

	fails := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-exited:
			return errOrExited(*waitErr)
		case <-e.restartReq:
			return errRestartRequested
		case <-tick.C:
		}

		if m.healthy(ctx, e, port) {
			fails = 0

			m.mu.Lock("recover")
			if e.status == StatusUnhealthy {
				e.status = StatusReady
			}
			m.mu.Unlock()

			continue
		}

		fails++

		m.setStatus(e, StatusUnhealthy, "health check failed")

		if fails >= e.spec.HealthFailures {
			return errUnhealthy
		}
	}
}

func (m *Manager) healthy(ctx context.Context, e *extension, port int) bool {
	ctx, cancel := context.WithTimeout(ctx, e.spec.HealthTimeout)
	defer cancel()

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	if e.spec.HealthPath == "" {
		var d net.Dialer

		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return false
		}

		_ = c.Close()

		return true
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+e.spec.HealthPath, nil)
	if err != nil {
		return false
	}

	resp, err := m.hookHTTP.Do(req)
	if err != nil {
		return false
	}

	_ = resp.Body.Close()

	return resp.StatusCode < http.StatusInternalServerError
}
