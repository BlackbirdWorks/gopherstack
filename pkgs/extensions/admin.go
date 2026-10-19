package extensions

import (
	"errors"
	"net/http"
	"strings"
)

const maxOutputLines = 500

// Errors returned by the management operations.
var (
	ErrUnknownExtension = errors.New("unknown extension")
	ErrNotRunning       = errors.New("extension manager is not running")
)

func (m *Manager) find(name string) *extension {
	for _, e := range m.exts {
		if e.spec.Name == name {
			return e
		}
	}

	return nil
}

func (m *Manager) appendOutput(e *extension, line string) {
	m.mu.Lock("appendOutput")
	defer m.mu.Unlock()

	e.output = append(e.output, line)
	if n := len(e.output) - maxOutputLines; n > 0 {
		e.output = append([]string(nil), e.output[n:]...)
	}
}

// Output returns the recent captured stdout/stderr lines (already redacted).
func (m *Manager) Output(name string) ([]string, error) {
	m.mu.RLock("Output")
	defer m.mu.RUnlock()

	e := m.find(name)
	if e == nil {
		return nil, ErrUnknownExtension
	}

	return append([]string{}, e.output...), nil
}

// Restart recycles a running extension, or relaunches one that failed or stopped.
func (m *Manager) Restart(name string) error {
	m.mu.Lock("Restart")
	defer m.mu.Unlock()

	e := m.find(name)
	if e == nil {
		return ErrUnknownExtension
	}

	if m.runCtx == nil || m.stopping.Load() {
		return ErrNotRunning
	}

	if e.running {
		select {
		case e.restartReq <- struct{}{}:
		default:
		}

		return nil
	}

	e.running, e.restarts, e.lastError = true, 0, ""
	e.status = StatusStarting

	ctx := m.runCtx

	m.wg.Go(func() { m.supervise(ctx, e) })

	return nil
}

// ServeRestart handles POST /_gopherstack/extensions/{name}/restart (loopback only).
func (m *Manager) ServeRestart(w http.ResponseWriter, r *http.Request, name string) {
	if !isLoopback(r.RemoteAddr) {
		writeJSON(w, http.StatusForbidden, map[string]string{keyMessage: "management API is loopback-only"})

		return
	}

	if !m.writeAdminError(w, m.Restart(name)) {
		writeJSON(w, http.StatusAccepted, map[string]string{keyName: name, "status": string(StatusRestarting)})
	}
}

// ServeOutput handles GET /_gopherstack/extensions/{name}/output (loopback only).
func (m *Manager) ServeOutput(w http.ResponseWriter, r *http.Request, name string) {
	if !isLoopback(r.RemoteAddr) {
		writeJSON(w, http.StatusForbidden, map[string]string{keyMessage: "management API is loopback-only"})

		return
	}

	lines, err := m.Output(name)
	if m.writeAdminError(w, err) {
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"name": name, "lines": lines})
}

func (m *Manager) writeAdminError(w http.ResponseWriter, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrUnknownExtension):
		writeJSON(w, http.StatusNotFound, map[string]string{keyMessage: err.Error()})
	default:
		writeJSON(w, http.StatusConflict, map[string]string{keyMessage: strings.TrimSpace(err.Error())})
	}

	return true
}
