package azurefunctions

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	kuduCommand   = "/api/command"
	kuduLogstream = "/api/logstream"
	homeDir       = "/home"
	maxCommandReq = 1 << 20
	commandLimit  = 10 * time.Minute
)

var errExecUnavailable = errors.New("the container runtime cannot run commands")

type commandRequest struct {
	Command string `json:"command"`
	Dir     string `json:"dir"`
}

// commandResult uses the capitalised keys Kudu answers with (Kudu CommandController, CommandResult).
type commandResult struct {
	Output   string `json:"Output"`
	Error    string `json:"Error"`
	ExitCode int    `json:"ExitCode"`
}

// kuduDir maps a Kudu directory (Windows or POSIX separators, relative to /home) to a path in the host container.
func kuduDir(dir string) string {
	d := strings.ReplaceAll(strings.TrimSpace(dir), "\\", "/")
	if d == "" {
		return siteRoot
	}

	if !strings.HasPrefix(d, "/") {
		d = homeDir + "/" + d
	}

	return path.Clean(d)
}

// hostExec starts the app's host if needed and returns the container to run commands in.
func (b *InMemoryBackend) hostExec(ctx context.Context, name string) (container.Execer, string, error) {
	if _, err := b.ensureStarted(ctx, name); err != nil {
		return nil, "", err
	}

	b.mu.RLock("hostExec")
	a, err := b.appLocked(name)

	id := ""
	if err == nil {
		id = a.containerID
	}
	b.mu.RUnlock()

	if err != nil {
		return nil, "", err
	}

	runner, err := b.runner()
	if err != nil {
		return nil, "", err
	}

	ex, ok := runner.(container.Execer)
	if !ok || id == "" {
		return nil, "", errExecUnavailable
	}

	return ex, id, nil
}

// RunCommand runs a shell command in dir inside the app's host container.
func (b *InMemoryBackend) RunCommand(ctx context.Context, name, command, dir string) (container.ExecResult, error) {
	ctx, cancel := context.WithTimeout(ctx, commandLimit)
	defer cancel()

	ex, id, err := b.hostExec(ctx, name)
	if err != nil {
		return container.ExecResult{}, err
	}

	return ex.Exec(ctx, id, []string{"sh", "-c", "cd " + shQuote(kuduDir(dir)) + " && " + command})
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

func execErrorCode(err error) int {
	if errors.Is(err, ErrAppNotFound) {
		return http.StatusNotFound
	}

	return http.StatusServiceUnavailable
}

func (b *InMemoryBackend) kuduCommand(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST to run a command", http.StatusMethodNotAllowed)

		return
	}

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCommandReq))

	var req commandRequest
	if err != nil || json.Unmarshal(raw, &req, jsonx.V1()) != nil || strings.TrimSpace(req.Command) == "" {
		http.Error(w, "the request body must be JSON with a command", http.StatusBadRequest)

		return
	}

	res, err := b.RunCommand(r.Context(), name, req.Command, req.Dir)
	if err != nil {
		http.Error(w, err.Error(), execErrorCode(err))

		return
	}

	writeJSON(w, commandResult{Output: res.Stdout, Error: res.Stderr, ExitCode: res.ExitCode})
}

type flushWriter struct {
	w http.ResponseWriter
	f http.Flusher
}

func (fw flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	fw.f.Flush()

	return n, err
}

func (b *InMemoryBackend) kuduLogstream(w http.ResponseWriter, r *http.Request, name string) {
	ctx := r.Context()

	if _, err := b.ensureStarted(ctx, name); err != nil {
		http.Error(w, err.Error(), execErrorCode(err))

		return
	}

	b.mu.RLock("kuduLogstream")
	a, err := b.appLocked(name)

	id := ""
	if err == nil {
		id = a.containerID
	}
	b.mu.RUnlock()

	runner, rerr := b.runner()

	lr, ok := runner.(container.LogReader)
	if err != nil || rerr != nil || !ok || id == "" {
		http.Error(w, "the container runtime cannot stream logs", http.StatusServiceUnavailable)

		return
	}

	f, _ := w.(http.Flusher)
	if f == nil {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	f.Flush()

	out := flushWriter{w: w, f: f}
	_ = lr.ReadLogs(ctx, id, "", true, out, out)
}
