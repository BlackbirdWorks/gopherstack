package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// handleSubscribe serves PUT /2022-07-01/telemetry and PUT /2020-08-15/logs.
func (s *runtimeServer) handleSubscribe(api string) http.HandlerFunc {
	errType := validationErrorType(api)

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		id := r.Header.Get(headerExtensionID)
		if id == "" {
			writeExtError(w, http.StatusBadRequest, errType, "Lambda-Extension-Identifier header is required")

			return
		}

		name, known, closed := s.exts.lookup(id)

		switch {
		case !known:
			writeExtError(w, http.StatusForbidden, "Extension.InvalidExtensionID", "unknown extension identifier")

			return
		case closed:
			writeExtError(w, http.StatusForbidden, "Extension.RegistrationClosed",
				"subscriptions are only accepted during the Init phase")

			return
		}

		var req subscribeRequest

		body, _ := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSubscribeBodyBytes))
		if err := json.Unmarshal(body, &req); err != nil {
			writeExtError(w, http.StatusBadRequest, errType, "malformed subscription body")

			return
		}

		sub, err := validateSubscription(api, &req)
		if err != nil {
			writeExtError(w, http.StatusBadRequest, errType, err.Error())

			return
		}

		host, _, splitErr := net.SplitHostPort(r.RemoteAddr)
		if splitErr != nil {
			host = ""
		}

		if err = s.tel.subscribe(id, name, host, sub); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, errHubClosed) {
				status = http.StatusInternalServerError
			}

			writeExtError(w, status, errType, err.Error())

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`"OK"`))
	}
}

// extensionLogMarker prefixes lines the launcher forwards from extension processes.
const extensionLogMarker = "\x1egopherstack-extension\x1e"

const maxLogLineBytes = 256 * 1024

// lineSplitter turns container output into function and extension log lines.
type lineSplitter struct {
	hub *telemetryHub
	buf []byte
}

func (l *lineSplitter) Write(p []byte) (int, error) {
	l.buf = append(l.buf, p...)

	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			break
		}

		l.emit(string(l.buf[:i]))
		l.buf = l.buf[i+1:]
	}

	if len(l.buf) > maxLogLineBytes {
		l.emit(string(l.buf))
		l.buf = nil
	}

	return len(p), nil
}

func (l *lineSplitter) flush() {
	if len(l.buf) > 0 {
		l.emit(string(l.buf))
		l.buf = nil
	}
}

func (l *lineSplitter) emit(line string) {
	line = strings.TrimSuffix(line, "\r")

	if rest, ok := strings.CutPrefix(line, extensionLogMarker); ok {
		l.hub.logLine(typeExtension, rest)

		return
	}

	l.hub.logLine(typeFunction, line)
}

// followLogs streams the container's output into the hub until ctx ends or the container goes away.
func (s *runtimeServer) followLogs(ctx context.Context, ls container.LogStreamer, containerID string) {
	out := &lineSplitter{hub: s.tel}
	errw := &lineSplitter{hub: s.tel}

	go func() {
		_ = ls.StreamLogs(ctx, containerID, out, errw)

		out.flush()
		errw.flush()
	}()
}

// attachContainer wires log streaming and the memory probe to the started container.
func (b *InMemoryBackend) attachContainer(srv *runtimeServer, containerID string) {
	if srv == nil || containerID == "" || b.docker == nil {
		return
	}

	if ls, ok := b.docker.(container.LogStreamer); ok {
		srv.tel.setLogSource(func() { srv.startLogFollow(ls, containerID) })
	}

	docker := b.docker

	srv.tel.ph.setMemoryProbe(func(ctx context.Context) (int64, bool) {
		res, err := docker.Exec(ctx, containerID, []string{
			shellPath, "-c",
			"cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes",
		})
		if err != nil || res.ExitCode != 0 {
			return 0, false
		}

		v, parseErr := strconv.ParseInt(strings.TrimSpace(res.Stdout), 10, 64)

		return v, parseErr == nil && v > 0
	})
}

func (s *runtimeServer) startLogFollow(ls container.LogStreamer, containerID string) {
	s.logOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.logCancel.Store(&cancel)
		s.followLogs(ctx, ls, containerID)
	})
}

// configureTelemetry stamps the function identity into platform events.
func (s *runtimeServer) configureTelemetry(fn *FunctionConfiguration) {
	s.tel.ph.setIdentity(hubIdentity{
		functionName: fn.FunctionName, version: functionVersionOrLatest(fn.Version), memoryMB: fn.MemorySize,
	})
	s.tel.setJSONLogs(fn.LoggingConfig != nil && fn.LoggingConfig.LogFormat == "JSON")
}
