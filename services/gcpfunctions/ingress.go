package gcpfunctions

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
	"time"

	"cloud.google.com/go/functions/apiv2/functionspb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	localhostSuffix = ".localhost"
	maxEventBytes   = 10 << 20
)

// target is the routing view of one function.
type target struct {
	name     string
	topic    string
	timeout  time.Duration
	state    functionspb.Function_State
	hasEvent bool
	retry    bool
}

// IngressHandler serves <function>-<hash>-<region>.localhost invocations plus the reserved /_gcf/ endpoints
// (source upload and download, Pub/Sub push delivery).
func (b *InMemoryBackend) IngressHandler() http.Handler {
	return http.HandlerFunc(b.serveIngress)
}

func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		hostport = h
	}

	return strings.ToLower(hostport)
}

func (b *InMemoryBackend) serveIngress(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, uploadPrefix):
		b.serveUpload(w, r)
	case strings.HasPrefix(r.URL.Path, downloadPrefix):
		b.serveDownload(w, r)
	case strings.HasPrefix(r.URL.Path, eventsPrefix):
		b.serveEvent(w, r)
	default:
		b.serveInvoke(w, r)
	}
}

func (b *InMemoryBackend) serveUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "use PUT to upload the source archive", http.StatusMethodNotAllowed)

		return
	}

	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSourceBytes))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusRequestEntityTooLarge)

		return
	}

	if !b.storeUpload(strings.TrimPrefix(r.URL.Path, uploadPrefix), data) {
		http.Error(w, "unknown upload URL; call GenerateUploadUrl first", http.StatusForbidden)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (b *InMemoryBackend) serveDownload(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, downloadPrefix), ".zip")

	data := b.functionZip(name)
	if data == nil {
		http.Error(w, "no source for that function", http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

func (b *InMemoryBackend) targetOf(name string, rec *fnRec) target {
	et := rec.pb.GetEventTrigger()

	return target{
		name:     name,
		state:    rec.pb.GetState(),
		timeout:  time.Duration(rec.pb.GetServiceConfig().GetTimeoutSeconds()) * time.Second,
		hasEvent: et != nil,
		topic:    et.GetPubsubTopic(),
		retry:    et.GetRetryPolicy() == functionspb.EventTrigger_RETRY_POLICY_RETRY,
	}
}

func (b *InMemoryBackend) byLabel(label string) (target, bool) {
	b.mu.RLock("byLabel")
	defer b.mu.RUnlock()

	for name, rec := range b.functions {
		if n, err := parseName(name); err == nil && hostLabel(n) == label {
			return b.targetOf(name, rec), true
		}
	}

	return target{}, false
}

func (b *InMemoryBackend) byName(name string) (target, bool) {
	b.mu.RLock("byName")
	defer b.mu.RUnlock()

	if rec, ok := b.functions[name]; ok {
		return b.targetOf(name, rec), true
	}

	return target{}, false
}

// awaitDeploy waits, bounded by the startup timeout and ctx, for t's deployment to finish and returns its fresh view.
func (b *InMemoryBackend) awaitDeploy(ctx context.Context, t target) target {
	b.mu.RLock("awaitDeploy")

	var done chan struct{}
	if rec := b.functions[t.name]; rec != nil {
		done = rec.deployed
	}

	limit := b.startupTimeout
	b.mu.RUnlock()

	if done != nil {
		wctx, cancel := context.WithTimeout(ctx, limit)
		defer cancel()

		select {
		case <-done:
		case <-wctx.Done():
		}
	}

	if cur, ok := b.byName(t.name); ok {
		return cur
	}

	return t
}

// readyPort starts t's container if needed and returns its host port, or writes the error response.
func (b *InMemoryBackend) readyPort(w http.ResponseWriter, r *http.Request, t target) (int, bool) {
	if t.state == functionspb.Function_DEPLOYING {
		t = b.awaitDeploy(r.Context(), t)
	}

	switch t.state { //nolint:exhaustive // other states are serviceable or handled below
	case functionspb.Function_DEPLOYING:
		http.Error(w, "function is still deploying", http.StatusServiceUnavailable)

		return 0, false
	case functionspb.Function_DELETING:
		http.Error(w, "function is being deleted", http.StatusNotFound)

		return 0, false
	case functionspb.Function_FAILED:
		http.Error(w, "function failed to deploy", http.StatusServiceUnavailable)

		return 0, false
	}

	if err := b.startFunction(r.Context(), t.name); err != nil {
		code := http.StatusBadGateway
		if errors.Is(err, errNoRuntime) {
			code = http.StatusServiceUnavailable
		}

		http.Error(w, "function is not serving: "+err.Error(), code)

		return 0, false
	}

	b.mu.RLock("readyPort")
	defer b.mu.RUnlock()

	if rec := b.functions[t.name]; rec != nil && rec.hostPort != 0 {
		return rec.hostPort, true
	}

	http.Error(w, "function was removed", http.StatusServiceUnavailable)

	return 0, false
}

func (b *InMemoryBackend) serveInvoke(w http.ResponseWriter, r *http.Request) {
	label, ok := strings.CutSuffix(hostOnly(r.Host), localhostSuffix)
	if !ok {
		http.Error(w, "unknown host; use <function>-<hash>-<region>.localhost", http.StatusNotFound)

		return
	}

	t, found := b.byLabel(label)
	if !found {
		http.Error(w, "unknown function", http.StatusNotFound)

		return
	}

	port, ready := b.readyPort(w, r, t)
	if !ready {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), t.timeout)
	defer cancel()

	b.serveLogged(w, r.WithContext(ctx), port, b.invokeTarget(t.name))
}

type pushMessage struct {
	Attributes  map[string]string `json:"attributes"`
	Data        string            `json:"data"`
	MessageID   string            `json:"messageId"`
	PublishTime string            `json:"publishTime"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

type pushEnvelope struct {
	Subscription string      `json:"subscription"`
	Message      pushMessage `json:"message"`
}

// serveEvent converts a Pub/Sub push request into a CloudEvent and delivers it to the function.
func (b *InMemoryBackend) serveEvent(w http.ResponseWriter, r *http.Request) {
	t, ok := b.byName(strings.TrimPrefix(r.URL.Path, eventsPrefix))
	if !ok || !t.hasEvent {
		http.Error(w, "unknown event-triggered function", http.StatusNotFound)

		return
	}

	var env pushEnvelope
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEventBytes)).Decode(&env); err != nil {
		http.Error(w, "invalid push envelope: "+err.Error(), http.StatusBadRequest)

		return
	}

	if t.state == functionspb.Function_DEPLOYING {
		http.Error(w, "function is still deploying; retry", http.StatusServiceUnavailable)

		return
	}

	rec := &statusRecorder{header: http.Header{}, code: http.StatusOK}
	if port, ready := b.readyPort(rec, r, t); ready {
		b.deliver(r.Context(), rec, port, t, env)
	}

	if rec.code >= http.StatusBadRequest && !t.retry {
		w.WriteHeader(http.StatusNoContent)

		return
	}

	w.WriteHeader(rec.code)
}

func (b *InMemoryBackend) deliver(ctx context.Context, rec *statusRecorder, port int, t target, env pushEnvelope) {
	body, err := json.Marshal(env)
	if err != nil {
		rec.code = http.StatusInternalServerError

		return
	}

	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	url := "http://" + net.JoinHostPort(container.LoopbackHost, strconv.Itoa(port)) + "/"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		rec.code = http.StatusInternalServerError

		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Ce-Specversion", "1.0")
	req.Header.Set("Ce-Id", env.Message.MessageID)
	req.Header.Set("Ce-Type", EventTypePubSub)
	req.Header.Set("Ce-Source", "//pubsub.googleapis.com/"+t.topic)
	req.Header.Set("Ce-Time", env.Message.PublishTime)

	resp, err := (&http.Client{Transport: b.transport}).Do(req)
	if err != nil {
		rec.code = http.StatusBadGateway

		return
	}
	defer resp.Body.Close()

	rec.code = resp.StatusCode
}

// statusRecorder captures the status an internal step would have written.
type statusRecorder struct {
	header http.Header
	code   int
}

func (s *statusRecorder) Header() http.Header         { return s.header }
func (s *statusRecorder) Write(p []byte) (int, error) { return len(p), nil }
func (s *statusRecorder) WriteHeader(code int)        { s.code = code }
