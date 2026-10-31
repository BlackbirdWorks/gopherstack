package gcpfunctions

import (
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
	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

const (
	localhostSuffix = ".localhost"
	maxEventBytes   = 10 << 20
)

// target is the routing view of one function.
type target struct {
	name      string
	topic     string
	eventType string
	timeout   time.Duration
	state     functionspb.Function_State
	ingress   functionspb.ServiceConfig_IngressSettings
	hasEvent  bool
	retry     bool
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

func requestTimeout(sc *functionspb.ServiceConfig) time.Duration {
	if t := sc.GetTimeoutSeconds(); t > 0 {
		return time.Duration(t) * time.Second
	}

	return defaultTO * time.Second
}

func (b *InMemoryBackend) targetOf(name string, rec *fnRec) target {
	et := rec.pb.GetEventTrigger()

	return target{
		name:      name,
		state:     rec.pb.GetState(),
		timeout:   requestTimeout(rec.pb.GetServiceConfig()),
		ingress:   rec.pb.GetServiceConfig().GetIngressSettings(),
		hasEvent:  et != nil,
		topic:     et.GetPubsubTopic(),
		eventType: et.GetEventType(),
		retry:     et.GetRetryPolicy() == functionspb.EventTrigger_RETRY_POLICY_RETRY,
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

func internalRequest(r *http.Request) bool { return gcpserve.IsInternal(r) }

// viaGCLB reports whether a Google Cloud load balancer forwarded the request (it adds "Via: 1.1 google").
func viaGCLB(r *http.Request) bool {
	return strings.Contains(strings.ToLower(strings.Join(r.Header.Values("Via"), ",")), "1.1 google")
}

func ingressAllowed(mode functionspb.ServiceConfig_IngressSettings, r *http.Request) bool {
	switch mode { //nolint:exhaustive // unspecified and ALLOW_ALL accept every caller
	case functionspb.ServiceConfig_ALLOW_INTERNAL_ONLY:
		return internalRequest(r)
	case functionspb.ServiceConfig_ALLOW_INTERNAL_AND_GCLB:
		return internalRequest(r) || viaGCLB(r)
	}

	return true
}

// invoke runs r on an instance of t's function: it enforces ingress settings, waits for a deployment in flight,
// takes a concurrency slot (starting an instance when allowed), applies the timeout and proxies the request.
func (b *InMemoryBackend) invoke(w http.ResponseWriter, r *http.Request, t target) {
	if !ingressAllowed(t.ingress, r) {
		http.Error(w, "function does not accept requests from this source (ingress_settings)", http.StatusNotFound)

		return
	}

	if t.state == functionspb.Function_DEPLOYING {
		t = b.awaitDeploy(r.Context(), t)
	}

	switch t.state { //nolint:exhaustive // other states are serviceable
	case functionspb.Function_DEPLOYING:
		http.Error(w, "function is still deploying", http.StatusServiceUnavailable)

		return
	case functionspb.Function_DELETING:
		http.Error(w, "function is being deleted", http.StatusNotFound)

		return
	case functionspb.Function_FAILED:
		http.Error(w, "function failed to deploy", http.StatusServiceUnavailable)

		return
	}

	pool, err := b.poolFor(t.name)
	if err != nil {
		http.Error(w, "function was removed", http.StatusServiceUnavailable)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), t.timeout)
	defer cancel()

	bx, release, err := pool.Acquire(ctx)
	if err != nil {
		code, msg := acquireFailure(err)
		http.Error(w, msg, code)

		return
	}
	defer release()

	r = r.WithContext(ctx)
	r.Header.Del(gcpserve.InternalNetworkHeader)
	b.serveLogged(w, r, bx.port, b.invokeTarget(t.name, bx.id))
}

func acquireFailure(err error) (int, string) {
	switch {
	case errors.Is(err, gcpscale.ErrSaturated):
		return http.StatusTooManyRequests, "no instance available: " + err.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "the request timed out before an instance served it"
	case errors.Is(err, gcpscale.ErrClosed):
		return http.StatusServiceUnavailable, "function is being redeployed; retry"
	case errors.Is(err, errNoRuntime):
		return http.StatusServiceUnavailable, "function is not serving: " + err.Error()
	}

	return http.StatusBadGateway, "function is not serving: " + err.Error()
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

	b.invoke(w, r, t)
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

	body, err := json.Marshal(env)
	if err != nil {
		http.Error(w, "encode event: "+err.Error(), http.StatusInternalServerError)

		return
	}

	ev := gcpevents.CloudEvent{
		ID: env.Message.MessageID, Type: EventTypePubSub, Source: "//pubsub.googleapis.com/" + t.topic,
		Time: env.Message.PublishTime, ContentType: "application/json", Data: body,
	}

	rec := &statusRecorder{header: http.Header{}, code: http.StatusOK}
	b.deliver(r.Context(), rec, t, ev)

	if rec.code >= http.StatusBadRequest && !t.retry {
		w.WriteHeader(http.StatusNoContent)

		return
	}

	w.WriteHeader(rec.code)
}

// deliver sends ev to t's function the way Eventarc does: a POST in binary content mode from inside the network.
func (b *InMemoryBackend) deliver(ctx context.Context, rec *statusRecorder, t target, ev gcpevents.CloudEvent) {
	host := b.hostOf(t.name)

	req, err := ev.NewRequest(ctx, "http://"+host+"/")
	if err != nil {
		rec.code = http.StatusInternalServerError

		return
	}

	req.Host = host
	req.RemoteAddr = net.JoinHostPort(container.LoopbackHost, "0")
	gcpserve.MarkInternal(req)

	b.invoke(rec, req, t)
}

func (b *InMemoryBackend) hostOf(name string) string {
	n, err := parseName(name)
	if err != nil {
		return "localhost"
	}

	b.mu.RLock("hostOf")
	defer b.mu.RUnlock()

	return b.host(n)
}

// statusRecorder captures the status an internal step would have written.
type statusRecorder struct {
	header http.Header
	code   int
}

func (s *statusRecorder) Header() http.Header         { return s.header }
func (s *statusRecorder) Write(p []byte) (int, error) { return len(p), nil }
func (s *statusRecorder) WriteHeader(code int)        { s.code = code }
