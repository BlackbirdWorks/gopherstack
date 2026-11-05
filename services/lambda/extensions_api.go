package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Lambda Extensions API 2020-01-01:
// https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html
const (
	extensionPathPrefix = "/2020-01-01/extension/"

	headerExtensionName      = "Lambda-Extension-Name"
	headerExtensionID        = "Lambda-Extension-Identifier"
	headerExtensionEventID   = "Lambda-Extension-Event-Identifier"
	headerExtensionErrorType = "Lambda-Extension-Function-Error-Type"
	headerExtensionFeature   = "Lambda-Extension-Accept-Feature"

	extEventInvoke   = "INVOKE"
	extEventShutdown = "SHUTDOWN"

	shutdownReasonSpindown = "spindown"
	shutdownReasonTimeout  = "timeout"
	shutdownReasonFailure  = "failure"

	jsonKeyEventType = "eventType"

	maxExtensions                   = 10
	extensionEventBuffer            = 8
	defaultExtensionShutdownTimeout = 2 * time.Second
	defaultExtensionInitTimeout     = 10 * time.Second
	maxExtensionBodyBytes           = 1 << 20
)

// extensionMeta is the function identity returned to extensions on register.
type extensionMeta struct {
	functionName string
	version      string
	handler      string
	functionARN  string
	accountID    string
}

type extensionEvent struct {
	delivered chan struct{}
	id        string
	invokeReq string
	body      []byte
}

type extension struct {
	events     chan extensionEvent
	id         string
	name       string
	delivered  string
	subscribed []string
	invoke     bool
	shutdown   bool
	exited     bool
	nexted     bool
}

// invokeWait tracks the extensions that still owe a Next call for one invocation.
type invokeWait struct {
	pending  map[string]struct{}
	done     chan struct{}
	deadline time.Time
}

// extensionRegistry is the per-function state machine behind the Extensions API.
type extensionRegistry struct {
	mu              *lockmetrics.RWMutex
	exts            map[string]*extension
	names           map[string]struct{}
	waits           map[string]*invokeWait
	ready           chan struct{}
	closing         chan struct{}
	hub             *telemetryHub
	externalNames   map[string]struct{}
	meta            extensionMeta
	expected        int
	shutdownTimeout time.Duration
	initTimeout     time.Duration
	closeOnce       sync.Once
	initDone        bool
	shutdownSent    bool
	initFailed      bool
	holdCh          chan struct{}
	holding         bool
	externalKnown   bool
}

func newExtensionRegistry() *extensionRegistry {
	return &extensionRegistry{
		mu:              lockmetrics.New("lambda.extensions"),
		exts:            make(map[string]*extension),
		names:           make(map[string]struct{}),
		waits:           make(map[string]*invokeWait),
		closing:         make(chan struct{}),
		shutdownTimeout: defaultExtensionShutdownTimeout,
		initTimeout:     defaultExtensionInitTimeout,
	}
}

func (r *extensionRegistry) close() {
	r.closeOnce.Do(func() {
		close(r.closing)
		r.mu.Close()
	})
}

// setExternal records the external extension files the launcher will start. Registrations under
// any other name come from the runtime process (internal extensions). The runtime's first Next
// waits until every external extension registered and called Next (Extension init phase).
func (r *extensionRegistry) setExternal(names []string) {
	r.mu.Lock("setExternal")
	defer r.mu.Unlock()

	r.externalKnown = true
	r.externalNames = make(map[string]struct{}, len(names))

	for _, n := range names {
		r.externalNames[n] = struct{}{}
	}

	r.endHoldLocked()

	if len(names) == 0 {
		r.markReadyLocked()

		return
	}

	r.expected = len(names)

	if r.ready == nil {
		r.ready = make(chan struct{})
	}

	r.markReadyLocked()
}

// holdInit makes the runtime's first Next wait until setExternal or releaseInit runs; used when
// the external extensions are only discovered after the container started.
func (r *extensionRegistry) holdInit() {
	r.mu.Lock("holdInit")
	r.holding = true
	r.holdCh = make(chan struct{})
	r.ready = make(chan struct{})
	r.mu.Unlock()
}

func (r *extensionRegistry) endHoldLocked() {
	if r.holding {
		close(r.holdCh)
	}

	r.holding = false
}

// awaitHold blocks while the external extension set is still being discovered.
func (r *extensionRegistry) awaitHold(ctx context.Context) {
	r.mu.RLock("awaitHold")
	ch, timeout := r.holdCh, r.initTimeout
	holding := r.holding
	r.mu.RUnlock()

	if !holding {
		return
	}

	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case <-ch:
	case <-t.C:
	case <-ctx.Done():
	case <-r.closing:
	}
}

// releaseInit ends a holdInit without knowing the extension set.
func (r *extensionRegistry) releaseInit() {
	r.mu.Lock("releaseInit")
	r.endHoldLocked()
	r.markReadyLocked()
	r.mu.Unlock()
}

// lookup reports an extension's name and whether registrations/subscriptions are already closed.
func (r *extensionRegistry) lookup(id string) (string, bool, bool) {
	r.mu.RLock("lookup")
	defer r.mu.RUnlock()

	ext, ok := r.exts[id]
	if !ok || ext.exited {
		return "", false, false
	}

	return ext.name, true, r.initDone || r.shutdownSent
}

func (r *extensionRegistry) setMeta(m extensionMeta) {
	r.mu.Lock("setMeta")
	r.meta = m
	r.mu.Unlock()
}

func (r *extensionRegistry) markReadyLocked() {
	if r.ready == nil {
		return
	}

	if r.holding {
		return
	}

	if (len(r.exts) >= r.expected && r.allInitedLocked()) || r.initFailed {
		select {
		case <-r.ready:
		default:
			close(r.ready)
		}
	}
}

func (r *extensionRegistry) allInitedLocked() bool {
	for _, e := range r.exts {
		if !e.nexted && !e.exited {
			return false
		}
	}

	return true
}

// awaitInit blocks the runtime's first Next until expected extensions registered.
func (r *extensionRegistry) awaitInit(ctx context.Context) {
	r.mu.RLock("awaitInit")
	ready, timeout, done := r.ready, r.initTimeout, r.initDone
	r.mu.RUnlock()

	if done {
		return
	}

	if ready != nil {
		t := time.NewTimer(timeout)
		defer t.Stop()

		select {
		case <-ready:
		case <-t.C:
			logger.Load(ctx).WarnContext(ctx, "lambda: extensions did not register before init timeout")
		case <-ctx.Done():
			return
		case <-r.closing:
			return
		}
	}

	r.mu.Lock("awaitInit-done")
	r.initDone = true
	r.mu.Unlock()
}

type extensionError struct {
	Type    string `json:"errorType"`
	Message string `json:"errorMessage"`
}

type registerRequest struct {
	Events []string `json:"events"`
}

type registerResponse struct {
	FunctionName    string `json:"functionName"`
	FunctionVersion string `json:"functionVersion"`
	Handler         string `json:"handler"`
	AccountID       string `json:"accountId,omitempty"`
}

func writeExtError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(extensionError{Type: typ, Message: msg})
}

// register adds an extension; it returns the response, identifier or an HTTP failure.
func (r *extensionRegistry) register(
	name string, events []string, withAccount bool,
) (registerResponse, string, int, *extensionError) {
	var invoke, shutdown bool

	for _, e := range events {
		switch e {
		case extEventInvoke:
			invoke = true
		case extEventShutdown:
			shutdown = true
		default:
			return registerResponse{}, "", http.StatusBadRequest,
				&extensionError{"Extension.InvalidEventType", "unsupported event type " + e}
		}
	}

	r.mu.Lock("register")
	defer r.mu.Unlock()

	switch {
	case r.initDone || r.shutdownSent:
		return registerResponse{}, "", http.StatusForbidden,
			&extensionError{"Extension.RegistrationClosed", "extensions can only register during the Init phase"}
	case len(r.exts) >= maxExtensions:
		return registerResponse{}, "", http.StatusForbidden,
			&extensionError{"Extension.TooManyExtensions", "at most 10 extensions can be registered"}
	}

	if shutdown && r.externalKnown {
		if _, external := r.externalNames[name]; !external {
			return registerResponse{}, "", http.StatusBadRequest,
				&extensionError{"Extension.InvalidEventType", "internal extensions cannot register for the SHUTDOWN event"}
		}
	}

	if _, dup := r.names[name]; dup {
		return registerResponse{}, "", http.StatusForbidden,
			&extensionError{"Extension.AlreadyRegistered", "extension " + name + " is already registered"}
	}

	ext := &extension{
		id: uuid.NewString(), name: name, invoke: invoke, shutdown: shutdown, subscribed: events,
		events: make(chan extensionEvent, extensionEventBuffer),
	}
	r.exts[ext.id] = ext
	r.names[name] = struct{}{}
	r.markReadyLocked()

	resp := registerResponse{
		FunctionName: r.meta.functionName, FunctionVersion: r.meta.version, Handler: r.meta.handler,
	}
	if withAccount {
		resp.AccountID = r.meta.accountID
	}

	return resp, ext.id, http.StatusOK, nil
}

// ackLocked records that ext asked for its next event, completing any delivered invocation.
func (r *extensionRegistry) ackLocked(ext *extension) {
	req := ext.delivered
	if req == "" {
		return
	}

	ext.delivered = ""

	w, ok := r.waits[req]
	if !ok {
		return
	}

	delete(w.pending, ext.id)

	if len(w.pending) == 0 {
		close(w.done)
		delete(r.waits, req)
	}
}

// fanoutInvoke queues an INVOKE event for every extension subscribed to it.
func (r *extensionRegistry) fanoutInvoke(reqID string, deadline time.Time, fnARN, trace string) {
	r.mu.Lock("fanoutInvoke")
	defer r.mu.Unlock()

	body := map[string]any{
		jsonKeyEventType:     extEventInvoke,
		"deadlineMs":         deadline.UnixMilli(),
		"requestId":          reqID,
		"invokedFunctionArn": fnARN,
	}
	if trace != "" {
		body["tracing"] = map[string]string{"type": "X-Amzn-Trace-Id", "value": trace}
	}

	raw, _ := json.Marshal(body)
	wait := &invokeWait{pending: map[string]struct{}{}, done: make(chan struct{}), deadline: deadline}

	for id, ext := range r.exts {
		if !ext.invoke || ext.exited {
			continue
		}

		select {
		case ext.events <- extensionEvent{id: uuid.NewString(), invokeReq: reqID, body: raw}:
			wait.pending[id] = struct{}{}
		default:
		}
	}

	if len(wait.pending) > 0 {
		r.waits[reqID] = wait
	}
}

// awaitIdle blocks until no extension owes a Next call for a finished invocation
// (the Invoke phase ends only then), bounded by that invocation's deadline.
func (r *extensionRegistry) awaitIdle(ctx context.Context) {
	for {
		r.mu.RLock("awaitIdle")

		var (
			id string
			w  *invokeWait
		)

		for k, v := range r.waits {
			id, w = k, v

			break
		}

		r.mu.RUnlock()

		if w == nil {
			return
		}

		t := time.NewTimer(time.Until(w.deadline))

		select {
		case <-w.done:
			t.Stop()
		case <-t.C:
			logger.Load(ctx).WarnContext(ctx, "lambda: extensions did not finish before the invocation deadline",
				"requestId", id)
			r.dropWait(id)
		case <-ctx.Done():
			t.Stop()

			return
		case <-r.closing:
			t.Stop()

			return
		}
	}
}

func (r *extensionRegistry) dropWait(id string) {
	r.mu.Lock("dropWait")
	delete(r.waits, id)
	r.mu.Unlock()
}

var errExtensionUnknown = errors.New("unknown extension identifier")

// next returns the extension's next event, acking its previous one.
func (r *extensionRegistry) next(ctx context.Context, id string) (extensionEvent, error) {
	r.mu.Lock("next")

	ext, ok := r.exts[id]
	if !ok || ext.exited {
		r.mu.Unlock()

		return extensionEvent{}, errExtensionUnknown
	}

	r.ackLocked(ext)

	first := !ext.nexted
	ext.nexted = true
	events := ext.events

	if first {
		r.markReadyLocked()
	}

	name, subscribed, hub := ext.name, ext.subscribed, r.hub
	r.mu.Unlock()

	if first && hub != nil {
		hub.extensionReady(name, subscribed)
	}

	select {
	case ev := <-events:
		if ev.invokeReq != "" {
			r.mu.Lock("next-delivered")
			ext.delivered = ev.invokeReq
			r.mu.Unlock()
		}

		if ev.delivered != nil {
			close(ev.delivered)
		}

		return ev, nil
	case <-ctx.Done():
		return extensionEvent{}, ctx.Err()
	case <-r.closing:
		return extensionEvent{}, context.Canceled
	}
}

// fail marks an extension as exited after init/error or exit/error.
func (r *extensionRegistry) fail(id string, init bool) bool {
	r.mu.Lock("fail")
	defer r.mu.Unlock()

	ext, ok := r.exts[id]
	if !ok || ext.exited {
		return false
	}

	r.ackLocked(ext)
	ext.exited = true

	if init {
		r.initFailed = true
		r.markReadyLocked()
	}

	return true
}

// shutdown sends SHUTDOWN to subscribed extensions and waits until each took it,
// at most shutdownTimeout.
func (r *extensionRegistry) shutdown(ctx context.Context, reason string) {
	r.mu.Lock("shutdown")

	if r.shutdownSent {
		r.mu.Unlock()

		return
	}

	r.shutdownSent = true
	deadline := time.Now().Add(r.shutdownTimeout)
	raw, _ := json.Marshal(map[string]any{
		jsonKeyEventType: extEventShutdown, "shutdownReason": reason, "deadlineMs": deadline.UnixMilli(),
	})

	var taken []chan struct{}

	for _, ext := range r.exts {
		if !ext.shutdown || ext.exited {
			continue
		}

		ev := extensionEvent{id: uuid.NewString(), body: raw, delivered: make(chan struct{})}

		select {
		case ext.events <- ev:
			taken = append(taken, ev.delivered)
		default:
		}
	}
	r.mu.Unlock()

	t := time.NewTimer(time.Until(deadline))
	defer t.Stop()

	for _, c := range taken {
		select {
		case <-c:
		case <-t.C:
			logger.Load(ctx).WarnContext(ctx, "lambda: extension did not take the SHUTDOWN event in time")

			return
		case <-ctx.Done():
			return
		}
	}
}

// handleExtensionRegister serves POST /2020-01-01/extension/register.
func (s *runtimeServer) handleExtensionRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	s.exts.awaitHold(r.Context())

	name := r.Header.Get(headerExtensionName)
	if name == "" || strings.ContainsAny(name, "/\\") {
		writeExtError(w, http.StatusBadRequest, "Extension.InvalidExtensionName",
			"Lambda-Extension-Name header must carry the extension's file name")

		return
	}

	var req registerRequest

	body, _ := io.ReadAll(http.MaxBytesReader(w, r.Body, maxExtensionBodyBytes))
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeExtError(w, http.StatusBadRequest, "Extension.InvalidRequestBody", "malformed registration body")

			return
		}
	}

	withAccount := false

	for f := range strings.SplitSeq(r.Header.Get(headerExtensionFeature), ",") {
		if strings.TrimSpace(f) == "accountId" {
			withAccount = true
		}
	}

	resp, id, status, regErr := s.exts.register(name, req.Events, withAccount)
	if regErr != nil {
		writeExtError(w, status, regErr.Type, regErr.Message)

		return
	}

	w.Header().Set(headerExtensionID, id)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleExtensionNext serves GET /2020-01-01/extension/event/next (long poll).
func (s *runtimeServer) handleExtensionNext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ev, err := s.exts.next(r.Context(), r.Header.Get(headerExtensionID))
	if err != nil {
		if errors.Is(err, errExtensionUnknown) {
			writeExtError(w, http.StatusForbidden, "Extension.InvalidExtensionID", "unknown extension identifier")
		}

		return
	}

	w.Header().Set(headerExtensionEventID, ev.id)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(ev.body)
}

// handleExtensionError serves POST /extension/init/error and /extension/exit/error.
func (s *runtimeServer) handleExtensionError(init bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		errType := r.Header.Get(headerExtensionErrorType)
		if errType == "" {
			writeExtError(w, http.StatusBadRequest, "Extension.MissingErrorType",
				"Lambda-Extension-Function-Error-Type header is required")

			return
		}

		body, _ := io.ReadAll(http.MaxBytesReader(w, r.Body, maxExtensionBodyBytes))

		if !s.exts.fail(r.Header.Get(headerExtensionID), init) {
			writeExtError(w, http.StatusForbidden, "Extension.InvalidExtensionID", "unknown extension identifier")

			return
		}

		logger.Load(r.Context()).ErrorContext(r.Context(), "lambda extension reported an error",
			"init", init, "errorType", errType, "body", string(body))

		if init {
			s.failPending(body)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"OK"}`))
	}
}

func functionVersionOrLatest(v string) string {
	if v == "" {
		return latestVersion
	}

	return v
}
