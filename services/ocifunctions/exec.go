package ocifunctions

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Fn FDK contract: the function image listens on a unix socket named by FN_LISTENER and serves
// POST /call (FN_FORMAT=http-stream); the socket lives in a directory shared with the host.
const (
	fdkSocketDir  = "/tmp/iofs"
	fdkSocketName = "lsnr.sock"
	fdkCallPath   = "/call"
	fdkFormat     = "http-stream"
	maxSockPath   = 100
	readyPoll     = 25 * time.Millisecond
	teardownLimit = 15 * time.Second
	maxPayload    = 6 << 20
	hdrCallID     = "Fn-Call-Id"
	hdrDeadline   = "Fn-Deadline"
	hdrHTTPMethod = "Fn-Http-Method"
	hdrHTTPURL    = "Fn-Http-Request-Url"
	hdrHTTPPrefix = "Fn-Http-H-"
	callIDLen     = 26
	nameSuffixLen = 8
)

// Runner is the slice of the container runtime that function execution needs.
type Runner = gcpserve.Runner

type imageStore interface {
	PullImage(ctx context.Context, imageRef string) error
	HasImage(ctx context.Context, imageRef string) (bool, error)
}

var (
	errEngineOff = errors.New("function engine is disabled")
	errNoRuntime = errors.New("no container runtime is configured")
)

// dropForward lists request headers that are never forwarded to the function as Fn-Http-H-*.
//
//nolint:gochecknoglobals // immutable
var dropForward = map[string]bool{
	"Authorization": true, "Date": true, "X-Date": true, "Host": true, "Content-Length": true,
	"X-Content-Sha256": true, "Connection": true, "Accept-Encoding": true, "User-Agent": true,
	"Opc-Request-Id": true, "Opc-Client-Info": true, "Opc-Retry-Token": true, "Transfer-Encoding": true,
}

// execPlan is everything needed to run one function, captured under the backend lock.
type execPlan struct {
	fnID, fnName, appID, appName, image, sig string
	env                                      []string
	timeout, detached                        time.Duration
}

// InvokeIn is one invocation request.
type InvokeIn struct {
	Header   http.Header
	URL      string
	Body     []byte
	Detached bool
	DryRun   bool
}

// InvokeOut is the function's response.
type InvokeOut struct {
	Header http.Header
	CallID string
	Body   []byte
	Status int
}

type warm struct {
	last   time.Time
	client *http.Client
	id     string
	dir    string
	sig    string
}

type executor struct {
	runCtx   context.Context
	idle     map[string][]*warm
	runtime  func() (Runner, error)
	onClose  func()
	now      func() time.Time
	cancel   context.CancelFunc
	mu       *lockmetrics.RWMutex
	noEngine bool
	wg       sync.WaitGroup
	startup  time.Duration
	idleTTL  time.Duration
	maxLive  int
	busy     int
	unit     time.Duration
}

func newExecutor(now func() time.Time) *executor {
	ctx, cancel := context.WithCancel(context.Background())
	d := DefaultSettings()

	return &executor{
		mu: lockmetrics.New("ocifunctions.exec"), now: now, runCtx: ctx, cancel: cancel, idle: map[string][]*warm{},
		startup: d.StartupTimeout, idleTTL: d.IdleTimeout, maxLive: d.MaxContainers,
		unit: time.Second,
	}
}

// Configure applies the engine, timeout and capacity settings.
func (b *InMemoryBackend) Configure(s Settings) {
	e := b.exec
	e.mu.Lock("Configure")
	defer e.mu.Unlock()

	e.noEngine = s.DisableEngine
	if s.StartupTimeout > 0 {
		e.startup = s.StartupTimeout
	}

	if s.IdleTimeout > 0 {
		e.idleTTL = s.IdleTimeout
	}

	if s.MaxContainers > 0 {
		e.maxLive = s.MaxContainers
	}
}

// SetRuntime installs the container runtime factory used to run functions.
func (b *InMemoryBackend) SetRuntime(fn func() (Runner, error)) {
	b.exec.mu.Lock("SetRuntime")
	b.exec.runtime = fn
	b.exec.mu.Unlock()
}

// SetOnClose registers a callback run by Close, used to release the runtime client.
func (b *InMemoryBackend) SetOnClose(fn func()) {
	b.exec.mu.Lock("SetOnClose")
	b.exec.onClose = fn
	b.exec.mu.Unlock()
}

// Close stops detached invocations, removes every function container and releases the runtime.
func (b *InMemoryBackend) Close() {
	e := b.exec
	e.cancel()
	e.wg.Wait()
	e.drain()

	e.mu.Lock("Close")
	fn := e.onClose
	e.mu.Unlock()

	if fn != nil {
		fn()
	}
}

// SetTimeoutUnit changes what one function timeout second lasts; tests shrink it to exercise timeouts quickly.
func (b *InMemoryBackend) SetTimeoutUnit(d time.Duration) {
	b.exec.mu.Lock("SetTimeoutUnit")
	b.exec.unit = d
	b.exec.mu.Unlock()
}

// ReapIdle removes warm containers unused for longer than the idle timeout.
func (b *InMemoryBackend) ReapIdle() { b.exec.reap() }

// LiveContainers reports how many function containers exist (busy plus warm).
func (b *InMemoryBackend) LiveContainers() int {
	e := b.exec
	e.mu.RLock("LiveContainers")
	defer e.mu.RUnlock()

	n := e.busy
	for _, ws := range e.idle {
		n += len(ws)
	}

	return n
}

func (e *executor) runner() (Runner, error) {
	e.mu.RLock("runner")
	noEngine, fn := e.noEngine, e.runtime
	e.mu.RUnlock()

	if noEngine {
		return nil, errEngineOff
	}

	if fn == nil {
		return nil, errNoRuntime
	}

	r, err := fn()
	if err != nil {
		return nil, fmt.Errorf("container runtime unavailable: %w", err)
	}

	return r, nil
}

func (e *executor) teardown(w *warm) {
	if w == nil {
		return
	}

	if w.client != nil {
		w.client.CloseIdleConnections()
	}

	if r, err := e.runner(); err == nil && w.id != "" {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(e.runCtx), teardownLimit)

		if stopErr := r.StopAndRemove(ctx, w.id); stopErr != nil {
			logger.Load(ctx).
				WarnContext(ctx, "ocifunctions: container removal failed", "container", w.id, "error", stopErr)
		}

		cancel()
	}

	if w.dir != "" {
		_ = os.RemoveAll(w.dir)
	}
}

func (e *executor) drain() {
	e.mu.Lock("drain")
	var all []*warm

	for k, ws := range e.idle {
		all = append(all, ws...)
		delete(e.idle, k)
	}
	e.mu.Unlock()

	for _, w := range all {
		e.teardown(w)
	}
}

func (e *executor) dropFunction(fnID string) {
	e.mu.Lock("dropFunction")
	ws := e.idle[fnID]
	delete(e.idle, fnID)
	e.mu.Unlock()

	for _, w := range ws {
		e.teardown(w)
	}
}

func (e *executor) reap() {
	e.mu.Lock("reap")
	now, ttl := e.now().UTC(), e.idleTTL

	var stale []*warm

	for k, ws := range e.idle {
		keep := ws[:0:0]

		for _, w := range ws {
			if now.Sub(w.last) > ttl {
				stale = append(stale, w)
			} else {
				keep = append(keep, w)
			}
		}

		if len(keep) == 0 {
			delete(e.idle, k)
		} else {
			e.idle[k] = keep
		}
	}
	e.mu.Unlock()

	for _, w := range stale {
		e.teardown(w)
	}
}

// acquire hands out a warm container whose configuration matches the plan, or reserves capacity for a new one.
// ok is false when the container cap is reached and nothing idle can be evicted.
func (e *executor) acquire(p execPlan) (*warm, bool) {
	e.mu.Lock("acquire")

	var w *warm

	var evict []*warm

	ws := e.idle[p.fnID]
	for len(ws) > 0 {
		last := ws[len(ws)-1]
		ws = ws[:len(ws)-1]

		if last.sig == p.sig {
			w = last

			break
		}

		evict = append(evict, last)
	}

	e.idle[p.fnID] = ws

	ok := true

	if w == nil {
		evict = append(evict, e.makeRoomLocked()...)
		ok = e.busy+e.idleCountLocked() < e.maxLive
	}

	if ok {
		e.busy++
	}
	e.mu.Unlock()

	for _, v := range evict {
		e.teardown(v)
	}

	return w, ok
}

func (e *executor) idleCountLocked() int {
	n := 0
	for _, ws := range e.idle {
		n += len(ws)
	}

	return n
}

// makeRoomLocked evicts the least recently used warm container when the cap is reached.
func (e *executor) makeRoomLocked() []*warm {
	if e.busy+e.idleCountLocked() < e.maxLive {
		return nil
	}

	var (
		oldest *warm
		key    string
	)

	for k, ws := range e.idle {
		for _, w := range ws {
			if oldest == nil || w.last.Before(oldest.last) {
				oldest, key = w, k
			}
		}
	}

	if oldest == nil {
		return nil
	}

	e.idle[key] = slices.DeleteFunc(e.idle[key], func(w *warm) bool { return w == oldest })

	return []*warm{oldest}
}

func (e *executor) abandon() {
	e.mu.Lock("abandon")
	e.busy--
	e.mu.Unlock()
}

func (e *executor) release(fnID string, w *warm) {
	e.mu.Lock("release")
	e.busy--
	w.last = e.now().UTC()
	e.idle[fnID] = append(e.idle[fnID], w)
	e.mu.Unlock()
}

func (e *executor) discard(w *warm) {
	e.abandon()
	e.teardown(w)
}

func scratchDir() (string, error) {
	base := os.TempDir()
	if len(filepath.Join(base, "ocifn-0000000000", fdkSocketName)) > maxSockPath {
		base = "/tmp"
	}

	dir, err := os.MkdirTemp(base, "ocifn-")
	if err != nil {
		return "", fmt.Errorf("create scratch dir: %w", err)
	}

	if err = os.Chmod(dir, 0o777); err != nil { //nolint:gosec // shared with the container's non-root function user
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("chmod scratch dir: %w", err)
	}

	return dir, nil
}

func unixClient(sock string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer

			return d.DialContext(ctx, "unix", sock)
		},
	}}
}

func waitSocket(ctx context.Context, sock string) error {
	t := time.NewTimer(0)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("function socket %s not ready: %w", fdkSocketName, ctx.Err())
		case <-t.C:
		}

		if st, err := os.Stat(sock); err == nil && st.Mode()&os.ModeSocket != 0 {
			conn, derr := (&net.Dialer{}).DialContext(ctx, "unix", sock)
			if derr == nil {
				_ = conn.Close()

				return nil
			}
		}

		t.Reset(readyPoll)
	}
}

func imageRef(image, digest string) string {
	if digest == "" || strings.Contains(image, "@") {
		return image
	}

	return image + "@" + digest
}

func (e *executor) start(ctx context.Context, rt Runner, p execPlan) (*warm, error) {
	e.mu.RLock("start")
	startup := e.startup
	e.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, startup)
	defer cancel()

	dir, err := scratchDir()
	if err != nil {
		return nil, err
	}

	w := &warm{dir: dir, sig: p.sig}

	if st, ok := rt.(imageStore); ok {
		if has, herr := st.HasImage(ctx, p.image); herr != nil || !has {
			if perr := st.PullImage(ctx, p.image); perr != nil {
				e.teardown(w)

				return nil, fmt.Errorf("pull image %s: %w", p.image, perr)
			}
		}
	}

	w.id, err = rt.CreateAndStart(ctx, container.Spec{
		Image:  p.image,
		Name:   "gopherstack-ocifn-" + awsid.NewN("", nameSuffixLen),
		Env:    p.env,
		Mounts: []string{dir + ":" + fdkSocketDir},
	})
	if err != nil {
		e.teardown(w)

		return nil, fmt.Errorf("start container: %w", err)
	}

	sock := filepath.Join(dir, fdkSocketName)
	w.client = unixClient(sock)

	if err = waitSocket(ctx, sock); err != nil {
		e.teardown(w)

		return nil, err
	}

	return w, nil
}

func newCallID() string { return strings.ToUpper(awsid.NewN("", callIDLen)) }

func callHeaders(in InvokeIn, callID string, deadline time.Time) http.Header {
	h := http.Header{}
	h.Set(hdrCallID, callID)
	h.Set(hdrDeadline, deadline.UTC().Format(time.RFC3339Nano))
	h.Set(hdrHTTPMethod, http.MethodPost)
	h.Set(hdrHTTPURL, in.URL)

	if ct := in.Header.Get("Content-Type"); ct != "" {
		h.Set("Content-Type", ct)
	}

	for k, vs := range in.Header {
		ck := http.CanonicalHeaderKey(k)
		if dropForward[ck] || ck == "Content-Type" || strings.HasPrefix(ck, "Fn-") {
			continue
		}

		for _, v := range vs {
			h.Add(hdrHTTPPrefix+ck, v)
		}
	}

	return h
}

// call performs one FDK call and classifies the outcome; keep reports whether the container is reusable.
func (e *executor) call(
	ctx context.Context,
	w *warm,
	p execPlan,
	in InvokeIn,
) (InvokeOut, bool, *APIError) {
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	callID := newCallID()

	r, err := http.NewRequestWithContext(callCtx, http.MethodPost, "http://fn"+fdkCallPath, bytes.NewReader(in.Body))
	if err != nil {
		return InvokeOut{}, false, errExecFailed("invalid function call: " + err.Error())
	}

	r.Header = callHeaders(in, callID, e.now().Add(p.timeout))

	resp, err := w.client.Do(r)
	if err != nil {
		return InvokeOut{}, false, callError(callCtx, ctx, p, err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPayload+1))
	if err != nil {
		return InvokeOut{}, false, callError(callCtx, ctx, p, err)
	}

	if len(body) > maxPayload {
		return InvokeOut{}, false, errExecFailed("function response exceeds the 6 MiB limit")
	}

	if resp.StatusCode >= http.StatusInternalServerError {
		return InvokeOut{}, true, errExecFailed("function failed")
	}

	hdr := http.Header{}

	for k, vs := range resp.Header {
		if ck := http.CanonicalHeaderKey(k); ck == "Content-Type" || strings.HasPrefix(ck, "Fn-") {
			hdr[ck] = vs
		}
	}

	hdr.Set(hdrCallID, callID)

	return InvokeOut{Status: http.StatusOK, Header: hdr, Body: body, CallID: callID}, true, nil
}

func callError(callCtx, parent context.Context, p execPlan, err error) *APIError {
	if errors.Is(callCtx.Err(), context.DeadlineExceeded) && parent.Err() == nil {
		return errExecTimeout("function did not finish within its " + p.timeout.String() + " timeout")
	}

	return errExecFailed("function container did not respond: " + err.Error())
}

// run executes one invocation on a warm container, starting one when none is available.
func (e *executor) run(ctx context.Context, p execPlan, in InvokeIn) (InvokeOut, *APIError) {
	rt, err := e.runner()
	if err != nil {
		return InvokeOut{}, errUnavailable("function engine unavailable: " + err.Error())
	}

	w, ok := e.acquire(p)
	if !ok {
		return InvokeOut{}, errThrottled("too many concurrent function containers")
	}

	if w == nil {
		if w, err = e.start(ctx, rt, p); err != nil {
			e.abandon()

			return InvokeOut{}, errExecFailed("function container failed to start: " + err.Error())
		}
	}

	out, keep, aerr := e.call(ctx, w, p, in)
	if keep {
		e.release(p.fnID, w)
	} else {
		e.discard(w)
	}

	return out, aerr
}

func execSig(image string, env []string) string {
	sum := sha256.Sum256([]byte(image + "\x00" + strings.Join(env, "\x00")))

	return hex.EncodeToString(sum[:])
}

func (b *InMemoryBackend) plan(fnID string) (execPlan, *APIError) {
	b.mu.RLock("plan")
	defer b.mu.RUnlock()

	f, ok := b.funcs[fnID]
	if !ok || f.State == stateDeleted {
		return execPlan{}, errNotFound(fnID)
	}

	if f.State != stateActive {
		return execPlan{}, errState("Function " + fnID + " is in state " + f.State + "; invoking requires ACTIVE")
	}

	app := b.apps[f.AppID]
	if app == nil {
		return execPlan{}, errNotFound(f.AppID)
	}

	cfg := maps.Clone(app.Config)
	if cfg == nil {
		cfg = map[string]string{}
	}

	maps.Copy(cfg, f.Config)
	cfg["FN_APP_NAME"], cfg["FN_FN_NAME"] = app.Name, f.Name
	cfg["FN_APP_ID"], cfg["FN_FN_ID"] = app.ID, f.ID
	cfg["FN_FORMAT"], cfg["FN_LISTENER"] = fdkFormat, "unix:"+fdkSocketDir+"/"+fdkSocketName
	cfg["FN_MEMORY"], cfg["FN_TMPSIZE"] = strconv.FormatInt(f.Memory, 10), "0"

	keys := slices.Sorted(maps.Keys(cfg))
	env := make([]string, 0, len(keys))

	for _, k := range keys {
		env = append(env, k+"="+cfg[k])
	}

	unit := b.exec.timeoutUnit()
	ref := imageRef(f.Image, f.ImageDigest)
	p := execPlan{
		fnID: f.ID, fnName: f.Name, appID: app.ID, appName: app.Name, image: ref, env: env, sig: execSig(ref, env),
		timeout: time.Duration(f.Timeout) * unit,
	}

	p.detached = p.timeout
	if f.Detached != nil {
		p.detached = time.Duration(*f.Detached) * unit
	}

	return p, nil
}

// Invoke runs a function: synchronously it returns the function's response, detached it returns once the
// call is accepted and the function runs in the background.
func (b *InMemoryBackend) Invoke(ctx context.Context, fnID string, in InvokeIn) (InvokeOut, error) {
	p, aerr := b.plan(fnID)
	if aerr != nil {
		return InvokeOut{}, aerr
	}

	if in.DryRun {
		return InvokeOut{Status: http.StatusOK, Header: http.Header{}}, nil
	}

	if in.Detached {
		return b.invokeDetached(p, in)
	}

	start := b.nowUTC()
	out, aerr := b.exec.run(ctx, p, in)
	b.observe(p, start, out.CallID, out.Status, aerr)

	if aerr != nil {
		return InvokeOut{}, aerr
	}

	return out, nil
}

func (b *InMemoryBackend) invokeDetached(p execPlan, in InvokeIn) (InvokeOut, error) {
	e := b.exec
	if _, err := e.runner(); err != nil {
		return InvokeOut{}, errUnavailable("function engine unavailable: " + err.Error())
	}

	if e.runCtx.Err() != nil {
		return InvokeOut{}, errUnavailable("the function service is shutting down")
	}

	in.Body = bytes.Clone(in.Body)
	in.Header = in.Header.Clone()

	e.wg.Go(func() {
		ctx, cancel := context.WithTimeout(e.runCtx, p.detached+e.startupTimeout())
		defer cancel()

		p.timeout = p.detached

		start := b.nowUTC()
		res, aerr := e.run(ctx, p, in)
		b.observe(p, start, res.CallID, res.Status, aerr)

		if aerr != nil {
			logger.Load(ctx).WarnContext(ctx, "ocifunctions: detached invocation failed",
				"function", p.fnID, "code", aerr.Code, "message", aerr.Message)
		}
	})

	return InvokeOut{Status: http.StatusAccepted, Header: http.Header{}, CallID: newCallID()}, nil
}

func (e *executor) timeoutUnit() time.Duration {
	e.mu.RLock("timeoutUnit")
	defer e.mu.RUnlock()

	return e.unit
}

func (e *executor) startupTimeout() time.Duration {
	e.mu.RLock("startupTimeout")
	defer e.mu.RUnlock()

	return e.startup
}
