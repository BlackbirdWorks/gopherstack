package azurearm_test

import (
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	flexRG       = "rg-flex"
	flexLoc      = "westus2"
	flexPassword = "Sup3rS3cret!pw"
	flexAPIPG    = "2024-08-01"
	flexAPIMy    = "2024-12-30"
)

// stepClock moves one second on every read so pending operations finish on the next status poll.
type stepClock struct {
	base time.Time
	n    atomic.Int64
}

func (c *stepClock) Now() time.Time { return c.base.Add(time.Duration(c.n.Add(1)) * time.Second) }

type flexStack struct {
	handler *azurearm.Handler
	pg      *azurearm.FlexDBProvider
	my      *azurearm.FlexDBProvider
	client  *http.Client
	opts    *arm.ClientOptions
	url     string
}

func newFlexStack(t *testing.T, engine azurearm.FlexEngine) *flexStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	return newFlexStackFor(t, h, engine)
}

func newFlexStackFor(t *testing.T, h *azurearm.Handler, engine azurearm.FlexEngine) *flexStack {
	t.Helper()

	clock := &stepClock{base: time.Now().UTC().Truncate(time.Second)}
	s := &flexStack{handler: h}

	for ns, dst := range map[string]**azurearm.FlexDBProvider{
		"Microsoft.DBforPostgreSQL": &s.pg, "Microsoft.DBforMySQL": &s.my,
	} {
		pp, found := h.Registry.ProviderNamed(ns)
		require.True(t, found)

		fp, isFlex := pp.(*azurearm.FlexDBProvider)
		require.True(t, isFlex)
		fp.SetClock(clock.Now)

		if engine != nil {
			fp.SetEngine(engine)
		}

		*dst = fp
	}

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	s.client, s.url = srv.Client(), srv.URL
	s.opts = &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}

	s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/"+flexRG, `{"location":"`+flexLoc+`"}`,
		"2021-04-01")

	return s
}

// raw sends a request and returns the status and decoded JSON body (nil when the body is empty).
func (s *flexStack) raw(t *testing.T, method, path, body, apiVersion string) (int, map[string]any) {
	t.Helper()

	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path+sep+"api-version="+apiVersion,
		strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	if len(data) > 0 {
		_ = json.Unmarshal(data, &out, jsonx.V1())
	}

	return resp.StatusCode, out
}

func flexErrCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	c, _ := e["code"].(string)

	return c
}

func flexServerPath(ns, name string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + flexRG + "/providers/" + ns + "/flexibleServers/" + name
}

type fakeFlexEngine struct {
	dbs      map[string][]string
	pw       map[string]string
	live     map[string]bool
	data     map[string]string
	loaded   map[string]string
	readOnly map[string]bool
	params   map[string]map[string]azurearm.FlexParam
	seen     map[string]chan struct{}
	// dumpHold, when set, parks every dump until it is closed; dumpEntered then receives the dumped server and
	// overlap is closed the moment two dumps run at once.
	dumpHold    chan struct{}
	dumpEntered chan string
	overlap     chan struct{}
	// applyEntered and applyRelease park the first ApplyParameters call, which then fails when applyFirstFails.
	applyEntered    chan struct{}
	applyRelease    chan struct{}
	calls           []string
	overlapOnce     sync.Once
	mu              sync.Mutex
	inDump          int
	maxDump         int
	applyCalls      int
	failEnsure      bool
	failApply       bool
	failRestore     bool
	failDump        bool
	applyFirstFails bool
}

func newFakeFlexEngine() *fakeFlexEngine {
	return &fakeFlexEngine{
		dbs: map[string][]string{}, pw: map[string]string{}, live: map[string]bool{}, data: map[string]string{},
		loaded: map[string]string{}, readOnly: map[string]bool{}, params: map[string]map[string]azurearm.FlexParam{},
	}
}

func (f *fakeFlexEngine) ApplyParameters(ctx context.Context, id string, ps []azurearm.FlexParam) error {
	f.record("apply")

	if f.failApply {
		return io.ErrUnexpectedEOF
	}

	f.mu.Lock()
	f.applyCalls++
	first, entered, release, fails := f.applyCalls == 1, f.applyEntered, f.applyRelease, f.applyFirstFails
	f.mu.Unlock()

	if first && entered != nil {
		entered <- struct{}{}

		select {
		case <-release:
		case <-ctx.Done():
		}

		if fails {
			return io.ErrUnexpectedEOF
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.params[id] == nil {
		f.params[id] = map[string]azurearm.FlexParam{}
	}

	for _, p := range ps {
		f.params[id][p.Name] = p
	}

	return nil
}

func (f *fakeFlexEngine) param(id, name string) (azurearm.FlexParam, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	p, ok := f.params[id][name]

	return p, ok
}

func (f *fakeFlexEngine) setData(id, data string) {
	f.mu.Lock()
	f.data[id] = data
	f.mu.Unlock()
}

func (f *fakeFlexEngine) Dump(ctx context.Context, id string, dbs []string, w io.Writer) error {
	f.record("dump")

	if f.failDump {
		return io.ErrUnexpectedEOF
	}

	f.mu.Lock()
	data := f.data[id]
	f.inDump++
	f.maxDump = max(f.maxDump, f.inDump)
	hold, entered, overlap := f.dumpHold, f.dumpEntered, f.overlap
	overlapping := f.inDump > 1
	f.mu.Unlock()

	if overlapping && overlap != nil {
		f.overlapOnce.Do(func() { close(overlap) })
	}

	if hold != nil {
		entered <- id

		select {
		case <-hold:
		case <-ctx.Done():
		}
	}

	runtime.Gosched()

	f.mu.Lock()
	f.inDump--
	f.mu.Unlock()

	_, err := io.WriteString(w, strings.Join(dbs, ",")+"|"+data)

	return err
}

func (f *fakeFlexEngine) Restore(_ context.Context, id string, r io.Reader) error {
	f.record("restore")

	if f.failRestore {
		return io.ErrUnexpectedEOF
	}

	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	f.mu.Lock()
	f.loaded[id] = string(b)
	f.mu.Unlock()

	return nil
}

func (f *fakeFlexEngine) CopyAdmin(_ context.Context, src, dst string) error {
	f.record("copyadmin")
	f.mu.Lock()
	f.pw[dst] = f.pw[src]
	f.mu.Unlock()

	return nil
}

func (f *fakeFlexEngine) SetReadOnly(_ context.Context, id string, on bool) error {
	f.record("readonly:" + strconv.FormatBool(on))
	f.mu.Lock()
	f.readOnly[id] = on
	f.mu.Unlock()

	return nil
}

func (f *fakeFlexEngine) isReadOnly(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.readOnly[id]
}

func (f *fakeFlexEngine) loadedData(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.loaded[id]
}

func (f *fakeFlexEngine) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, call)

	ch := f.seenLocked(call)
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (f *fakeFlexEngine) seenLocked(call string) chan struct{} {
	if f.seen == nil {
		f.seen = map[string]chan struct{}{}
	}

	ch, ok := f.seen[call]
	if !ok {
		ch = make(chan struct{})
		f.seen[call] = ch
	}

	return ch
}

// awaitCall blocks until the engine has recorded call; async operations run on a goroutine.
func (f *fakeFlexEngine) awaitCall(t *testing.T, call string) {
	t.Helper()

	f.mu.Lock()
	ch := f.seenLocked(call)
	f.mu.Unlock()

	select {
	case <-ch:
	case <-t.Context().Done():
		t.Fatalf("engine call %q never happened", call)
	}
}

func (f *fakeFlexEngine) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.calls...)
}

func (f *fakeFlexEngine) Ensure(_ context.Context, id string, spec azurearm.FlexSpec) (azurearm.FlexEndpoint, error) {
	f.record("ensure:" + spec.AdminLogin + ":" + spec.Version)

	if f.failEnsure {
		return azurearm.FlexEndpoint{}, io.ErrUnexpectedEOF
	}

	f.mu.Lock()
	f.live[id], f.pw[id] = true, spec.AdminPassword
	f.mu.Unlock()

	return azurearm.FlexEndpoint{Host: "127.0.0.1", Port: 15432}, nil
}

func (f *fakeFlexEngine) Endpoint(id string) (azurearm.FlexEndpoint, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.live[id] {
		return azurearm.FlexEndpoint{}, false
	}

	return azurearm.FlexEndpoint{Host: "127.0.0.1", Port: 15432}, true
}

func (f *fakeFlexEngine) SetPassword(_ context.Context, id, login, password string) error {
	f.record("password:" + login)
	f.mu.Lock()
	f.pw[id] = password
	f.mu.Unlock()

	return nil
}

func (f *fakeFlexEngine) Stop(context.Context, string) error {
	f.record("stop")

	return nil
}

func (f *fakeFlexEngine) Start(context.Context, string) error {
	f.record("start")

	return nil
}

func (f *fakeFlexEngine) Restart(context.Context, string) error {
	f.record("restart")

	return nil
}

func (f *fakeFlexEngine) CreateDatabase(_ context.Context, id, name, charset, collation string) error {
	f.record("createdb:" + name + ":" + charset + ":" + collation)
	f.mu.Lock()
	f.dbs[id] = append(f.dbs[id], name)
	f.mu.Unlock()

	return nil
}

func (f *fakeFlexEngine) DropDatabase(_ context.Context, _, name string) error {
	f.record("dropdb:" + name)

	return nil
}

func (f *fakeFlexEngine) Delete(_ context.Context, id string) error {
	f.record("delete")
	f.mu.Lock()
	delete(f.live, id)
	f.mu.Unlock()

	return nil
}
