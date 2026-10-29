package azurearm_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
		_ = json.Unmarshal(data, &out)
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
	dbs        map[string][]string
	pw         map[string]string
	live       map[string]bool
	calls      []string
	mu         sync.Mutex
	failEnsure bool
}

func newFakeFlexEngine() *fakeFlexEngine {
	return &fakeFlexEngine{dbs: map[string][]string{}, pw: map[string]string{}, live: map[string]bool{}}
}

func (f *fakeFlexEngine) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
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
