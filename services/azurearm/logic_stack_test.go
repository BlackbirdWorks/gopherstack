package azurearm_test

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const logicRG = "logic-rg"

type logicTimer struct {
	clock *logicClock
	ch    chan time.Time
	fn    func()
	at    time.Time
	armed bool
}

func (t *logicTimer) C() <-chan time.Time { return t.ch }

func (t *logicTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()

	was := t.armed
	t.armed = false

	return was
}

// logicClock is a manually advanced gcpscale.Clock.
type logicClock struct {
	now     time.Time
	changed chan struct{}
	timers  []*logicTimer
	mu      sync.Mutex
}

func newLogicClock() *logicClock {
	return &logicClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), changed: make(chan struct{}, 4096)}
}

func (c *logicClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *logicClock) add(d time.Duration, fn func()) *logicTimer {
	c.mu.Lock()
	t := &logicTimer{clock: c, ch: make(chan time.Time, 1), fn: fn, at: c.now.Add(d), armed: true}
	c.timers = append(c.timers, t)
	c.mu.Unlock()

	select {
	case c.changed <- struct{}{}:
	default:
	}

	return t
}

func (c *logicClock) NewTimer(d time.Duration) gcpscale.Timer { return c.add(d, nil) }

func (c *logicClock) AfterFunc(d time.Duration, f func()) gcpscale.Timer { return c.add(d, f) }

// Advance moves time forward, firing every timer that comes due (including ones armed by callbacks).
func (c *logicClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()

	for {
		c.mu.Lock()

		var next *logicTimer

		for _, t := range c.timers {
			if t.armed && !t.at.After(target) && (next == nil || t.at.Before(next.at)) {
				next = t
			}
		}

		if next == nil {
			c.now = target
			c.mu.Unlock()

			return
		}

		next.armed = false
		if next.at.After(c.now) {
			c.now = next.at
		}

		at := c.now
		c.mu.Unlock()

		if next.fn != nil {
			next.fn()
		} else {
			next.ch <- at
		}
	}
}

func (c *logicClock) pending(timersOnly bool) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0

	for _, t := range c.timers {
		if t.armed && (!timersOnly || t.fn == nil) {
			n++
		}
	}

	return n
}

// waitTimers blocks until n run timers (Wait, retries) are armed.
func (c *logicClock) waitTimers(t *testing.T, n int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for c.pending(true) < n {
		select {
		case <-c.changed:
		case <-ctx.Done():
			require.Failf(t, "timers", "waiting for %d armed timers, have %d", n, c.pending(true))
		}
	}
}

type logicStack struct {
	factory *armlogic.ClientFactory
	clock   *logicClock
	prov    *azurearm.LogicProvider
	handler *azurearm.Handler
	client  *http.Client
	url     string
}

func newLogicStack(t *testing.T) *logicStack {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	pr, ok := h.Registry.ProviderNamed("Microsoft.Logic")
	require.True(t, ok)

	prov, ok := pr.(*azurearm.LogicProvider)
	require.True(t, ok)

	clock := newLogicClock()
	prov.SetClock(clock)

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	f, err := armlogic.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return &logicStack{factory: f, clock: clock, prov: prov, handler: h, client: srv.Client(), url: srv.URL}
}

// waitRuns blocks until every run has finished or parked on an unfired timer, failing on a hang.
func (s *logicStack) waitRuns(t *testing.T) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		s.prov.WaitRuns()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		require.Fail(t, "runs did not finish")
	}
}

func logicDef(triggers, actions map[string]any) map[string]any {
	return map[string]any{
		"$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/" +
			"workflowdefinition.json#",
		"contentVersion": "1.0.0.0",
		"triggers":       triggers,
		"actions":        actions,
	}
}

func requestTrigger(schema map[string]any) map[string]any {
	in := map[string]any{"method": "POST"}
	if schema != nil {
		in["schema"] = schema
	}

	return map[string]any{"manual": map[string]any{"type": "Request", "kind": "Http", "inputs": in}}
}

func (s *logicStack) create(t *testing.T, name string, def map[string]any) armlogic.Workflow {
	t.Helper()

	res, err := s.factory.NewWorkflowsClient().CreateOrUpdate(t.Context(), logicRG, name, armlogic.Workflow{
		Location:   new("westus"),
		Properties: &armlogic.WorkflowProperties{Definition: def},
	}, nil)
	require.NoError(t, err)

	return res.Workflow
}

func (s *logicStack) callback(t *testing.T, name string) string {
	t.Helper()

	res, err := s.factory.NewWorkflowTriggersClient().ListCallbackURL(t.Context(), logicRG, name, "manual", nil)
	require.NoError(t, err)
	require.NotNil(t, res.Value)

	return *res.Value
}

func (s *logicStack) post(t *testing.T, url, body string) (int, []byte, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, raw, resp.Header
}

type logicOutcome struct {
	run     armlogic.WorkflowRun
	actions map[string]armlogic.WorkflowRunAction
	body    []byte
	status  int
}

// invoke posts to the workflow's request trigger, waits for the run, and returns the run with its actions.
func (s *logicStack) invoke(t *testing.T, name, body string) logicOutcome {
	t.Helper()

	code, raw, _ := s.post(t, s.callback(t, name), body)
	s.waitRuns(t)

	return logicOutcome{status: code, body: raw, run: s.latestRun(t, name), actions: s.actions(t, name)}
}

func (s *logicStack) latestRun(t *testing.T, name string) armlogic.WorkflowRun {
	t.Helper()

	pager := s.factory.NewWorkflowRunsClient().NewListPager(logicRG, name, nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, page.Value)

	return *page.Value[0]
}

func (s *logicStack) actions(t *testing.T, name string) map[string]armlogic.WorkflowRunAction {
	t.Helper()

	run := s.latestRun(t, name)
	out := map[string]armlogic.WorkflowRunAction{}
	pager := s.factory.NewWorkflowRunActionsClient().NewListPager(logicRG, name, *run.Name, nil)

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, a := range page.Value {
			out[*a.Name] = *a
		}
	}

	return out
}

func (s *logicStack) content(t *testing.T, link *armlogic.ContentLink) any {
	t.Helper()
	require.NotNil(t, link)

	resp, err := s.client.Get(*link.URI)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var v any

	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(resp.Body), &v, jsonx.V1()))

	return v
}

// rawJSON sends a request to a workflow path under the test resource group and decodes the JSON response.
func (s *logicStack) rawJSON(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()

	full := s.url + "/subscriptions/" + aciSub + "/resourceGroups/" + logicRG + "/providers/Microsoft.Logic" + path +
		"?api-version=2019-05-01"

	req, err := http.NewRequestWithContext(t.Context(), method, full, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	var out map[string]any

	_ = json.UnmarshalDecode(jsontext.NewDecoder(resp.Body), &out, jsonx.V1())

	return resp.StatusCode, out
}
