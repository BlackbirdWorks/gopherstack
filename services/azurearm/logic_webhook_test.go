package azurearm_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type hookCall struct {
	path, callback string
}

type hookBackend struct {
	calls  []hookCall
	status int
	mu     sync.Mutex
}

func (b *hookBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)

	var body map[string]any
	_ = json.Unmarshal(raw, &body, jsonx.V1())

	cb, _ := body["callback"].(string)

	b.mu.Lock()
	b.calls = append(b.calls, hookCall{path: r.URL.Path, callback: cb})
	status := b.status
	b.mu.Unlock()

	if status != 0 {
		w.WriteHeader(status)
	}
}

func (b *hookBackend) seen() []hookCall {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]hookCall(nil), b.calls...)
}

func (b *hookBackend) paths() []string {
	calls := b.seen()
	out := make([]string, 0, len(calls))

	for _, c := range calls {
		out = append(out, c.path)
	}

	return out
}

func webhookDef(uri string, subExtra string) la {
	call := func(path string) la {
		return la{
			"method": "POST", "uri": uri + path + subExtra, "body": la{"callback": "@{listCallbackUrl()}"},
			"retryPolicy": la{"type": "none"},
		}
	}

	return logicDef(la{"hook": la{
		"type": "HttpWebhook", "inputs": la{"subscribe": call("/sub"), "unsubscribe": call("/unsub")},
	}}, la{"Mark": compose("@triggerBody()", nil)})
}

func TestLogicHTTPWebhookLifecycle(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	b := &hookBackend{}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)

	wc := s.factory.NewWorkflowsClient()
	s.create(t, "hooked", webhookDef(srv.URL, ""))
	s.waitRuns(t)

	calls := b.seen()
	require.Len(t, calls, 1)
	assert.Equal(t, "/sub", calls[0].path)

	callback := calls[0].callback
	require.Contains(t, callback, s.url+"/_logic/workflows/")

	res, err := s.factory.NewWorkflowTriggersClient().ListCallbackURL(t.Context(), logicRG, "hooked", "hook", nil)
	require.NoError(t, err)
	assert.Equal(t, callback, *res.Value, "listCallbackUrl() is the trigger's signed callback URL")

	code, _, _ := s.post(t, callback, `{"event":"x"}`)
	require.Equal(t, http.StatusAccepted, code)
	s.waitRuns(t)

	run := s.latestRun(t, "hooked")
	assert.Equal(t, "hook", *run.Properties.Trigger.Name)

	out, _ := s.content(t, run.Properties.Trigger.OutputsLink).(map[string]any)
	assert.Equal(t, map[string]any{"event": "x"}, out["body"])

	code, _, _ = s.post(t, callback+"0", `{}`)
	assert.Equal(t, http.StatusUnauthorized, code, "a tampered signature is refused")

	_, err = wc.Disable(t.Context(), logicRG, "hooked", nil)
	require.NoError(t, err)
	s.waitRuns(t)
	assert.Equal(t, []string{"/sub", "/unsub"}, b.paths())
	assert.Equal(t, callback, b.seen()[1].callback)

	_, err = wc.Enable(t.Context(), logicRG, "hooked", nil)
	require.NoError(t, err)
	s.waitRuns(t)
	assert.Equal(t, []string{"/sub", "/unsub", "/sub"}, b.paths())

	_, err = wc.Delete(t.Context(), logicRG, "hooked", nil)
	require.NoError(t, err)
	s.waitRuns(t)
	assert.Equal(t, []string{"/sub", "/unsub", "/sub", "/unsub"}, b.paths())
}

func TestLogicHTTPWebhookDefinitionChange(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	b := &hookBackend{}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)

	s.create(t, "hooked", webhookDef(srv.URL, ""))
	s.waitRuns(t)
	s.create(t, "hooked", webhookDef(srv.URL, ""))
	s.waitRuns(t)
	assert.Equal(t, []string{"/sub"}, b.paths(), "an unchanged definition does not resubscribe")

	s.create(t, "hooked", webhookDef(srv.URL, "?v=2"))
	s.waitRuns(t)
	assert.Equal(t, []string{"/sub", "/unsub", "/sub"}, b.paths(), "a changed subscribe call replaces the subscription")

	s.create(t, "hooked", logicDef(requestTrigger(nil), la{"Mark": compose("1", nil)}))
	s.waitRuns(t)
	assert.Equal(t, []string{"/sub", "/unsub", "/sub", "/unsub"}, b.paths(), "removing the trigger unsubscribes")
}

func TestLogicHTTPWebhookSubscribeFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		uri    func(srv *httptest.Server) string
		name   string
		status int
	}{
		{
			name:   "backend error",
			uri:    func(s *httptest.Server) string { return s.URL },
			status: http.StatusInternalServerError,
		},
		{name: "external host refused", uri: func(*httptest.Server) string { return "http://203.0.113.9" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			b := &hookBackend{status: tc.status}
			srv := httptest.NewServer(b)
			t.Cleanup(srv.Close)

			s.create(t, "hooked", webhookDef(tc.uri(srv), ""))
			s.waitRuns(t)

			page, err := s.factory.NewWorkflowTriggerHistoriesClient().NewListPager(logicRG, "hooked", "hook", nil).
				NextPage(t.Context())
			require.NoError(t, err)
			require.Len(t, page.Value, 1)
			assert.Equal(t, armlogic.WorkflowStatusFailed, *page.Value[0].Properties.Status)

			_, err = s.factory.NewWorkflowsClient().Delete(t.Context(), logicRG, "hooked", nil)
			require.NoError(t, err)
			s.waitRuns(t)

			for _, p := range b.paths() {
				assert.NotEqual(t, "/unsub", p, "no unsubscribe without a subscription")
			}
		})
	}
}

func TestLogicHTTPWebhookValidation(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)

	tests := []struct {
		inputs la
		name   string
	}{
		{name: "no subscribe", inputs: la{}},
		{name: "subscribe without uri", inputs: la{"subscribe": la{"method": "POST"}}},
		{name: "bad unsubscribe", inputs: la{
			"subscribe": la{
				"method": "POST",
				"uri":    "http://x",
			},
			"unsubscribe": la{"method": "FETCH", "uri": "http://x"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := s.factory.NewWorkflowsClient().CreateOrUpdate(t.Context(), logicRG, "badhook",
				armlogic.Workflow{
					Location: new("westus"),
					Properties: &armlogic.WorkflowProperties{
						Definition: logicDef(
							la{"hook": la{"type": "HttpWebhook", "inputs": tc.inputs}}, la{}),
					},
				}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "InvalidTemplate")
		})
	}
}

func TestLogicHTTPWebhookPersistence(t *testing.T) {
	t.Parallel()

	a := newLogicStack(t)
	b := &hookBackend{}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)

	a.create(t, "hooked", webhookDef(srv.URL, ""))
	a.waitRuns(t)
	require.Equal(t, []string{"/sub"}, b.paths())

	c := newLogicStack(t)
	require.NoError(t, c.handler.Restore(t.Context(), a.handler.Snapshot(t.Context())))
	c.waitRuns(t)
	assert.Equal(t, []string{"/sub"}, b.paths(), "a restored subscription is not repeated")

	_, err := c.factory.NewWorkflowsClient().Delete(t.Context(), logicRG, "hooked", nil)
	require.NoError(t, err)
	c.waitRuns(t)
	assert.Equal(t, []string{"/sub", "/unsub"}, b.paths(), "the persisted subscription is unsubscribed on delete")
}
