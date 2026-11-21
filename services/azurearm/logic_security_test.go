package azurearm_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logicSeenRequest struct {
	method, auth, cookie, poll, workflow string
}

type logicRecordingBackend struct {
	reply func(n int, w http.ResponseWriter)
	seen  []logicSeenRequest
	mu    sync.Mutex
}

func (b *logicRecordingBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.seen = append(b.seen, logicSeenRequest{
		method: r.Method, auth: r.Header.Get("Authorization"), cookie: r.Header.Get("Cookie"),
		poll: r.Header.Get("X-Poll"), workflow: r.Header.Get("X-Ms-Workflow-Name"),
	})
	n := len(b.seen)
	b.mu.Unlock()

	if b.reply != nil {
		b.reply(n, w)
	}
}

func (b *logicRecordingBackend) requests() []logicSeenRequest {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]logicSeenRequest(nil), b.seen...)
}

func authedPollDef(uri string) la {
	return logicDef(la{"poll": la{
		"type":       "Http",
		"recurrence": la{"frequency": "Minute", "interval": 1, "startTime": "2026-01-01T00:00:00Z"},
		"inputs": la{
			"method": "GET", "uri": uri, "headers": la{"X-Poll": "1"}, "cookie": "sid=1",
			"authentication": la{"type": "Raw", "value": "Bearer sek"}, "retryPolicy": la{"type": "none"},
		},
	}}, la{"Mark": compose("@triggerBody()", nil)})
}

func (s *logicStack) pollSteps(t *testing.T, steps ...time.Duration) {
	t.Helper()

	s.clock.Advance(0)

	for _, d := range steps {
		s.clock.Advance(d)
		s.waitRuns(t)
	}
}

func TestLogicPollLocationOtherOriginKeepsCredentials(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	other := &logicRecordingBackend{}
	otherSrv := httptest.NewServer(other)
	t.Cleanup(otherSrv.Close)

	first := &logicRecordingBackend{reply: func(_ int, w http.ResponseWriter) {
		w.Header().Set("Location", otherSrv.URL+"/next")
		_, _ = w.Write([]byte("x"))
	}}
	firstSrv := httptest.NewServer(first)
	t.Cleanup(firstSrv.Close)

	s.create(t, "poller", authedPollDef(firstSrv.URL+"/poll"))
	s.pollSteps(t, time.Minute, time.Minute)

	f := first.requests()
	require.Len(t, f, 1)
	assert.Equal(t, "Bearer sek", f[0].auth, "the configured endpoint receives the credential")

	o := other.requests()
	require.Len(t, o, 1)
	assert.Equal(t, http.MethodGet, o[0].method)
	assert.Empty(t, o[0].auth, "a Location on another origin never receives the credential")
	assert.Empty(t, o[0].cookie)
	assert.Empty(t, o[0].poll)
	assert.Equal(t, "poller", o[0].workflow)
}

func TestLogicPollLocationExternalHostFailsTrigger(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	first := &logicRecordingBackend{reply: func(_ int, w http.ResponseWriter) {
		w.Header().Set("Location", "http://203.0.113.9/next")
		_, _ = w.Write([]byte("x"))
	}}
	srv := httptest.NewServer(first)
	t.Cleanup(srv.Close)

	s.create(t, "poller", authedPollDef(srv.URL+"/poll"))
	s.pollSteps(t, time.Minute, time.Minute)

	assert.Equal(t, 1, s.runCount(t, "poller"))

	failed := 0

	for _, h := range s.pollHistories(t, "poller") {
		if *h.Properties.Status == armlogic.WorkflowStatusFailed {
			failed++

			assert.Equal(t, "TriggerRequestFailed", *h.Properties.Code)
		}
	}

	assert.Equal(t, 1, failed, "the poll of a disallowed Location is a failed trigger")
}

func TestLogicPollRetryAfter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		header    func(now time.Time) string
		name      string
		steps     []time.Duration
		wantPolls int
	}{
		{
			name:      "http date",
			header:    func(now time.Time) string { return now.Add(150 * time.Second).UTC().Format(http.TimeFormat) },
			steps:     []time.Duration{time.Minute, time.Minute, time.Minute, time.Minute},
			wantPolls: 2,
		},
		{
			name:      "past http date keeps the recurrence",
			header:    func(now time.Time) string { return now.Add(-time.Hour).UTC().Format(http.TimeFormat) },
			steps:     []time.Duration{time.Minute, time.Minute},
			wantPolls: 2,
		},
		{
			name:      "absurd seconds are clamped",
			header:    func(time.Time) string { return strconv.Itoa(1<<31 - 1) },
			steps:     []time.Duration{time.Minute, 25 * time.Hour},
			wantPolls: 2,
		},
		{
			name:      "beyond int32 seconds are clamped",
			header:    func(time.Time) string { return "99999999999999" },
			steps:     []time.Duration{time.Minute, 25 * time.Hour},
			wantPolls: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			b := &logicRecordingBackend{reply: func(_ int, w http.ResponseWriter) {
				w.Header().Set("Retry-After", tc.header(s.clock.Now()))
				w.WriteHeader(http.StatusAccepted)
			}}
			srv := httptest.NewServer(b)
			t.Cleanup(srv.Close)

			s.create(t, "poller", httpTriggerDef(srv.URL+"/poll"))
			s.pollSteps(t, tc.steps...)

			assert.Len(t, b.requests(), tc.wantPolls)
		})
	}
}

func (s *logicStack) postWithHost(t *testing.T, path, host string) {
	t.Helper()

	full := s.url + "/subscriptions/" + aciSub + "/resourceGroups/" + logicRG +
		"/providers/Microsoft.Logic" + path + "?api-version=2019-05-01"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, full, nil)
	require.NoError(t, err)

	req.Host = host

	resp, err := s.client.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestLogicWebhookCallbackHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		host     string
		wantBase string
	}{
		{
			name: "service fqdn", host: "gopherstack.default.svc.cluster.local:443",
			wantBase: "https://gopherstack.default.svc.cluster.local:443",
		},
		{name: "docker host", host: "host.docker.internal:4566", wantBase: "https://host.docker.internal:4566"},
		{name: "userinfo refused", host: "user@evil.example.com:443"},
		{name: "bad characters refused", host: "evil.example.com/x:443"},
		{name: "bad port refused", host: "evil.example.com:99999"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			b := &hookBackend{}
			srv := httptest.NewServer(b)
			t.Cleanup(srv.Close)

			s.create(t, "hooked", webhookDef(srv.URL, ""))
			s.waitRuns(t)

			_, err := s.factory.NewWorkflowsClient().Disable(t.Context(), logicRG, "hooked", nil)
			require.NoError(t, err)
			s.waitRuns(t)
			s.postWithHost(t, "/workflows/hooked/enable", tc.host)
			s.waitRuns(t)

			calls := b.seen()
			require.Len(t, calls, 3, "subscribe, unsubscribe, subscribe")

			if tc.wantBase != "" {
				assert.True(t, strings.HasPrefix(calls[2].callback, tc.wantBase+"/_logic/workflows/"),
					calls[2].callback)

				return
			}

			assert.True(t, strings.HasPrefix(calls[2].callback, s.url+"/_logic/workflows/"),
				"a refused host keeps the base of the workflow's earlier write: %s", calls[2].callback)
		})
	}
}

func TestLogicWebhookCallbackHostIsPerWorkflow(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	b := &hookBackend{}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)

	s.create(t, "hooked", webhookDef(srv.URL, ""))
	s.create(t, "other", logicDef(requestTrigger(nil), la{"Mark": compose("1", nil)}))
	s.waitRuns(t)

	_, err := s.factory.NewWorkflowsClient().Disable(t.Context(), logicRG, "hooked", nil)
	require.NoError(t, err)
	s.waitRuns(t)

	s.postWithHost(t, "/workflows/other/disable", "evil.example.com:443")

	_, err = s.factory.NewWorkflowsClient().Enable(t.Context(), logicRG, "hooked", nil)
	require.NoError(t, err)
	s.waitRuns(t)

	calls := b.seen()
	require.Len(t, calls, 3)
	assert.True(t, strings.HasPrefix(calls[2].callback, s.url+"/_logic/workflows/"), calls[2].callback)
}

func TestLogicResetDropsQueuedWebhookCalls(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)

	var (
		mu      sync.Mutex
		paths   []string
		started = make(chan struct{})
		release = make(chan struct{})
		once    sync.Once
	)

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()

		once.Do(func() {
			close(started)
			<-release
		})
	}))
	t.Cleanup(srv.Close)

	s.create(t, "one", webhookDef(srv.URL+"/one", ""))
	<-started
	s.create(t, "two", webhookDef(srv.URL+"/two", ""))

	s.prov.Reset()
	close(release)
	s.waitRuns(t)

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, []string{"/one/sub", "/one/unsub"}, paths,
		"the queued subscribe is dropped and the subscribe that finished after the reset is undone")
}

func TestLogicExpressionTracesHideSecrets(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	def := logicDef(requestTrigger(nil), la{"Act": la{
		"type": "Compose", "runAfter": la{},
		"inputs": la{
			"a": "@concat('p-', parameters('sec'))",
			"b": "x@{parameters('sec')}y",
			"headers": la{
				"Authorization": "@concat('Bearer ', parameters('tok'))", "Cookie": "@concat('sid=', parameters('ck'))",
			},
			"ok": "@add(1, 2)",
		},
	}})
	def["parameters"] = la{
		"sec": la{"type": "SecureString", "defaultValue": "TOPSECRETVALUE"},
		"tok": la{"type": "String", "defaultValue": "zzzbearer"},
		"ck":  la{"type": "String", "defaultValue": "zzzcookie"},
	}

	s.create(t, "trace", def)
	s.invoke(t, "trace", `{}`)

	res, err := s.factory.NewWorkflowRunActionsClient().
		NewListExpressionTracesPager(logicRG, "trace", s.latestRunName(t, "trace"), "Act", nil).
		NextPage(t.Context())
	require.NoError(t, err)

	raw := toJSON(t, res)
	for _, secret := range []string{"TOPSECRETVALUE", "zzzbearer", "zzzcookie"} {
		assert.NotContains(t, raw, secret)
	}

	got := map[string]any{}

	for _, r := range res.Inputs {
		got[*r.Path] = r.Value
	}

	assert.Len(t, got, 5, "every expression is still traced")
	assert.InDelta(t, 3, got["inputs.ok"], 0, "a plain expression keeps its value")
}

func TestLogicRunBodyBudget(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	body := strings.Repeat("x", 200<<10)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(backend.Close)

	items := make([]any, 40)
	for i := range items {
		items[i] = i
	}

	s.create(t, "loop", logicDef(requestTrigger(nil), la{"Loop": la{
		"type": "Foreach", "foreach": items, "operationOptions": "Sequential", "runAfter": la{},
		"actions": la{"Call": la{
			"type": "Http", "runAfter": la{},
			"inputs": la{"method": "GET", "uri": backend.URL, "retryPolicy": la{"type": "none"}},
		}},
	}}))
	s.invoke(t, "loop", `{}`)

	run := s.latestRunName(t, "loop")
	rc := s.factory.NewWorkflowRunActionRepetitionsRequestHistoriesClient()
	calls, bodies := 0, 0

	var names []string

	for pager := s.factory.NewWorkflowRunActionRepetitionsClient().
		NewListPager(logicRG, "loop", run, "Call", nil); pager.More(); {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, rep := range page.Value {
			names = append(names, *rep.Name)
		}
	}

	require.Len(t, names, len(items))

	for _, name := range names {
		page, lerr := rc.NewListPager(logicRG, "loop", run, "Call", name, nil).NextPage(t.Context())
		require.NoError(t, lerr)

		for _, h := range page.Value {
			calls++

			if h.Properties.Response.BodyLink != nil {
				bodies++
			}
		}
	}

	assert.Equal(t, len(items), calls, "every call is still recorded")
	assert.Positive(t, bodies)
	assert.LessOrEqual(t, bodies*len(body), 4<<20, "the stored bodies of one run are bounded")
}
