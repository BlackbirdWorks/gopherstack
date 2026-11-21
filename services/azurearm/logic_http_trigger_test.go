package azurearm_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func httpTriggerDef(uri string) la {
	return logicDef(la{"poll": la{
		"type":       "Http",
		"recurrence": la{"frequency": "Minute", "interval": 1, "startTime": "2026-01-01T00:00:00Z"},
		"inputs":     la{"method": "GET", "uri": uri, "headers": la{"X-Poll": "1"}, "retryPolicy": la{"type": "none"}},
	}}, la{"Mark": compose("@triggerBody()", nil)})
}

type pollBackend struct {
	reply func(n int, w http.ResponseWriter)
	paths []string
	hdrs  []string
	mu    sync.Mutex
}

func (b *pollBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.paths = append(b.paths, r.URL.Path)
	b.hdrs = append(b.hdrs, r.Header.Get("X-Poll")+"|"+r.Header.Get("X-Ms-Workflow-Name"))
	n := len(b.paths)
	b.mu.Unlock()

	b.reply(n, w)
}

func (b *pollBackend) seen() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]string(nil), b.paths...)
}

func (s *logicStack) pollHistories(t *testing.T, wf string) []*armlogic.WorkflowTriggerHistory {
	t.Helper()

	page, err := s.factory.NewWorkflowTriggerHistoriesClient().NewListPager(logicRG, wf, "poll", nil).
		NextPage(t.Context())
	require.NoError(t, err)

	return page.Value
}

func TestLogicHTTPPollingTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		reply     func(n int, w http.ResponseWriter)
		steps     []time.Duration
		wantPaths []string
		wantRuns  int
		wantSkip  int
	}{
		{
			name: "200 starts a run",
			reply: func(_ int, w http.ResponseWriter) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"n":1}`))
			},
			steps: []time.Duration{time.Minute}, wantPaths: []string{"/poll"}, wantRuns: 1,
		},
		{
			name:  "202 does not start a run",
			reply: func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusAccepted) },
			steps: []time.Duration{time.Minute}, wantPaths: []string{"/poll"}, wantSkip: 1,
		},
		{
			name:  "404 does not start a run",
			reply: func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
			steps: []time.Duration{time.Minute}, wantPaths: []string{"/poll"}, wantSkip: 1,
		},
		{
			name: "location header is the next poll url",
			reply: func(_ int, w http.ResponseWriter) {
				w.Header().Set("Location", "/next")
				_, _ = w.Write([]byte("x"))
			},
			steps: []time.Duration{time.Minute, time.Minute}, wantPaths: []string{"/poll", "/next"}, wantRuns: 2,
		},
		{
			name: "retry-after longer than the recurrence delays the next poll",
			reply: func(_ int, w http.ResponseWriter) {
				w.Header().Set("Retry-After", "150")
				w.WriteHeader(http.StatusAccepted)
			},
			steps:     []time.Duration{time.Minute, time.Minute, time.Minute, time.Minute},
			wantPaths: []string{"/poll", "/poll"}, wantSkip: 2,
		},
		{
			name: "retry-after shorter than the recurrence keeps the recurrence",
			reply: func(_ int, w http.ResponseWriter) {
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(http.StatusAccepted)
			},
			steps: []time.Duration{time.Minute, time.Minute}, wantPaths: []string{"/poll", "/poll"}, wantSkip: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			b := &pollBackend{reply: tc.reply}
			srv := httptest.NewServer(b)
			t.Cleanup(srv.Close)

			s.create(t, "poller", httpTriggerDef(srv.URL+"/poll"))
			s.clock.Advance(0)

			for _, d := range tc.steps {
				s.clock.Advance(d)
				s.waitRuns(t)
			}

			assert.Equal(t, tc.wantPaths, b.seen())
			assert.Equal(t, tc.wantRuns, s.runCount(t, "poller"))

			skipped := 0

			for _, h := range s.pollHistories(t, "poller") {
				if *h.Properties.Status == armlogic.WorkflowStatusSkipped {
					skipped++

					assert.False(t, *h.Properties.Fired)
				}
			}

			assert.Equal(t, tc.wantSkip, skipped)

			b.mu.Lock()
			defer b.mu.Unlock()

			for _, h := range b.hdrs {
				assert.Equal(t, "1|poller", h)
			}
		})
	}
}

func TestLogicHTTPPollingTriggerOutputs(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	srv := httptest.NewServer(&pollBackend{reply: func(_ int, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Reply", "yes")
		_, _ = w.Write([]byte(`{"n":42}`))
	}})
	t.Cleanup(srv.Close)

	s.create(t, "poller", httpTriggerDef(srv.URL))
	s.clock.Advance(0)
	s.clock.Advance(time.Minute)
	s.waitRuns(t)

	run := s.latestRun(t, "poller")
	assert.Equal(t, "poll", *run.Properties.Trigger.Name)

	out, ok := s.content(t, run.Properties.Trigger.OutputsLink).(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 200, out["statusCode"], 0)
	assert.Equal(t, map[string]any{"n": float64(42)}, out["body"])

	hdr, _ := out["headers"].(map[string]any)
	assert.Equal(t, "yes", hdr["X-Reply"])

	assert.Equal(
		t,
		map[string]any{"n": float64(42)},
		s.content(t, s.actions(t, "poller")["Mark"].Properties.OutputsLink),
	)
}

func TestLogicHTTPPollingTriggerRunAndDisable(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	b := &pollBackend{reply: func(_ int, w http.ResponseWriter) { _, _ = w.Write([]byte("hi")) }}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)

	s.create(t, "poller", httpTriggerDef(srv.URL+"/poll"))

	tc := s.factory.NewWorkflowTriggersClient()
	_, err := tc.Run(t.Context(), logicRG, "poller", "poll", nil)
	require.NoError(t, err)
	s.waitRuns(t)
	assert.Equal(t, 1, s.runCount(t, "poller"), "Run polls the endpoint now")
	assert.Equal(t, []string{"/poll"}, b.seen())

	_, err = s.factory.NewWorkflowsClient().Disable(t.Context(), logicRG, "poller", nil)
	require.NoError(t, err)
	s.clock.Advance(0)
	s.clock.Advance(5 * time.Minute)
	s.waitRuns(t)
	assert.Len(t, b.seen(), 1, "a disabled workflow does not poll")

	_, err = tc.Run(t.Context(), logicRG, "poller", "poll", nil)
	require.Error(t, err)
}

func TestLogicHTTPPollingTriggerRefusesExternalHost(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "poller", httpTriggerDef("http://203.0.113.9/poll"))
	s.clock.Advance(0)
	s.clock.Advance(time.Minute)
	s.waitRuns(t)

	assert.Equal(t, 0, s.runCount(t, "poller"))

	hs := s.pollHistories(t, "poller")
	require.Len(t, hs, 1)
	assert.Equal(t, armlogic.WorkflowStatusFailed, *hs[0].Properties.Status)
	assert.Equal(t, "TriggerRequestFailed", *hs[0].Properties.Code)
}

func TestLogicHTTPTriggerValidation(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	rec := la{"frequency": "Minute", "interval": 1}

	tests := []struct {
		inputs la
		rec    la
		name   string
	}{
		{name: "no uri", inputs: la{"method": "GET"}, rec: rec},
		{name: "bad method", inputs: la{"method": "FETCH", "uri": "http://x"}, rec: rec},
		{name: "no recurrence", inputs: la{"method": "GET", "uri": "http://x"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			trig := la{"type": "Http", "inputs": tc.inputs}
			if tc.rec != nil {
				trig["recurrence"] = tc.rec
			}

			_, err := s.factory.NewWorkflowsClient().
				CreateOrUpdate(t.Context(), logicRG, "bad"+strconv.Itoa(len(tc.name)),
					armlogic.Workflow{
						Location:   new("westus"),
						Properties: &armlogic.WorkflowProperties{Definition: logicDef(la{"poll": trig}, la{})},
					}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "InvalidTemplate")
		})
	}
}
