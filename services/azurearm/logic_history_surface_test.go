package azurearm_test

import (
	"context"
	"encoding/json/v2"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (s *logicStack) latestRunName(t *testing.T, wf string) string {
	t.Helper()

	return *s.latestRun(t, wf).Name
}

func (s *logicStack) requestHistories(t *testing.T, wf, action string) []*armlogic.RequestHistory {
	t.Helper()

	var out []*armlogic.RequestHistory

	pager := s.factory.NewWorkflowRunActionRequestHistoriesClient().
		NewListPager(logicRG, wf, s.latestRunName(t, wf), action, nil)
	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		out = append(out, page.Value...)
	}

	return out
}

func httpCall(uri string, extra la) la {
	in := la{
		"method": "POST", "uri": uri, "headers": la{"X-Keep": "1"}, "body": la{"hello": "world"},
		"authentication": la{"type": "Raw", "value": "Bearer secret-token"},
		"retryPolicy":    la{"type": "fixed", "count": 1, "interval": "PT1S"},
	}
	call := la{"type": "Http", "runAfter": la{}, "inputs": in}

	maps.Copy(call, extra)

	return call
}

func TestLogicRequestHistories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra       la
		name        string
		statuses    []int
		wantHidden  bool
		wantBodyLnk bool
	}{
		{name: "single call", statuses: []int{http.StatusOK}, wantBodyLnk: true},
		{
			name:        "retry records every attempt",
			statuses:    []int{http.StatusInternalServerError, http.StatusOK},
			wantBodyLnk: true,
		},
		{
			name: "secure inputs hide the request details", statuses: []int{http.StatusOK}, wantBodyLnk: true,
			extra:      la{"runtimeConfiguration": la{"secureData": la{"properties": []any{"inputs"}}}},
			wantHidden: true,
		},
		{
			name: "secure outputs hide the response body", statuses: []int{http.StatusOK},
			extra: la{"runtimeConfiguration": la{"secureData": la{"properties": []any{"outputs"}}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)

			var (
				mu    = make(chan struct{}, 1)
				calls int
				auth  []string
			)

			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu <- struct{}{}
				n := calls
				calls++
				auth = append(auth, r.Header.Get("Authorization"))
				<-mu

				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Backend", "yes")
				w.WriteHeader(tc.statuses[min(n, len(tc.statuses)-1)])
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			t.Cleanup(backend.Close)

			s.create(t, "hist", logicDef(requestTrigger(nil), la{"Call": httpCall(backend.URL+"/x?q=1", tc.extra)}))

			code, _, _ := s.post(t, s.callback(t, "hist"), `{}`)
			require.Equal(t, http.StatusAccepted, code)

			if len(tc.statuses) > 1 {
				s.clock.waitTimer(t)
				s.clock.Advance(time.Second)
			}

			s.waitRuns(t)

			hs := s.requestHistories(t, "hist", "Call")
			require.Len(t, hs, len(tc.statuses))

			for i, h := range hs {
				p := h.Properties
				require.NotNil(t, p)
				assert.Equal(t, "Microsoft.Logic/workflows/runs/actions/requestHistories", *h.Type)
				assert.Equal(t, "POST", *p.Request.Method)
				assert.Equal(t, int32(tc.statuses[i]), *p.Response.StatusCode)
				assert.False(t, p.EndTime.Before(*p.StartTime))

				if tc.wantHidden {
					assert.Nil(t, p.Request.URI)
					assert.Nil(t, p.Request.Headers)
				} else {
					assert.Equal(t, backend.URL+"/x?q=1", *p.Request.URI)

					hdr, ok := p.Request.Headers.(map[string]any)
					require.True(t, ok)
					assert.Equal(t, "1", hdr["X-Keep"])
					assert.Equal(t, "hist", hdr["X-Ms-Workflow-Name"])
					assert.NotContains(t, hdr, "Authorization", "credentials are never recorded")
				}

				if tc.wantBodyLnk {
					assert.JSONEq(t, `{"ok":true}`, toJSON(t, s.content(t, p.Response.BodyLink)))
					assert.EqualValues(t, len(`{"ok":true}`), *p.Response.BodyLink.ContentSize)
				} else {
					assert.Nil(t, p.Response.BodyLink)
					assert.Nil(t, p.Response.Headers)
				}
			}

			got, err := s.factory.NewWorkflowRunActionRequestHistoriesClient().
				Get(t.Context(), logicRG, "hist", s.latestRunName(t, "hist"), "Call", *hs[0].Name, nil)
			require.NoError(t, err)
			assert.Equal(t, *hs[0].ID, *got.ID)

			_, err = s.factory.NewWorkflowRunActionRequestHistoriesClient().
				Get(t.Context(), logicRG, "hist", s.latestRunName(t, "hist"), "Call", "missing", nil)
			require.Error(t, err)

			assert.Equal(t, append([]string(nil), auth...), auth)

			for _, a := range auth {
				assert.Equal(t, "Bearer secret-token", a, "the backend still receives the credential")
			}
		})
	}
}

func TestLogicRequestHistoryNotFound(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "plain", logicDef(requestTrigger(nil), la{"Mark": compose("1", nil)}))
	s.invoke(t, "plain", `{}`)

	run := s.latestRunName(t, "plain")
	c := s.factory.NewWorkflowRunActionRequestHistoriesClient()

	tests := []struct {
		name   string
		run    string
		action string
	}{
		{name: "unknown run", run: "nope", action: "Mark"},
		{name: "unknown action", run: run, action: "Nope"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.NewListPager(logicRG, "plain", tc.run, tc.action, nil).NextPage(t.Context())
			require.Error(t, err)
		})
	}

	page, err := c.NewListPager(logicRG, "plain", run, "Mark", nil).NextPage(t.Context())
	require.NoError(t, err)
	assert.Empty(t, page.Value, "an action that made no HTTP call has no request histories")
}

func TestLogicRepetitionRequestHistories(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.URL.Path))
	}))
	t.Cleanup(backend.Close)

	s.create(t, "loop", logicDef(requestTrigger(nil), la{"Loop": la{
		"type": "Foreach", "foreach": []any{"a", "b"}, "operationOptions": "Sequential", "runAfter": la{},
		"actions": la{"Call": la{
			"type": "Http", "runAfter": la{},
			"inputs": la{"method": "GET", "uri": backend.URL + "/@{item()}", "retryPolicy": la{"type": "none"}},
		}},
	}}))
	s.invoke(t, "loop", `{}`)

	run := s.latestRunName(t, "loop")
	reps := s.factory.NewWorkflowRunActionRepetitionsClient().NewListPager(logicRG, "loop", run, "Call", nil)
	rpage, err := reps.NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, rpage.Value, 2)

	rc := s.factory.NewWorkflowRunActionRepetitionsRequestHistoriesClient()

	for i, item := range []string{"a", "b"} {
		page, lerr := rc.NewListPager(logicRG, "loop", run, "Call", *rpage.Value[i].Name, nil).NextPage(t.Context())
		require.NoError(t, lerr)
		require.Len(t, page.Value, 1)
		assert.Equal(t, backend.URL+"/"+item, *page.Value[0].Properties.Request.URI)
		assert.Equal(t, "/"+item, s.content(t, page.Value[0].Properties.Response.BodyLink))

		got, gerr := rc.Get(t.Context(), logicRG, "loop", run, "Call", *rpage.Value[i].Name, *page.Value[0].Name, nil)
		require.NoError(t, gerr)
		assert.Equal(t, *page.Value[0].ID, *got.ID)
	}

	_, err = rc.NewListPager(logicRG, "loop", run, "Call", "999999", nil).NextPage(t.Context())
	require.Error(t, err)
}

func TestLogicExpressionTraces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     la
		wantPath   string
		wantText   string
		wantValue  any
		wantSubs   []string
		wantHidden bool
	}{
		{
			name:     "function call with nested call",
			action:   compose("@concat('a', toUpper('b'))", nil),
			wantPath: "inputs", wantText: "@concat('a', toUpper('b'))", wantValue: "aB",
			wantSubs: []string{"'a'", "toUpper('b')"},
		},
		{
			name:     "property of an object input",
			action:   la{"type": "Compose", "runAfter": la{}, "inputs": la{"n": "@add(1, 2)"}},
			wantPath: "inputs.n", wantText: "@add(1, 2)", wantValue: float64(3), wantSubs: []string{"1", "2"},
		},
		{
			name: "interpolation",
			action: la{
				"type": "Compose", "runAfter": la{}, "inputs": "x@{string(5)}y",
			},
			wantPath: "inputs", wantText: "x@{string(5)}y", wantValue: "x5y", wantSubs: []string{"string(5)"},
		},
		{
			name: "secure inputs hide the values",
			action: la{
				"type": "Compose", "runAfter": la{}, "inputs": "@concat('a', 'b')",
				"runtimeConfiguration": la{"secureData": la{"properties": []any{"inputs"}}},
			},
			wantPath: "inputs", wantText: "@concat('a', 'b')", wantSubs: []string{"'a'", "'b'"}, wantHidden: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "trace", logicDef(requestTrigger(nil), la{"Act": tc.action}))
			s.invoke(t, "trace", `{}`)

			res, err := s.factory.NewWorkflowRunActionsClient().
				NewListExpressionTracesPager(logicRG, "trace", s.latestRunName(t, "trace"), "Act", nil).
				NextPage(t.Context())
			require.NoError(t, err)
			require.Len(t, res.Inputs, 1)

			root := res.Inputs[0]
			assert.Equal(t, tc.wantPath, *root.Path)
			assert.Equal(t, tc.wantText, *root.Text)

			subs := make([]string, 0, len(root.Subexpressions))
			for _, sub := range root.Subexpressions {
				subs = append(subs, *sub.Text)
			}

			assert.Equal(t, tc.wantSubs, subs)

			if tc.wantHidden {
				assert.Nil(t, root.Value)
				assert.Nil(t, root.Subexpressions[0].Value)

				return
			}

			assert.Equal(t, tc.wantValue, root.Value)
		})
	}
}

func TestLogicExpressionTracesErrorAndBounds(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "bad", logicDef(requestTrigger(nil), la{
		"Boom": compose("@div(1, 0)", nil),
		"Many": la{"type": "Compose", "runAfter": la{}, "inputs": manyExpressions(250)},
	}))
	s.invoke(t, "bad", `{}`)

	c := s.factory.NewWorkflowRunActionsClient()
	run := s.latestRunName(t, "bad")

	boom, err := c.NewListExpressionTracesPager(logicRG, "bad", run, "Boom", nil).NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, boom.Inputs, 1)
	require.NotNil(t, boom.Inputs[0].Error)
	assert.NotEmpty(t, *boom.Inputs[0].Error.Message)

	many, err := c.NewListExpressionTracesPager(logicRG, "bad", run, "Many", nil).NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, many.Inputs, 100, "traces are bounded per action")

	_, err = c.NewListExpressionTracesPager(logicRG, "bad", run, "Nope", nil).NextPage(t.Context())
	require.Error(t, err)
}

func manyExpressions(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = "@utcNow()"
	}

	return out
}

func TestLogicRepetitionExpressionTraces(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "reptrace", logicDef(requestTrigger(nil), la{"Loop": la{
		"type": "Foreach", "foreach": []any{"a", "b"}, "operationOptions": "Sequential", "runAfter": la{},
		"actions": la{"Echo": compose("@toUpper(item())", nil)},
	}}))
	s.invoke(t, "reptrace", `{}`)

	run := s.latestRunName(t, "reptrace")
	c := s.factory.NewWorkflowRunActionRepetitionsClient()
	reps, err := c.NewListPager(logicRG, "reptrace", run, "Echo", nil).NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, reps.Value, 2)

	for i, want := range []string{"A", "B"} {
		res, terr := c.NewListExpressionTracesPager(logicRG, "reptrace", run, "Echo", *reps.Value[i].Name, nil).
			NextPage(t.Context())
		require.NoError(t, terr)
		require.Len(t, res.Inputs, 1)
		assert.Equal(t, want, res.Inputs[0].Value)
	}
}

func TestLogicRunOperations(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "ops", logicDef(la{"manual": la{"type": "Request", "kind": "Http"}},
		la{"Mark": compose("@triggerBody()?['n']", nil)}))
	s.create(t, "other", logicDef(la{"manual": la{"type": "Request", "kind": "Http"}},
		la{"Mark": compose("1", nil)}))

	code, _, _ := s.post(t, s.callback(t, "ops"), `{"n":7}`)
	require.Equal(t, http.StatusAccepted, code)
	s.waitRuns(t)

	hc := s.factory.NewWorkflowTriggerHistoriesClient()
	page, err := hc.NewListPager(logicRG, "ops", "manual", nil).NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, page.Value, 1)

	ctx, resp := captureResponse(t.Context())
	_, err = hc.Resubmit(ctx, logicRG, "ops", "manual", *page.Value[0].Name, nil)
	require.NoError(t, err)
	s.waitRuns(t)

	loc := (*resp).Header.Get("Location")
	require.NotEmpty(t, loc)

	opID := opIDFrom(loc)
	run := s.latestRunName(t, "ops")
	oc := s.factory.NewWorkflowRunOperationsClient()

	got, err := oc.Get(t.Context(), logicRG, "ops", run, opID, nil)
	require.NoError(t, err)
	assert.Equal(t, run, *got.Name)
	assert.Equal(t, armlogic.WorkflowStatusSucceeded, *got.Properties.Status)

	tests := []struct {
		name string
		wf   string
		run  string
		op   string
	}{
		{name: "unknown operation", wf: "ops", run: run, op: "00000000-0000-0000-0000-000000000000"},
		{name: "operation of another run", wf: "ops", run: *page.Value[0].Name, op: opID},
		{name: "unknown run", wf: "ops", run: "nope", op: opID},
		{name: "unknown workflow", wf: "missing", run: run, op: opID},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, gerr := oc.Get(t.Context(), logicRG, tc.wf, tc.run, tc.op, nil)
			require.Error(t, gerr)
		})
	}
}

func TestLogicCancelRecordsOperation(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "cancelme", logicDef(requestTrigger(nil), la{
		"Pause": la{"type": "Wait", "runAfter": la{}, "inputs": la{"interval": la{"count": 1, "unit": "Hour"}}},
	}))

	code, _, _ := s.post(t, s.callback(t, "cancelme"), `{}`)
	require.Equal(t, http.StatusAccepted, code)
	s.clock.waitTimer(t)

	run := s.latestRunName(t, "cancelme")

	ctx, resp := captureResponse(t.Context())
	_, err := s.factory.NewWorkflowRunsClient().Cancel(ctx, logicRG, "cancelme", run, nil)
	require.NoError(t, err)
	s.waitRuns(t)

	loc := (*resp).Header.Get("Location")
	require.Contains(t, loc, "/runs/"+run+"/operations/")

	status, body := s.rawJSON(t, http.MethodGet, "/workflows/cancelme/runs/"+run+"/operations/"+opIDFrom(loc), "")
	require.Equal(t, http.StatusOK, status)
	props, _ := body["properties"].(map[string]any)
	assert.Equal(t, "Cancelled", props["status"])
}

func captureResponse(ctx context.Context) (context.Context, **http.Response) {
	var resp *http.Response

	return policy.WithCaptureResponse(ctx, &resp), &resp
}

func opIDFrom(loc string) string {
	_, after, _ := strings.Cut(loc, "/operations/")
	id, _, _ := strings.Cut(after, "?")

	return id
}

func toJSON(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v, jsonx.V1())
	require.NoError(t, err)

	return string(b)
}

func TestLogicHistorySurfacePersistence(t *testing.T) {
	t.Parallel()

	a := newLogicStack(t)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	}))
	t.Cleanup(backend.Close)

	a.create(t, "keep", logicDef(la{"manual": la{"type": "Request", "kind": "Http"}}, la{
		"Call": httpCall(backend.URL, nil),
		"Mark": la{"type": "Compose", "runAfter": la{"Call": []any{"Succeeded"}}, "inputs": "@concat('x', 'y')"},
	}))

	code, _, _ := a.post(t, a.callback(t, "keep"), `{}`)
	require.Equal(t, http.StatusAccepted, code)
	a.waitRuns(t)

	hc := a.factory.NewWorkflowTriggerHistoriesClient()
	page, err := hc.NewListPager(logicRG, "keep", "manual", nil).NextPage(t.Context())
	require.NoError(t, err)

	ctx, resp := captureResponse(t.Context())
	_, err = hc.Resubmit(ctx, logicRG, "keep", "manual", *page.Value[0].Name, nil)
	require.NoError(t, err)
	a.waitRuns(t)

	opID := opIDFrom((*resp).Header.Get("Location"))
	run := a.latestRunName(t, "keep")

	b := newLogicStack(t)
	require.NoError(t, b.handler.Restore(t.Context(), a.handler.Snapshot(t.Context())))

	hs := b.requestHistories(t, "keep", "Call")
	require.Len(t, hs, 1)
	assert.Equal(t, "pong", b.content(t, hs[0].Properties.Response.BodyLink))

	tr, err := b.factory.NewWorkflowRunActionsClient().
		NewListExpressionTracesPager(logicRG, "keep", run, "Mark", nil).NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, tr.Inputs, 1)
	assert.Equal(t, "xy", tr.Inputs[0].Value)

	got, err := b.factory.NewWorkflowRunOperationsClient().Get(t.Context(), logicRG, "keep", run, opID, nil)
	require.NoError(t, err)
	assert.Equal(t, run, *got.Name)
}
