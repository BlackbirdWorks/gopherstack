package azurearm_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type la = map[string]any

func after(pairs ...any) la {
	out := la{}

	for i := 0; i+1 < len(pairs); i += 2 {
		name, _ := pairs[i].(string)
		sts := []any{}

		switch v := pairs[i+1].(type) {
		case string:
			sts = append(sts, v)
		case []string:
			for _, x := range v {
				sts = append(sts, x)
			}
		}

		out[name] = sts
	}

	return out
}

func compose(inputs any, ra la) la {
	if ra == nil {
		ra = la{}
	}

	return la{"type": "Compose", "inputs": inputs, "runAfter": ra}
}

func initVar(name, typ string, value any) la {
	return la{
		"type": "InitializeVariable", "runAfter": la{},
		"inputs": la{"variables": []any{la{"name": name, "type": typ, "value": value}}},
	}
}

func varOp(typ, name string, value any, ra la) la {
	in := la{"name": name}
	if value != nil {
		in["value"] = value
	}

	return la{"type": typ, "inputs": in, "runAfter": ra}
}

type seenRequest struct{ body, name, auth, query, test, run string }

type engineCase struct {
	actions     la
	wantActions map[string]string
	wantOutputs map[string]any
	wantCodes   map[string]string
	name        string
	body        string
	wantRun     string
}

func TestLogicEngine(t *testing.T) {
	t.Parallel()

	failDiv := compose("@div(1, 0)", nil)

	tests := []engineCase{
		{
			name: "failed handler runs and run succeeds",
			actions: la{
				"A": failDiv,
				"B": compose("handled", after("A", "Failed")),
				"C": compose("skipped", after("A", "Succeeded")),
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"A": "Failed", "B": "Succeeded", "C": "Skipped"},
			wantCodes:   map[string]string{"A": "InvalidTemplate", "C": "ActionConditionFailed"},
		},
		{
			name: "unhandled failure fails the run",
			actions: la{
				"A": failDiv,
				"B": compose("x", after("A", "Succeeded")),
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"A": "Failed", "B": "Skipped"},
		},
		{
			name: "runAfter skipped",
			actions: la{
				"A": failDiv,
				"B": compose("x", after("A", "Succeeded")),
				"C": compose("after skipped", after("B", "Skipped")),
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"A": "Failed", "B": "Skipped", "C": "Succeeded"},
		},
		{
			name: "runAfter multiple statuses",
			actions: la{
				"A": failDiv,
				"B": compose("x", after("A", []string{"Succeeded", "Failed"})),
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"A": "Failed", "B": "Succeeded"},
		},
		{
			name: "parallel branches join",
			actions: la{
				"A": compose(1, nil),
				"B": compose(2, nil),
				"C": compose("@add(outputs('A'), outputs('B'))", after("A", "Succeeded", "B", "Succeeded")),
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"A": "Succeeded", "B": "Succeeded", "C": "Succeeded"},
			wantOutputs: map[string]any{"C": float64(3)},
		},
		{
			name: "foreach sequential accumulates in order",
			actions: la{
				"Init": initVar("arr", "Array", []any{}),
				"Loop": la{
					"type": "Foreach", "foreach": []any{1, 2, 3}, "operationOptions": "Sequential",
					"runAfter": after("Init", "Succeeded"),
					"actions":  la{"Add": varOp("AppendToArrayVariable", "arr", "@item()", la{})},
				},
				"Out": compose("@variables('arr')", after("Loop", "Succeeded")),
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"Loop": "Succeeded", "Add": "Succeeded", "Out": "Succeeded"},
			wantOutputs: map[string]any{"Out": []any{float64(1), float64(2), float64(3)}},
		},
		{
			name: "foreach concurrency counts every item",
			actions: la{
				"Init": initVar("n", "Integer", 0),
				"Loop": la{
					"type": "Foreach", "foreach": "@range(0, 25)", "runAfter": after("Init", "Succeeded"),
					"runtimeConfiguration": la{"concurrency": la{"repetitions": 5}},
					"actions":              la{"Inc": varOp("IncrementVariable", "n", 2, la{})},
				},
				"Out": compose("@variables('n')", after("Loop", "Succeeded")),
			},
			wantRun:     "Succeeded",
			wantOutputs: map[string]any{"Out": float64(50)},
		},
		{
			name: "foreach inner failure fails loop",
			actions: la{
				"Loop": la{
					"type": "Foreach", "foreach": []any{1, 2, 3}, "runAfter": la{},
					"actions": la{"Div": compose("@div(1, sub(item(), 2))", nil)},
				},
				"Handler": compose("recovered", after("Loop", "Failed")),
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"Loop": "Failed", "Div": "Failed", "Handler": "Succeeded"},
		},
		{
			name: "foreach over non array fails",
			actions: la{
				"Loop": la{"type": "Foreach", "foreach": "notarray", "runAfter": la{}, "actions": la{}},
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"Loop": "Failed"},
		},
		{
			name: "nested foreach uses items by loop name",
			actions: la{
				"Init": initVar("s", "String", ""),
				"Outer": la{
					"type": "Foreach", "foreach": []any{"a", "b"}, "operationOptions": "Sequential",
					"runAfter": after("Init", "Succeeded"),
					"actions": la{
						"Inner": la{
							"type":             "Foreach",
							"foreach":          []any{1, 2},
							"operationOptions": "Sequential",
							"runAfter":         la{},
							"actions": la{
								"App": varOp(
									"AppendToStringVariable",
									"s",
									"@concat(items('Outer'), item(), ',')",
									la{},
								),
							},
						},
					},
				},
				"Out": compose("@variables('s')", after("Outer", "Succeeded")),
			},
			wantRun:     "Succeeded",
			wantOutputs: map[string]any{"Out": "a1,a2,b1,b2,"},
		},
		{
			name: "until counts to three",
			actions: la{
				"Init": initVar("i", "Integer", 0),
				"Loop": la{
					"type": "Until", "expression": "@equals(variables('i'), 3)", "runAfter": after("Init", "Succeeded"),
					"limit":   la{"count": 10, "timeout": "PT1H"},
					"actions": la{"Inc": varOp("IncrementVariable", "i", 1, la{})},
				},
				"Out": compose("@variables('i')", after("Loop", "Succeeded")),
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"Loop": "Succeeded"},
			wantOutputs: map[string]any{"Out": float64(3)},
		},
		{
			name: "until limit times out and TimedOut handler runs",
			actions: la{
				"Init": initVar("i", "Integer", 0),
				"Loop": la{
					"type":       "Until",
					"expression": "@equals(variables('i'), 99)",
					"runAfter":   after("Init", "Succeeded"),
					"limit":      la{"count": 3},
					"actions":    la{"Inc": varOp("IncrementVariable", "i", 1, la{})},
				},
				"Skip":    compose("x", after("Loop", "Succeeded")),
				"Handler": compose("@variables('i')", after("Loop", "TimedOut")),
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"Loop": "TimedOut", "Skip": "Skipped", "Handler": "Succeeded"},
			wantOutputs: map[string]any{"Handler": float64(3)},
		},
		{
			name: "switch takes matching case",
			body: `{"k":"b"}`,
			actions: la{
				"Sw": la{
					"type": "Switch", "expression": "@triggerBody()?['k']", "runAfter": la{},
					"cases": la{
						"Case_a": la{"case": "a", "actions": la{"OnA": compose("a", nil)}},
						"Case_b": la{"case": "b", "actions": la{"OnB": compose("b", nil)}},
					},
					"default": la{"actions": la{"OnDefault": compose("d", nil)}},
				},
			},
			wantRun: "Succeeded",
			wantActions: map[string]string{
				"Sw": "Succeeded", "OnA": "Skipped", "OnB": "Succeeded", "OnDefault": "Skipped",
			},
			wantCodes: map[string]string{"OnA": "ActionBranchingConditionNotSatisfied"},
		},
		{
			name: "switch default",
			body: `{"k":"zzz"}`,
			actions: la{
				"Sw": la{
					"type": "Switch", "expression": "@triggerBody()?['k']", "runAfter": la{},
					"cases":   la{"Case_a": la{"case": "a", "actions": la{"OnA": compose("a", nil)}}},
					"default": la{"actions": la{"OnDefault": compose("d", nil)}},
				},
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"OnA": "Skipped", "OnDefault": "Succeeded"},
		},
		{
			name: "if takes else branch",
			body: `{"n":-1}`,
			actions: la{
				"Cond": la{
					"type": "If", "runAfter": la{},
					"expression": la{"and": []any{la{"greater": []any{"@triggerBody()?['n']", 0}}}},
					"actions":    la{"Then": compose("then", nil)},
					"else":       la{"actions": la{"Else": compose("else", nil)}},
				},
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"Cond": "Succeeded", "Then": "Skipped", "Else": "Succeeded"},
		},
		{
			name: "if string expression",
			body: `{"n":5}`,
			actions: la{
				"Cond": la{
					"type": "If", "runAfter": la{}, "expression": "@greater(triggerBody()?['n'], 0)",
					"actions": la{"Then": compose("then", nil)},
				},
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"Then": "Succeeded"},
		},
		{
			name: "terminate failed skips the rest",
			actions: la{
				"A": compose(1, nil),
				"T": la{
					"type": "Terminate", "runAfter": after("A", "Succeeded"),
					"inputs": la{"runStatus": "Failed", "runError": la{"code": "Boom", "message": "stopped here"}},
				},
				"B": compose(2, after("T", "Succeeded")),
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"A": "Succeeded", "T": "Succeeded", "B": "Skipped"},
		},
		{
			name: "terminate cancelled",
			actions: la{
				"T": la{"type": "Terminate", "runAfter": la{}, "inputs": la{"runStatus": "Cancelled"}},
				"B": compose(2, after("T", "Succeeded")),
			},
			wantRun:     "Cancelled",
			wantActions: map[string]string{"B": "Skipped"},
		},
		{
			name: "terminate succeeded",
			actions: la{
				"T": la{"type": "Terminate", "runAfter": la{}, "inputs": la{"runStatus": "Succeeded"}},
				"B": compose(2, after("T", "Succeeded")),
			},
			wantRun:     "Succeeded",
			wantActions: map[string]string{"B": "Skipped"},
		},
		{
			name: "scope failure handled by catch scope",
			actions: la{
				"Try": la{
					"type": "Scope", "runAfter": la{},
					"actions": la{"Bad": failDiv, "After": compose("x", after("Bad", "Succeeded"))},
				},
				"Catch": la{
					"type": "Scope", "runAfter": after("Try", "Failed"),
					"actions": la{"Log": compose("@actions('Bad').status", nil)},
				},
			},
			wantRun: "Succeeded",
			wantActions: map[string]string{
				"Try": "Failed", "Bad": "Failed", "After": "Skipped", "Catch": "Succeeded", "Log": "Succeeded",
			},
			wantOutputs: map[string]any{"Log": "Failed"},
		},
		{
			name: "scope without handler fails run",
			actions: la{
				"Try": la{"type": "Scope", "runAfter": la{}, "actions": la{"Bad": failDiv}},
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"Try": "Failed"},
		},
		{
			name: "data operations",
			actions: la{
				"Sel": la{
					"type":     "Select",
					"runAfter": la{},
					"inputs":   la{"from": []any{1, 2, 3}, "select": la{"n": "@item()"}},
				},
				"Qry": la{
					"type":     "Query",
					"runAfter": la{},
					"inputs":   la{"from": []any{1, 3, 0, 5, 4, 2}, "where": "@greater(item(), 2)"},
				},
				"Join": la{"type": "Join", "runAfter": la{}, "inputs": la{"from": []any{1, 2, 3, 4}, "joinWith": ","}},
				"Tbl": la{"type": "Table", "runAfter": la{}, "inputs": la{
					"format": "CSV",
					"from":   []any{la{"id": 1, "name": "a"}, la{"id": 2, "name": "b"}},
					"columns": []any{
						la{"header": "ID", "value": "@item().id"},
						la{"header": "Name", "value": "@toUpper(item().name)"},
					},
				}},
				"Parse": la{"type": "ParseJson", "runAfter": la{}, "inputs": la{
					"content": `{"Email":"x@y.z","Age":5}`,
					"schema": la{
						"type":       "object",
						"properties": la{"Email": la{"type": "string"}, "Age": la{"type": "integer"}},
					},
				}},
				"Out": compose(la{
					"sel": "@body('Sel')", "qry": "@body('Qry')", "join": "@body('Join')", "tbl": "@body('Tbl')",
					"mail": "@body('Parse')?['Email']",
				}, after("Sel", "Succeeded", "Qry", "Succeeded", "Join", "Succeeded", "Tbl", "Succeeded", "Parse", "Succeeded")),
			},
			wantRun: "Succeeded",
			wantOutputs: map[string]any{"Out": la{
				"sel":  []any{la{"n": float64(1)}, la{"n": float64(2)}, la{"n": float64(3)}},
				"qry":  []any{float64(3), float64(5), float64(4)},
				"join": "1,2,3,4",
				"tbl":  "ID,Name\n1,A\n2,B\n",
				"mail": "x@y.z",
			}},
		},
		{
			name: "parse json schema mismatch fails",
			actions: la{
				"Parse": la{"type": "ParseJson", "runAfter": la{}, "inputs": la{
					"content": `{"Age":"five"}`,
					"schema":  la{"type": "object", "properties": la{"Age": la{"type": "integer"}}},
				}},
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"Parse": "Failed"},
		},
		{
			name: "variable type mismatch fails",
			actions: la{
				"Init": initVar("s", "String", "x"),
				"Set":  varOp("SetVariable", "s", 5, after("Init", "Succeeded")),
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"Init": "Succeeded", "Set": "Failed"},
		},
		{
			name: "undeclared variable fails",
			actions: la{
				"Set": varOp("SetVariable", "nope", 5, la{}),
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"Set": "Failed"},
		},
		{
			name: "workflow context expressions",
			body: `{"a":{"b":7}}`,
			actions: la{
				"Out": compose(la{
					"name": "@workflow().name", "deep": "@triggerBody()?['a']?['b']",
					"hdr": "@triggerOutputs()['headers']['content-type']", "trig": "@trigger().name",
					"param": "@parameters('who')",
				}, nil),
			},
			wantRun: "Succeeded",
			wantOutputs: map[string]any{"Out": la{
				"name": "engine", "deep": float64(7), "hdr": "application/json", "trig": "manual", "param": "world",
			}},
		},
		{
			name: "unsupported action type fails honestly",
			actions: la{
				"Fn": la{"type": "Function", "runAfter": la{}, "inputs": la{}},
			},
			wantRun:     "Failed",
			wantActions: map[string]string{"Fn": "Failed"},
			wantCodes:   map[string]string{"Fn": "NotImplemented"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)

			def := logicDef(requestTrigger(nil), tc.actions)
			def["parameters"] = la{"who": la{"type": "String", "defaultValue": "world"}}

			_, err := s.factory.NewWorkflowsClient().CreateOrUpdate(t.Context(), logicRG, "engine", armlogic.Workflow{
				Location: new("westus"), Properties: &armlogic.WorkflowProperties{Definition: def},
			}, nil)
			require.NoError(t, err)

			body := tc.body
			if body == "" {
				body = "{}"
			}

			out := s.invoke(t, "engine", body)
			assert.Equal(t, tc.wantRun, string(*out.run.Properties.Status))

			for name, want := range tc.wantActions {
				require.Contains(t, out.actions, name)
				assert.Equal(t, want, string(*out.actions[name].Properties.Status), "action %s", name)
			}

			for name, want := range tc.wantCodes {
				require.Contains(t, out.actions, name)
				assert.Equal(t, want, *out.actions[name].Properties.Code, "action %s", name)
			}

			for name, want := range tc.wantOutputs {
				require.Contains(t, out.actions, name)
				assert.Equal(t, want, s.content(t, out.actions[name].Properties.OutputsLink), "action %s", name)
			}
		})
	}
}

func TestLogicEngineTerminateError(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "term", logicDef(requestTrigger(nil), la{
		"T": la{
			"type": "Terminate", "runAfter": la{},
			"inputs": la{"runStatus": "Failed", "runError": la{"code": "Boom", "message": "stopped here"}},
		},
	}))

	out := s.invoke(t, "term", "{}")
	assert.Equal(t, armlogic.WorkflowStatusFailed, *out.run.Properties.Status)

	raw, err := json.Marshal(out.run.Properties.Error, jsonx.V1())
	require.NoError(t, err)
	assert.JSONEq(t, `{"code":"Boom","message":"stopped here"}`, string(raw))
}

func TestLogicEngineWaitOnClock(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "waiter", logicDef(requestTrigger(nil), la{
		"Pause": la{"type": "Wait", "runAfter": la{}, "inputs": la{"interval": la{"count": 5, "unit": "Minute"}}},
		"Done":  compose("@utcNow()", after("Pause", "Succeeded")),
	}))

	code, _, _ := s.post(t, s.callback(t, "waiter"), "{}")
	require.Equal(t, http.StatusAccepted, code)

	s.clock.waitTimers(t, 1)

	run := s.latestRun(t, "waiter")
	assert.Equal(t, armlogic.WorkflowStatusRunning, *run.Properties.Status)
	assert.Nil(t, run.Properties.EndTime)

	s.clock.Advance(4*time.Minute + 59*time.Second)
	assert.Equal(t, armlogic.WorkflowStatusRunning, *s.latestRun(t, "waiter").Properties.Status)

	s.clock.Advance(time.Second)
	s.waitRuns(t)

	run = s.latestRun(t, "waiter")
	assert.Equal(t, armlogic.WorkflowStatusSucceeded, *run.Properties.Status)

	acts := s.actions(t, "waiter")
	assert.Equal(t, "2026-01-01T00:05:00.0000000Z", s.content(t, acts["Done"].Properties.OutputsLink))
	assert.Equal(t, 5*time.Minute, acts["Pause"].Properties.EndTime.Sub(*acts["Pause"].Properties.StartTime))
}

func TestLogicEngineWaitUntilAndCancel(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "until", logicDef(requestTrigger(nil), la{
		"Pause": la{"type": "Wait", "runAfter": la{}, "inputs": la{"until": la{"timestamp": "2026-01-01T01:00:00Z"}}},
	}))

	code, _, _ := s.post(t, s.callback(t, "until"), "{}")
	require.Equal(t, http.StatusAccepted, code)
	s.clock.waitTimers(t, 1)

	run := s.latestRun(t, "until")
	_, err := s.factory.NewWorkflowRunsClient().Cancel(t.Context(), logicRG, "until", *run.Name, nil)
	require.NoError(t, err)
	s.waitRuns(t)

	run = s.latestRun(t, "until")
	assert.Equal(t, armlogic.WorkflowStatusCancelled, *run.Properties.Status)
	assert.Equal(t, armlogic.WorkflowStatusCancelled, *s.actions(t, "until")["Pause"].Properties.Status)

	_, err = s.factory.NewWorkflowRunsClient().Cancel(t.Context(), logicRG, "until", *run.Name, nil)
	require.Error(t, err)

	_, err = s.factory.NewWorkflowRunsClient().Cancel(t.Context(), logicRG, "until", "nope", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestLogicEngineHTTPAction(t *testing.T) {
	t.Parallel()

	var (
		mu   sync.Mutex
		seen = map[string]seenRequest{}
	)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)

		mu.Lock()
		seen[r.URL.Path] = seenRequest{
			body: string(raw), name: r.Header.Get("X-Ms-Workflow-Name"), auth: r.Header.Get("Authorization"),
			query: r.URL.RawQuery, test: r.Header.Get("X-Test"), run: r.Header.Get("X-Ms-Workflow-Run-Id"),
		}
		mu.Unlock()

		if r.URL.Path == "/fail" {
			http.Error(w, "boom", http.StatusInternalServerError)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"echo":"ok","n":3}`))
	}))
	t.Cleanup(backend.Close)

	s := newLogicStack(t)
	s.create(t, "caller", logicDef(requestTrigger(nil), la{
		"Call": la{
			"type": "Http", "runAfter": la{},
			"inputs": la{
				"method": "POST", "uri": backend.URL + "/echo", "queries": la{"q": "1"},
				"headers": la{"X-Test": "yes"}, "body": la{"msg": "@triggerBody()?['msg']"},
				"authentication": la{"type": "Basic", "username": "u", "password": "p"},
			},
		},
		"Bad": la{
			"type": "Http", "runAfter": after("Call", "Succeeded"),
			"inputs": la{"method": "GET", "uri": backend.URL + "/fail", "retryPolicy": la{"type": "none"}},
		},
		"Recovered": compose("@actions('Bad').outputs.statusCode", after("Bad", "Failed")),
		"Respond": la{
			"type": "Response", "runAfter": after("Recovered", "Succeeded"),
			"inputs": la{"statusCode": 200, "body": "@body('Call')"},
		},
	}))

	out := s.invoke(t, "caller", `{"msg":"hi"}`)

	assert.Equal(t, http.StatusOK, out.status)
	assert.JSONEq(t, `{"echo":"ok","n":3}`, string(out.body))
	assert.Equal(t, armlogic.WorkflowStatusSucceeded, *out.run.Properties.Status)
	assert.Equal(t, armlogic.WorkflowStatusFailed, *out.actions["Bad"].Properties.Status)
	assert.Equal(t, "InternalServerError", *out.actions["Bad"].Properties.Code)
	assert.EqualValues(t, 500, s.content(t, out.actions["Recovered"].Properties.OutputsLink))

	call, ok := s.content(t, out.actions["Call"].Properties.OutputsLink).(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 200, call["statusCode"])

	mu.Lock()
	echo := seen["/echo"]
	mu.Unlock()

	assert.Equal(t, "caller", echo.name)
	assert.Equal(t, *out.run.Name, echo.run)
	assert.Equal(t, "Basic dTpw", echo.auth)
	assert.Equal(t, "q=1", echo.query)
	assert.Equal(t, "yes", echo.test)
	assert.JSONEq(t, `{"msg":"hi"}`, echo.body)

	in, ok := s.content(t, out.actions["Call"].Properties.InputsLink).(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{"msg": "hi"}, in["body"])
	assert.NotContains(t, in, "authentication")
}

func TestLogicEngineHTTPLoopbackOnly(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "ext", logicDef(requestTrigger(nil), la{
		"Call": la{
			"type": "Http", "runAfter": la{},
			"inputs": la{"method": "GET", "uri": "http://192.0.2.1:9/x", "retryPolicy": la{"type": "none"}},
		},
	}))

	out := s.invoke(t, "ext", "{}")
	assert.Equal(t, armlogic.WorkflowStatusFailed, *out.run.Properties.Status)
	assert.Equal(t, armlogic.WorkflowStatusFailed, *out.actions["Call"].Properties.Status)

	raw, err := json.Marshal(out.actions["Call"].Properties.Error, jsonx.V1())
	require.NoError(t, err)
	assert.Contains(t, string(raw), "azure-arm-logic-allow-external")
}

func TestLogicEngineResponseSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		actions  la
		name     string
		wantBody string
		want     int
	}{
		{
			name: "custom status and text body",
			actions: la{"R": la{"type": "Response", "runAfter": la{}, "inputs": la{
				"statusCode": 404, "body": "nothing here", "headers": la{"Content-Type": "text/plain"},
			}}},
			want: http.StatusNotFound, wantBody: "nothing here",
		},
		{
			name:    "no response action is accepted",
			actions: la{"A": compose(1, nil)},
			want:    http.StatusAccepted,
		},
		{
			name: "response skipped gives accepted",
			actions: la{
				"A": compose("@div(1, 0)", nil),
				"R": la{"type": "Response", "runAfter": after("A", "Succeeded"), "inputs": la{"statusCode": 200}},
			},
			want: http.StatusAccepted,
		},
		{
			name: "response before further work",
			actions: la{
				"R": la{"type": "Response", "runAfter": la{}, "inputs": la{"statusCode": 201, "body": "early"}},
				"A": compose(1, after("R", "Succeeded")),
			},
			want: http.StatusCreated, wantBody: "early",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "resp", logicDef(requestTrigger(nil), tc.actions))

			out := s.invoke(t, "resp", "{}")
			assert.Equal(t, tc.want, out.status)
			assert.Equal(t, tc.wantBody, string(out.body))
		})
	}
}

func TestLogicEngineQuotesInMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		actions la
		name    string
		want    string
	}{
		{
			name:    "undeclared variable",
			actions: la{"Set": varOp("SetVariable", "it's", 5, la{})},
			want:    "The variable 'it''s' has not been declared.",
		},
		{
			name: "duplicate variable",
			actions: la{
				"Init": la{
					"type": "InitializeVariable", "runAfter": la{},
					"inputs": la{"variables": []any{
						la{"name": "a'b", "type": "Integer", "value": 1},
						la{"name": "a'b", "type": "Integer", "value": 2},
					}},
				},
			},
			want: "The variable 'a''b' has already been declared.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "quotes", logicDef(requestTrigger(nil), tc.actions))

			out := s.invoke(t, "quotes", "{}")

			msgs := make([]string, 0, len(out.actions))

			for _, a := range out.actions {
				raw, err := json.Marshal(a.Properties.Error, jsonx.V1())
				require.NoError(t, err)

				msgs = append(msgs, string(raw))
			}

			assert.Contains(t, strings.Join(msgs, "\n"), tc.want)
		})
	}
}
