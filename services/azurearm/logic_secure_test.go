package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func secureCfg(props ...any) la {
	return la{"secureData": la{"properties": props}}
}

func TestLogicSecureData(t *testing.T) {
	t.Parallel()

	withCfg := func(a la, props ...any) la {
		a["runtimeConfiguration"] = secureCfg(props...)

		return a
	}

	join := func(a la) la {
		a["type"], a["runAfter"], a["inputs"] = "Join", la{}, la{"from": []any{1, 2}, "joinWith": ","}

		return a
	}

	tests := []struct {
		actions la
		hideIn  map[string]bool
		hideOut map[string]bool
		name    string
	}{
		{
			name: "each side hides independently",
			actions: la{
				"JI": withCfg(join(la{}), "inputs"),
				"JO": withCfg(join(la{}), "outputs"),
				"JN": join(la{}),
			},
			hideIn:  map[string]bool{"JI": true, "JO": false, "JN": false},
			hideOut: map[string]bool{"JI": false, "JO": true, "JN": false},
		},
		{
			name: "compose hides outputs with inputs and ignores the outputs setting",
			actions: la{
				"C": withCfg(compose("x", nil), "inputs"),
				"D": withCfg(compose("y", nil), "outputs"),
			},
			hideIn:  map[string]bool{"C": true, "D": false},
			hideOut: map[string]bool{"C": true, "D": false},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "sec", logicDef(requestTrigger(nil), tc.actions))
			out := s.invoke(t, "sec", `{}`)

			for name, hidden := range tc.hideIn {
				assert.Equal(t, hidden, out.actions[name].Properties.InputsLink == nil, "inputs "+name)
			}

			for name, hidden := range tc.hideOut {
				assert.Equal(t, hidden, out.actions[name].Properties.OutputsLink == nil, "outputs "+name)
			}
		})
	}
}

func TestLogicSecureOutputsPropagate(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "prop", logicDef(requestTrigger(nil), la{
		"Src": la{
			"type":     "Http",
			"runAfter": la{},
			"inputs": la{
				"method":      "GET",
				"uri":         "http://127.0.0.1:1/x",
				"retryPolicy": la{"type": "none"},
			},
			"runtimeConfiguration": secureCfg("outputs"),
		},
		"Use": compose("@body('Src')", after("Src", "Failed")),
	}))

	out := s.invoke(t, "prop", `{}`)

	require.NotNil(t, out.actions["Src"].Properties.InputsLink)
	assert.Nil(t, out.actions["Src"].Properties.OutputsLink)
	assert.Nil(t, out.actions["Use"].Properties.InputsLink)
	assert.Nil(t, out.actions["Use"].Properties.OutputsLink)
}

func TestLogicSecureTriggerContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		props   []any
		wantIn  bool
		wantOut bool
	}{
		{name: "outputs", props: []any{"outputs"}, wantOut: true},
		{name: "both", props: []any{"inputs", "outputs"}, wantOut: true, wantIn: true},
		{name: "none"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			trig := requestTrigger(nil)
			manual, _ := trig["manual"].(map[string]any)

			if tc.props != nil {
				manual["runtimeConfiguration"] = secureCfg(tc.props...)
			}

			s.create(t, "trig", logicDef(trig, la{"A": compose("x", nil)}))
			out := s.invoke(t, "trig", `{"k":1}`)

			assert.Equal(t, tc.wantOut, out.run.Properties.Trigger.OutputsLink == nil)

			if tc.wantIn {
				assert.Nil(t, out.run.Properties.Trigger.InputsLink)
			}
		})
	}
}

func TestLogicStaticResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		static     la
		wantBody   any
		wantStatus armlogic.WorkflowStatus
		wantCode   string
		name       string
	}{
		{
			name:       "succeeded mock outputs",
			static:     la{"status": "Succeeded", "outputs": la{"statusCode": "OK", "body": "mock"}},
			wantStatus: armlogic.WorkflowStatusSucceeded, wantBody: "mock",
		},
		{
			name: "failed mock error",
			static: la{
				"status": "Failed", "code": "BadGateway",
				"error": la{"code": "BadGateway", "message": "forced"},
			},
			wantStatus: armlogic.WorkflowStatusFailed, wantCode: "BadGateway",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			def := logicDef(requestTrigger(nil), la{
				"Call": la{
					"type": "Http", "runAfter": la{},
					"inputs": la{"method": "GET", "uri": "http://127.0.0.1:1/never", "retryPolicy": la{"type": "none"}},
					"runtimeConfiguration": la{
						"staticResult": la{"name": "Call0", "staticResultOptions": "Enabled"},
					},
				},
			})
			def["staticResults"] = la{"Call0": tc.static}
			s.create(t, "static", def)

			out := s.invoke(t, "static", `{}`)
			call := out.actions["Call"]

			assert.Equal(t, tc.wantStatus, *call.Properties.Status)

			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, *call.Properties.Code)
			}

			if tc.wantBody != nil {
				got, ok := s.content(t, call.Properties.OutputsLink).(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tc.wantBody, got["body"])
			}
		})
	}
}

func TestLogicSecureDataPersists(t *testing.T) {
	t.Parallel()

	a := newLogicStack(t)
	a.create(t, "secrets", logicDef(requestTrigger(nil), la{
		"J": la{
			"type": "Join", "runAfter": la{}, "inputs": la{"from": []any{1, 2}, "joinWith": ","},
			"runtimeConfiguration": secureCfg("outputs"),
		},
	}))
	a.invoke(t, "secrets", `{}`)

	b := newLogicStack(t)
	require.NoError(t, b.handler.Restore(t.Context(), a.handler.Snapshot(t.Context())))

	got := b.actions(t, "secrets")["J"].Properties
	assert.NotNil(t, got.InputsLink)
	assert.Nil(t, got.OutputsLink)
}

func TestLogicSecureTriggerOutputsPropagate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		props      []any
		wantHidden bool
	}{
		{name: "secured trigger outputs hide the reader inputs", props: []any{"outputs"}, wantHidden: true},
		{name: "secured trigger inputs hide the reader inputs", props: []any{"inputs"}, wantHidden: true},
		{name: "plain trigger leaves the reader visible"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			trig := requestTrigger(nil)
			manual, _ := trig["manual"].(map[string]any)

			if tc.props != nil {
				manual["runtimeConfiguration"] = secureCfg(tc.props...)
			}

			s.create(t, "trigprop", logicDef(trig, la{"Use": la{
				"type": "Join", "runAfter": la{},
				"inputs": la{"from": "@triggerBody()?['items']", "joinWith": ","},
			}}))

			out := s.invoke(t, "trigprop", `{"items":[1,2]}`)

			assert.Equal(t, tc.wantHidden, out.actions["Use"].Properties.InputsLink == nil)
			assert.NotNil(t, out.actions["Use"].Properties.OutputsLink)
		})
	}
}
