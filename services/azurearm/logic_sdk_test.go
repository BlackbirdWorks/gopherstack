package azurearm_test

import (
	"encoding/json/v2"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func greetingDef() map[string]any {
	return logicDef(
		requestTrigger(map[string]any{
			"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}},
			"required": []any{"name"},
		}),
		map[string]any{
			"Compose": map[string]any{
				"type": "Compose", "inputs": "@concat('hello ', triggerBody()?['name'])", "runAfter": map[string]any{},
			},
			"Response": map[string]any{
				"type": "Response", "kind": "Http", "runAfter": map[string]any{"Compose": []any{"Succeeded"}},
				"inputs": map[string]any{
					"statusCode": 200, "headers": map[string]any{"X-Greeting": "yes"},
					"body": map[string]any{"greeting": "@outputs('Compose')"},
				},
			},
		},
	)
}

func TestLogicSDK_RequestComposeResponse(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	wf := s.create(t, "greeter", greetingDef())

	require.Equal(t, "Microsoft.Logic/workflows", *wf.Type)
	assert.Equal(t, armlogic.WorkflowStateEnabled, *wf.Properties.State)
	assert.NotEmpty(t, *wf.Properties.Version)
	assert.Contains(t, *wf.Properties.AccessEndpoint, "/_logic/workflows/")

	cb, err := s.factory.NewWorkflowTriggersClient().ListCallbackURL(t.Context(), logicRG, "greeter", "manual", nil)
	require.NoError(t, err)
	assert.Equal(t, "POST", *cb.Method)
	assert.Equal(t, "/triggers/manual/run", *cb.Queries.Sp)
	assert.Equal(t, "1.0", *cb.Queries.Sv)
	assert.NotEmpty(t, *cb.Queries.Sig)
	assert.Contains(t, *cb.Value, "sig=")
	assert.True(t, strings.HasPrefix(*cb.Value, s.url+"/_logic/workflows/"))

	code, raw, hdr := s.post(t, *cb.Value, `{"name":"bob"}`)
	s.waitRuns(t)

	require.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{"greeting":"hello bob"}`, string(raw))
	assert.Equal(t, "yes", hdr.Get("X-Greeting"))
	assert.Equal(t, "application/json", hdr.Get("Content-Type"))

	run := s.latestRun(t, "greeter")
	assert.Equal(t, armlogic.WorkflowStatusSucceeded, *run.Properties.Status)
	assert.Equal(t, hdr.Get("X-Ms-Workflow-Run-Id"), *run.Name)
	assert.Equal(t, "manual", *run.Properties.Trigger.Name)
	assert.Equal(t, armlogic.WorkflowStatusSucceeded, *run.Properties.Trigger.Status)

	body, ok := s.content(t, run.Properties.Trigger.OutputsLink).(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{"name": "bob"}, body["body"])

	acts := s.actions(t, "greeter")
	require.Len(t, acts, 2)
	assert.Equal(t, armlogic.WorkflowStatusSucceeded, *acts["Compose"].Properties.Status)
	assert.Equal(t, "hello bob", s.content(t, acts["Compose"].Properties.OutputsLink))
	assert.Equal(t, "hello bob", s.content(t, acts["Compose"].Properties.InputsLink))

	got, err := s.factory.NewWorkflowRunActionsClient().Get(t.Context(), logicRG, "greeter", *run.Name, "Response", nil)
	require.NoError(t, err)
	assert.Equal(t, armlogic.WorkflowStatusSucceeded, *got.Properties.Status)

	resp, ok := s.content(t, got.Properties.OutputsLink).(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 200, resp["statusCode"])

	hist := s.factory.NewWorkflowTriggerHistoriesClient().NewListPager(logicRG, "greeter", "manual", nil)
	page, err := hist.NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, page.Value, 1)
	assert.True(t, *page.Value[0].Properties.Fired)
	assert.Equal(t, *run.Name, *page.Value[0].Properties.Run.Name)

	one, err := s.factory.NewWorkflowTriggerHistoriesClient().
		Get(t.Context(), logicRG, "greeter", "manual", *page.Value[0].Name, nil)
	require.NoError(t, err)
	assert.Equal(t, *page.Value[0].ID, *one.ID)

	trig, err := s.factory.NewWorkflowTriggersClient().Get(t.Context(), logicRG, "greeter", "manual", nil)
	require.NoError(t, err)
	assert.NotNil(t, trig.Properties.LastExecutionTime)
	assert.Equal(t, armlogic.WorkflowStateEnabled, *trig.Properties.State)

	schema, err := s.factory.NewWorkflowTriggersClient().GetSchemaJSON(t.Context(), logicRG, "greeter", "manual", nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`, *schema.Content)
}

func TestLogicSDK_Invocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(rawURL string) string
		name     string
		body     string
		wantCode string
		want     int
	}{
		{
			name:     "schema mismatch",
			body:     `{"other":1}`,
			want:     http.StatusBadRequest,
			wantCode: "TriggerInputSchemaMismatch",
		},
		{name: "wrong type", body: `{"name":5}`, want: http.StatusBadRequest, wantCode: "TriggerInputSchemaMismatch"},
		{name: "invalid json", body: `{`, want: http.StatusBadRequest, wantCode: "InvalidRequestContent"},
		{
			name: "missing signature", body: `{"name":"x"}`, want: http.StatusUnauthorized,
			wantCode: "DirectApiAuthorizationRequired",
			mutate: func(raw string) string {
				u, _ := url.Parse(raw)
				q := u.Query()
				q.Del("sig")
				u.RawQuery = q.Encode()

				return u.String()
			},
		},
		{
			name:     "tampered signature",
			body:     `{"name":"x"}`,
			want:     http.StatusUnauthorized,
			wantCode: "AuthorizationFailed",
			mutate: func(raw string) string {
				u, _ := url.Parse(raw)
				q := u.Query()
				q.Set("sig", "AAAA"+q.Get("sig"))
				u.RawQuery = q.Encode()

				return u.String()
			},
		},
		{
			name:     "tampered scope",
			body:     `{"name":"x"}`,
			want:     http.StatusUnauthorized,
			wantCode: "AuthorizationFailed",
			mutate: func(raw string) string {
				u, _ := url.Parse(raw)
				q := u.Query()
				q.Set("sp", "/triggers/other/run")
				u.RawQuery = q.Encode()

				return u.String()
			},
		},
		{name: "ok", body: `{"name":"x"}`, want: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "greeter", greetingDef())

			target := s.callback(t, "greeter")
			if tc.mutate != nil {
				target = tc.mutate(target)
			}

			code, raw, _ := s.post(t, target, tc.body)
			s.waitRuns(t)

			assert.Equal(t, tc.want, code)

			if tc.wantCode != "" {
				var e struct {
					Error struct {
						Code string `json:"code"`
					} `json:"error"`
				}

				require.NoError(t, json.Unmarshal(raw, &e, jsonx.V1()))
				assert.Equal(t, tc.wantCode, e.Error.Code)
			}
		})
	}
}

func TestLogicSDK_KeysAndState(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "greeter", greetingDef())

	old := s.callback(t, "greeter")
	code, _, _ := s.post(t, old, `{"name":"a"}`)
	require.Equal(t, http.StatusOK, code)

	wc := s.factory.NewWorkflowsClient()
	_, err := wc.RegenerateAccessKey(t.Context(), logicRG, "greeter",
		armlogic.RegenerateActionParameter{KeyType: new(armlogic.KeyTypePrimary)}, nil)
	require.NoError(t, err)

	code, _, _ = s.post(t, old, `{"name":"a"}`)
	assert.Equal(t, http.StatusUnauthorized, code)

	fresh := s.callback(t, "greeter")
	assert.NotEqual(t, old, fresh)

	code, _, _ = s.post(t, fresh, `{"name":"a"}`)
	assert.Equal(t, http.StatusOK, code)

	sec, err := wc.ListCallbackURL(t.Context(), logicRG, "greeter",
		armlogic.GetCallbackURLParameters{KeyType: new(armlogic.KeyTypeSecondary)}, nil)
	require.NoError(t, err)

	code, _, _ = s.post(t, *sec.Value, `{"name":"a"}`)
	assert.Equal(t, http.StatusOK, code)

	_, err = wc.RegenerateAccessKey(t.Context(), logicRG, "greeter",
		armlogic.RegenerateActionParameter{KeyType: new(armlogic.KeyTypeNotSpecified)}, nil)
	require.Error(t, err)

	_, err = wc.Disable(t.Context(), logicRG, "greeter", nil)
	require.NoError(t, err)

	got, err := wc.Get(t.Context(), logicRG, "greeter", nil)
	require.NoError(t, err)
	assert.Equal(t, armlogic.WorkflowStateDisabled, *got.Properties.State)

	trig, err := s.factory.NewWorkflowTriggersClient().Get(t.Context(), logicRG, "greeter", "manual", nil)
	require.NoError(t, err)
	assert.Equal(t, armlogic.WorkflowStateDisabled, *trig.Properties.State)

	code, raw, _ := s.post(t, fresh, `{"name":"a"}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, string(raw), "WorkflowDisabled")

	_, err = wc.Enable(t.Context(), logicRG, "greeter", nil)
	require.NoError(t, err)

	code, _, _ = s.post(t, fresh, `{"name":"a"}`)
	assert.Equal(t, http.StatusOK, code)

	exp, err := wc.ListCallbackURL(t.Context(), logicRG, "greeter",
		armlogic.GetCallbackURLParameters{NotAfter: new(s.clock.Now().Add(-1))}, nil)
	require.NoError(t, err)

	code, _, _ = s.post(t, *exp.Value, `{"name":"a"}`)
	assert.Equal(t, http.StatusUnauthorized, code)
}

func TestLogicSDK_CRUDAndVersions(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	wc := s.factory.NewWorkflowsClient()

	first := s.create(t, "wf1", greetingDef())
	s.create(t, "wf2", greetingDef())

	updated, err := wc.CreateOrUpdate(t.Context(), logicRG, "wf1", armlogic.Workflow{
		Location: new("westus"), Tags: map[string]*string{"env": new("test")},
		Properties: &armlogic.WorkflowProperties{Definition: logicDef(requestTrigger(nil), map[string]any{
			"Compose": map[string]any{"type": "Compose", "inputs": "v2", "runAfter": map[string]any{}},
		})},
	}, nil)
	require.NoError(t, err)
	assert.NotEqual(t, *first.Properties.Version, *updated.Properties.Version)
	assert.Equal(t, "test", *updated.Tags["env"])

	noop, err := wc.Update(t.Context(), logicRG, "wf1", nil)
	require.NoError(t, err)
	assert.Equal(t, *updated.Properties.Version, *noop.Properties.Version)

	code, patched := s.rawJSON(t, http.MethodPatch, "/workflows/wf1", `{"tags":{"env":"prod"}}`)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, map[string]any{"env": "prod"}, patched["tags"])

	props, ok := patched["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, *updated.Properties.Version, props["version"])

	var byRG, bySub int

	for p := wc.NewListByResourceGroupPager(logicRG, nil); p.More(); {
		page, perr := p.NextPage(t.Context())
		require.NoError(t, perr)

		byRG += len(page.Value)
	}

	for p := wc.NewListBySubscriptionPager(nil); p.More(); {
		page, perr := p.NextPage(t.Context())
		require.NoError(t, perr)

		bySub += len(page.Value)
	}

	assert.Equal(t, 2, byRG)
	assert.Equal(t, 2, bySub)

	top := wc.NewListByResourceGroupPager(
		logicRG,
		&armlogic.WorkflowsClientListByResourceGroupOptions{Top: new(int32(1))},
	)
	pages := 0

	for top.More() {
		page, perr := top.NextPage(t.Context())
		require.NoError(t, perr)
		assert.Len(t, page.Value, 1)

		pages++
	}

	assert.Equal(t, 2, pages)

	filtered := wc.NewListByResourceGroupPager(logicRG, &armlogic.WorkflowsClientListByResourceGroupOptions{
		Filter: new("state eq 'Disabled'"),
	})
	page, err := filtered.NextPage(t.Context())
	require.NoError(t, err)
	assert.Empty(t, page.Value)

	vp := s.factory.NewWorkflowVersionsClient().NewListPager(logicRG, "wf1", nil)
	vpage, err := vp.NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, vpage.Value, 2)
	assert.Equal(t, *updated.Properties.Version, *vpage.Value[0].Name)

	ver, err := s.factory.NewWorkflowVersionsClient().Get(t.Context(), logicRG, "wf1", *first.Properties.Version, nil)
	require.NoError(t, err)
	assert.NotNil(t, ver.Properties.Definition)

	vcb, err := s.factory.NewWorkflowVersionTriggersClient().ListCallbackURL(t.Context(), logicRG, "wf1",
		*first.Properties.Version, "manual", nil)
	require.NoError(t, err)
	assert.Contains(t, *vcb.Value, "sig=")

	_, err = wc.Get(t.Context(), logicRG, "missing", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	_, err = wc.Delete(t.Context(), logicRG, "wf1", nil)
	require.NoError(t, err)

	_, err = wc.Get(t.Context(), logicRG, "wf1", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	_, err = wc.Delete(t.Context(), logicRG, "wf1", nil)
	require.NoError(t, err)
}

func TestLogicSDK_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		def  map[string]any
		code string
	}{
		{
			name: "unknown function",
			def: logicDef(requestTrigger(nil), map[string]any{
				"A": map[string]any{"type": "Compose", "inputs": "@nope()", "runAfter": map[string]any{}},
			}),
			code: "InvalidTemplate",
		},
		{
			name: "runAfter missing action",
			def: logicDef(requestTrigger(nil), map[string]any{
				"A": map[string]any{
					"type": "Compose", "inputs": 1, "runAfter": map[string]any{"B": []any{"Succeeded"}},
				},
			}),
			code: "InvalidTemplate",
		},
		{
			name: "runAfter cycle",
			def: logicDef(requestTrigger(nil), map[string]any{
				"A": map[string]any{
					"type":     "Compose",
					"inputs":   1,
					"runAfter": map[string]any{"B": []any{"Succeeded"}},
				},
				"B": map[string]any{
					"type":     "Compose",
					"inputs":   1,
					"runAfter": map[string]any{"A": []any{"Succeeded"}},
				},
			}),
			code: "InvalidTemplate",
		},
		{
			name: "unknown action type",
			def: logicDef(requestTrigger(nil), map[string]any{
				"A": map[string]any{"type": "Bogus", "runAfter": map[string]any{}},
			}),
			code: "InvalidTemplate",
		},
		{
			name: "response without request trigger",
			def: logicDef(
				map[string]any{
					"R": map[string]any{
						"type":       "Recurrence",
						"recurrence": map[string]any{"frequency": "Minute", "interval": 1},
					},
				},
				map[string]any{
					"A": map[string]any{"type": "Response", "inputs": map[string]any{}, "runAfter": map[string]any{}},
				},
			),
			code: "InvalidTemplate",
		},
		{
			name: "bad recurrence frequency",
			def: logicDef(
				map[string]any{
					"R": map[string]any{
						"type":       "Recurrence",
						"recurrence": map[string]any{"frequency": "Fortnight", "interval": 1},
					},
				},
				map[string]any{},
			),
			code: "InvalidTemplate",
		},
		{
			name: "terminate in foreach",
			def: logicDef(requestTrigger(nil), map[string]any{
				"Loop": map[string]any{
					"type": "Foreach", "foreach": []any{1}, "runAfter": map[string]any{},
					"actions": map[string]any{
						"T": map[string]any{
							"type":     "Terminate",
							"inputs":   map[string]any{"runStatus": "Cancelled"},
							"runAfter": map[string]any{},
						},
					},
				},
			}),
			code: "InvalidTemplate",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			wc := s.factory.NewWorkflowsClient()
			wf := armlogic.Workflow{
				Location:   new("westus"),
				Properties: &armlogic.WorkflowProperties{Definition: tc.def},
			}

			_, err := wc.CreateOrUpdate(t.Context(), logicRG, "bad", wf, nil)
			require.Error(t, err)
			assert.Equal(t, tc.code, sdkErrCode(err))

			_, err = wc.ValidateByResourceGroup(t.Context(), logicRG, "bad", wf, nil)
			require.Error(t, err)
			assert.Equal(t, tc.code, sdkErrCode(err))

			_, err = wc.ValidateByLocation(t.Context(), logicRG, "westus", "bad", wf, nil)
			require.Error(t, err)

			_, err = wc.Get(t.Context(), logicRG, "bad", nil)
			assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
		})
	}

	t.Run("valid", func(t *testing.T) {
		t.Parallel()

		s := newLogicStack(t)
		wf := armlogic.Workflow{
			Location:   new("westus"),
			Properties: &armlogic.WorkflowProperties{Definition: greetingDef()},
		}

		_, err := s.factory.NewWorkflowsClient().ValidateByResourceGroup(t.Context(), logicRG, "ok", wf, nil)
		require.NoError(t, err)
	})

	t.Run("missing location", func(t *testing.T) {
		t.Parallel()

		s := newLogicStack(t)
		_, err := s.factory.NewWorkflowsClient().CreateOrUpdate(t.Context(), logicRG, "x",
			armlogic.Workflow{Properties: &armlogic.WorkflowProperties{Definition: greetingDef()}}, nil)
		assert.Equal(t, "LocationRequired", sdkErrCode(err))
	})
}
