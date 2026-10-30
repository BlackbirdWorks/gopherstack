package ocievents_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuleLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created := e.createRule(t, "r1", `{"eventType":"com.oraclecloud.objectstorage.createobject"}`,
		onsAction(), ossAction(), faasAction())

	require.NotNil(t, created.Etag)
	assert.Equal(t, events.RuleLifecycleStateActive, created.LifecycleState)
	require.Len(t, created.Actions.Actions, 3)

	ons, ok := created.Actions.Actions[0].(events.NotificationServiceAction)
	require.True(t, ok)
	assert.Equal(t, testTopic, *ons.TopicId)
	assert.NotEmpty(t, *ons.Id)

	id := created.Id

	got, err := e.client.GetRule(t.Context(), events.GetRuleRequest{RuleId: id})
	require.NoError(t, err)
	assert.Equal(t, "r1", *got.DisplayName)
	assert.Equal(t, testCompartment, *got.CompartmentId)

	upd, err := e.client.UpdateRule(t.Context(), events.UpdateRuleRequest{
		RuleId: id, IfMatch: created.Etag,
		IsEnabled: new(false), DisplayName: new("r2"), Actions: &events.ActionDetailsList{
			Actions: []events.ActionDetails{faasAction()},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, events.RuleLifecycleStateInactive, upd.LifecycleState)
	assert.Equal(t, "r2", *upd.DisplayName)
	assert.Len(t, upd.Actions.Actions, 1)

	_, err = e.client.UpdateRule(t.Context(), events.UpdateRuleRequest{
		RuleId: id, IfMatch: created.Etag, Description: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.ChangeRuleCompartment(t.Context(), events.ChangeRuleCompartmentRequest{
		RuleId: id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	moved, err := e.client.GetRule(t.Context(), events.GetRuleRequest{RuleId: id})
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *moved.CompartmentId)

	_, err = e.client.DeleteRule(t.Context(), events.DeleteRuleRequest{RuleId: id})
	require.NoError(t, err)

	gone, err := e.client.GetRule(t.Context(), events.GetRuleRequest{RuleId: id})
	require.NoError(t, err)
	assert.Equal(t, events.RuleLifecycleStateDeleted, gone.LifecycleState)

	_, err = e.client.DeleteRule(t.Context(), events.DeleteRuleRequest{RuleId: id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.GetRule(t.Context(), events.GetRuleRequest{RuleId: new("ocid1.eventrule.oc1..nope")})
	assert.Equal(t, "NotAuthorizedOrNotFound", svcErr(t, err).GetCode())
}

func TestListRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	for _, n := range []string{"b", "a", "c"} {
		e.createRule(t, n, matchAll, onsAction())
		e.clock.Advance(1e9)
	}

	tests := []struct {
		name string
		req  events.ListRulesRequest
		want []string
	}{
		{"default newest first", events.ListRulesRequest{}, []string{"c", "a", "b"}},
		{"by name", events.ListRulesRequest{
			SortBy: events.ListRulesSortByDisplayName, SortOrder: events.ListRulesSortOrderAsc,
		}, []string{"a", "b", "c"}},
		{"name filter", events.ListRulesRequest{DisplayName: new("A")}, []string{"a"}},
		{"inactive", events.ListRulesRequest{LifecycleState: events.RuleLifecycleStateInactive}, nil},
		{"limit", events.ListRulesRequest{Limit: new(2)}, []string{"c", "a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.req.CompartmentId = new(testCompartment)

			resp, err := e.client.ListRules(t.Context(), tt.req)
			require.NoError(t, err)

			var names []string
			for _, r := range resp.Items {
				names = append(names, *r.DisplayName)
			}

			assert.Equal(t, tt.want, names)
		})
	}

	first, err := e.client.ListRules(
		t.Context(),
		events.ListRulesRequest{CompartmentId: new(testCompartment), Limit: new(2)},
	)
	require.NoError(t, err)
	require.NotNil(t, first.OpcNextPage)

	rest, err := e.client.ListRules(t.Context(), events.ListRulesRequest{
		CompartmentId: new(testCompartment), Limit: new(2), Page: first.OpcNextPage,
	})
	require.NoError(t, err)
	assert.Len(t, rest.Items, 1)

	status, body := rawDo(t, http.MethodGet, e.base+"/20181201/rules", "")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "MissingParameter")
}

func TestConditionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		condition string
		wantCode  string
	}{
		{"empty object", `{}`, ""},
		{"event type string", `{"eventType":"com.oraclecloud.objectstorage.createobject"}`, ""},
		{"event type list", `{"eventType":["a.b","c.d"]}`, ""},
		{"wildcard value", `{"eventType":"com.oraclecloud.objectstorage.*bucket"}`, ""},
		{"nested data", `{"data":{"additionalDetails":{"bucketName":["x","y*"]}}}`, ""},
		{"number and bool", `{"data":{"a":1,"b":true}}`, ""},
		{"blank", ``, "MissingParameter"},
		{"not json", `{eventType`, "InvalidParameter"},
		{"array", `["a"]`, "InvalidParameter"},
		{"string", `"a"`, "InvalidParameter"},
		{"trailing data", `{} {}`, "InvalidParameter"},
		{"empty array", `{"eventType":[]}`, "InvalidParameter"},
		{"null value", `{"eventType":null}`, "InvalidParameter"},
		{"nested array", `{"eventType":[["a"]]}`, "InvalidParameter"},
		{"object in array", `{"eventType":[{"a":"b"}]}`, "InvalidParameter"},
		{"wildcard key", `{"data":{"res*":"x"}}`, "InvalidParameter"},
	}

	e := newEnv(t, envOpts{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreateRule(t.Context(), events.CreateRuleRequest{
				DisplayName: new("c"), IsEnabled: new(true), Condition: new(tt.condition),
				CompartmentId: new(testCompartment),
				Actions:       &events.ActionDetailsList{Actions: []events.ActionDetails{onsAction()}}})
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.wantCode, se.GetCode())
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
		})
	}
}

func validRuleBody() map[string]any {
	return map[string]any{
		"displayName": "n", "isEnabled": true, "condition": "{}", "compartmentId": "c",
		"actions": map[string]any{"actions": []any{
			map[string]any{"actionType": "ONS", "isEnabled": true, "topicId": "t"},
		}},
	}
}

func setActions(m map[string]any, acts ...map[string]any) {
	list := make([]any, 0, len(acts))
	for _, a := range acts {
		list = append(list, a)
	}

	m["actions"] = map[string]any{"actions": list}
}

func TestCreateRuleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(m map[string]any)
		name     string
		wantCode string
	}{
		{func(m map[string]any) { setActions(m) }, "no actions", "MissingParameter"},
		{func(m map[string]any) { delete(m, "actions") }, "actions absent", "MissingParameter"},
		{func(m map[string]any) {
			acts := make([]map[string]any, 11)
			for i := range acts {
				acts[i] = map[string]any{"actionType": "ONS", "isEnabled": true, "topicId": "t"}
			}

			setActions(m, acts...)
		}, "eleven actions", "LimitExceeded"},
		{func(m map[string]any) {
			setActions(m, map[string]any{"actionType": "SMS", "isEnabled": true})
		}, "bad type", "InvalidParameter"},
		{func(m map[string]any) {
			setActions(m, map[string]any{"actionType": "ONS", "isEnabled": true})
		}, "ons without topic", "MissingParameter"},
		{func(m map[string]any) {
			setActions(m, map[string]any{"actionType": "OSS", "isEnabled": true})
		}, "oss without stream", "MissingParameter"},
		{func(m map[string]any) {
			setActions(m, map[string]any{"actionType": "FAAS", "isEnabled": true})
		}, "faas without function", "MissingParameter"},
		{func(m map[string]any) {
			setActions(m, map[string]any{"actionType": "ONS", "topicId": "t"})
		}, "action without isEnabled", "MissingParameter"},
		{func(m map[string]any) { delete(m, "displayName") }, "no name", "MissingParameter"},
		{func(m map[string]any) { delete(m, "isEnabled") }, "no isEnabled", "MissingParameter"},
		{func(m map[string]any) { delete(m, "compartmentId") }, "no compartment", "MissingParameter"},
		{func(m map[string]any) { m["condition"] = "" }, "blank condition", "MissingParameter"},
	}

	e := newEnv(t, envOpts{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := validRuleBody()
			tt.mutate(m)

			raw, err := json.Marshal(m)
			require.NoError(t, err)

			status, body := rawDo(t, http.MethodPost, e.base+"/20181201/rules", string(raw))
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Contains(t, body, fmt.Sprintf("%q", tt.wantCode))
		})
	}

	status, body := rawDo(t, http.MethodPost, e.base+"/20181201/rules", "nope")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "CannotParseRequest")
}

func TestRuleLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	for i := range 50 {
		e.createRule(t, fmt.Sprintf("r%d", i), matchAll, onsAction())
	}

	_, err := e.client.CreateRule(t.Context(), events.CreateRuleRequest{
		DisplayName: new("extra"), IsEnabled: new(true), Condition: new(matchAll), CompartmentId: new(testCompartment),
		Actions: &events.ActionDetailsList{Actions: []events.ActionDetails{onsAction()}}})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())
}

func TestAuthEnforced(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true})

	status, _ := rawDo(t, http.MethodGet, e.base+"/20181201/rules?compartmentId=c", "")
	assert.Equal(t, http.StatusUnauthorized, status)

	_, err := e.client.ListRules(t.Context(), events.ListRulesRequest{CompartmentId: new(testCompartment)})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

	reg := newEnv(t, envOpts{enforce: true, register: true})
	_, err = reg.client.ListRules(t.Context(), events.ListRulesRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
}
