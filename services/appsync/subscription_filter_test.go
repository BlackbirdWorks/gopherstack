package appsync_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func (f *rtFixture) setSubscriptionResolver(t *testing.T, response string) {
	t.Helper()

	_, err := f.backend.CreateResolver(f.apiID, "Subscription", &appsync.Resolver{
		FieldName: "onPut", DataSourceName: "none",
		RequestMappingTemplate:  `{"version":"2018-05-29","payload":{}}`,
		ResponseMappingTemplate: response,
	})
	require.NoError(t, err)
}

func (f *rtFixture) subscribe(t *testing.T) (map[string]any, func() map[string]any) {
	t.Helper()

	c := f.dial(t, f.apiKey)

	require.NoError(t, c.WriteJSON(map[string]string{"type": "connection_init"}))
	readMsg(t, c)
	readMsg(t, c)
	require.NoError(t, c.WriteJSON(startMsg(t, "s1", `subscription { onPut { id title } }`, f.apiKey)))

	return readMsg(t, c), func() map[string]any { return readMsg(t, c) }
}

func (f *rtFixture) put(t *testing.T, id, title string) {
	t.Helper()

	_, err := f.backend.ExecuteGraphQL(t.Context(), f.apiID,
		`mutation { putTodo(id: "`+id+`", title: "`+title+`") { id title } }`, "", nil,
		appsync.GraphQLAuth{APIKey: f.apiKey})
	require.NoError(t, err)
}

func filterTmpl(groups string) string {
	return `$extensions.setSubscriptionFilter({"filterGroup":` + groups + `})$util.toJson($ctx.result)`
}

func filterOf(field, op, value string) string {
	return `{"filters":[{"fieldName":"` + field + `","operator":"` + op + `","value":` + value + `}]}`
}

// AppSync developer guide, "Enhanced subscription filtering": filters in a
// group are AND-ed, groups OR-ed; non-matching mutations are not delivered.
func TestSubscriptionFilter_AppliedToPublishedEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		groups  string
		skipped string
		want    string
	}{
		{name: "beginsWith", groups: `[` + filterOf("title", "beginsWith", `"b"`) + `]`, skipped: "sell", want: "buy"},
		{name: "eq", groups: `[` + filterOf("title", "eq", `"buy"`) + `]`, skipped: "sell", want: "buy"},
		{name: "ne", groups: `[` + filterOf("title", "ne", `"sell"`) + `]`, skipped: "sell", want: "buy"},
		{name: "contains", groups: `[` + filterOf("title", "contains", `"uy"`) + `]`, skipped: "sell", want: "buy"},
		{
			name:    "notContains",
			groups:  `[` + filterOf("title", "notContains", `"ell"`) + `]`,
			skipped: "sell",
			want:    "buy",
		},
		{name: "in", groups: `[` + filterOf("title", "in", `["buy","hold"]`) + `]`, skipped: "sell", want: "hold"},
		{name: "notIn", groups: `[` + filterOf("title", "notIn", `["sell"]`) + `]`, skipped: "sell", want: "buy"},
		{name: "between", groups: `[` + filterOf("title", "between", `["a","c"]`) + `]`, skipped: "sell", want: "buy"},
		{
			name: "groups are OR-ed", skipped: "sell", want: "hold",
			groups: `[` + filterOf("title", "eq", `"buy"`) + `,` + filterOf("title", "eq", `"hold"`) + `]`,
		},
		{
			name: "filters in a group are AND-ed", skipped: "buy", want: "buyer",
			groups: `[{"filters":[{"fieldName":"title","operator":"beginsWith","value":"buy"},` +
				`{"fieldName":"title","operator":"ne","value":"buy"}]}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealtimeFixture(t)
			f.setSubscriptionResolver(t, filterTmpl(tt.groups))

			ack, next := f.subscribe(t)
			require.Equal(t, "start_ack", ack["type"], ack)

			f.put(t, "1", tt.skipped)
			f.put(t, "2", tt.want)

			msg := next()
			require.Equal(t, "data", msg["type"], msg)

			raw, err := json.Marshal(msg["payload"])
			require.NoError(t, err)
			assert.JSONEq(t, `{"data":{"onPut":{"id":"2","title":"`+tt.want+`"}}}`, string(raw),
				"the filtered-out mutation must not be delivered before the matching one")
		})
	}
}

func TestSubscriptionFilter_InvalidFilterRejectedAtStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tmpl string
	}{
		{name: "unknown operator", tmpl: filterTmpl(`[` + filterOf("title", "like", `"x"`) + `]`)},
		{name: "no groups", tmpl: filterTmpl(`[]`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealtimeFixture(t)
			f.setSubscriptionResolver(t, tt.tmpl)

			ack, _ := f.subscribe(t)
			assert.Equal(t, "error", ack["type"], ack)
		})
	}
}

// $extensions.invalidateSubscriptions closes subscriptions whose
// setSubscriptionInvalidationFilter matches the invalidation payload.
func TestSubscriptionInvalidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterID   string
		wantClosed bool
	}{
		{name: "matching payload completes subscription", filterID: "1", wantClosed: true},
		{name: "non-matching payload keeps subscription", filterID: "9"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealtimeFixture(t)
			f.setSubscriptionResolver(t,
				`$extensions.setSubscriptionInvalidationFilter({"filterGroup":[`+
					filterOf("id", "eq", `"`+tt.filterID+`"`)+`]})$util.toJson($ctx.result)`)

			_, err := f.backend.UpdateResolver(f.apiID, "Mutation", &appsync.Resolver{
				FieldName: "putTodo", DataSourceName: "none",
				RequestMappingTemplate: `{"version":"2018-05-29",` +
					`"payload":{"id":"$ctx.args.id","title":"$ctx.args.title"}}`,
				ResponseMappingTemplate: `$extensions.invalidateSubscriptions({"subscriptionField":"onPut",` +
					`"payload":{"id":"$ctx.args.id"}})$util.toJson($ctx.result)`,
			})
			require.NoError(t, err)

			ack, next := f.subscribe(t)
			require.Equal(t, "start_ack", ack["type"], ack)

			f.put(t, "1", "buy")
			assert.Equal(t, "data", next()["type"])

			if tt.wantClosed {
				assert.Equal(t, "complete", next()["type"])

				return
			}

			f.put(t, "1", "again")
			assert.Equal(t, "data", next()["type"], "an unmatched invalidation must leave the subscription open")
		})
	}
}

func TestSubscriptionFilter_AppSyncJS(t *testing.T) {
	t.Parallel()

	f := newRealtimeFixture(t)

	_, err := f.backend.CreateResolver(f.apiID, "Subscription", &appsync.Resolver{
		FieldName: "onPut", DataSourceName: "none",
		Runtime: &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
		Code: `import { extensions, util } from '@aws-appsync/utils';
export function request(ctx) { return { payload: {} }; }
export function response(ctx) {
  extensions.setSubscriptionFilter(util.transform.toSubscriptionFilter({ title: { beginsWith: 'b' } }));
  return null;
}`,
	})
	require.NoError(t, err)

	ack, next := f.subscribe(t)
	require.Equal(t, "start_ack", ack["type"], ack)

	f.put(t, "1", "sell")
	f.put(t, "2", "buy")

	raw, err := json.Marshal(next()["payload"])
	require.NoError(t, err)
	assert.JSONEq(t, `{"data":{"onPut":{"id":"2","title":"buy"}}}`, string(raw))
}
