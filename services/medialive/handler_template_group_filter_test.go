package medialive_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListCWAlarmTemplates_FilterByGroupIdentifier verifies
// ListCloudWatchAlarmTemplatesInput.GroupIdentifier (api_op_
// ListCloudWatchAlarmTemplates.go, bound as the real "groupIdentifier"
// query param in serializers.go). The handler previously read only
// maxResults/nextToken and returned every template regardless of which
// group a caller asked for.
func TestListCWAlarmTemplates_FilterByGroupIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	groupA := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-template-groups",
		map[string]any{"name": "cw-group-a"}).Body.Bytes())["id"].(string)
	groupB := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-template-groups",
		map[string]any{"name": "cw-group-b"}).Body.Bytes())["id"].(string)

	tmplA := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", map[string]any{
		"name": "cw-tmpl-a", "groupIdentifier": groupA, "metricName": "InputLossSeconds",
	}).Body.Bytes())["id"].(string)

	require.Equal(t, http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", map[string]any{
			"name": "cw-tmpl-b", "groupIdentifier": groupB, "metricName": "OutputLossSeconds",
		}).Code)

	rec := doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates?groupIdentifier="+groupA, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	items := decodeBody(t, rec.Body.Bytes())["cloudWatchAlarmTemplates"].([]any)
	require.Len(t, items, 1)
	assert.Equal(t, tmplA, items[0].(map[string]any)["id"])
}

// TestListEBRuleTemplates_FilterByGroupIdentifier is the same missing-filter
// bug as TestListCWAlarmTemplates_FilterByGroupIdentifier, for
// ListEventBridgeRuleTemplates' GroupIdentifier.
func TestListEBRuleTemplates_FilterByGroupIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	groupA := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/eventbridge-rule-template-groups",
		map[string]any{"name": "eb-group-a"}).Body.Bytes())["id"].(string)
	groupB := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/eventbridge-rule-template-groups",
		map[string]any{"name": "eb-group-b"}).Body.Bytes())["id"].(string)

	tmplA := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/eventbridge-rule-templates", map[string]any{
		"name": "eb-tmpl-a", "groupIdentifier": groupA, "eventType": "MEDIALIVE_MULTIPLEX_ALERT",
	}).Body.Bytes())["id"].(string)

	require.Equal(t, http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/prod/eventbridge-rule-templates", map[string]any{
			"name": "eb-tmpl-b", "groupIdentifier": groupB, "eventType": "MEDIALIVE_MULTIPLEX_ALERT",
		}).Code)

	rec := doRequest(t, h, http.MethodGet, "/prod/eventbridge-rule-templates?groupIdentifier="+groupA, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	items := decodeBody(t, rec.Body.Bytes())["eventBridgeRuleTemplates"].([]any)
	require.Len(t, items, 1)
	assert.Equal(t, tmplA, items[0].(map[string]any)["id"])
}

// TestListCWAlarmTemplateGroups_FilterBySignalMapIdentifier verifies
// ListCloudWatchAlarmTemplateGroupsInput.SignalMapIdentifier (matched
// against the signal map's cloudWatchAlarmTemplateGroupIds).
func TestListCWAlarmTemplateGroups_FilterBySignalMapIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	groupA := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-template-groups",
		map[string]any{"name": "cw-sm-group-a"}).Body.Bytes())["id"].(string)
	require.Equal(t, http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-template-groups",
			map[string]any{"name": "cw-sm-group-b"}).Code)

	sm := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
		"name":                   "sm-cw-filter",
		"discoveryEntryPointArn": "arn:aws:medialive:us-east-1:000000000000:channel:abc123",
		"cloudWatchAlarmTemplateGroupIdentifiers": []any{groupA},
	}).Body.Bytes())
	smID := sm["id"].(string)

	rec := doRequest(t, h, http.MethodGet,
		"/prod/cloudwatch-alarm-template-groups?signalMapIdentifier="+smID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	items := decodeBody(t, rec.Body.Bytes())["cloudWatchAlarmTemplateGroups"].([]any)
	require.Len(t, items, 1)
	assert.Equal(t, groupA, items[0].(map[string]any)["id"])
}

// TestListCWAlarmTemplates_FilterBySignalMapIdentifier verifies
// ListCloudWatchAlarmTemplatesInput.SignalMapIdentifier, a two-hop filter:
// templates belonging to any CloudWatch alarm template group the signal map
// references.
func TestListCWAlarmTemplates_FilterBySignalMapIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	groupA := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-template-groups",
		map[string]any{"name": "cw-sm-tmpl-group-a"}).Body.Bytes())["id"].(string)
	groupB := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-template-groups",
		map[string]any{"name": "cw-sm-tmpl-group-b"}).Body.Bytes())["id"].(string)

	tmplA := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", map[string]any{
		"name": "cw-sm-tmpl-a", "groupIdentifier": groupA, "metricName": "InputLossSeconds",
	}).Body.Bytes())["id"].(string)

	require.Equal(t, http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/prod/cloudwatch-alarm-templates", map[string]any{
			"name": "cw-sm-tmpl-b", "groupIdentifier": groupB, "metricName": "OutputLossSeconds",
		}).Code)

	sm := decodeBody(t, doRequest(t, h, http.MethodPost, "/prod/signal-maps", map[string]any{
		"name":                   "sm-cw-tmpl-filter",
		"discoveryEntryPointArn": "arn:aws:medialive:us-east-1:000000000000:channel:abc123",
		"cloudWatchAlarmTemplateGroupIdentifiers": []any{groupA},
	}).Body.Bytes())
	smID := sm["id"].(string)

	rec := doRequest(t, h, http.MethodGet, "/prod/cloudwatch-alarm-templates?signalMapIdentifier="+smID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	items := decodeBody(t, rec.Body.Bytes())["cloudWatchAlarmTemplates"].([]any)
	require.Len(t, items, 1)
	assert.Equal(t, tmplA, items[0].(map[string]any)["id"])
}
