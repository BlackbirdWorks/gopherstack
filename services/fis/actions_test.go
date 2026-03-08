package fis_test

import (
"net/http"
"testing"
"time"

"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"

"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestParseISODuration(t *testing.T) {
t.Parallel()

tests := []struct {
name  string
input string
want  time.Duration
}{
{name: "empty", input: "", want: 0},
{name: "PT5M", input: "PT5M", want: 5 * time.Minute},
{name: "PT1H", input: "PT1H", want: time.Hour},
{name: "PT30S", input: "PT30S", want: 30 * time.Second},
{name: "PT1H30M", input: "PT1H30M", want: 90 * time.Minute},
{name: "P1D", input: "P1D", want: 24 * time.Hour},
{name: "P1W", input: "P1W", want: 7 * 24 * time.Hour},
{name: "pt5m_lowercase", input: "pt5m", want: 5 * time.Minute},
{name: "invalid_no_P", input: "T5M", want: 0},
{name: "only_P", input: "P", want: 0},
{name: "PT0.1S", input: "PT0.1S", want: 100 * time.Millisecond},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

got := fis.ParseISODurationForTest(tt.input)
assert.Equal(t, tt.want, got)
})
}
}

func TestParsePercentage(t *testing.T) {
t.Parallel()

tests := []struct {
name  string
input string
want  float64
}{
{name: "empty", input: "", want: 1.0},
{name: "100", input: "100", want: 1.0},
{name: "50", input: "50", want: 0.5},
{name: "0", input: "0", want: 1.0},
{name: "negative", input: "-10", want: 1.0},
{name: "invalid", input: "abc", want: 1.0},
{name: "25", input: "25", want: 0.25},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

got := fis.ParsePercentageForTest(tt.input)
assert.InDelta(t, tt.want, got, 0.001)
})
}
}

func TestParseOperations(t *testing.T) {
t.Parallel()

tests := []struct {
name  string
input string
want  []string
}{
{name: "empty", input: "", want: nil},
{name: "single", input: "GetItem", want: []string{"GetItem"}},
{name: "multiple", input: "GetItem,PutItem,DeleteItem", want: []string{"GetItem", "PutItem", "DeleteItem"}},
{name: "with_spaces", input: "GetItem, PutItem", want: []string{"GetItem", "PutItem"}},
{name: "blank_parts", input: "GetItem,,PutItem", want: []string{"GetItem", "PutItem"}},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

got := fis.ParseOperationsForTest(tt.input)
assert.Equal(t, tt.want, got)
})
}
}

func TestFaultErrorForAction(t *testing.T) {
t.Parallel()

tests := []struct {
name       string
actionID   string
wantCode   string
wantStatus int
}{
{name: "throttle", actionID: "aws:fis:inject-api-throttle-error", wantCode: "ThrottlingException", wantStatus: 400},
{name: "internal_error", actionID: "aws:fis:inject-api-internal-error", wantCode: "InternalServerError", wantStatus: 500},
{name: "unavailable", actionID: "aws:fis:inject-api-unavailable-error", wantCode: "ServiceUnavailable", wantStatus: 503},
{name: "unknown", actionID: "aws:fis:unknown", wantCode: "ServiceUnavailable", wantStatus: 503},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

got := fis.FaultErrorForActionForTest(tt.actionID)
assert.Equal(t, tt.wantCode, got.Code)
assert.Equal(t, tt.wantStatus, got.StatusCode)
})
}
}

func TestBuildFaultRules(t *testing.T) {
t.Parallel()

tests := []struct {
name      string
action    fis.ExperimentTemplateActionForTest
wantCount int
wantEmpty bool
}{
{
name: "no_service",
action: fis.ExperimentTemplateActionForTest{
ActionID:   "aws:fis:inject-api-internal-error",
Parameters: map[string]string{},
},
wantEmpty: true,
},
{
name: "service_only_no_ops",
action: fis.ExperimentTemplateActionForTest{
ActionID:   "aws:fis:inject-api-throttle-error",
Parameters: map[string]string{"service": "dynamodb"},
},
wantCount: 1,
},
{
name: "service_with_ops",
action: fis.ExperimentTemplateActionForTest{
ActionID:   "aws:fis:inject-api-internal-error",
Parameters: map[string]string{"service": "s3", "operations": "GetObject,PutObject"},
},
wantCount: 2,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

rules := fis.BuildFaultRulesForTest(tt.action)
if tt.wantEmpty {
assert.Empty(t, rules)
} else {
assert.Len(t, rules, tt.wantCount)
}
})
}
}

func TestBackend_SetFaultStore(t *testing.T) {
t.Parallel()

b := fis.NewTestBackend()
b.SetFaultStore(nil)
}

func TestBackend_SetActionProviders(t *testing.T) {
t.Parallel()

b := fis.NewTestBackend()
b.SetActionProviders(nil)

actions := b.ListActions()
assert.NotEmpty(t, actions)
}

func TestFISHandler_CreateTemplate_WithLogConfiguration(t *testing.T) {
t.Parallel()

h := newTestHandler(t)

body := map[string]any{
"description":    "template with log config",
"stopConditions": []map[string]any{{"source": "none"}},
"targets":        map[string]any{},
"actions":        map[string]any{},
"logConfiguration": map[string]any{
"logSchemaVersion": 1,
"cloudWatchLogsConfiguration": map[string]any{
"logGroupArn": "arn:aws:logs:us-east-1:000000000000:log-group:/fis/experiments",
},
},
"experimentOptions": map[string]any{
"accountTargeting":          "single-account",
"emptyTargetResolutionMode": "fail",
},
}

rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
require.Equal(t, http.StatusCreated, rec.Code)

var resp struct {
ExperimentTemplate struct {
LogConfiguration *struct {
LogSchemaVersion int `json:"logSchemaVersion"`
} `json:"logConfiguration"`
} `json:"experimentTemplate"`
}

mustJSON(t, rec, &resp)
require.NotNil(t, resp.ExperimentTemplate.LogConfiguration)
assert.Equal(t, 1, resp.ExperimentTemplate.LogConfiguration.LogSchemaVersion)
}

func TestFISHandler_CreateTemplate_WithS3LogConfiguration(t *testing.T) {
t.Parallel()

h := newTestHandler(t)

body := map[string]any{
"stopConditions": []map[string]any{{"source": "none"}},
"targets":        map[string]any{},
"actions":        map[string]any{},
"logConfiguration": map[string]any{
"logSchemaVersion": 1,
"s3Configuration": map[string]any{
"bucketName": "my-fis-logs",
"prefix":     "experiments/",
},
},
}

rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
require.Equal(t, http.StatusCreated, rec.Code)
}

func TestFISHandler_UpdateExperimentTemplate_NotFound(t *testing.T) {
t.Parallel()

h := newTestHandler(t)
rec := doRequest(t, h, http.MethodPatch, "/experimentTemplates/EXTnonexistent0000000000", map[string]any{
"description": "updated",
})
assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_DeleteExperimentTemplate_NotFound(t *testing.T) {
t.Parallel()

h := newTestHandler(t)
rec := doRequest(t, h, http.MethodDelete, "/experimentTemplates/EXTnonexistent0000000000", nil)
assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_StopExperiment_NotFound(t *testing.T) {
t.Parallel()

h := newTestHandler(t)
rec := doRequest(t, h, http.MethodDelete, "/experiments/EXPnonexistent0000000000", nil)
assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_CreateTemplate_WithTargets(t *testing.T) {
t.Parallel()

h := newTestHandler(t)

body := map[string]any{
"stopConditions": []map[string]any{{"source": "none"}},
"targets": map[string]any{
"Instances": map[string]any{
"resourceType":  "aws:ec2:instance",
"selectionMode": "COUNT(2)",
"resourceArns":  []string{"arn:aws:ec2:us-east-1:000000000000:instance/i-abc123"},
"resourceTags":  map[string]string{"env": "staging"},
"filters": []map[string]any{
{"path": "State.Name", "values": []string{"running"}},
},
},
},
"actions": map[string]any{
"stopInstances": map[string]any{
"actionId": "aws:ec2:stop-instances",
"targets":  map[string]string{"Instances": "Instances"},
},
},
}

rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
require.Equal(t, http.StatusCreated, rec.Code)

var resp struct {
ExperimentTemplate struct {
Targets map[string]struct {
ResourceType string `json:"resourceType"`
} `json:"targets"`
} `json:"experimentTemplate"`
}

mustJSON(t, rec, &resp)
assert.Contains(t, resp.ExperimentTemplate.Targets, "Instances")
}

func TestFISHandler_UnknownRoute(t *testing.T) {
t.Parallel()

h := newTestHandler(t)
rec := doRequest(t, h, http.MethodPost, "/unknown", nil)
assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_ExtractOperation(t *testing.T) {
t.Parallel()

h := newTestHandler(t)
c := fis.CreateTestEchoForExtract(t, h, http.MethodGet, "/experimentTemplates/EXTabc")
op := h.ExtractOperation(c)
assert.Equal(t, "GetExperimentTemplate", op)
}

func TestFISHandler_ExtractResource(t *testing.T) {
t.Parallel()

h := newTestHandler(t)
c := fis.CreateTestEchoForExtract(t, h, http.MethodGet, "/experimentTemplates/EXTabc")
res := h.ExtractResource(c)
assert.Equal(t, "EXTabc", res)
}

func TestFISHandler_CreateTemplate_WithStopCondition(t *testing.T) {
t.Parallel()

h := newTestHandler(t)

body := map[string]any{
"stopConditions": []map[string]any{
{
"source": "aws:cloudwatch:alarm",
"value":  "arn:aws:cloudwatch:us-east-1:000000000000:alarm:MyAlarm",
},
},
"targets": map[string]any{},
"actions": map[string]any{},
}

rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
require.Equal(t, http.StatusCreated, rec.Code)

var resp struct {
ExperimentTemplate struct {
StopConditions []struct {
Source string `json:"source"`
Value  string `json:"value"`
} `json:"stopConditions"`
} `json:"experimentTemplate"`
}

mustJSON(t, rec, &resp)
require.Len(t, resp.ExperimentTemplate.StopConditions, 1)
assert.Equal(t, "aws:cloudwatch:alarm", resp.ExperimentTemplate.StopConditions[0].Source)
}

func TestFISHandler_CreateTemplate_InjectAPIAction(t *testing.T) {
t.Parallel()

h := newTestHandler(t)

body := map[string]any{
"stopConditions": []map[string]any{{"source": "none"}},
"targets":        map[string]any{},
"actions": map[string]any{
"injectError": map[string]any{
"actionId": "aws:fis:inject-api-internal-error",
"parameters": map[string]string{
"service":    "dynamodb",
"operations": "GetItem,PutItem",
"percentage": "50",
"duration":   "PT5M",
},
},
},
}

rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
require.Equal(t, http.StatusCreated, rec.Code)

var createResp struct {
ExperimentTemplate struct {
ID string `json:"id"`
} `json:"experimentTemplate"`
}

mustJSON(t, rec, &createResp)
templateID := createResp.ExperimentTemplate.ID
require.NotEmpty(t, templateID)

rec2 := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
"experimentTemplateId": templateID,
})
require.Equal(t, http.StatusCreated, rec2.Code)
}
