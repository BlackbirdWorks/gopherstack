package eventbridge_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// makeRequest is a helper to send a POST request to the EventBridge handler.
func makeRequest(t *testing.T, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend)

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", nil)
	}

	if action != "" {
		req.Header.Set("X-Amz-Target", "AmazonEventBridge."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

// makeRequestWithHandler sends a POST to a specific handler instance.
func makeRequestWithHandler(
	t *testing.T,
	handler *eventbridge.Handler,
	e *echo.Echo,
	action, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", nil)
	}

	if action != "" {
		req.Header.Set("X-Amz-Target", "AmazonEventBridge."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantOps []string
	}{
		{
			name:    "GET returns list of supported operations",
			wantOps: []string{"CreateEventBus", "PutEvents"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)

			var ops []string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
			for _, op := range tt.wantOps {
				assert.Contains(t, ops, op)
			}
		})
	}
}

func TestHandler_DispatchErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		method        string
		target        string
		body          string
		wantBody      string
		wantErrorType string
		wantCode      int
	}{
		{
			name:     "missing X-Amz-Target header returns bad request",
			method:   http.MethodPost,
			body:     "{}",
			wantCode: http.StatusBadRequest,
			wantBody: "Missing X-Amz-Target",
		},
		{
			name:     "PUT method returns method not allowed",
			method:   http.MethodPut,
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:          "unknown action returns bad request with UnknownOperationException",
			method:        http.MethodPost,
			target:        "AmazonEventBridge.NonExistentAction",
			body:          "{}",
			wantCode:      http.StatusBadRequest,
			wantErrorType: "UnknownOperationException",
		},
		{
			name:          "delete default event bus returns IllegalStatusException",
			method:        http.MethodPost,
			target:        "AmazonEventBridge.DeleteEventBus",
			body:          `{"Name":"default"}`,
			wantCode:      http.StatusBadRequest,
			wantErrorType: "IllegalStatusException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			handler := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(tt.method, "/", strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequest(tt.method, "/", nil)
			}
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
			if tt.wantErrorType != "" {
				var errResp service.JSONErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrorType, errResp.Type)
			}
		})
	}
}

func TestHandler_CreateEventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantArnPart string
		wantCode    int
	}{
		{
			name:        "create event bus returns ARN containing bus name",
			body:        `{"Name":"test-bus","Description":"my bus"}`,
			wantCode:    http.StatusOK,
			wantArnPart: "test-bus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeRequest(t, "CreateEventBus", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp["EventBusArn"], tt.wantArnPart)
		})
	}
}

func TestHandler_CreateAndListEventBuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		busNames     []string
		wantMinCount int
	}{
		{
			name:         "create multiple buses then list shows all buses including default",
			busNames:     []string{"bus-a", "bus-b"},
			wantMinCount: 3, // default + bus-a + bus-b
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			for _, name := range tt.busNames {
				makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"`+name+`"}`)
			}

			rec := makeRequestWithHandler(t, handler, e, "ListEventBuses", `{}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				NextToken  string                 `json:"NextToken"`
				EventBuses []eventbridge.EventBus `json:"EventBuses"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.GreaterOrEqual(t, len(resp.EventBuses), tt.wantMinCount)
		})
	}
}

func TestHandler_DeleteEventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		createBus        string
		deleteBus        string
		wantDeleteCode   int
		describeAfter    bool
		wantDescribeCode int
	}{
		{
			name:             "create then delete then describe returns not found",
			createBus:        "temp-bus",
			deleteBus:        "temp-bus",
			wantDeleteCode:   http.StatusOK,
			describeAfter:    true,
			wantDescribeCode: http.StatusNotFound,
		},
		{
			name:           "cannot delete default bus returns bad request",
			deleteBus:      "default",
			wantDeleteCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			if tt.createBus != "" {
				makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"`+tt.createBus+`"}`)
			}

			rec := makeRequestWithHandler(t, handler, e, "DeleteEventBus", `{"Name":"`+tt.deleteBus+`"}`)
			assert.Equal(t, tt.wantDeleteCode, rec.Code)

			if tt.describeAfter {
				rec = makeRequestWithHandler(t, handler, e, "DescribeEventBus", `{"Name":"`+tt.deleteBus+`"}`)
				assert.Equal(t, tt.wantDescribeCode, rec.Code)
			}
		})
	}
}

func TestHandler_PutRuleAndListRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		putBody      string
		wantArnPart  string
		wantRuleName string
		wantRuleLen  int
	}{
		{
			name:         "put rule then list returns that rule with correct ARN",
			putBody:      `{"Name":"my-rule","EventPattern":"{\"source\":[\"my.app\"]}","State":"ENABLED"}`,
			wantArnPart:  "my-rule",
			wantRuleName: "my-rule",
			wantRuleLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			rec := makeRequestWithHandler(t, handler, e, "PutRule", tt.putBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var putResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			assert.Contains(t, putResp["RuleArn"], tt.wantArnPart)

			rec = makeRequestWithHandler(t, handler, e, "ListRules", `{}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp struct {
				Rules []eventbridge.Rule `json:"Rules"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Rules, tt.wantRuleLen)
			assert.Equal(t, tt.wantRuleName, listResp.Rules[0].Name)
		})
	}
}

func TestHandler_DescribeRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		putBody         string
		describeBody    string
		wantRuleName    string
		wantDescription string
		wantState       string
	}{
		{
			name:            "describe rule returns name description and state",
			putBody:         `{"Name":"desc-rule","Description":"a description","State":"DISABLED"}`,
			describeBody:    `{"Name":"desc-rule"}`,
			wantRuleName:    "desc-rule",
			wantDescription: "a description",
			wantState:       "DISABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			makeRequestWithHandler(t, handler, e, "PutRule", tt.putBody)

			rec := makeRequestWithHandler(t, handler, e, "DescribeRule", tt.describeBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var rule eventbridge.Rule
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
			assert.Equal(t, tt.wantRuleName, rule.Name)
			assert.Equal(t, tt.wantDescription, rule.Description)
			assert.Equal(t, tt.wantState, rule.State)
		})
	}
}

func TestHandler_EnableDisableRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		ruleName          string
		wantDisabledState string
		wantEnabledState  string
	}{
		{
			name:              "toggle rule: disable then re-enable changes state correctly",
			ruleName:          "toggle",
			wantDisabledState: "DISABLED",
			wantEnabledState:  "ENABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			makeRequestWithHandler(t, handler, e, "PutRule", `{"Name":"`+tt.ruleName+`","State":"ENABLED"}`)

			rec := makeRequestWithHandler(t, handler, e, "DisableRule", `{"Name":"`+tt.ruleName+`"}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = makeRequestWithHandler(t, handler, e, "DescribeRule", `{"Name":"`+tt.ruleName+`"}`)
			var rule eventbridge.Rule
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
			assert.Equal(t, tt.wantDisabledState, rule.State)

			makeRequestWithHandler(t, handler, e, "EnableRule", `{"Name":"`+tt.ruleName+`"}`)

			rec = makeRequestWithHandler(t, handler, e, "DescribeRule", `{"Name":"`+tt.ruleName+`"}`)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
			assert.Equal(t, tt.wantEnabledState, rule.State)
		})
	}
}

func TestHandler_PutTargetsListAndRemove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		ruleName           string
		targets            string
		removeIDs          string
		wantRemainingID    string
		wantInitialCount   int
		wantRemainingCount int
	}{
		{
			name:     "put two targets then remove one leaves the other",
			ruleName: "rule-t",
			targets: `[{"Id":"t1","Arn":"arn:aws:lambda:us-east-1:123:function:fn"},` +
				`{"Id":"t2","Arn":"arn:aws:sqs:us-east-1:123:q"}]`,
			wantInitialCount:   2,
			removeIDs:          `["t1"]`,
			wantRemainingID:    "t2",
			wantRemainingCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			makeRequestWithHandler(t, handler, e, "PutRule", `{"Name":"`+tt.ruleName+`"}`)

			rec := makeRequestWithHandler(t, handler, e, "PutTargets",
				`{"Rule":"`+tt.ruleName+`","Targets":`+tt.targets+`}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var putResp struct {
				FailedEntries    []eventbridge.FailedEntry `json:"FailedEntries"`
				FailedEntryCount int                       `json:"FailedEntryCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			assert.Equal(t, 0, putResp.FailedEntryCount)

			rec = makeRequestWithHandler(t, handler, e, "ListTargetsByRule", `{"Rule":"`+tt.ruleName+`"}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp struct {
				Targets []eventbridge.Target `json:"Targets"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Targets, tt.wantInitialCount)

			rec = makeRequestWithHandler(t, handler, e, "RemoveTargets",
				`{"Rule":"`+tt.ruleName+`","Ids":`+tt.removeIDs+`}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = makeRequestWithHandler(t, handler, e, "ListTargetsByRule", `{"Rule":"`+tt.ruleName+`"}`)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Targets, tt.wantRemainingCount)
			assert.Equal(t, tt.wantRemainingID, listResp.Targets[0].ID)
		})
	}
}

func TestHandler_PutEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		wantCode        int
		wantEntryCount  int
		wantFailedCount int
	}{
		{
			name: "put multiple events returns entries with IDs and no failures",
			body: `{"Entries":[` +
				`{"Source":"my.app","DetailType":"UserCreated","Detail":"{\"userId\":\"1\"}"},` +
				`{"Source":"my.app","DetailType":"UserDeleted","Detail":"{}"}]}`,
			wantCode:        http.StatusOK,
			wantEntryCount:  2,
			wantFailedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			rec := makeRequestWithHandler(t, handler, e, "PutEvents", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp struct {
				Entries          []eventbridge.EventResultEntry `json:"Entries"`
				FailedEntryCount int                            `json:"FailedEntryCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantFailedCount, resp.FailedEntryCount)
			assert.Len(t, resp.Entries, tt.wantEntryCount)
			for _, entry := range resp.Entries {
				assert.NotEmpty(t, entry.EventID)
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches AmazonEventBridge target",
			target:    "AmazonEventBridge.PutEvents",
			wantMatch: true,
		},
		{
			name:      "does not match non-EventBridge target",
			target:    "AmazonSSM.GetParameter",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			handler := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			matcher := handler.RouteMatcher()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "extracts PutRule from AmazonEventBridge target header",
			target: "AmazonEventBridge.PutRule",
			wantOp: "PutRule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			handler := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, handler.ExtractOperation(c))
		})
	}
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	const resourceARN = "arn:aws:events:us-east-1:123456789012:rule/my-rule"

	tests := []struct {
		wantTags     map[string]string
		name         string
		setupTags    string
		untagKeys    string
		wantTagCount int
	}{
		{
			name: "tag resource then list shows all tags",
			setupTags: `[{"Key":"env","Value":"prod"},` +
				`{"Key":"team","Value":"platform"}]`,
			wantTagCount: 2,
			wantTags:     map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name: "tag then untag one key leaves remaining tag",
			setupTags: `[{"Key":"env","Value":"prod"},` +
				`{"Key":"team","Value":"platform"}]`,
			untagKeys:    `["env"]`,
			wantTagCount: 1,
			wantTags:     map[string]string{"team": "platform"},
		},
		{
			name:         "list tags for resource with no tags returns empty",
			wantTagCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			if tt.setupTags != "" {
				rec := makeRequestWithHandler(t, handler, e, "TagResource",
					`{"ResourceARN":"`+resourceARN+`","Tags":`+tt.setupTags+`}`)
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.untagKeys != "" {
				rec := makeRequestWithHandler(t, handler, e, "UntagResource",
					`{"ResourceARN":"`+resourceARN+`","TagKeys":`+tt.untagKeys+`}`)
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			rec := makeRequestWithHandler(t, handler, e, "ListTagsForResource",
				`{"ResourceARN":"`+resourceARN+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Tags, tt.wantTagCount)

			if len(tt.wantTags) > 0 {
				tagMap := make(map[string]string, tt.wantTagCount)
				for _, tag := range listResp.Tags {
					tagMap[tag.Key] = tag.Value
				}
				for k, v := range tt.wantTags {
					assert.Equal(t, v, tagMap[k])
				}
			}
		})
	}
}
