package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const logsTarget = "Logs_20140328."

// makeLogsRequest sends a POST to the CloudWatch Logs handler with a fresh backend.
func makeLogsRequest(t *testing.T, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	handler := cloudwatchlogs.NewHandler(backend)

	return doLogsRequest(t, handler, e, action, body)
}

// doLogsRequest sends a POST to the given handler.
func doLogsRequest(
	t *testing.T,
	handler *cloudwatchlogs.Handler,
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
		req.Header.Set("X-Amz-Target", logsTarget+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, handler.Handler()(c))

	return rec
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var ops []string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
	assert.Contains(t, ops, "CreateLogGroup")
	assert.Contains(t, ops, "PutLogEvents")
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	req := httptest.NewRequest(http.MethodGet, "/notroot", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", "InvalidTarget")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "CreateLogGroup", "not-json")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// newTestHandler returns a fresh Handler + Echo for completeness handler tests.
func newTestHandler(t *testing.T) (*cloudwatchlogs.Handler, *echo.Echo) {
	t.Helper()
	b := cloudwatchlogs.NewInMemoryBackend()
	t.Cleanup(func() { b.Close() })
	h := cloudwatchlogs.NewHandler(b)

	return h, echo.New()
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{"Match", "Logs_20140328.CreateLogGroup", true},
		{"NoMatch", "AmazonSQS.CreateQueue", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			assert.Equal(t, tt.want, h.RouteMatcher()(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{"WithTarget", "Logs_20140328.PutLogEvents", "PutLogEvents"},
		{"NoTarget", "", "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			assert.Equal(t, tt.want, h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	e := echo.New()

	tests := []struct {
		name string
		body string
		want string
	}{
		{"LogGroupName", `{"logGroupName":"my-group"}`, "my-group"},
		{"LogStreamName", `{"logStreamName":"my-stream"}`, "my-stream"},
		{"Empty", `{}`, ""},
		{"BadJSON", `not-json`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			assert.Equal(t, tt.want, h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	assert.Equal(t, "CloudWatchLogs", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_GetSupportedOperationsDirect(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateLogGroup")
	assert.Contains(t, ops, "FilterLogEvents")
}

func TestHandler_ChaosProvider(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	assert.Equal(t, "logs", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_LookupTables(t *testing.T) {
	t.Parallel()

	const csvBody = `id,name
1,foo
2,bar
`

	tests := []struct {
		setup      func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body       map[string]any
		name       string
		action     string
		wantFields []string
		wantCode   int
	}{
		{
			name:   "CreateLookupTable/OK",
			action: "CreateLookupTable",
			body: map[string]any{
				"lookupTableName": "my_table",
				"tableBody":       csvBody,
			},
			wantFields: []string{"createdAt", "lookupTableArn"},
			wantCode:   http.StatusOK,
		},
		{
			name:   "CreateLookupTable/InvalidName",
			action: "CreateLookupTable",
			body: map[string]any{
				"lookupTableName": "bad name!",
				"tableBody":       csvBody,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetLookupTable/NotFound",
			action:   "GetLookupTable",
			body:     map[string]any{"lookupTableArn": "arn:aws:logs:us-east-1:000000000000:lookup-table:ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DescribeLookupTables/Empty",
			action: "DescribeLookupTables",
			body:   map[string]any{},
			wantFields: []string{
				"lookupTables",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteLookupTable/NotFound",
			action:   "DeleteLookupTable",
			body:     map[string]any{"lookupTableArn": "arn:aws:logs:us-east-1:000000000000:lookup-table:ghost"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				for _, field := range tt.wantFields {
					assert.Contains(t, resp, field, "response should contain field %q", field)
				}
			}
		})
	}
}

func TestHandler_LookupTable_GetRealResponseShape(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	createRec := doLogsRequest(t, h, e, "CreateLookupTable",
		`{"lookupTableName":"shape_table","tableBody":"id,name\n1,foo\n"}`)
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	tableArn, ok := created["lookupTableArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, tableArn)

	getRec := doLogsRequest(t, h, e, "GetLookupTable", `{"lookupTableArn":"`+tableArn+`"}`)
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
	assert.Equal(t, "shape_table", got["lookupTableName"])
	assert.Equal(t, "id,name\n1,foo\n", got["tableBody"])
	assert.Contains(t, got, "sizeBytes")
}

func TestHandler_SyslogConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutSyslogConfiguration/RequiresRealLogGroup",
			action: "PutSyslogConfiguration",
			body: map[string]any{
				"logGroupIdentifier": "/ghost/group",
				"vpcEndpointId":      "vpce-1",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "PutSyslogConfiguration/OK",
			action: "PutSyslogConfiguration",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				rec := doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/syslog/grp"}`)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"logGroupIdentifier": "/syslog/grp",
				"vpcEndpointId":      "vpce-1",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "PutSyslogConfiguration/EmptyIdentifier",
			action:   "PutSyslogConfiguration",
			body:     map[string]any{"logGroupIdentifier": ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteSyslogConfiguration/NotFound",
			action:   "DeleteSyslogConfiguration",
			body:     map[string]any{"logGroupIdentifier": "/ghost/group"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListSyslogConfigurations/Empty",
			action:   "ListSyslogConfigurations",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_SyslogConfiguration_ListResponseShape(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	rec := doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/syslog/shape"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doLogsRequest(t, h, e, "PutSyslogConfiguration",
		`{"logGroupIdentifier":"/syslog/shape","vpcEndpointId":"vpce-42"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doLogsRequest(t, h, e, "ListSyslogConfigurations", `{"logGroupIdentifier":"/syslog/shape"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	list, ok := resp["syslogConfigurations"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)

	entry, ok := list[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "VPCE", entry["sourceType"])
	assert.Equal(t, "vpce-42", entry["vpcEndpointId"])
	assert.Contains(t, entry, "logGroupArn")
	assert.Contains(t, entry, "createdAt")
}

func TestHandler_StorageTierPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body       map[string]any
		name       string
		action     string
		wantTier   string
		wantCode   int
		wantHasLUT bool
	}{
		{
			name:     "GetStorageTierPolicy/DefaultsToStandard",
			action:   "GetStorageTierPolicy",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			wantTier: "STANDARD",
		},
		{
			name:       "PutStorageTierPolicy/IntelligentTiering",
			action:     "PutStorageTierPolicy",
			body:       map[string]any{"storageTier": "INTELLIGENT_TIERING"},
			wantCode:   http.StatusOK,
			wantTier:   "INTELLIGENT_TIERING",
			wantHasLUT: true,
		},
		{
			name:     "PutStorageTierPolicy/InvalidTier",
			action:   "PutStorageTierPolicy",
			body:     map[string]any{"storageTier": "BOGUS"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetStorageTierPolicy/AfterPutHasLastUpdatedTime",
			action: "GetStorageTierPolicy",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				rec := doLogsRequest(t, h, e, "PutStorageTierPolicy", `{"storageTier":"STANDARD"}`)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:       map[string]any{},
			wantCode:   http.StatusOK,
			wantTier:   "STANDARD",
			wantHasLUT: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantTier, resp["storageTier"])

			if tt.wantHasLUT {
				assert.Contains(t, resp, "lastUpdatedTime")
			} else {
				assert.NotContains(t, resp, "lastUpdatedTime")
			}
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body              map[string]any
		name              string
		action            string
		wantListField     string
		wantNotEmptyField string
		wantCode          int
		wantListLen       int
	}{
		{
			name:     "UnknownOperation",
			action:   "UnknownOp",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp[tt.wantListField].([]any), tt.wantListLen)
			}

			if tt.wantNotEmptyField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantNotEmptyField])
			}
		})
	}
}
