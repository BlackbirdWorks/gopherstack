package kinesisanalyticsv2_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestKAV2Handler(t *testing.T) *kinesisanalyticsv2.Handler {
	t.Helper()

	backend := kinesisanalyticsv2.NewInMemoryBackend(testAccountID, testRegion)

	return kinesisanalyticsv2.NewHandler(backend)
}

func doKAV2Request(t *testing.T, h *kinesisanalyticsv2.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	var req *http.Request
	if bodyBytes != nil {
		req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	}

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "KinesisAnalytics_20180523."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestKAV2_Provider_Name(t *testing.T) {
	t.Parallel()

	p := &kinesisanalyticsv2.Provider{}
	assert.Equal(t, "KinesisAnalyticsV2", p.Name())
}

func TestKAV2_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &kinesisanalyticsv2.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "KinesisAnalyticsV2", svc.Name())
}

func TestKAV2_Name(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	assert.Equal(t, "KinesisAnalyticsV2", h.Name())
}

func TestKAV2_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateApplication")
	assert.Contains(t, ops, "DescribeApplication")
	assert.Contains(t, ops, "ListApplications")
	assert.Contains(t, ops, "DeleteApplication")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "ListTagsForResource")
}

func TestKAV2_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	assert.Equal(t, "kinesisanalyticsv2", h.ChaosServiceName())
}

func TestKAV2_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		matches bool
	}{
		{
			name:    "matching target",
			target:  "KinesisAnalytics_20180523.CreateApplication",
			matches: true,
		},
		{
			name:    "matching list target",
			target:  "KinesisAnalytics_20180523.ListApplications",
			matches: true,
		},
		{
			name:    "non-matching target",
			target:  "AWSIdentityStore.CreateUser",
			matches: false,
		},
		{
			name:    "empty target",
			target:  "",
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.matches, matcher(c))
		})
	}
}

func TestKAV2_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "success",
			input: map[string]any{
				"ApplicationName":      "test-app",
				"RuntimeEnvironment":   "FLINK-1_18",
				"ServiceExecutionRole": "arn:aws:iam::000000000000:role/service-role",
			},
			wantStatus: http.StatusOK,
			wantName:   "test-app",
		},
		{
			name:       "invalid json",
			input:      nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			var body any = tt.input
			if tt.name == "invalid json" {
				// Send raw invalid JSON
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("not-json")))
				req.Header.Set("X-Amz-Target", "KinesisAnalytics_20180523.CreateApplication")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			rec := doKAV2Request(t, h, "CreateApplication", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				detail, ok := out["ApplicationDetail"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, detail["ApplicationName"])
				assert.Equal(t, "READY", detail["ApplicationStatus"])
			}
		})
	}
}

func TestKAV2_CreateApplication_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	input := map[string]any{
		"ApplicationName":    "dup-app",
		"RuntimeEnvironment": "FLINK-1_18",
	}

	rec := doKAV2Request(t, h, "CreateApplication", input)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "CreateApplication", input)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestKAV2_DescribeApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		create     bool
		wantStatus int
	}{
		{
			name:       "found",
			appName:    "existing-app",
			create:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			appName:    "missing-app",
			create:     false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.create {
				createRec := doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    tt.appName,
					"RuntimeEnvironment": "FLINK-1_18",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
			}

			rec := doKAV2Request(t, h, "DescribeApplication", map[string]any{
				"ApplicationName": tt.appName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestKAV2_ListApplications(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	for _, name := range []string{"app1", "app2"} {
		rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    name,
			"RuntimeEnvironment": "FLINK-1_18",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doKAV2Request(t, h, "ListApplications", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	summaries, ok := out["ApplicationSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 2)
}

func TestKAV2_DeleteApplication(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "del-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	rec := doKAV2Request(t, h, "DeleteApplication", map[string]any{
		"ApplicationName": "del-app",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "del-app",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestKAV2_StartStopApplication(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "lifecycle-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	rec := doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "lifecycle-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "lifecycle-app",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))
	detail := out["ApplicationDetail"].(map[string]any)
	assert.Equal(t, "RUNNING", detail["ApplicationStatus"])

	rec = doKAV2Request(t, h, "StopApplication", map[string]any{
		"ApplicationName": "lifecycle-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestKAV2_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "snap-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	startRec := doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "snap-app",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	// Create snapshot.
	rec := doKAV2Request(t, h, "CreateApplicationSnapshot", map[string]any{
		"ApplicationName": "snap-app",
		"SnapshotName":    "snap-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List snapshots.
	listRec := doKAV2Request(t, h, "ListApplicationSnapshots", map[string]any{
		"ApplicationName": "snap-app",
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	snaps, ok := listOut["SnapshotSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, snaps, 1)

	// Describe snapshot.
	descRec := doKAV2Request(t, h, "DescribeApplicationSnapshot", map[string]any{
		"ApplicationName": "snap-app",
		"SnapshotName":    "snap-1",
	})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Delete snapshot.
	delRec := doKAV2Request(t, h, "DeleteApplicationSnapshot", map[string]any{
		"ApplicationName": "snap-app",
		"SnapshotName":    "snap-1",
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

func TestKAV2_TaggingOperations(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	createRec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "tagged-app",
		"RuntimeEnvironment": "FLINK-1_18",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	detail := createOut["ApplicationDetail"].(map[string]any)
	appARN := detail["ApplicationARN"].(string)

	// ListTagsForResource.
	listRec := doKAV2Request(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": appARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	tags, ok := listOut["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 1)

	// TagResource - add tag.
	tagRec := doKAV2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": appARN,
		"Tags":        []map[string]string{{"Key": "team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// UntagResource.
	untagRec := doKAV2Request(t, h, "UntagResource", map[string]any{
		"ResourceARN": appARN,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)

	// Verify only 1 tag remains.
	listRec2 := doKAV2Request(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": appARN,
	})
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listOut2))
	tags2, ok := listOut2["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags2, 1)
}

func TestKAV2_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	rec := doKAV2Request(t, h, "UnknownOp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestKAV2_MissingTarget(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	// No X-Amz-Target header
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestKAV2_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantDesc   string
		rawBody    []byte
		wantStatus int
	}{
		{
			name: "update_description",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "upd-app",
					"RuntimeEnvironment": "FLINK-1_18",
				})
			},
			body: map[string]any{
				"ApplicationName":             "upd-app",
				"ApplicationDescription":      "new description",
				"CurrentApplicationVersionId": 1,
			},
			wantStatus: http.StatusOK,
			wantDesc:   "new description",
		},
		{
			name:       "not_found",
			body:       map[string]any{"ApplicationName": "missing"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "bad_json",
			rawBody:    []byte("bad json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "version_mismatch",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "upd-app-conflict",
					"RuntimeEnvironment": "FLINK-1_18",
				})
			},
			body: map[string]any{
				"ApplicationName":             "upd-app-conflict",
				"ApplicationDescription":      "should not apply",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			var rec *httptest.ResponseRecorder
			if tt.rawBody != nil {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tt.rawBody))
				req.Header.Set("Content-Type", "application/x-amz-json-1.1")
				req.Header.Set("X-Amz-Target", "KinesisAnalytics_20180523.UpdateApplication")
				r := httptest.NewRecorder()
				c := e.NewContext(req, r)
				require.NoError(t, h.Handler()(c))
				rec = r
			} else {
				rec = doKAV2Request(t, h, "UpdateApplication", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDesc != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				detail := out["ApplicationDetail"].(map[string]any)
				assert.Equal(t, tt.wantDesc, detail["ApplicationDescription"])
			}
		})
	}
}

func TestKAV2_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "DeleteApplication_not_found",
			op:         "DeleteApplication",
			body:       map[string]any{"ApplicationName": "no-such-app"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "StartApplication_not_found",
			op:         "StartApplication",
			body:       map[string]any{"ApplicationName": "no-such-app"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "StopApplication_not_found",
			op:         "StopApplication",
			body:       map[string]any{"ApplicationName": "no-such-app"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteApplicationSnapshot_not_found",
			op:         "DeleteApplicationSnapshot",
			body:       map[string]any{"ApplicationName": "no-app", "SnapshotName": "no-snap"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "TagResource_not_found",
			op:   "TagResource",
			body: map[string]any{
				"ResourceARN": "arn:aws:kinesisanalytics:us-east-1:000000000000:application/no-app",
				"Tags":        []map[string]string{},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "UntagResource_not_found",
			op:   "UntagResource",
			body: map[string]any{
				"ResourceARN": "arn:aws:kinesisanalytics:us-east-1:000000000000:application/no-app",
				"TagKeys":     []string{"k"},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)
			rec := doKAV2Request(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestKAV2_AddApplicationCloudWatchLoggingOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantCWLLen int
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "cwl-app",
					"RuntimeEnvironment": "SQL-1_0",
				})
			},
			body: map[string]any{
				"ApplicationName": "cwl-app",
				"CloudWatchLoggingOption": map[string]any{
					"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:my-group:log-stream:my-stream",
					"RoleARN":      "arn:aws:iam::000000000000:role/my-role",
				},
			},
			wantStatus: http.StatusOK,
			wantCWLLen: 1,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"CloudWatchLoggingOption": map[string]any{
					"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_cwl_option",
			body: map[string]any{
				"ApplicationName": "cwl-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				descriptions, ok := out["CloudWatchLoggingOptionDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, descriptions, tt.wantCWLLen)
			}
		})
	}
}

func TestKAV2_AddApplicationInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantInputs int
		wantStatus int
	}{
		{
			name: "success_kinesis_streams",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "input-app",
					"RuntimeEnvironment": "SQL-1_0",
				})
			},
			body: map[string]any{
				"ApplicationName": "input-app",
				"Input": map[string]any{
					"NamePrefix": "SOURCE_SQL_STREAM",
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
					},
				},
			},
			wantStatus: http.StatusOK,
			wantInputs: 1,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"Input": map[string]any{
					"NamePrefix": "PREFIX",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_input",
			body: map[string]any{
				"ApplicationName": "input-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationInput", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				inputs, ok := out["InputDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, inputs, tt.wantInputs)
			}
		})
	}
}

func TestKAV2_AddApplicationInputProcessingConfiguration(t *testing.T) {
	t.Parallel()

	// Create an app and add an input, return the input ID.
	setupInputApp := func(h *kinesisanalyticsv2.Handler, appName string) string {
		doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    appName,
			"RuntimeEnvironment": "SQL-1_0",
		})
		rec := doKAV2Request(t, h, "AddApplicationInput", map[string]any{
			"ApplicationName": appName,
			"Input": map[string]any{
				"NamePrefix": "SOURCE_SQL_STREAM",
				"KinesisStreamsInput": map[string]any{
					"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
				},
			},
		})

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		inputs := out["InputDescriptions"].([]any)
		input := inputs[0].(map[string]any)

		return input["InputId"].(string)
	}

	tests := []struct {
		appName    string
		inputID    string
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			appName:    "proc-app",
			wantStatus: http.StatusOK,
		},
		{
			name:       "input_not_found",
			appName:    "proc-app2",
			inputID:    "nonexistent-input",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "app_not_found",
			appName:    "missing-app",
			inputID:    "any-id",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			inputID := tt.inputID

			switch tt.name {
			case "success":
				inputID = setupInputApp(h, tt.appName)
			case "input_not_found":
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    tt.appName,
					"RuntimeEnvironment": "SQL-1_0",
				})
			}

			rec := doKAV2Request(t, h, "AddApplicationInputProcessingConfiguration", map[string]any{
				"ApplicationName": tt.appName,
				"InputId":         inputID,
				"InputProcessingConfiguration": map[string]any{
					"InputLambdaProcessor": map[string]any{
						"ResourceARN": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
					},
				},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestKAV2_AddApplicationOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(h *kinesisanalyticsv2.Handler)
		body        map[string]any
		name        string
		wantOutputs int
		wantStatus  int
	}{
		{
			name: "success_kinesis_streams",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "output-app",
					"RuntimeEnvironment": "SQL-1_0",
				})
			},
			body: map[string]any{
				"ApplicationName": "output-app",
				"Output": map[string]any{
					"Name": "DESTINATION_STREAM",
					"KinesisStreamsOutput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
					},
					"DestinationSchema": map[string]any{
						"RecordFormatType": "JSON",
					},
				},
			},
			wantStatus:  http.StatusOK,
			wantOutputs: 1,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"Output": map[string]any{
					"Name": "output",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_output",
			body: map[string]any{
				"ApplicationName": "output-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationOutput", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				outputs, ok := out["OutputDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, outputs, tt.wantOutputs)
			}
		})
	}
}

func TestKAV2_AddApplicationReferenceDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantRefs   int
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "ref-app",
					"RuntimeEnvironment": "SQL-1_0",
				})
			},
			body: map[string]any{
				"ApplicationName": "ref-app",
				"ReferenceDataSource": map[string]any{
					"TableName": "REFERENCE_TABLE",
					"S3ReferenceDataSource": map[string]any{
						"BucketARN": "arn:aws:s3:::my-bucket",
						"FileKey":   "data/reference.csv",
					},
				},
			},
			wantStatus: http.StatusOK,
			wantRefs:   1,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"ReferenceDataSource": map[string]any{
					"TableName": "TABLE",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_ref_source",
			body: map[string]any{
				"ApplicationName": "ref-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationReferenceDataSource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				refs, ok := out["ReferenceDataSourceDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, refs, tt.wantRefs)
			}
		})
	}
}

func TestKAV2_AddApplicationVpcConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "vpc-app",
					"RuntimeEnvironment": "FLINK-1_18",
				})
			},
			body: map[string]any{
				"ApplicationName": "vpc-app",
				"VpcConfiguration": map[string]any{
					"SubnetIds":        []string{"subnet-abc123", "subnet-def456"},
					"SecurityGroupIds": []string{"sg-abc123"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"VpcConfiguration": map[string]any{
					"SubnetIds":        []string{"subnet-abc123"},
					"SecurityGroupIds": []string{"sg-abc123"},
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_vpc_config",
			body: map[string]any{
				"ApplicationName": "vpc-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationVpcConfiguration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotNil(t, out["VpcConfigurationDescription"])
				vpcDesc, ok := out["VpcConfigurationDescription"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, vpcDesc["VpcConfigurationId"])
			}
		})
	}
}

func TestKAV2_CreateApplicationPresignedUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "flink-app",
					"RuntimeEnvironment": "FLINK-1_18",
				})
			},
			body: map[string]any{
				"ApplicationName": "flink-app",
				"UrlType":         "FLINK_DASHBOARD_URL",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"UrlType":         "FLINK_DASHBOARD_URL",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "CreateApplicationPresignedUrl", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				url, ok := out["AuthorizedUrl"].(string)
				require.True(t, ok)
				assert.Contains(t, url, "FLINK_DASHBOARD_URL")
			}
		})
	}
}

func TestKAV2_DeleteApplicationCloudWatchLoggingOption(t *testing.T) {
	t.Parallel()

	// addCWLOption creates app and adds a CWL option, returning the option ID.
	addCWLOption := func(h *kinesisanalyticsv2.Handler, appName string) string {
		doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    appName,
			"RuntimeEnvironment": "SQL-1_0",
		})
		rec := doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
			"ApplicationName": appName,
			"CloudWatchLoggingOption": map[string]any{
				"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
			},
		})

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		descs := out["CloudWatchLoggingOptionDescriptions"].([]any)
		desc := descs[0].(map[string]any)

		return desc["CloudWatchLoggingOptionId"].(string)
	}

	tests := []struct {
		appName      string
		optionID     string
		name         string
		wantCWLCount int
		wantStatus   int
	}{
		{
			name:         "success",
			appName:      "del-cwl-app",
			wantStatus:   http.StatusOK,
			wantCWLCount: 0,
		},
		{
			name:       "app_not_found",
			appName:    "missing-app",
			optionID:   "cwl-1",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			optionID := tt.optionID

			if tt.name == "success" {
				optionID = addCWLOption(h, tt.appName)
			}

			rec := doKAV2Request(t, h, "DeleteApplicationCloudWatchLoggingOption", map[string]any{
				"ApplicationName":           tt.appName,
				"CloudWatchLoggingOptionId": optionID,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				descs, ok := out["CloudWatchLoggingOptionDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, descs, tt.wantCWLCount)
			}
		})
	}
}

func TestKAV2_DeleteApplicationInputProcessingConfiguration(t *testing.T) {
	t.Parallel()

	setupInputWithProc := func(h *kinesisanalyticsv2.Handler, appName string) string {
		doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    appName,
			"RuntimeEnvironment": "SQL-1_0",
		})
		rec := doKAV2Request(t, h, "AddApplicationInput", map[string]any{
			"ApplicationName": appName,
			"Input": map[string]any{
				"NamePrefix": "PREFIX",
				"KinesisStreamsInput": map[string]any{
					"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
				},
			},
		})

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		inputs := out["InputDescriptions"].([]any)
		input := inputs[0].(map[string]any)
		inputID := input["InputId"].(string)

		doKAV2Request(t, h, "AddApplicationInputProcessingConfiguration", map[string]any{
			"ApplicationName": appName,
			"InputId":         inputID,
			"InputProcessingConfiguration": map[string]any{
				"InputLambdaProcessor": map[string]any{
					"ResourceARN": "arn:aws:lambda:us-east-1:000000000000:function:fn",
				},
			},
		})

		return inputID
	}

	tests := []struct {
		appName    string
		inputID    string
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			appName:    "del-proc-app",
			wantStatus: http.StatusOK,
		},
		{
			name:       "app_not_found",
			appName:    "missing-app",
			inputID:    "input-1",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			inputID := tt.inputID

			if tt.name == "success" {
				inputID = setupInputWithProc(h, tt.appName)
			}

			rec := doKAV2Request(t, h, "DeleteApplicationInputProcessingConfiguration", map[string]any{
				"ApplicationName": tt.appName,
				"InputId":         inputID,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestKAV2_DeleteApplicationOutput(t *testing.T) {
	t.Parallel()

	addOutput := func(h *kinesisanalyticsv2.Handler, appName string) string {
		doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    appName,
			"RuntimeEnvironment": "SQL-1_0",
		})
		rec := doKAV2Request(t, h, "AddApplicationOutput", map[string]any{
			"ApplicationName": appName,
			"Output": map[string]any{
				"Name": "OUTPUT_STREAM",
				"KinesisStreamsOutput": map[string]any{
					"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
				},
				"DestinationSchema": map[string]any{
					"RecordFormatType": "JSON",
				},
			},
		})

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		outputs := out["OutputDescriptions"].([]any)
		output := outputs[0].(map[string]any)

		return output["OutputId"].(string)
	}

	tests := []struct {
		appName      string
		outputID     string
		name         string
		wantOutCount int
		wantStatus   int
	}{
		{
			name:         "success",
			appName:      "del-out-app",
			wantStatus:   http.StatusOK,
			wantOutCount: 0,
		},
		{
			name:       "app_not_found",
			appName:    "missing-app",
			outputID:   "output-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "output_not_found",
			appName:    "del-out-notfound-app",
			outputID:   "nonexistent-output",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			outputID := tt.outputID

			switch tt.name {
			case "success":
				outputID = addOutput(h, tt.appName)
			case "output_not_found":
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    tt.appName,
					"RuntimeEnvironment": "SQL-1_0",
				})
			}

			rec := doKAV2Request(t, h, "DeleteApplicationOutput", map[string]any{
				"ApplicationName": tt.appName,
				"OutputId":        outputID,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["ApplicationARN"])
			}
		})
	}
}

func TestKAV2_NewOpsVersionBump(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupAdd func(h *kinesisanalyticsv2.Handler, appName string) []byte
		name     string
	}{
		{
			name: "AddApplicationCloudWatchLoggingOption",
			setupAdd: func(h *kinesisanalyticsv2.Handler, appName string) []byte {
				rec := doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
					"ApplicationName": appName,
					"CloudWatchLoggingOption": map[string]any{
						"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
					},
				})

				return rec.Body.Bytes()
			},
		},
		{
			name: "AddApplicationVpcConfiguration",
			setupAdd: func(h *kinesisanalyticsv2.Handler, appName string) []byte {
				rec := doKAV2Request(t, h, "AddApplicationVpcConfiguration", map[string]any{
					"ApplicationName": appName,
					"VpcConfiguration": map[string]any{
						"SubnetIds":        []string{"subnet-1"},
						"SecurityGroupIds": []string{"sg-1"},
					},
				})

				return rec.Body.Bytes()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)
			appName := "version-bump-app-" + tt.name

			doKAV2Request(t, h, "CreateApplication", map[string]any{
				"ApplicationName":    appName,
				"RuntimeEnvironment": "FLINK-1_18",
			})

			respBody := tt.setupAdd(h, appName)

			var out map[string]any
			require.NoError(t, json.Unmarshal(respBody, &out))
			versionID, ok := out["ApplicationVersionId"].(float64)
			require.True(t, ok)
			assert.InEpsilon(t, 2.0, versionID, 1e-9)
		})
	}
}

func TestKAV2_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AddApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"AddApplicationReferenceDataSource",
		"AddApplicationVpcConfiguration",
		"CreateApplicationPresignedUrl",
		"DeleteApplicationCloudWatchLoggingOption",
		"DeleteApplicationInputProcessingConfiguration",
		"DeleteApplicationOutput",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "expected %q in GetSupportedOperations", op)
	}
}

// TestKAV2_StartStopUpdate_ReturnOperationId verifies that StartApplication,
// StopApplication, and UpdateApplication surface a non-empty OperationId,
// which real AWS clients use to poll DescribeApplicationOperation /
// ListApplicationOperations for the request's outcome.
func TestKAV2_StartStopUpdate_ReturnOperationId(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "opid-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	startRec := doKAV2Request(t, h, "StartApplication", map[string]any{"ApplicationName": "opid-app"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	assert.NotEmpty(t, startOut["OperationId"])

	stopRec := doKAV2Request(t, h, "StopApplication", map[string]any{"ApplicationName": "opid-app"})
	require.Equal(t, http.StatusOK, stopRec.Code)

	var stopOut map[string]any
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &stopOut))
	assert.NotEmpty(t, stopOut["OperationId"])

	updateRec := doKAV2Request(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             "opid-app",
		"ApplicationDescription":      "updated",
		"CurrentApplicationVersionId": 1,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateOut))
	assert.NotEmpty(t, updateOut["OperationId"])
}

// TestKAV2_ApplicationOperationsLifecycle exercises
// DescribeApplicationOperation and ListApplicationOperations end to end.
// Before recordOperation wired StartApplication/StopApplication/
// UpdateApplication/RollbackApplication into the backend's operations map,
// both of these read ops were permanently empty / not-found regardless of
// how many lifecycle actions had run.
func TestKAV2_ApplicationOperationsLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "ops-lifecycle-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	startRec := doKAV2Request(t, h, "StartApplication", map[string]any{"ApplicationName": "ops-lifecycle-app"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	opID, ok := startOut["OperationId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, opID)

	descRec := doKAV2Request(t, h, "DescribeApplicationOperation", map[string]any{
		"ApplicationName": "ops-lifecycle-app",
		"OperationId":     opID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	details, ok := descOut["ApplicationOperationInfoDetails"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "StartApplication", details["Operation"])
	assert.Equal(t, "SUCCESSFUL", details["OperationStatus"])
	assert.Positive(t, details["StartTime"])
	assert.Positive(t, details["EndTime"])

	listRec := doKAV2Request(t, h, "ListApplicationOperations", map[string]any{
		"ApplicationName": "ops-lifecycle-app",
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	items, ok := listOut["ApplicationOperationInfoList"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, opID, item["OperationId"])
	assert.Equal(t, "StartApplication", item["Operation"])

	// Unknown OperationId must 404, not silently succeed.
	missRec := doKAV2Request(t, h, "DescribeApplicationOperation", map[string]any{
		"ApplicationName": "ops-lifecycle-app",
		"OperationId":     "op-does-not-exist",
	})
	assert.Equal(t, http.StatusNotFound, missRec.Code)
}

// TestKAV2_CreateApplication_InlineConfiguration verifies that CreateApplication
// seeds inputs/outputs/reference-data-sources/VPC configs/CloudWatch logging
// options supplied inline via ApplicationConfiguration and
// CloudWatchLoggingOptions -- the standard Terraform/CloudFormation
// provisioning pattern (a single CreateApplication call, not a
// CreateApplication followed by a series of Add* calls). Before this fix,
// ApplicationConfiguration/CloudWatchLoggingOptions were silently discarded:
// CreateApplication returned 200 OK but every application came back with
// empty configuration.
func TestKAV2_CreateApplication_InlineConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":      "inline-config-app",
		"RuntimeEnvironment":   "SQL-1_0",
		"ServiceExecutionRole": "arn:aws:iam::000000000000:role/svc",
		"ApplicationConfiguration": map[string]any{
			"SqlApplicationConfiguration": map[string]any{
				"Inputs": []map[string]any{
					{
						"NamePrefix": "SOURCE_SQL_STREAM",
						"KinesisStreamsInput": map[string]any{
							"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/in",
						},
					},
				},
				"Outputs": []map[string]any{
					{
						"Name": "OUTPUT",
						"KinesisStreamsOutput": map[string]any{
							"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/out",
						},
					},
				},
				"ReferenceDataSources": []map[string]any{
					{
						"TableName": "REF_TABLE",
						"S3ReferenceDataSource": map[string]any{
							"BucketARN": "arn:aws:s3:::bucket",
							"FileKey":   "key",
						},
					},
				},
			},
			"VpcConfigurations": []map[string]any{
				{
					"SubnetIds":        []string{"subnet-1"},
					"SecurityGroupIds": []string{"sg-1"},
				},
			},
		},
		"CloudWatchLoggingOptions": []map[string]any{
			{"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	detail := out["ApplicationDetail"].(map[string]any)

	// Inline config must not bump the version past 1 -- real AWS keeps a
	// freshly created application, even with inline config, at version 1.
	assert.InEpsilon(t, 1.0, detail["ApplicationVersionId"], 1e-9)

	appConfig, ok := detail["ApplicationConfigurationDescription"].(map[string]any)
	require.True(t, ok, "expected ApplicationConfigurationDescription to be populated")
	sqlConfig, ok := appConfig["SqlApplicationConfigurationDescription"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, sqlConfig["InputDescriptions"], 1)
	assert.Len(t, sqlConfig["OutputDescriptions"], 1)
	assert.Len(t, sqlConfig["ReferenceDataSourceDescriptions"], 1)

	assert.Len(t, detail["VpcConfigurationDescriptions"], 1)
	assert.Len(t, detail["CloudWatchLoggingOptionDescriptions"], 1)
}

// TestKAV2_RollbackApplication exercises RollbackApplication over HTTP --
// previously untested at any layer, and (before the version-history fix in
// backend.go) permanently broken: RollbackApplication requires at least 2
// recorded versions, but nothing besides CreateApplication ever populated
// the version history, so this call always failed with InvalidArgumentException.
func TestKAV2_RollbackApplication(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":        "rollback-http-app",
		"RuntimeEnvironment":     "FLINK-1_18",
		"ApplicationDescription": "original",
	})

	updateRec := doKAV2Request(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             "rollback-http-app",
		"ApplicationDescription":      "changed",
		"CurrentApplicationVersionId": 1,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	rollbackRec := doKAV2Request(t, h, "RollbackApplication", map[string]any{
		"ApplicationName":             "rollback-http-app",
		"CurrentApplicationVersionId": 2,
	})
	require.Equal(t, http.StatusOK, rollbackRec.Code)

	var rollbackOut map[string]any
	require.NoError(t, json.Unmarshal(rollbackRec.Body.Bytes(), &rollbackOut))
	assert.NotEmpty(t, rollbackOut["OperationId"])

	detail, ok := rollbackOut["ApplicationDetail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "original", detail["ApplicationDescription"])
	assert.InEpsilon(t, 3.0, detail["ApplicationVersionId"], 1e-9)
}
