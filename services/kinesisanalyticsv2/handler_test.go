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
