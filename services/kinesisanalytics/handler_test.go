package kinesisanalytics_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

func newTestHandlerWithBackend(t *testing.T) (*kinesisanalytics.Handler, *kinesisanalytics.InMemoryBackend) {
	t.Helper()

	backend := kinesisanalytics.NewInMemoryBackend(testRegion, testAccountID)
	h := kinesisanalytics.NewHandler(backend)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h, backend
}

func newTestHandler(t *testing.T) *kinesisanalytics.Handler {
	t.Helper()

	h, _ := newTestHandlerWithBackend(t)

	return h
}

func doRequest(t *testing.T, h *kinesisanalytics.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "KinesisAnalytics_20150814."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "creates application",
			input:      map[string]any{"ApplicationName": "my-app"},
			wantStatus: http.StatusOK,
			wantKey:    "ApplicationSummary",
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateApplication", tt.input)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantKey)
			}
		})
	}
}

func TestHandler_DescribeApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:    "describes existing application",
			appName: "existing-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "existing-app", "", "", nil)
			},
			input:      map[string]any{"ApplicationName": "existing-app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found for missing application",
			input:      map[string]any{"ApplicationName": "missing"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "DescribeApplication", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing application",
			setup: func(b *kinesisanalytics.InMemoryBackend) map[string]any {
				app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-app", "", "", nil)

				return map[string]any{
					"ApplicationName": "del-app",
					"CreateTimestamp": float64(app.CreateTimestamp.Unix()),
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found for missing application",
			setup: func(_ *kinesisanalytics.InMemoryBackend) map[string]any {
				return map[string]any{"ApplicationName": "ghost", "CreateTimestamp": float64(1234567890)}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			input := tt.setup(b)

			rec := doRequest(t, h, "DeleteApplication", input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "lists all applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-1", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-2", "", "", nil)
			},
			input:      map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "empty list",
			input:      map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "ListApplications", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, _ := resp["ApplicationSummaries"].([]any)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

func TestHandler_StartStopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "starts application",
			op:   "StartApplication",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "start-app", "", "", nil)
			},
			input:      map[string]any{"ApplicationName": "start-app"},
			wantStatus: http.StatusOK,
		},
		{
			name: "stops application",
			op:   "StopApplication",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "stop-app", "", "", nil)
				_ = kinesisanalytics.StartAppNoConfig(b, "stop-app")
			},
			input:      map[string]any{"ApplicationName": "stop-app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "start not found",
			op:         "StartApplication",
			input:      map[string]any{"ApplicationName": "ghost"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			// For stop operations, wait until app is RUNNING before proceeding
			if tt.op == "StopApplication" && tt.input["ApplicationName"] != nil {
				kinesisanalytics.WaitForStatus(t, b, tt.input["ApplicationName"].(string), "RUNNING")
			}

			rec := doRequest(t, h, tt.op, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates application",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "upd-app", "", "SELECT 1", nil)
			},
			input: map[string]any{
				"ApplicationName":             "upd-app",
				"CurrentApplicationVersionId": 1,
				"ApplicationUpdate":           map[string]any{"ApplicationCodeUpdate": "SELECT 2"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found for missing application",
			input:      map[string]any{"ApplicationName": "ghost", "CurrentApplicationVersionId": 1},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch returns error",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "UpdateApplication", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UpdateApplication_NestedWireShapes exercises UpdateApplication's
// InputUpdates/OutputUpdates/ReferenceDataSourceUpdates nested payloads using the real AWS
// "Update"-suffixed field names (e.g. "ResourceARNUpdate", not "ResourceARN"). These nested
// shapes are distinct wire types from their Add* counterparts; reusing the Add* JSON field
// names here previously caused UpdateApplication to silently no-op (or wipe fields to empty
// strings) instead of applying the update.
func TestHandler_UpdateApplication_NestedWireShapes(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "wire-upd-app", "", "", nil)
	require.NoError(t, err)

	require.NoError(t, b.AddApplicationInput(
		context.Background(), app.ApplicationName, app.ApplicationVersionID,
		kinesisanalytics.InputDescription{
			NamePrefix:       "IN",
			InputParallelism: &kinesisanalytics.InputParallelism{Count: 1},
			KinesisStreamsInputDescription: &kinesisanalytics.KinesisStreamsInputDesc{
				ResourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/old-in",
				RoleARN:     "arn:aws:iam::000000000000:role/old-in-role",
			},
		},
	))

	require.NoError(t, b.AddApplicationOutput(
		context.Background(), app.ApplicationName, 2,
		kinesisanalytics.OutputDescription{
			Name: "OUT",
			KinesisStreamsOutputDescription: &kinesisanalytics.KinesisStreamsOutputDesc{
				ResourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/old-out",
				RoleARN:     "arn:aws:iam::000000000000:role/old-out-role",
			},
			DestinationSchema: &kinesisanalytics.DestinationSchemaDesc{RecordFormatType: "JSON"},
		},
	))

	require.NoError(t, b.AddApplicationReferenceDataSource(
		context.Background(), app.ApplicationName, 3,
		kinesisanalytics.ReferenceDataSourceDescription{
			TableName: "TBL",
			S3ReferenceDataSourceDescription: &kinesisanalytics.S3ReferenceDataSourceDesc{
				BucketARN:        "arn:aws:s3:::old-bucket",
				FileKey:          "old.csv",
				ReferenceRoleARN: "arn:aws:iam::000000000000:role/old-ref-role",
			},
		},
	))

	seeded, err := b.DescribeApplication(context.Background(), app.ApplicationName)
	require.NoError(t, err)
	require.Len(t, seeded.Inputs, 1)
	require.Len(t, seeded.Outputs, 1)
	require.Len(t, seeded.ReferenceDataSources, 1)

	inputID := seeded.Inputs[0].InputID
	outputID := seeded.Outputs[0].OutputID
	referenceID := seeded.ReferenceDataSources[0].ReferenceID

	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             app.ApplicationName,
		"CurrentApplicationVersionId": seeded.ApplicationVersionID,
		"ApplicationUpdate": map[string]any{
			"InputUpdates": []map[string]any{
				{
					"InputId":          inputID,
					"NamePrefixUpdate": "IN2",
					"KinesisStreamsInputUpdate": map[string]any{
						"ResourceARNUpdate": "arn:aws:kinesis:us-east-1:000000000000:stream/new-in",
						"RoleARNUpdate":     "arn:aws:iam::000000000000:role/new-in-role",
					},
					"InputParallelismUpdate": map[string]any{"CountUpdate": 3},
				},
			},
			"OutputUpdates": []map[string]any{
				{
					"OutputId":   outputID,
					"NameUpdate": "OUT2",
					"KinesisStreamsOutputUpdate": map[string]any{
						"ResourceARNUpdate": "arn:aws:kinesis:us-east-1:000000000000:stream/new-out",
						"RoleARNUpdate":     "arn:aws:iam::000000000000:role/new-out-role",
					},
				},
			},
			"ReferenceDataSourceUpdates": []map[string]any{
				{
					"ReferenceId":     referenceID,
					"TableNameUpdate": "TBL2",
					"S3ReferenceDataSourceUpdate": map[string]any{
						"BucketARNUpdate":        "arn:aws:s3:::new-bucket",
						"FileKeyUpdate":          "new.csv",
						"ReferenceRoleARNUpdate": "arn:aws:iam::000000000000:role/new-ref-role",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	describeRec := doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationName": app.ApplicationName})
	require.Equal(t, http.StatusOK, describeRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	detail := resp["ApplicationDetail"].(map[string]any)

	inList := detail["InputDescriptions"].([]any)
	require.Len(t, inList, 1)
	in := inList[0].(map[string]any)
	assert.Equal(t, "IN2", in["NamePrefix"])
	assert.InDelta(t, float64(3), in["InputParallelism"].(map[string]any)["Count"], 0)
	assert.Len(t, in["InAppStreamNames"].([]any), 3)
	ksInput := in["KinesisStreamsInputDescription"].(map[string]any)
	assert.Equal(t, "arn:aws:kinesis:us-east-1:000000000000:stream/new-in", ksInput["ResourceARN"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/new-in-role", ksInput["RoleARN"])

	outList := detail["OutputDescriptions"].([]any)
	require.Len(t, outList, 1)
	out := outList[0].(map[string]any)
	assert.Equal(t, "OUT2", out["Name"])
	ksOutput := out["KinesisStreamsOutputDescription"].(map[string]any)
	assert.Equal(t, "arn:aws:kinesis:us-east-1:000000000000:stream/new-out", ksOutput["ResourceARN"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/new-out-role", ksOutput["RoleARN"])

	refList := detail["ReferenceDataSourceDescriptions"].([]any)
	require.Len(t, refList, 1)
	ref := refList[0].(map[string]any)
	assert.Equal(t, "TBL2", ref["TableName"])
	s3Ref := ref["S3ReferenceDataSourceDescription"].(map[string]any)
	assert.Equal(t, "arn:aws:s3:::new-bucket", s3Ref["BucketARN"])
	assert.Equal(t, "new.csv", s3Ref["FileKey"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/new-ref-role", s3Ref["ReferenceRoleARN"])
}

// TestHandler_UpdateApplication_InputSchemaUpdateIsPartialPatch verifies that InputSchemaUpdate
// only overwrites the sub-fields supplied by the caller (RecordFormatUpdate / RecordEncodingUpdate
// / RecordColumnUpdates), unlike ReferenceSchemaUpdate which replaces the whole schema.
func TestHandler_UpdateApplication_InputSchemaUpdateIsPartialPatch(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "schema-upd-app", "", "", nil)
	require.NoError(t, err)

	require.NoError(t, b.AddApplicationInput(
		context.Background(), app.ApplicationName, app.ApplicationVersionID,
		kinesisanalytics.InputDescription{
			NamePrefix: "IN",
			InputSchema: &kinesisanalytics.SourceSchema{
				RecordEncoding: "UTF-8",
				RecordFormat:   kinesisanalytics.RecordFormat{RecordFormatType: "CSV"},
				RecordColumns:  []kinesisanalytics.RecordColumn{{Name: "COL1", SQLType: "VARCHAR(4)"}},
			},
		},
	))

	seeded, err := b.DescribeApplication(context.Background(), app.ApplicationName)
	require.NoError(t, err)
	require.Len(t, seeded.Inputs, 1)

	// Only RecordEncodingUpdate is supplied; RecordFormat/RecordColumns must survive untouched.
	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             app.ApplicationName,
		"CurrentApplicationVersionId": seeded.ApplicationVersionID,
		"ApplicationUpdate": map[string]any{
			"InputUpdates": []map[string]any{
				{
					"InputId": seeded.Inputs[0].InputID,
					"InputSchemaUpdate": map[string]any{
						"RecordEncodingUpdate": "UTF-16",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	after, err := b.DescribeApplication(context.Background(), app.ApplicationName)
	require.NoError(t, err)
	require.NotNil(t, after.Inputs[0].InputSchema)
	assert.Equal(t, "UTF-16", after.Inputs[0].InputSchema.RecordEncoding)
	assert.Equal(t, "CSV", after.Inputs[0].InputSchema.RecordFormat.RecordFormatType)
	require.Len(t, after.Inputs[0].InputSchema.RecordColumns, 1)
	assert.Equal(t, "COL1", after.Inputs[0].InputSchema.RecordColumns[0].Name)
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*kinesisanalytics.InMemoryBackend) string
		tags       []map[string]string
		tagKeys    []string
		wantStatus int
	}{
		{
			name: "list tags returns tags",
			op:   "ListTagsForResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(
					b,
					testRegion,
					testAccountID,
					"tag-app",
					"",
					"",
					map[string]string{"env": "test"},
				)

				return app.ApplicationARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tag resource succeeds",
			op:   "TagResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "tag2-app", "", "", nil)

				return app.ApplicationARN
			},
			tags:       []map[string]string{{"Key": "new", "Value": "val"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "untag resource succeeds",
			op:   "UntagResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(
					b,
					testRegion,
					testAccountID,
					"untag-app",
					"",
					"",
					map[string]string{"remove": "me"},
				)

				return app.ApplicationARN
			},
			tagKeys:    []string{"remove"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			resourceARN := tt.setup(b)

			var input map[string]any

			switch tt.op {
			case "ListTagsForResource":
				input = map[string]any{"ResourceARN": resourceARN}
			case "TagResource":
				input = map[string]any{"ResourceARN": resourceARN, "Tags": tt.tags}
			case "UntagResource":
				input = map[string]any{"ResourceARN": resourceARN, "TagKeys": tt.tagKeys}
			}

			rec := doRequest(t, h, tt.op, input)
			assert.Equal(t, tt.wantStatus, rec.Code)
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
			name:      "matches kinesis analytics target",
			target:    "KinesisAnalytics_20150814.CreateApplication",
			wantMatch: true,
		},
		{
			name:      "does not match other targets",
			target:    "Firehose_20150804.CreateDeliveryStream",
			wantMatch: false,
		},
		{
			name:      "does not match empty target",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)

			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "NonExistentAction", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ServiceMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "KinesisAnalytics", h.Name())
	assert.Equal(t, "kinesisanalytics", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, "us-east-1", h.ChaosRegions()[0])
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "extracts operation from target",
			target: "KinesisAnalytics_20150814.CreateApplication",
			wantOp: "CreateApplication",
		},
		{
			name:   "returns unknown for empty target",
			target: "",
			wantOp: "Unknown",
		},
		{
			name:   "returns unknown for non-matching prefix",
			target: "Firehose_20150804.CreateDeliveryStream",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantName string
	}{
		{
			name:     "extracts application name",
			body:     `{"ApplicationName":"my-app"}`,
			wantName: "my-app",
		},
		{
			name:     "returns empty for missing name",
			body:     `{}`,
			wantName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

func TestHandler_MissingResourceARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "list tags missing ARN",
			op:         "ListTagsForResource",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag resource missing ARN",
			op:         "TagResource",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "untag resource missing ARN",
			op:         "UntagResource",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_StopApplication_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StopApplication", map[string]any{"ApplicationName": "ghost"})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "initializes with defaults",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &kinesisanalytics.Provider{}
			assert.Equal(t, "KinesisAnalytics", p.Name())
		})
	}
}

func TestHandler_AddApplicationCloudWatchLoggingOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "adds logging option successfully",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cwl-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "cwl-app",
				"CurrentApplicationVersionId": 1,
				"CloudWatchLoggingOption": map[string]any{
					"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:test:log-stream:s1",
					"RoleARN":      "arn:aws:iam::000000000000:role/role",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app not found",
			input: map[string]any{
				"ApplicationName":             "ghost",
				"CurrentApplicationVersionId": 1,
				"CloudWatchLoggingOption": map[string]any{
					"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
					"RoleARN":      "arn:aws:iam::000000000000:role/r",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cwl-ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "cwl-ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "AddApplicationCloudWatchLoggingOption", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "cwl-app")
				require.NoError(t, err)
				assert.NotEmpty(t, app.CloudWatchLoggingOptions)
			}
		})
	}
}

func TestHandler_AddApplicationInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "adds kinesis streams input",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "input-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "input-app",
				"CurrentApplicationVersionId": 1,
				"Input": map[string]any{
					"NamePrefix": "SOURCE_SQL_STREAM",
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app not found",
			input: map[string]any{
				"ApplicationName":             "ghost",
				"CurrentApplicationVersionId": 1,
				"Input": map[string]any{
					"NamePrefix": "SOURCE",
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
						"RoleARN":     "arn:aws:iam::000000000000:role/r",
					},
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "input-ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "input-ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// AWS models Input.NamePrefix as a required member; a request missing it must be
			// rejected, not silently stored with an empty prefix (which also breaks
			// InAppStreamNames derivation).
			name: "missing NamePrefix is rejected",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "input-noprefix-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "input-noprefix-app",
				"CurrentApplicationVersionId": 1,
				"Input": map[string]any{
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "AddApplicationInput", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "input-app")
				require.NoError(t, err)
				assert.NotEmpty(t, app.Inputs)
			}
		})
	}
}

func TestHandler_AddApplicationInputProcessingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(inputID string) map[string]any
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:    "adds processing config to existing input",
			appName: "proc-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "proc-app", "", "", nil)
				_ = b.AddApplicationInput(
					context.Background(),
					"proc-app",
					1,
					kinesisanalytics.InputDescription{NamePrefix: "PREFIX"},
				)
				app, _ := b.DescribeApplication(context.Background(), "proc-app")

				return app.Inputs[0].InputID
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "proc-app",
					"CurrentApplicationVersionId": 2,
					"InputId":                     inputID,
					"InputProcessingConfiguration": map[string]any{
						"InputLambdaProcessor": map[string]any{
							"ResourceARN": "arn:aws:lambda:us-east-1:000000000000:function:fn",
							"RoleARN":     "arn:aws:iam::000000000000:role/role",
						},
					},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "input id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "proc-notfound-app", "", "", nil)

				return "nonexistent-id"
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "proc-notfound-app",
					"CurrentApplicationVersionId": 1,
					"InputId":                     inputID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			inputID := tt.setup(b)
			rec := doRequest(t, h, "AddApplicationInputProcessingConfiguration", tt.input(inputID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), tt.appName)
				require.NoError(t, err)
				require.NotEmpty(t, app.Inputs)
				assert.NotNil(t, app.Inputs[0].InputProcessingConfigurationDescription)
			}
		})
	}
}

func TestHandler_AddApplicationOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "adds kinesis streams output",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "output-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "output-app",
				"CurrentApplicationVersionId": 1,
				"Output": map[string]any{
					"Name": "DESTINATION_SQL_STREAM",
					"KinesisStreamsOutput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/out",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
					"DestinationSchema": map[string]any{"RecordFormatType": "JSON"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app not found",
			input: map[string]any{
				"ApplicationName":             "ghost",
				"CurrentApplicationVersionId": 1,
				"Output": map[string]any{
					"Name": "OUT",
					"KinesisStreamsOutput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/out",
						"RoleARN":     "arn:aws:iam::000000000000:role/r",
					},
					"DestinationSchema": map[string]any{"RecordFormatType": "JSON"},
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "output-ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "output-ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// AWS models Output.Name as a required member; a request missing it must be
			// rejected rather than silently stored with an empty name.
			name: "missing Name is rejected",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "output-noname-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "output-noname-app",
				"CurrentApplicationVersionId": 1,
				"Output": map[string]any{
					"KinesisStreamsOutput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/out",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
					"DestinationSchema": map[string]any{"RecordFormatType": "JSON"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "AddApplicationOutput", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "output-app")
				require.NoError(t, err)
				assert.NotEmpty(t, app.Outputs)
			}
		})
	}
}

func TestHandler_AddApplicationReferenceDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "adds reference data source",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "ref-app",
				"CurrentApplicationVersionId": 1,
				"ReferenceDataSource": map[string]any{
					"TableName": "MY_REF_TABLE",
					"S3ReferenceDataSource": map[string]any{
						"BucketARN":        "arn:aws:s3:::my-bucket",
						"FileKey":          "data.csv",
						"ReferenceRoleARN": "arn:aws:iam::000000000000:role/role",
					},
					"ReferenceSchema": map[string]any{
						"RecordFormat":  map[string]any{"RecordFormatType": "CSV"},
						"RecordColumns": []map[string]any{{"Name": "COL1", "SqlType": "VARCHAR(4)"}},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app not found",
			input: map[string]any{
				"ApplicationName":             "ghost",
				"CurrentApplicationVersionId": 1,
				"ReferenceDataSource": map[string]any{
					"TableName": "MY_REF",
					"S3ReferenceDataSource": map[string]any{
						"BucketARN":        "arn:aws:s3:::my-bucket",
						"FileKey":          "data.csv",
						"ReferenceRoleARN": "arn:aws:iam::000000000000:role/r",
					},
					"ReferenceSchema": map[string]any{
						"RecordFormat":  map[string]any{"RecordFormatType": "CSV"},
						"RecordColumns": []map[string]any{{"Name": "COL1", "SqlType": "VARCHAR(4)"}},
					},
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "ref-ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "AddApplicationReferenceDataSource", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "ref-app")
				require.NoError(t, err)
				assert.NotEmpty(t, app.ReferenceDataSources)
			}
		})
	}
}

func TestHandler_DeleteApplicationCloudWatchLoggingOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(optID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing logging option",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-cwl-app", "", "", nil)
				_ = b.AddApplicationCloudWatchLoggingOption(
					context.Background(),
					"del-cwl-app",
					1,
					kinesisanalytics.CloudWatchLoggingOptionDesc{
						LogStreamARN: "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
						RoleARN:      "arn:aws:iam::000000000000:role/r",
					},
				)
				app, _ := b.DescribeApplication(context.Background(), "del-cwl-app")

				return app.CloudWatchLoggingOptions[0].CloudWatchLoggingOptionID
			},
			input: func(optID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-cwl-app",
					"CurrentApplicationVersionId": 2,
					"CloudWatchLoggingOptionId":   optID,
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "logging option id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-cwl-notfound", "", "", nil)

				return "nonexistent"
			},
			input: func(optID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-cwl-notfound",
					"CurrentApplicationVersionId": 1,
					"CloudWatchLoggingOptionId":   optID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			optID := tt.setup(b)
			rec := doRequest(t, h, "DeleteApplicationCloudWatchLoggingOption", tt.input(optID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "del-cwl-app")
				require.NoError(t, err)
				assert.Empty(t, app.CloudWatchLoggingOptions)
			}
		})
	}
}

func TestHandler_DeleteApplicationInputProcessingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(inputID string) map[string]any
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:    "removes processing config from input",
			appName: "del-proc-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-proc-app", "", "", nil)
				_ = b.AddApplicationInput(
					context.Background(),
					"del-proc-app",
					1,
					kinesisanalytics.InputDescription{NamePrefix: "STREAM"},
				)
				app, _ := b.DescribeApplication(context.Background(), "del-proc-app")
				inputID := app.Inputs[0].InputID
				cfg := &kinesisanalytics.InputProcessingConfigurationDesc{
					InputLambdaProcessor: &kinesisanalytics.LambdaProcessorDesc{ResourceARN: "arn:aws:lambda::fn"},
				}
				_ = b.AddApplicationInputProcessingConfiguration(context.Background(), "del-proc-app", 2, inputID, cfg)

				return inputID
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-proc-app",
					"CurrentApplicationVersionId": 3,
					"InputId":                     inputID,
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "input id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-proc-notfound", "", "", nil)

				return "nonexistent"
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-proc-notfound",
					"CurrentApplicationVersionId": 1,
					"InputId":                     inputID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			inputID := tt.setup(b)
			rec := doRequest(t, h, "DeleteApplicationInputProcessingConfiguration", tt.input(inputID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), tt.appName)
				require.NoError(t, err)
				require.NotEmpty(t, app.Inputs)
				assert.Nil(t, app.Inputs[0].InputProcessingConfigurationDescription)
			}
		})
	}
}

func TestHandler_DeleteApplicationOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(outputID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing output",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-out-app", "", "", nil)
				_ = b.AddApplicationOutput(
					context.Background(),
					"del-out-app",
					1,
					kinesisanalytics.OutputDescription{Name: "STREAM_OUT"},
				)
				app, _ := b.DescribeApplication(context.Background(), "del-out-app")

				return app.Outputs[0].OutputID
			},
			input: func(outputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-out-app",
					"CurrentApplicationVersionId": 2,
					"OutputId":                    outputID,
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "output id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-out-notfound", "", "", nil)

				return "nonexistent"
			},
			input: func(outputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-out-notfound",
					"CurrentApplicationVersionId": 1,
					"OutputId":                    outputID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			outputID := tt.setup(b)
			rec := doRequest(t, h, "DeleteApplicationOutput", tt.input(outputID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "del-out-app")
				require.NoError(t, err)
				assert.Empty(t, app.Outputs)
			}
		})
	}
}

func TestHandler_DeleteApplicationReferenceDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(refID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing reference data source",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-ref-app", "", "", nil)
				_ = b.AddApplicationReferenceDataSource(
					context.Background(),
					"del-ref-app",
					1,
					kinesisanalytics.ReferenceDataSourceDescription{TableName: "REF_TBL"},
				)
				app, _ := b.DescribeApplication(context.Background(), "del-ref-app")

				return app.ReferenceDataSources[0].ReferenceID
			},
			input: func(refID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-ref-app",
					"CurrentApplicationVersionId": 2,
					"ReferenceId":                 refID,
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "reference id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-ref-notfound", "", "", nil)

				return "nonexistent"
			},
			input: func(refID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-ref-notfound",
					"CurrentApplicationVersionId": 1,
					"ReferenceId":                 refID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			refID := tt.setup(b)
			rec := doRequest(t, h, "DeleteApplicationReferenceDataSource", tt.input(refID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "del-ref-app")
				require.NoError(t, err)
				assert.Empty(t, app.ReferenceDataSources)
			}
		})
	}
}

func TestHandler_DiscoverInputSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "returns synthetic schema",
			input: map[string]any{
				"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
				"RoleARN":     "arn:aws:iam::000000000000:role/role",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "returns schema for empty request",
			input:      map[string]any{},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DiscoverInputSchema", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "InputSchema")
				assert.Contains(t, resp, "ParsedInputRecords")
			}
		})
	}
}
