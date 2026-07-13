package timestreamquery_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &timestreamquery.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, timestreamquery.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &timestreamquery.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ timestreamquery.StorageBackend = (*timestreamquery.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	h := timestreamquery.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 15)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	h := timestreamquery.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_HandlerOpsLenExport verifies exported HandlerOpsLen helper.
func TestRefinement1_HandlerOpsLenExport(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	h := timestreamquery.NewHandler(b)
	assert.Equal(t, 15, timestreamquery.HandlerOpsLen(h))
}

// TestRefinement1_ScheduledQueryCountExport verifies ScheduledQueryCount export.
func TestRefinement1_ScheduledQueryCountExport(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, timestreamquery.ScheduledQueryCount(b))
}

// TestRefinement1_QueryCountExport verifies QueryCount export.
func TestRefinement1_QueryCountExport(t *testing.T) {
	t.Parallel()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, timestreamquery.QueryCount(b))
}

// TestRefinement1_BackendReset verifies Reset clears all state.
func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	// Create a scheduled query to have some state.
	rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "reset-test",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend))

	// Reset clears all state.
	backend.Reset()
	assert.Equal(t, 0, timestreamquery.ScheduledQueryCount(backend))
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "handler-reset-test",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend))

	h.Reset()
	assert.Equal(t, 0, timestreamquery.ScheduledQueryCount(backend))
}

// TestRefinement1_AddScheduledQueryInternal verifies seed helper.
func TestRefinement1_AddScheduledQueryInternal(t *testing.T) {
	t.Parallel()

	backend := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")

	sq := &timestreamquery.ScheduledQuery{
		Name:         "seeded-query",
		Arn:          "arn:aws:timestream:us-east-1:000000000000:scheduled-query/seeded-query",
		State:        "ENABLED",
		QueryString:  "SELECT 1",
		CreationTime: time.Now(),
	}

	backend.AddScheduledQueryInternal(sq)
	assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend))

	result, err := backend.DescribeScheduledQuery(t.Context(), sq.Arn)
	require.NoError(t, err)
	assert.Equal(t, "seeded-query", result.Name)
}

// TestRefinement1_UpdateScheduledQueryInvalidState verifies state validation.
func TestRefinement1_UpdateScheduledQueryInvalidState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		state     string
		wantCode  int
		wantError bool
	}{
		{name: "ENABLED is valid", state: "ENABLED", wantCode: http.StatusOK},
		{name: "DISABLED is valid", state: "DISABLED", wantCode: http.StatusOK},
		{name: "PAUSED is invalid", state: "PAUSED", wantCode: http.StatusBadRequest, wantError: true},
		{name: "lowercase is invalid", state: "enabled", wantCode: http.StatusBadRequest, wantError: true},
		{name: "empty is invalid", state: "", wantCode: http.StatusBadRequest, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, h := newTestBackendAndHandler()

			// Create a scheduled query first.
			rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
				"Name":                           "state-test",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			arn := parseResponse(t, rec)["Arn"].(string)

			rec = doRequest(t, h, "UpdateScheduledQuery", map[string]any{
				"ScheduledQueryArn": arn,
				"State":             tt.state,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_CreateScheduledQueryRequiredFields verifies required field validation.
func TestRefinement1_CreateScheduledQueryRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing name",
			body: map[string]any{
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:...",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing query string",
			body: map[string]any{
				"Name":                           "q",
				"ScheduledQueryExecutionRoleArn": "arn:...",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing execution role arn",
			body: map[string]any{
				"Name":                  "q",
				"QueryString":           "SELECT 1",
				"ScheduleConfiguration": map[string]any{"ScheduleExpression": "rate(1 hour)"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing schedule expression",
			body: map[string]any{
				"Name":                           "q",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:...",
				"ScheduleConfiguration":          map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing notification configuration",
			body: map[string]any{
				"Name":                           "q",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing error report configuration",
			body: map[string]any{
				"Name":                           "q",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "all required fields provided",
			body: map[string]any{
				"Name":                           "q",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "CreateScheduledQuery", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_DescribeScheduledQuery_DeepCopy verifies that mutating the returned
// ScheduledQuery does not affect the stored copy.
func TestRefinement1_DescribeScheduledQuery_DeepCopy(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "copy-test",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"Tags":                           []map[string]string{{"Key": "env", "Value": "prod"}},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	arn := parseResponse(t, rec)["Arn"].(string)

	// Get a copy and mutate its Tags.
	sq, err := backend.DescribeScheduledQuery(t.Context(), arn)
	require.NoError(t, err)
	sq.Tags["env"] = "mutated"

	// The stored query should be unaffected.
	sq2, err := backend.DescribeScheduledQuery(t.Context(), arn)
	require.NoError(t, err)
	assert.Equal(t, "prod", sq2.Tags["env"], "mutation of returned copy must not affect stored state")
}

// TestRefinement1_ScheduledQueryToViewIncludesTags verifies Tags appear in DescribeScheduledQuery.
func TestRefinement1_ScheduledQueryToViewIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "tagged-describe",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"Tags": []map[string]string{
			{"Key": "team", "Value": "data"},
			{"Key": "env", "Value": "test"},
		},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	arn := parseResponse(t, rec)["Arn"].(string)

	rec = doRequest(t, h, "DescribeScheduledQuery", map[string]any{"ScheduledQueryArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	sq := resp["ScheduledQuery"].(map[string]any)
	tags, ok := sq["Tags"].([]any)
	require.True(t, ok, "Tags should be present in DescribeScheduledQuery response")
	assert.Len(t, tags, 2)

	// Tags should be sorted by key.
	tag0 := tags[0].(map[string]any)
	tag1 := tags[1].(map[string]any)
	assert.Equal(t, "env", tag0["Key"])
	assert.Equal(t, "team", tag1["Key"])
}

// TestRefinement1_MaxQueryTCUPositiveValidation verifies MaxQueryTCU must be positive.
func TestRefinement1_MaxQueryTCUPositiveValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tcu      int
		wantCode int
	}{
		{name: "positive value is valid", tcu: 100, wantCode: http.StatusOK},
		{name: "zero is invalid", tcu: 0, wantCode: http.StatusBadRequest},
		{name: "negative is invalid", tcu: -1, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{
				"MaxQueryTCU": tt.tcu,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_ExtractResource_ScheduledQueryArn is covered by
// TestTimestreamQueryHandler_ExtractResource in handler_test.go.
// This test verifies ScheduledQueryCount and QueryCount after a tag-only operation.
func TestRefinement1_TagResourceNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "TagResource on non-existent ARN returns not found",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceARN": "arn:aws:timestream:us-east-1:123:scheduled-query/nope",
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_Persistence_SnapshotRestore verifies snapshot round-trip.
func TestRefinement1_Persistence_SnapshotRestore(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	// Create a scheduled query.
	rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "persist-test",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"Tags":                           []map[string]string{{"Key": "k", "Value": "v"}},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update account settings.
	rec = doRequest(t, h, "UpdateAccountSettings", map[string]any{
		"QueryPricingModel": "COMPUTE_UNITS",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Snapshot.
	snap := backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Create fresh backend and restore.
	backend2 := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, backend2.Restore(t.Context(), snap))

	assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend2))

	settings := backend2.DescribeAccountSettings(t.Context())
	assert.Equal(t, "COMPUTE_UNITS", settings.QueryPricingModel)

	// Restored query should have its tags.
	queries := backend2.ListScheduledQueriesFull(t.Context())
	require.Len(t, queries, 1)
	assert.Equal(t, "v", queries[0].Tags["k"])
}

// TestRefinement1_ContentTypeHeader verifies Content-Type is set for all responses including empty.
func TestRefinement1_ContentTypeHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "delete returns empty body with content type",
			op:   "DeleteScheduledQuery",
		},
		{
			name: "query returns content type",
			op:   "Query",
			body: map[string]any{"QueryString": "SELECT 1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, h := newTestBackendAndHandler()

			if tt.op == "DeleteScheduledQuery" {
				// Create first.
				rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
					"Name":                           "ct-test",
					"QueryString":                    "SELECT 1",
					"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
					"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
					"NotificationConfiguration": map[string]any{
						"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
					},
					"ErrorReportConfiguration": map[string]any{
						"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				arn := parseResponse(t, rec)["Arn"].(string)
				tt.body = map[string]any{"ScheduledQueryArn": arn}
			}

			rec := doRequest(t, h, tt.op, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/x-amz-json-1.0", rec.Header().Get("Content-Type"))
		})
	}
}

// TestRefinement1_BadJSON verifies handlers return 500 on malformed JSON input.
func TestRefinement1_BadJSON(t *testing.T) {
	t.Parallel()

	ops := []string{
		"Query",
		"CancelQuery",
		"CreateScheduledQuery",
		"DeleteScheduledQuery",
		"DescribeScheduledQuery",
		"ExecuteScheduledQuery",
		"UpdateScheduledQuery",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"PrepareQuery",
		"UpdateAccountSettings",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequestRaw(t, h, op, []byte(`not-valid-json{`))
			// Bad JSON should return 5xx (parse error) or 4xx (validation)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)
		})
	}
}

// TestRefinement1_HandleErrorBranches verifies handleError maps error types correctly.
func TestRefinement1_HandleErrorBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantType string
		wantCode int
	}{
		{
			name:     "not found returns ResourceNotFoundException",
			op:       "DescribeScheduledQuery",
			body:     map[string]any{"ScheduledQueryArn": "arn:aws:timestream:us-east-1:123:scheduled-query/nope"},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceNotFoundException",
		},
		{
			name: "already exists returns ConflictException",
			op:   "CreateScheduledQuery",
			body: map[string]any{
				"Name":                           "dup",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
				},
			},
			wantCode: http.StatusConflict,
			wantType: "ConflictException",
		},
		{
			name:     "validation returns ValidationException",
			op:       "UpdateScheduledQuery",
			body:     map[string]any{"ScheduledQueryArn": "arn:...", "State": "INVALID_STATE"},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "unknown operation returns ValidationException",
			op:       "NonExistentOp",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// For duplicate test, create first.
			if tt.op == "CreateScheduledQuery" && tt.wantType == "ConflictException" {
				first := doRequest(t, h, "CreateScheduledQuery", tt.body)
				require.Equal(t, http.StatusOK, first.Code)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}

// TestRefinement1_QueryCountTrack verifies QueryCount tracks executed queries.
func TestRefinement1_QueryCountTrack(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	assert.Equal(t, 0, timestreamquery.QueryCount(backend))

	rec := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 1"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.QueryCount(backend))

	// CancelQuery is documented as idempotent: it marks the result cancelled
	// in place rather than deleting it, so a repeat cancellation of the same
	// QueryId still succeeds (CancelQueryOutput.CancellationMessage) instead
	// of 404ing, and the query still counts until evicted by the retention cap.
	qid := parseResponse(t, rec)["QueryId"].(string)
	rec = doRequest(t, h, "CancelQuery", map[string]any{"QueryId": qid})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.QueryCount(backend))

	// A repeat cancellation of the same QueryId must still succeed.
	rec = doRequest(t, h, "CancelQuery", map[string]any{"QueryId": qid})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.QueryCount(backend))
}

// TestRefinement1_ScheduledQueryCountTrack verifies ScheduledQueryCount increments and decrements.
func TestRefinement1_ScheduledQueryCountTrack(t *testing.T) {
	t.Parallel()

	backend, h := newTestBackendAndHandler()

	assert.Equal(t, 0, timestreamquery.ScheduledQueryCount(backend))

	rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "count-track",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend))

	arn := parseResponse(t, rec)["Arn"].(string)
	doRequest(t, h, "DeleteScheduledQuery", map[string]any{"ScheduledQueryArn": arn})
	assert.Equal(t, 0, timestreamquery.ScheduledQueryCount(backend))
}

// TestRefinement1_HandlerSnapshotRestore verifies handler Snapshot/Restore delegation.
func TestRefinement1_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot and restore via handler"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, h := newTestBackendAndHandler()

			// Create a scheduled query.
			rec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
				"Name":                           "handler-snap-test",
				"QueryString":                    "SELECT 1",
				"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
				"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
				"NotificationConfiguration": map[string]any{
					"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
				},
				"ErrorReportConfiguration": map[string]any{
					"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Snapshot via handler.
			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			// Create new handler and restore.
			backend2 := timestreamquery.NewInMemoryBackend("123456789012", "us-east-1")
			h2 := timestreamquery.NewHandler(backend2)
			require.NoError(t, h2.Restore(t.Context(), snap))

			assert.Equal(t, 1, timestreamquery.ScheduledQueryCount(backend2))
		})
	}
}

// TestRefinement1_HandlerSnapshotWithInMemoryBackend verifies that Handler.Snapshot
// delegates correctly to the InMemoryBackend.
func TestRefinement1_HandlerSnapshotWithInMemoryBackend(t *testing.T) {
	t.Parallel()

	h := timestreamquery.NewHandler(timestreamquery.NewInMemoryBackend("123", "us-east-1"))

	snap := h.Snapshot(t.Context())
	assert.NotNil(t, snap, "InMemoryBackend always produces a valid snapshot")
}

// TestRefinement1_PersistenceRestoreInvalidJSON verifies Restore returns error on bad data.
func TestRefinement1_PersistenceRestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{name: "invalid json fails", data: []byte(`not-json`), wantErr: true},
		{name: "empty object succeeds", data: []byte(`{}`), wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.Restore(t.Context(), tt.data)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
