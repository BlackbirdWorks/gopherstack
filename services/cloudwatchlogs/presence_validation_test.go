package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestGetLogFields_DataSourceTypePresenceValidation covers gopherstack-wl0s:
// GetLogFields never read dataSourceType from the request body at all (it
// wasn't even a field on the decode struct), so a request omitting it was
// accepted rather than rejected, matching aws-sdk-go-v2/service/
// cloudwatchlogs@v1.81.1/validators.go's validateOpGetLogFieldsInput. This
// proves both directions: omitting the field is rejected with
// InvalidParameterException (the code GetLogFields' own
// awsAwsjson11_deserializeOpErrorGetLogFields switch declares), and supplying
// it is accepted.
func TestGetLogFields_DataSourceTypePresenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_data_source_type_rejected",
			body:     map[string]any{"dataSourceName": "grp"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "present_data_source_type_accepted",
			body:     map[string]any{"dataSourceName": "grp", "dataSourceType": "LOG_GROUP"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			handler := cloudwatchlogs.NewHandler(backend)
			doLogsRequest(t, handler, e, "CreateLogGroup", `{"logGroupName":"grp"}`)

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, handler, e, "GetLogFields", string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "InvalidParameterException", resp["__type"])
			}
		})
	}
}

// TestListAggregateLogGroupSummaries_GroupByPresenceValidation covers
// gopherstack-wl0s: ListAggregateLogGroupSummaries ignored its request body
// entirely (the handler took only a context, discarding the body via a `_
// []byte` parameter), so groupBy -- required per validateOpListAggregate
// LogGroupSummariesInput -- was silently unused rather than rejected when
// absent. It also caught a materially worse bug beside it: the response was
// wrapped as "logGroupSummaries", a key the real ListAggregateLogGroupSummar
// iesOutput shape does not have at all (confirmed against
// awsAwsjson11_deserializeOpDocumentListAggregateLogGroupSummariesOutput,
// which only recognizes "aggregateLogGroupSummaries") -- so populated
// summaries never actually round-tripped to a real SDK client before this
// fix, regardless of groupBy. Both are fixed together here since they sit in
// the same handler function.
func TestListAggregateLogGroupSummaries_GroupByPresenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantError string
		wantCode  int
	}{
		{
			name:      "missing_group_by_rejected",
			body:      map[string]any{},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:      "unrecognized_group_by_rejected",
			body:      map[string]any{"groupBy": "NOT_A_GROUP_BY"},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:     "data_source_name_and_type_accepted",
			body:     map[string]any{"groupBy": "DATA_SOURCE_NAME_AND_TYPE"},
			wantCode: http.StatusOK,
		},
		{
			name:     "data_source_name_type_and_format_accepted",
			body:     map[string]any{"groupBy": "DATA_SOURCE_NAME_TYPE_AND_FORMAT"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := makeLogsRequest(t, "ListAggregateLogGroupSummaries", string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, tt.wantError, resp["__type"])

				return
			}

			list, ok := resp["aggregateLogGroupSummaries"].([]any)
			require.True(t, ok, "response must wrap the list as aggregateLogGroupSummaries")
			assert.Empty(t, list)
		})
	}
}
