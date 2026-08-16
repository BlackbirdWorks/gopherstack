package cloudwatchlogs_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestHandler_PutSubscriptionFilter_Validation(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)
	_, _ = backend.CreateLogGroup(context.Background(), "/grp", "", "")

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "missing_group_name",
			body:     `{"logGroupName":"","filterName":"f1","destinationArn":"arn:aws:lambda:us-east-1:123:function:fn"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_filter_name",
			body:     `{"logGroupName":"/grp","filterName":"","destinationArn":"arn:aws:lambda:us-east-1:123:function:fn"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_destination_arn",
			body:     `{"logGroupName":"/grp","filterName":"f1","destinationArn":""}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doLogsRequest(t, h, e, "PutSubscriptionFilter", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_SubscriptionFilterOperations(t *testing.T) {
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
			name: "PutSubscriptionFilter",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"sub-grp"}`)
			},
			action: "PutSubscriptionFilter",
			body: map[string]any{
				"logGroupName":   "sub-grp",
				"filterName":     "my-filter",
				"filterPattern":  "",
				"destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:target",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutSubscriptionFilter/GroupNotFound",
			action: "PutSubscriptionFilter",
			body: map[string]any{
				"logGroupName":   "nonexistent",
				"filterName":     "f",
				"destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:target",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "DescribeSubscriptionFilters",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"sub-grp"}`)
				doLogsRequest(
					t, h, e, "PutSubscriptionFilter",
					`{"logGroupName":"sub-grp","filterName":"f1","filterPattern":"",`+
						`"destinationArn":"arn:aws:lambda:us-east-1:123456789012:function:a"}`,
				)
			},
			action:        "DescribeSubscriptionFilters",
			body:          map[string]any{"logGroupName": "sub-grp"},
			wantCode:      http.StatusOK,
			wantListField: "subscriptionFilters",
			wantListLen:   1,
		},
		{
			name:     "DescribeSubscriptionFilters/GroupNotFound",
			action:   "DescribeSubscriptionFilters",
			body:     map[string]any{"logGroupName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "DeleteSubscriptionFilter",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"sub-grp"}`)
				doLogsRequest(
					t, h, e, "PutSubscriptionFilter",
					`{"logGroupName":"sub-grp","filterName":"f1","filterPattern":"",`+
						`"destinationArn":"arn:aws:lambda:us-east-1:123456789012:function:a"}`,
				)
			},
			action:   "DeleteSubscriptionFilter",
			body:     map[string]any{"logGroupName": "sub-grp", "filterName": "f1"},
			wantCode: http.StatusOK,
		},
		{
			name: "DeleteSubscriptionFilter/NotFound",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"sub-grp"}`)
			},
			action:   "DeleteSubscriptionFilter",
			body:     map[string]any{"logGroupName": "sub-grp", "filterName": "nonexistent"},
			wantCode: http.StatusNotFound,
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
