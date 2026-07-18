package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Destination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutDestination/OK",
			action: "PutDestination",
			body: map[string]any{
				"destinationName": "my-dest",
				"targetArn":       "arn:aws:kinesis:::stream/my-stream",
				"roleArn":         "arn:aws:iam:::role/my-role",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutDestination/EmptyName",
			action: "PutDestination",
			body: map[string]any{
				"destinationName": "",
				"targetArn":       "arn:aws:kinesis:::stream/s",
				"roleArn":         "arn:aws:iam:::role/r",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "PutDestinationPolicy/OK",
			action: "PutDestinationPolicy",
			body: map[string]any{
				"destinationName": "my-dest",
				"accessPolicy":    `{"Statement":[]}`,
			},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDestination",
					`{"destinationName":"my-dest","targetArn":"arn:t","roleArn":"arn:r"}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutDestinationPolicy/NotFound",
			action: "PutDestinationPolicy",
			body: map[string]any{
				"destinationName": "ghost",
				"accessPolicy":    `{}`,
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DescribeDestinations/WithPrefix",
			action: "DescribeDestinations",
			body:   map[string]any{"DestinationNamePrefix": "prod"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDestination",
					`{"destinationName":"prod-dest","targetArn":"arn:t","roleArn":"arn:r"}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"PutDestination",
					`{"destinationName":"dev-dest","targetArn":"arn:t2","roleArn":"arn:r2"}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDestination/OK",
			action: "DeleteDestination",
			body:   map[string]any{"destinationName": "my-dest"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDestination",
					`{"destinationName":"my-dest","targetArn":"arn:t","roleArn":"arn:r"}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteDestination/NotFound",
			action:   "DeleteDestination",
			body:     map[string]any{"destinationName": "ghost"},
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
		})
	}
}

func TestHandler_DestinationResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body       map[string]any
		name       string
		action     string
		wantFields []string
		wantCode   int
	}{
		{
			name:   "PutDestination/HasDestination",
			action: "PutDestination",
			body: map[string]any{
				"destinationName": "d",
				"targetArn":       "arn:t",
				"roleArn":         "arn:r",
			},
			wantFields: []string{"destination"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "DescribeDestinations/HasDestinations",
			action:     "DescribeDestinations",
			body:       map[string]any{},
			wantFields: []string{"destinations"},
			wantCode:   http.StatusOK,
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

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "response should contain field %q", field)
			}
		})
	}
}
