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
			// PutDestinationPolicy's own deserializer declares
			// InvalidParameterException/OperationAbortedException/
			// ServiceUnavailableException, not ResourceNotFoundException, so an
			// unknown destination reports 400 InvalidParameterException here,
			// not 404 -- unlike DeleteDestination/NotFound below, whose op does
			// declare ResourceNotFoundException.
			name:   "PutDestinationPolicy/InvalidParameter",
			action: "PutDestinationPolicy",
			body: map[string]any{
				"destinationName": "ghost",
				"accessPolicy":    `{}`,
			},
			wantCode: http.StatusBadRequest,
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

// TestHandler_Destination_WireShapeIncludesCreationTimeAndAccessPolicy locks
// the full aws-sdk-go-v2 types.Destination wire shape: accessPolicy and
// creationTime are real top-level fields (alongside arn/destinationName/
// roleArn/targetArn) that a previous version of this handler silently
// dropped from PutDestination and DescribeDestinations responses.
func TestHandler_Destination_WireShapeIncludesCreationTimeAndAccessPolicy(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	putRec := doLogsRequest(t, h, e, "PutDestination",
		`{"destinationName":"my-dest","targetArn":"arn:aws:kinesis:::stream/s","roleArn":"arn:aws:iam:::role/r"}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
	putDest, ok := putOut["destination"].(map[string]any)
	require.True(t, ok)
	ct, hasCreationTime := putDest["creationTime"]
	assert.True(t, hasCreationTime, "PutDestination response must include creationTime")
	assert.Greater(t, ct, float64(0))
	_, hasAccessPolicy := putDest["accessPolicy"]
	assert.True(t, hasAccessPolicy, "PutDestination response must include accessPolicy")

	policyRec := doLogsRequest(t, h, e, "PutDestinationPolicy",
		`{"destinationName":"my-dest","accessPolicy":"{\"Statement\":[]}"}`)
	require.Equal(t, http.StatusOK, policyRec.Code)

	describeRec := doLogsRequest(t, h, e, "DescribeDestinations", `{}`)
	require.Equal(t, http.StatusOK, describeRec.Code)

	var describeOut map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeOut))
	dests, ok := describeOut["destinations"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)
	dest, ok := dests[0].(map[string]any)
	require.True(t, ok)
	_, hasCreationTime = dest["creationTime"]
	assert.True(t, hasCreationTime, "DescribeDestinations entries must include creationTime")
	accessPolicy, ok := dest["accessPolicy"].(string)
	require.True(t, ok)
	assert.JSONEq(t, `{"Statement":[]}`, accessPolicy)
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
