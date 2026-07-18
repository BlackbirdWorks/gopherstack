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

func TestHandler_TagRoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	// Create a log group and tag it.
	doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
	doLogsRequest(
		t,
		h,
		e,
		"TagLogGroup",
		`{"logGroupName":"grp","tags":{"env":"prod","team":"ops"}}`,
	)

	// ListTagsLogGroup returns both tags.
	rec := doLogsRequest(t, h, e, "ListTagsLogGroup", `{"logGroupName":"grp"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Equal(t, "prod", listResp["tags"]["env"])
	assert.Equal(t, "ops", listResp["tags"]["team"])

	// ListTagsForResource also works.
	rec2 := doLogsRequest(t, h, e, "ListTagsForResource", `{"resourceArn":"grp"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp2 map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listResp2))
	assert.Len(t, listResp2["tags"], 2)

	// Remove one tag.
	doLogsRequest(t, h, e, "UntagLogGroup", `{"logGroupName":"grp","tags":["env"]}`)

	// Verify only "team" remains.
	rec3 := doLogsRequest(t, h, e, "ListTagsLogGroup", `{"logGroupName":"grp"}`)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listResp3 map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp3))
	assert.Len(t, listResp3["tags"], 1)
	assert.Equal(t, "ops", listResp3["tags"]["team"])
}

func TestHandler_TagResource_UntagResource(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	arn := "arn:aws:logs:us-east-1:123456789012:log-group:/my-group"

	// TagResource — set two tags.
	rec := doLogsRequest(t, h, e, "TagResource",
		`{"resourceArn":"`+arn+`","tags":{"env":"prod","team":"platform"}}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource — verify tags present.
	rec = doLogsRequest(t, h, e, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var listOut map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Equal(t, "prod", listOut["tags"]["env"])
	assert.Equal(t, "platform", listOut["tags"]["team"])

	// UntagResource — remove one tag.
	rec = doLogsRequest(t, h, e, "UntagResource",
		`{"resourceArn":"`+arn+`","tagKeys":["env"]}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify env tag removed, team tag remains.
	rec = doLogsRequest(t, h, e, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut["tags"]["env"])
	assert.Equal(t, "platform", listOut["tags"]["team"])
}

func TestHandler_Reset_ClearsTags(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	// Set a tag.
	rec := doLogsRequest(t, h, e, "TagResource",
		`{"resourceArn":"arn:aws:logs:us-east-1:123:log-group:/g","tags":{"k":"v"}}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Reset.
	h.Reset()

	// Tags should be gone after reset.
	rec = doLogsRequest(t, h, e, "ListTagsForResource",
		`{"resourceArn":"arn:aws:logs:us-east-1:123:log-group:/g"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var listOut map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut["tags"])
}

func TestHandler_TagOperations(t *testing.T) {
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
			name: "TagLogGroup",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"tag-grp"}`)
			},
			action: "TagLogGroup",
			body: map[string]any{
				"logGroupName": "tag-grp",
				"tags":         map[string]string{"env": "prod", "team": "ops"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "ListTagsLogGroup",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"tag-grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"TagLogGroup",
					`{"logGroupName":"tag-grp","tags":{"env":"prod","team":"ops"}}`,
				)
			},
			action:   "ListTagsLogGroup",
			body:     map[string]any{"logGroupName": "tag-grp"},
			wantCode: http.StatusOK,
		},
		{
			name: "ListTagsForResource",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"tag-grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"TagLogGroup",
					`{"logGroupName":"tag-grp","tags":{"env":"prod"}}`,
				)
			},
			action:   "ListTagsForResource",
			body:     map[string]any{"resourceArn": "tag-grp"},
			wantCode: http.StatusOK,
		},
		{
			name: "UntagLogGroup",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"tag-grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"TagLogGroup",
					`{"logGroupName":"tag-grp","tags":{"env":"prod","team":"ops"}}`,
				)
			},
			action:   "UntagLogGroup",
			body:     map[string]any{"logGroupName": "tag-grp", "tags": []string{"env"}},
			wantCode: http.StatusOK,
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
