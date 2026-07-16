package eventbridge_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetEventBusPolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "PutPermission", map[string]any{
		"StatementId": "allow-123",
		"Action":      "events:PutEvents",
		"Principal":   "123456789013",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "GetEventBusPolicy", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "allow-123")
}

func TestHandler_UpdateEventBus(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateEventBus(context.Background(), "describable-bus", "old desc")
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "UpdateEventBus", map[string]any{
		"Name":        "describable-bus",
		"Description": "new desc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCreateEventBus_TagsPersisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]any
		wantTags map[string]string
		name     string
		busName  string
	}{
		{
			name:    "tags supplied at creation are stored",
			busName: "tagged-bus",
			tags:    map[string]any{"env": "prod", "team": "platform"},
			wantTags: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
		},
		{
			name:     "no tags is still valid",
			busName:  "untagged-bus",
			tags:     nil,
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			body := map[string]any{"Name": tt.busName}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}
			rec := auditMakeRequest(t, h, e, "CreateEventBus", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			busArn := createResp["EventBusArn"]
			require.NotEmpty(t, busArn)

			rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{"ResourceARN": busArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var tagsResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))

			got := make(map[string]string, len(tagsResp.Tags))
			for _, kv := range tagsResp.Tags {
				got[kv.Key] = kv.Value
			}
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

func TestHandler_ListWithPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupAction string
		action      string
		body        string
		setupBodies []string
		wantCode    int
	}{
		{
			name:        "list rules with name prefix",
			setupAction: "PutRule",
			setupBodies: []string{
				`{"Name":"alpha-rule","EventBusName":"default","EventPattern":"{}","State":"ENABLED"}`,
				`{"Name":"beta-rule","EventBusName":"default","EventPattern":"{}","State":"ENABLED"}`,
			},
			action:   "ListRules",
			body:     `{"EventBusName":"default","NamePrefix":"alpha"}`,
			wantCode: http.StatusOK,
		},
		{
			name:        "list event buses with name prefix",
			setupAction: "CreateEventBus",
			setupBodies: []string{
				`{"Name":"prod-bus"}`,
				`{"Name":"dev-bus"}`,
			},
			action:   "ListEventBuses",
			body:     `{"NamePrefix":"prod"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()

			bk := eventbridge.NewInMemoryBackend()
			h := eventbridge.NewHandler(bk)
			for _, setupBody := range tt.setupBodies {
				makeRequestWithHandler(t, h, e, tt.setupAction, setupBody)
			}
			rec := makeRequestWithHandler(t, h, e, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateEventBus_AlreadyExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		busBody  string
		wantCode int
	}{
		{
			name:     "conflict when creating duplicate bus",
			busBody:  `{"Name":"dup-bus"}`,
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()

			bk := eventbridge.NewInMemoryBackend()
			h := eventbridge.NewHandler(bk)
			makeRequestWithHandler(t, h, e, "CreateEventBus", tt.busBody)
			rec := makeRequestWithHandler(t, h, e, "CreateEventBus", tt.busBody)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteDefaultBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "cannot delete default bus",
			body:     `{"Name":"default"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, "DeleteEventBus", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestDeleteEventBus_CleansUpTags verifies that deleting an event bus
// removes its tag entry from the handler so the tags map doesn't grow unbounded.
func TestDeleteEventBus_CleansUpTags(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend)
	e := echo.New()

	const busARN = "arn:aws:events:us-east-1:000000000000:event-bus/temp-bus"

	// Create and tag a bus.
	rec := makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"temp-bus"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	tagBody := `{"ResourceARN":"` + busARN + `","Tags":[{"Key":"owner","Value":"test"}]}`
	rec = makeRequestWithHandler(t, handler, e, "TagResource", tagBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Tags should be present.
	rec = makeRequestWithHandler(t, handler, e, "ListTagsForResource", `{"ResourceARN":"`+busARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "owner")

	// Delete the bus.
	rec = makeRequestWithHandler(t, handler, e, "DeleteEventBus", `{"Name":"temp-bus"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Tags should now return empty (map entry cleaned up).
	rec = makeRequestWithHandler(t, handler, e, "ListTagsForResource", `{"ResourceARN":"`+busARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "owner")
}

func TestHandler_CreateEventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantArnPart string
		wantCode    int
	}{
		{
			name:        "create event bus returns ARN containing bus name",
			body:        `{"Name":"test-bus","Description":"my bus"}`,
			wantCode:    http.StatusOK,
			wantArnPart: "test-bus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeRequest(t, "CreateEventBus", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp["EventBusArn"], tt.wantArnPart)
		})
	}
}

func TestHandler_CreateAndListEventBuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		busNames     []string
		wantMinCount int
	}{
		{
			name:         "create multiple buses then list shows all buses including default",
			busNames:     []string{"bus-a", "bus-b"},
			wantMinCount: 3, // default + bus-a + bus-b
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			for _, name := range tt.busNames {
				makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"`+name+`"}`)
			}

			rec := makeRequestWithHandler(t, handler, e, "ListEventBuses", `{}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				NextToken  string                 `json:"NextToken"`
				EventBuses []eventbridge.EventBus `json:"EventBuses"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.GreaterOrEqual(t, len(resp.EventBuses), tt.wantMinCount)
		})
	}
}

func TestHandler_DeleteEventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		createBus        string
		deleteBus        string
		wantDeleteCode   int
		describeAfter    bool
		wantDescribeCode int
	}{
		{
			name:             "create then delete then describe returns not found",
			createBus:        "temp-bus",
			deleteBus:        "temp-bus",
			wantDeleteCode:   http.StatusOK,
			describeAfter:    true,
			wantDescribeCode: http.StatusNotFound,
		},
		{
			name:           "cannot delete default bus returns bad request",
			deleteBus:      "default",
			wantDeleteCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			if tt.createBus != "" {
				makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"`+tt.createBus+`"}`)
			}

			rec := makeRequestWithHandler(t, handler, e, "DeleteEventBus", `{"Name":"`+tt.deleteBus+`"}`)
			assert.Equal(t, tt.wantDeleteCode, rec.Code)

			if tt.describeAfter {
				rec = makeRequestWithHandler(t, handler, e, "DescribeEventBus", `{"Name":"`+tt.deleteBus+`"}`)
				assert.Equal(t, tt.wantDescribeCode, rec.Code)
			}
		})
	}
}
