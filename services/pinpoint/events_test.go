package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutEvents_AppLifeCycleEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "lifecycle_open_event",
			body: map[string]any{
				"BatchItem": map[string]any{
					"ep-001": map[string]any{
						"Endpoint": map[string]any{"ChannelType": "APNS"},
						"Events": map[string]any{
							"ev-1": map[string]any{
								"EventType": "_session.start",
								"Timestamp": "2026-05-01T00:00:00Z",
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "lifecycle_stop_event",
			body: map[string]any{
				"BatchItem": map[string]any{
					"ep-002": map[string]any{
						"Endpoint": map[string]any{"ChannelType": "GCM"},
						"Events": map[string]any{
							"ev-2": map[string]any{
								"EventType": "_session.stop",
								"Timestamp": "2026-05-01T01:00:00Z",
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "lifecycle_pause_event",
			body: map[string]any{
				"BatchItem": map[string]any{
					"ep-003": map[string]any{
						"Endpoint": map[string]any{"ChannelType": "EMAIL"},
						"Events": map[string]any{
							"ev-3": map[string]any{
								"EventType": "_session.pause",
								"Timestamp": "2026-05-01T02:00:00Z",
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "lifecycle_resume_event",
			body: map[string]any{
				"BatchItem": map[string]any{
					"ep-004": map[string]any{
						"Endpoint": map[string]any{"ChannelType": "SMS"},
						"Events": map[string]any{
							"ev-4": map[string]any{
								"EventType": "_session.resume",
								"Timestamp": "2026-05-01T03:00:00Z",
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "custom_app_lifecycle_event",
			body: map[string]any{
				"BatchItem": map[string]any{
					"ep-005": map[string]any{
						"Endpoint": map[string]any{"ChannelType": "PUSH"},
						"Events": map[string]any{
							"ev-5": map[string]any{
								"EventType": "app.purchase",
								"Timestamp": "2026-05-01T04:00:00Z",
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "events-app")

			rec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/events", tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// ──────────────────────────────────────────────────
// SegmentVersion deeper
// ──────────────────────────────────────────────────

func TestPutEvents_ReturnsEventResults(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "put-events-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/events",
		map[string]any{
			"BatchItem": map[string]any{
				"ep-001": map[string]any{
					"Endpoint": map[string]any{"ChannelType": "EMAIL"},
					"Events": map[string]any{
						"ev-1": map[string]any{
							"EventType": "_session.start",
							"Timestamp": "2026-01-01T00:00:00Z",
						},
						"ev-2": map[string]any{
							"EventType": "custom.button.click",
							"Timestamp": "2026-01-01T00:00:01Z",
						},
					},
				},
			},
		})

	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["Results"].(map[string]any)
	require.True(t, ok, "EventsResponse must have Results")

	epResult, ok := results["ep-001"].(map[string]any)
	require.True(t, ok, "Results must contain ep-001")

	evResults, ok := epResult["EventsItemResponse"].(map[string]any)
	require.True(t, ok, "ep-001 must have EventsItemResponse")

	ev1, ok := evResults["ev-1"].(map[string]any)
	require.True(t, ok, "EventsItemResponse must contain ev-1")
	assert.EqualValues(t, http.StatusAccepted, ev1["StatusCode"])
	assert.Equal(t, "Accepted", ev1["Message"])

	ev2, ok := evResults["ev-2"].(map[string]any)
	require.True(t, ok, "EventsItemResponse must contain ev-2")
	assert.EqualValues(t, http.StatusAccepted, ev2["StatusCode"])
}

func TestPutEvents_MultipleEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "put-events-multi-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/events",
		map[string]any{
			"BatchItem": map[string]any{
				"ep-a": map[string]any{
					"Endpoint": map[string]any{},
					"Events": map[string]any{
						"ev-a1": map[string]any{"EventType": "login"},
					},
				},
				"ep-b": map[string]any{
					"Endpoint": map[string]any{},
					"Events": map[string]any{
						"ev-b1": map[string]any{"EventType": "purchase"},
						"ev-b2": map[string]any{"EventType": "view"},
					},
				},
			},
		})

	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results := resp["Results"].(map[string]any)

	epA := results["ep-a"].(map[string]any)
	evA := epA["EventsItemResponse"].(map[string]any)
	assert.Len(t, evA, 1, "ep-a has 1 event")

	epB := results["ep-b"].(map[string]any)
	evB := epB["EventsItemResponse"].(map[string]any)
	assert.Len(t, evB, 2, "ep-b has 2 events")
}

func TestPutEvents_EmptyBatch(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "put-events-empty-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/events",
		map[string]any{
			"BatchItem": map[string]any{},
		})

	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results := resp["Results"].(map[string]any)
	assert.Empty(t, results, "empty BatchItem → empty Results")
}

func TestPutEvents_UnknownApp(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/events",
		map[string]any{
			"BatchItem": map[string]any{},
		})

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// ──────────────────────────────────────────────────
// Finding #23: PhoneNumberValidate returns country info
// ──────────────────────────────────────────────────
