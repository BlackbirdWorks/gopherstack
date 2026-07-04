package eventbridge_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// makeRequestWithFreshHandler sends a single POST to a given handler instance
// using a freshly created echo.Echo. Each call creates a new Echo so the
// context/router state is isolated per request, which mirrors how the actual
// HTTP server dispatches individual incoming requests.
func makeRequestWithFreshHandler(
	t *testing.T,
	h *eventbridge.Handler,
	action, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	return makeRequestWithHandler(t, h, e, action, body)
}

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *eventbridge.InMemoryBackend) string
		verify func(t *testing.T, b *eventbridge.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *eventbridge.InMemoryBackend) string {
				bus, err := b.CreateEventBus(context.Background(), "test-bus", "")
				if err != nil {
					return ""
				}

				return bus.Name
			},
			verify: func(t *testing.T, b *eventbridge.InMemoryBackend, id string) {
				t.Helper()

				bus, err := b.DescribeEventBus(context.Background(), id)
				require.NoError(t, err)
				assert.Equal(t, id, bus.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *eventbridge.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *eventbridge.InMemoryBackend, _ string) {
				t.Helper()

				// The default event bus always exists; just verify restore worked
				buses, _, err := b.ListEventBuses(context.Background(), "", "", 0)
				require.NoError(t, err)
				assert.NotNil(t, buses)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := eventbridge.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := eventbridge.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestHandler_Snapshot_IncludesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags         map[string]string
		name         string
		resourceARN  string
		wantTagCount int
	}{
		{
			name:         "snapshot includes tags set via TagResource",
			resourceARN:  "arn:aws:events:us-east-1:123456789012:rule/snap-rule",
			tags:         map[string]string{"env": "prod", "team": "core"},
			wantTagCount: 2,
		},
		{
			name:         "snapshot with no tags produces empty tag map",
			resourceARN:  "arn:aws:events:us-east-1:123456789012:rule/empty-rule",
			tags:         nil,
			wantTagCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1"))
			e := echo.New()

			for k, v := range tt.tags {
				tagBody, err := json.Marshal(map[string]any{
					"ResourceARN": tt.resourceARN,
					"Tags":        []map[string]string{{"Key": k, "Value": v}},
				})
				require.NoError(t, err)
				rec := makeRequestWithHandler(t, h, e, "TagResource", string(tagBody))
				require.Equal(t, http.StatusOK, rec.Code)
			}

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			// Restore into a fresh handler and verify tags round-tripped.
			fresh := eventbridge.NewHandler(eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1"))
			require.NoError(t, fresh.Restore(t.Context(), snap))

			listBody, err := json.Marshal(map[string]string{"ResourceARN": tt.resourceARN})
			require.NoError(t, err)
			rec := makeRequestWithFreshHandler(t, fresh, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Tags, tt.wantTagCount)

			if tt.wantTagCount > 0 {
				got := make(map[string]string, len(resp.Tags))
				for _, tag := range resp.Tags {
					got[tag.Key] = tag.Value
				}
				for k, v := range tt.tags {
					assert.Equal(t, v, got[k])
				}
			}
		})
	}
}

func TestHandler_Restore_InvalidData(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	err := h.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestHandler_Reset_ClearsTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupTags   map[string]string
		name        string
		resourceARN string
	}{
		{
			name:        "reset clears tags so they don't bleed across test runs",
			resourceARN: "arn:aws:events:us-east-1:123456789012:rule/bleed-rule",
			setupTags:   map[string]string{"bleed": "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1"))
			e := echo.New()

			for k, v := range tt.setupTags {
				tagBody, err := json.Marshal(map[string]any{
					"ResourceARN": tt.resourceARN,
					"Tags":        []map[string]string{{"Key": k, "Value": v}},
				})
				require.NoError(t, err)
				rec := makeRequestWithHandler(t, h, e, "TagResource", string(tagBody))
				require.Equal(t, http.StatusOK, rec.Code)
			}

			h.Reset()

			// After reset, listing tags for the same resource should return empty.
			listBody, err := json.Marshal(map[string]string{"ResourceARN": tt.resourceARN})
			require.NoError(t, err)
			rec := makeRequestWithHandler(t, h, e, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Empty(t, resp.Tags, "tags should be cleared after Reset()")
		})
	}
}

func TestHandler_StartWorkerAndChaos(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  any
		check func(h *eventbridge.Handler) any
		name  string
	}{
		{
			name:  "StartWorker returns nil error",
			check: func(h *eventbridge.Handler) any { return h.StartWorker(t.Context()) },
			want:  nil,
		},
		{
			name:  "ChaosServiceName returns events",
			check: func(h *eventbridge.Handler) any { return h.ChaosServiceName() },
			want:  "events",
		},
		{
			name:  "ChaosOperations returns non-empty list",
			check: func(h *eventbridge.Handler) any { return len(h.ChaosOperations()) > 0 },
			want:  true,
		},
		{
			name:  "ChaosRegions returns non-empty list",
			check: func(h *eventbridge.Handler) any { return len(h.ChaosRegions()) > 0 },
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			assert.Equal(t, tt.want, tt.check(h))
		})
	}
}
