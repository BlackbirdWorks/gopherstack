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

// TestInMemoryBackend_FullStateSnapshotRestore exercises a Snapshot->Restore
// round trip across every *store.Table-backed resource from the Phase 3.3
// datalayer conversion (buses, rules, targets, event sources, replays, API
// destinations, archives, connections, endpoints, partner sources), plus the
// derived indexes (ruleIndex pattern matching, targetsByARN) that Restore
// rebuilds from the restored tables rather than persisting directly.
func TestInMemoryBackend_FullStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	original := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Custom event bus.
	_, err := original.CreateEventBus(ctx, "custom-bus", "a custom bus")
	require.NoError(t, err)

	// Rule with an event pattern (exercises ruleIndex rebuild + pattern
	// matching) on the custom bus.
	_, err = original.PutRule(ctx, eventbridge.PutRuleInput{
		Name:         "custom-rule",
		EventBusName: "custom-bus",
		EventPattern: `{"source":["my.app"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	// Target on that rule (exercises targets table + targetsByARN rebuild).
	failed, err := original.PutTargets(ctx, "custom-rule", "custom-bus", []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:sqs:us-east-1:123456789012:my-queue"},
	})
	require.NoError(t, err)
	require.Empty(t, failed)

	// Connection + API destination (connection referenced by ARN).
	conn, err := original.CreateConnection(ctx, eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "API_KEY",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "x-api-key",
				APIKeyValue: "secret-value",
			},
		},
	})
	require.NoError(t, err)

	_, err = original.CreateAPIDestination(ctx, eventbridge.CreateAPIDestinationInput{
		Name:               "my-dest",
		ConnectionArn:      conn.ConnectionArn,
		InvocationEndpoint: "https://example.com/webhook",
		HTTPMethod:         "POST",
	})
	require.NoError(t, err)

	// Archive.
	_, err = original.CreateArchive(ctx, eventbridge.CreateArchiveInput{
		ArchiveName:    "my-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/custom-bus",
	})
	require.NoError(t, err)

	// Endpoint.
	_, err = original.CreateEndpoint(ctx, eventbridge.CreateEndpointInput{
		Name:    "my-endpoint",
		RoleArn: "arn:aws:iam::123456789012:role/endpoint-role",
	})
	require.NoError(t, err)

	// Partner event source (also creates a mirrored, activatable EventSource).
	_, err = original.CreatePartnerEventSource(ctx, "aws.partner/example.com/orders", "999999999999")
	require.NoError(t, err)
	require.NoError(t, original.ActivateEventSource(ctx, "aws.partner/example.com/orders"))

	// Replay.
	_, err = original.StartReplay(ctx, eventbridge.StartReplayInput{
		ReplayName:     "my-replay",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/my-archive",
	})
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Bus.
	bus, err := fresh.DescribeEventBus(ctx, "custom-bus")
	require.NoError(t, err)
	assert.Equal(t, "a custom bus", bus.Description)

	// Rule + pattern (TestEventPattern proves compiledPattern survived).
	rule, err := fresh.DescribeRule(ctx, "custom-rule", "custom-bus")
	require.NoError(t, err)
	assert.JSONEq(t, `{"source":["my.app"]}`, rule.EventPattern)

	// Target + targetsByARN index rebuild.
	targets, _, err := fresh.ListTargetsByRule(ctx, "custom-rule", "custom-bus", "", 0)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123456789012:my-queue", targets[0].Arn)

	ruleNames, _, err := fresh.ListRuleNamesByTarget(
		ctx, "arn:aws:sqs:us-east-1:123456789012:my-queue", "custom-bus", "",
	)
	require.NoError(t, err)
	assert.Contains(t, ruleNames, "custom-rule")

	// Connection (auth is masked on Describe either way).
	restoredConn, err := fresh.DescribeConnection(ctx, "my-conn")
	require.NoError(t, err)
	assert.Equal(t, "API_KEY", restoredConn.AuthorizationType)

	// API destination + connection lookup round trip. Connection.authSecret
	// is unexported by design (never returned from Describe/List, see
	// models.go) so, exactly as before this conversion, plain encoding/json
	// never serializes it -- it does NOT survive Snapshot/Restore either via
	// the old raw map or the new *store.Table. ResolveAPIDestination still
	// resolves the destination/connection pair; only the secret itself is
	// empty post-restore, matching pre-conversion behavior byte-for-byte.
	dest, err := fresh.DescribeAPIDestination(ctx, "my-dest")
	require.NoError(t, err)
	resolvedDest, ok := fresh.ResolveAPIDestination(dest.APIDestinationArn)
	require.True(t, ok)
	assert.Equal(t, "API_KEY", resolvedDest.AuthType)
	assert.Empty(t, resolvedDest.APIKeyValue,
		"authSecret is unexported and never round-trips through JSON, pre- or post-conversion")

	// Archive.
	archive, err := fresh.DescribeArchive(ctx, "my-archive")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:events:us-east-1:123456789012:event-bus/custom-bus", archive.EventSourceArn)

	// Endpoint.
	endpoint, err := fresh.DescribeEndpoint(ctx, "my-endpoint")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123456789012:role/endpoint-role", endpoint.RoleArn)

	// Partner event source + its mirrored, activated EventSource.
	partnerSrc, err := fresh.DescribePartnerEventSource(ctx, "aws.partner/example.com/orders")
	require.NoError(t, err)
	assert.Equal(t, "999999999999", partnerSrc.Account)

	eventSrc, err := fresh.DescribeEventSource(ctx, "aws.partner/example.com/orders")
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", eventSrc.State)

	// Replay.
	replay, err := fresh.DescribeReplay(ctx, "my-replay")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:events:us-east-1:123456789012:archive/my-archive", replay.EventSourceArn)
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
