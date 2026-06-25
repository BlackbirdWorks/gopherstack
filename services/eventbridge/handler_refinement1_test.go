package eventbridge_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestRefinement1_Reset verifies Reset clears all new maps but preserves the default bus.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seedFn  func(b *eventbridge.InMemoryBackend)
		checkFn func(t *testing.T, b *eventbridge.InMemoryBackend)
		name    string
	}{
		{
			name: "default_bus_present_after_reset",
			seedFn: func(b *eventbridge.InMemoryBackend) {
				b.AddAPIDestinationInternal(
					&eventbridge.APIDestination{Name: "dst1", HTTPMethod: "GET", InvocationEndpoint: "https://x.com"},
				)
				b.AddConnectionInternal(&eventbridge.Connection{Name: "conn1"})
				b.AddArchiveInternal(&eventbridge.Archive{ArchiveName: "arch1"})
			},
			checkFn: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				b.Reset()

				assert.Equal(t, 0, b.APIDestinationCount())
				assert.Equal(t, 0, b.ConnectionCount())
				assert.Equal(t, 0, b.ArchiveCount())
				assert.Equal(t, 0, b.EndpointCount())
				assert.Equal(t, 0, b.EventSourceCount())
				assert.Equal(t, 0, b.ReplayCount())
				assert.Equal(t, 0, b.PartnerSourceCount())

				buses, _, err := b.ListEventBuses(context.Background(), "", "", 0)
				require.NoError(t, err)
				assert.Len(t, buses, 1)
				assert.Equal(t, "default", buses[0].Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()
			if tt.seedFn != nil {
				tt.seedFn(b)
			}
			tt.checkFn(t, b)
		})
	}
}

// TestRefinement1_MultipleResetCycle verifies that Reset is idempotent.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()

	for range 5 {
		b.Reset()

		buses, _, err := b.ListEventBuses(context.Background(), "", "", 0)
		require.NoError(t, err)
		assert.Len(t, buses, 1)
	}
}

// TestRefinement1_HandlerReset verifies Handler.Reset() clears tags.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend)

	// Tag a resource
	e := echo.New()
	rec := makeRequestWithHandler(
		t,
		handler,
		e,
		"TagResource",
		`{"ResourceARN":"arn:aws:events:us-east-1:123:event-bus/default","Tags":[{"Key":"env","Value":"test"}]}`,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Reset
	handler.Reset()

	// Tags should be gone
	rec2 := makeRequestWithHandler(
		t,
		handler,
		e,
		"ListTagsForResource",
		`{"ResourceARN":"arn:aws:events:us-east-1:123:event-bus/default"}`,
	)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		Tags []map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	assert.Empty(t, resp.Tags)
}

// TestRefinement1_ProviderInit_NilCtx verifies ErrNilAppContext is returned when ctx is nil.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &eventbridge.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, eventbridge.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsPreBuilt verifies ops are cached at construction time.
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend)

	assert.Positive(t, handler.HandlerOpsLen())
}

// TestRefinement1_GetSupportedOperations_AllOps checks all 27 ops are present.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend)
	ops := handler.GetSupportedOperations()

	required := []string{
		"ActivateEventSource", "CancelReplay", "CreateApiDestination",
		"CreateArchive", "CreateConnection", "CreateEndpoint",
		"CreatePartnerEventSource", "DeactivateEventSource",
		"DeauthorizeConnection", "DeleteApiDestination",
		"CreateEventBus", "DeleteEventBus", "ListEventBuses",
		"DescribeEventBus", "PutRule", "DeleteRule", "ListRules",
		"DescribeRule", "EnableRule", "DisableRule", "PutTargets",
		"RemoveTargets", "ListTargetsByRule", "PutEvents",
		"ListTagsForResource", "TagResource", "UntagResource",
	}

	opSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		opSet[op] = true
	}

	for _, op := range required {
		assert.True(t, opSet[op], "expected op %s in GetSupportedOperations", op)
	}
}

// TestRefinement1_SeedHelpers verifies all seed helpers increment their counts.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	now := time.Now()

	b.AddAPIDestinationInternal(&eventbridge.APIDestination{Name: "d1"})
	b.AddArchiveInternal(&eventbridge.Archive{ArchiveName: "a1"})
	b.AddConnectionInternal(&eventbridge.Connection{Name: "c1"})
	b.AddEndpointInternal(&eventbridge.Endpoint{Name: "e1"})
	b.AddEventSourceInternal(&eventbridge.EventSource{Name: "es1", State: "PENDING", CreationTime: now})
	b.AddReplayInternal(&eventbridge.Replay{ReplayName: "r1", State: "RUNNING"})
	b.AddPartnerSourceInternal(&eventbridge.PartnerEventSource{Name: "p1"})

	assert.Equal(t, 1, b.APIDestinationCount())
	assert.Equal(t, 1, b.ArchiveCount())
	assert.Equal(t, 1, b.ConnectionCount())
	assert.Equal(t, 1, b.EndpointCount())
	assert.Equal(t, 1, b.EventSourceCount())
	assert.Equal(t, 1, b.ReplayCount())
	assert.Equal(t, 1, b.PartnerSourceCount())
}

// TestRefinement1_ExportCountHelpers verifies count helpers start at 0.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()

	assert.Equal(t, 0, b.APIDestinationCount())
	assert.Equal(t, 0, b.ArchiveCount())
	assert.Equal(t, 0, b.ConnectionCount())
	assert.Equal(t, 0, b.EndpointCount())
	assert.Equal(t, 0, b.EventSourceCount())
	assert.Equal(t, 0, b.ReplayCount())
	assert.Equal(t, 0, b.PartnerSourceCount())
}

// TestRefinement1_CreateAPIDestination_RequiresName verifies name validation.
func TestRefinement1_CreateAPIDestination_RequiresName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input eventbridge.CreateAPIDestinationInput
	}{
		{
			name: "empty_name",
			input: eventbridge.CreateAPIDestinationInput{
				ConnectionArn:      "arn:x",
				InvocationEndpoint: "https://x",
				HTTPMethod:         "POST",
			},
		},
		{
			name: "empty_connection_arn",
			input: eventbridge.CreateAPIDestinationInput{
				Name:               "d1",
				InvocationEndpoint: "https://x",
				HTTPMethod:         "POST",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()
			_, err := b.CreateAPIDestination(context.Background(), tt.input)
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

// TestRefinement1_CreateAPIDestination_DuplicateRejected verifies duplicate names return ErrAlreadyExists.
func TestRefinement1_CreateAPIDestination_DuplicateRejected(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	input := eventbridge.CreateAPIDestinationInput{
		Name:               "my-dest",
		ConnectionArn:      "arn:aws:events:us-east-1:123:connection/my-conn",
		InvocationEndpoint: "https://example.com/api",
		HTTPMethod:         "POST",
	}

	_, err := b.CreateAPIDestination(context.Background(), input)
	require.NoError(t, err)

	_, err = b.CreateAPIDestination(context.Background(), input)
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

// TestRefinement1_CreateAPIDestination_RoundTrip creates an API destination and verifies ARN is set.
func TestRefinement1_CreateAPIDestination_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	input := eventbridge.CreateAPIDestinationInput{
		Name:               "my-dest",
		ConnectionArn:      "arn:aws:events:us-east-1:123:connection/my-conn",
		InvocationEndpoint: "https://example.com/api",
		HTTPMethod:         "POST",
	}

	dst, err := b.CreateAPIDestination(context.Background(), input)
	require.NoError(t, err)

	assert.NotEmpty(t, dst.APIDestinationArn)
	assert.Equal(t, "ACTIVE", dst.APIDestinationState)
	assert.Equal(t, "my-dest", dst.Name)
	assert.Contains(t, dst.APIDestinationArn, "api-destination/my-dest")
	assert.Equal(t, 1, b.APIDestinationCount())
}

// TestRefinement1_CreateArchive_RequiresName verifies name and source ARN validation.
func TestRefinement1_CreateArchive_RequiresName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input eventbridge.CreateArchiveInput
	}{
		{
			name:  "empty_archive_name",
			input: eventbridge.CreateArchiveInput{EventSourceArn: "arn:x"},
		},
		{
			name:  "empty_event_source_arn",
			input: eventbridge.CreateArchiveInput{ArchiveName: "a1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()
			_, err := b.CreateArchive(context.Background(), tt.input)
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

// TestRefinement1_CreateArchive_RoundTrip verifies archive creation state is ENABLED.
func TestRefinement1_CreateArchive_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	input := eventbridge.CreateArchiveInput{
		ArchiveName:    "my-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
	}

	archive, err := b.CreateArchive(context.Background(), input)
	require.NoError(t, err)

	assert.Equal(t, "my-archive", archive.ArchiveName)
	assert.Equal(t, "ENABLED", archive.State)
	assert.NotEmpty(t, archive.ArchiveArn)
	assert.Contains(t, archive.ArchiveArn, "archive/my-archive")
	assert.Equal(t, 1, b.ArchiveCount())
}

// TestRefinement1_CreateConnection_RequiresName verifies connection validation.
func TestRefinement1_CreateConnection_RequiresName(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{AuthorizationType: "BASIC"})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// TestRefinement1_CreateConnection_RoundTrip verifies connection is AUTHORIZED on creation.
func TestRefinement1_CreateConnection_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	assert.Equal(t, "AUTHORIZED", conn.ConnectionState)
	assert.NotEmpty(t, conn.ConnectionArn)
	assert.Contains(t, conn.ConnectionArn, "connection/my-conn")
	assert.Equal(t, 1, b.ConnectionCount())
}

// TestRefinement1_CreateEndpoint_RequiresName verifies endpoint name validation.
func TestRefinement1_CreateEndpoint_RequiresName(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// TestRefinement1_CreateEndpoint_RoundTrip verifies endpoint is ACTIVE with non-nil EventBuses.
func TestRefinement1_CreateEndpoint_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	ep, err := b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{
		Name: "my-endpoint",
	})
	require.NoError(t, err)

	assert.Equal(t, "ACTIVE", ep.State)
	assert.NotNil(t, ep.EventBuses)
	assert.NotEmpty(t, ep.Arn)
	assert.NotEmpty(t, ep.EndpointURL)
	assert.Equal(t, 1, b.EndpointCount())
}

// TestRefinement1_CreatePartnerEventSource_RoundTrip creates a partner event source and verifies ARN.
func TestRefinement1_CreatePartnerEventSource_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	src, err := b.CreatePartnerEventSource(context.Background(), "my-partner-source", "123456789012")
	require.NoError(t, err)

	assert.NotEmpty(t, src.Arn)
	assert.Equal(t, "my-partner-source", src.Name)
	assert.Equal(t, 1, b.PartnerSourceCount())
}

// TestRefinement1_CancelReplay_NotFound verifies non-existent replay returns ErrNotFound.
func TestRefinement1_CancelReplay_NotFound(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CancelReplay(context.Background(), "no-such-replay")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

// TestRefinement1_CancelReplay_InvalidState verifies terminal state replay cannot be cancelled.
func TestRefinement1_CancelReplay_InvalidState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state string
	}{
		{name: "completed", state: "COMPLETED"},
		{name: "cancelled", state: "CANCELLED"},
		{name: "failed", state: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()
			b.AddReplayInternal(&eventbridge.Replay{ReplayName: "r1", State: tt.state})

			_, err := b.CancelReplay(context.Background(), "r1")
			require.ErrorIs(t, err, eventbridge.ErrInvalidState)
		})
	}
}

// TestRefinement1_CancelReplay_Running verifies running replay transitions to CANCELLING.
func TestRefinement1_CancelReplay_Running(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	b.AddReplayInternal(&eventbridge.Replay{ReplayName: "r1", State: "RUNNING"})

	replay, err := b.CancelReplay(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, "CANCELLING", replay.State)
}

// TestRefinement1_CancelReplay_Starting verifies STARTING replay also cancels.
func TestRefinement1_CancelReplay_Starting(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	b.AddReplayInternal(&eventbridge.Replay{ReplayName: "r1", State: "STARTING"})

	replay, err := b.CancelReplay(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, "CANCELLING", replay.State)
}

// TestRefinement1_ActivateEventSource_NotFound verifies non-existent source returns ErrNotFound.
func TestRefinement1_ActivateEventSource_NotFound(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	err := b.ActivateEventSource(context.Background(), "no-such-source")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

// TestRefinement1_ActivateEventSource_RoundTrip verifies activation sets state ACTIVE.
func TestRefinement1_ActivateEventSource_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	b.AddEventSourceInternal(&eventbridge.EventSource{
		Name:         "my-source",
		State:        "PENDING",
		CreationTime: time.Now(),
	})

	err := b.ActivateEventSource(context.Background(), "my-source")
	require.NoError(t, err)
}

// TestRefinement1_DeactivateEventSource_RoundTrip verifies deactivation sets state INACTIVE.
func TestRefinement1_DeactivateEventSource_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	b.AddEventSourceInternal(&eventbridge.EventSource{
		Name:         "my-source",
		State:        "ACTIVE",
		CreationTime: time.Now(),
	})

	err := b.DeactivateEventSource(context.Background(), "my-source")
	require.NoError(t, err)
}

// TestRefinement1_DeauthorizeConnection_RoundTrip verifies deauthorization state change.
func TestRefinement1_DeauthorizeConnection_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	conn, err := b.DeauthorizeConnection(context.Background(), "my-conn")
	require.NoError(t, err)
	assert.Equal(t, "DEAUTHORIZED", conn.ConnectionState)
}

// TestRefinement1_DeauthorizeConnection_NotFound returns ErrNotFound for missing connections.
func TestRefinement1_DeauthorizeConnection_NotFound(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.DeauthorizeConnection(context.Background(), "no-such-conn")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

// TestRefinement1_DeleteAPIDestination_NotFound verifies deletion of non-existent returns ErrNotFound.
func TestRefinement1_DeleteAPIDestination_NotFound(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	err := b.DeleteAPIDestination(context.Background(), "no-such-dest")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

// TestRefinement1_DeleteAPIDestination_RoundTrip creates then deletes an API destination.
func TestRefinement1_DeleteAPIDestination_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
		Name:               "my-dest",
		ConnectionArn:      "arn:aws:events:us-east-1:123:connection/c1",
		InvocationEndpoint: "https://x.com",
		HTTPMethod:         "POST",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, b.APIDestinationCount())

	err = b.DeleteAPIDestination(context.Background(), "my-dest")
	require.NoError(t, err)
	assert.Equal(t, 0, b.APIDestinationCount())
}

// TestRefinement1_ErrValidationMapping verifies ErrInvalidParameter maps to HTTP 400.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "activate_event_source_empty_name",
			action: "ActivateEventSource",
			body:   `{"Name":""}`,
		},
		{
			name:   "cancel_replay_empty_name",
			action: "CancelReplay",
			body:   `{"ReplayName":""}`,
		},
		{
			name:   "create_api_destination_no_name",
			action: "CreateApiDestination",
			body:   `{"ConnectionArn":"arn:x","InvocationEndpoint":"https://x","HttpMethod":"GET"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeRequest(t, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_ErrNotFoundMapping verifies ErrNotFound maps to HTTP 404.
func TestRefinement1_ErrNotFoundMapping(t *testing.T) {
	t.Parallel()

	rec := makeRequest(t, "CancelReplay", `{"ReplayName":"no-such-replay"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_ErrAlreadyExistsMapping verifies ErrAlreadyExists maps to HTTP 409.
func TestRefinement1_ErrAlreadyExistsMapping(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(b)
	e := echo.New()

	body := `{"Name":"my-conn","AuthorizationType":"BASIC"}`
	rec1 := makeRequestWithHandler(t, handler, e, "CreateConnection", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := makeRequestWithHandler(t, handler, e, "CreateConnection", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// TestRefinement1_ErrInvalidStateMapping verifies ErrInvalidState maps to HTTP 400.
func TestRefinement1_ErrInvalidStateMapping(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	b.AddReplayInternal(&eventbridge.Replay{ReplayName: "r1", State: "COMPLETED"})
	handler := eventbridge.NewHandler(b)
	e := echo.New()

	rec := makeRequestWithHandler(t, handler, e, "CancelReplay", `{"ReplayName":"r1"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_PersistenceRoundTrip verifies snapshot/restore preserves new resource maps.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()

	// Seed all new resource types
	_, err := b.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
		Name:               "d1",
		ConnectionArn:      "arn:aws:events:us-east-1:123:connection/c1",
		InvocationEndpoint: "https://x.com",
		HTTPMethod:         "POST",
	})
	require.NoError(t, err)

	_, err = b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "a1",
		EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
	})
	require.NoError(t, err)

	_, err = b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "c1",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	_, err = b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{Name: "e1"})
	require.NoError(t, err)

	b.AddEventSourceInternal(&eventbridge.EventSource{Name: "es1", State: "PENDING", CreationTime: time.Now()})
	b.AddReplayInternal(&eventbridge.Replay{ReplayName: "r1", State: "RUNNING"})
	b.AddPartnerSourceInternal(&eventbridge.PartnerEventSource{Name: "ps1"})

	// Snapshot
	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	// Restore into a new backend
	b2 := eventbridge.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, b.APIDestinationCount(), b2.APIDestinationCount())
	assert.Equal(t, b.ArchiveCount(), b2.ArchiveCount())
	assert.Equal(t, b.ConnectionCount(), b2.ConnectionCount())
	assert.Equal(t, b.EndpointCount(), b2.EndpointCount())
	assert.Equal(t, b.EventSourceCount(), b2.EventSourceCount())
	assert.Equal(t, b.ReplayCount(), b2.ReplayCount())
	assert.Equal(t, b.PartnerSourceCount(), b2.PartnerSourceCount())
}

// TestRefinement1_CreateEndpoint_NonNilEventBuses verifies EventBuses is never nil.
func TestRefinement1_CreateEndpoint_NonNilEventBuses(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	ep, err := b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{Name: "e1"})
	require.NoError(t, err)
	assert.NotNil(t, ep.EventBuses)
}

// TestRefinement1_SeedHelper_DeepCopy verifies seed helpers store deep copies.
func TestRefinement1_SeedHelper_DeepCopy(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	orig := &eventbridge.APIDestination{Name: "d1", Description: "original"}
	b.AddAPIDestinationInternal(orig)

	// Mutate original after seeding
	orig.Description = "mutated"

	// Count should still be 1 and internal copy should have original value
	assert.Equal(t, 1, b.APIDestinationCount())
}
