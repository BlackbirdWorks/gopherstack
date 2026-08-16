package eventbridge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestEndpoint_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "primary-bus"})
	require.NoError(t, err)
	_, err = b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "secondary-bus"})
	require.NoError(t, err)

	primaryARN := "arn:aws:events:us-east-1:123456789012:event-bus/primary-bus"
	secondaryARN := "arn:aws:events:us-west-2:123456789012:event-bus/secondary-bus"

	ep, err := b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{
		Name: "my-endpoint",
		RoutingConfig: &eventbridge.RoutingConfig{
			FailoverConfig: &eventbridge.FailoverConfig{
				Primary:   &eventbridge.Primary{HealthCheck: "arn:aws:route53:::healthcheck/abc"},
				Secondary: &eventbridge.Secondary{Route: "us-west-2"},
			},
		},
		ReplicationConfig: &eventbridge.ReplicationConfig{State: "ENABLED"},
		EventBuses:        []eventbridge.EndpointEventBus{{EventBusArn: primaryARN}, {EventBusArn: secondaryARN}},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-endpoint", ep.Name)
	assert.Equal(t, "ACTIVE", ep.State)
	assert.NotEmpty(t, ep.EndpointURL)
	assert.Len(t, ep.EventBuses, 2)

	described, err := b.DescribeEndpoint(context.Background(), "my-endpoint")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", described.ReplicationConfig.State)

	updated, err := b.UpdateEndpoint(context.Background(), eventbridge.UpdateEndpointInput{
		Name:        "my-endpoint",
		Description: "updated endpoint",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated endpoint", updated.Description)

	eps, _, err := b.ListEndpoints(context.Background(), "my-", "")
	require.NoError(t, err)
	assert.Len(t, eps, 1)

	err = b.DeleteEndpoint(context.Background(), "my-endpoint")
	require.NoError(t, err)

	_, err = b.DescribeEndpoint(context.Background(), "my-endpoint")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestEndpointCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeEndpoint(context.Background(), "missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		require.ErrorIs(t, b.DeleteEndpoint(context.Background(), "missing"), eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{
			Name: "my-ep",
			EventBuses: []eventbridge.EndpointEventBus{
				{EventBusArn: "arn:aws:events:us-east-1:123:event-bus/default"},
			},
		})
		require.NoError(t, err)

		got, err := b.DescribeEndpoint(context.Background(), "my-ep")
		require.NoError(t, err)
		assert.Equal(t, "my-ep", got.Name)

		updated, err := b.UpdateEndpoint(context.Background(), eventbridge.UpdateEndpointInput{
			Name:        "my-ep",
			Description: "updated",
		})
		require.NoError(t, err)
		assert.Equal(t, "updated", updated.Description)

		eps, _, err := b.ListEndpoints(context.Background(), "my-", "")
		require.NoError(t, err)
		assert.Len(t, eps, 1)

		require.NoError(t, b.DeleteEndpoint(context.Background(), "my-ep"))
		_, err = b.DescribeEndpoint(context.Background(), "my-ep")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})
}

// TestCreateEndpoint_RequiresName verifies endpoint name validation.
func TestCreateEndpoint_RequiresName(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// TestCreateEndpoint_RoundTrip verifies endpoint is ACTIVE with non-nil EventBuses.
func TestCreateEndpoint_RoundTrip(t *testing.T) {
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

// TestCreateEndpoint_NonNilEventBuses verifies EventBuses is never nil.
func TestCreateEndpoint_NonNilEventBuses(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	ep, err := b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{Name: "e1"})
	require.NoError(t, err)
	assert.NotNil(t, ep.EventBuses)
}
