package eventbridge_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestDiscoverer_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *eventbridge.InMemoryBackend)
		name string
	}{
		{
			name: "create missing sourcearn",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{})
				require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
			},
		},
		{
			name: "describe not found",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.DescribeDiscoverer(context.Background(), "missing")
				require.ErrorIs(t, err, eventbridge.ErrNotFound)
			},
		},
		{
			name: "update not found",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.UpdateDiscoverer(context.Background(), eventbridge.UpdateDiscovererInput{
					DiscovererID: "missing",
				})
				require.ErrorIs(t, err, eventbridge.ErrNotFound)
			},
		},
		{
			name: "delete not found",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				err := b.DeleteDiscoverer(context.Background(), "missing")
				require.ErrorIs(t, err, eventbridge.ErrNotFound)
			},
		},
		{
			name: "start not found",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.StartDiscoverer(context.Background(), "missing")
				require.ErrorIs(t, err, eventbridge.ErrNotFound)
			},
		},
		{
			name: "stop not found",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.StopDiscoverer(context.Background(), "missing")
				require.ErrorIs(t, err, eventbridge.ErrNotFound)
			},
		},
		{
			name: "duplicate source arn",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{
					SourceArn: "arn:aws:events:us-east-1:000000000000:event-bus/dup",
				})
				require.NoError(t, err)

				_, err = b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{
					SourceArn: "arn:aws:events:us-east-1:000000000000:event-bus/dup",
				})
				require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newBackend())
		})
	}
}

func TestDiscoverer_ListFiltering(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{
		SourceArn: "arn:aws:events:us-east-1:000000000000:event-bus/alpha",
	})
	require.NoError(t, err)

	_, err = b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{
		SourceArn: "arn:aws:events:us-east-1:000000000000:event-bus/beta",
	})
	require.NoError(t, err)

	all, _, err := b.ListDiscoverers(context.Background(), "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, all, 2)

	filtered, _, err := b.ListDiscoverers(context.Background(), "alp", "", "", 0)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, "alpha", filtered[0].DiscovererID)

	bySource, _, err := b.ListDiscoverers(
		context.Background(), "", "arn:aws:events:us-east-1:000000000000:event-bus/beta", "", 0,
	)
	require.NoError(t, err)
	require.Len(t, bySource, 1)
	assert.Equal(t, "beta", bySource[0].DiscovererID)
}

func TestDiscoverer_CrossAccountDefault(t *testing.T) {
	t.Parallel()
	b := newBackend()

	created, err := b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{
		SourceArn: "arn:aws:events:us-east-1:000000000000:event-bus/default-cross",
	})
	require.NoError(t, err)
	assert.True(t, created.CrossAccount)

	explicit, err := b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{
		SourceArn:    "arn:aws:events:us-east-1:000000000000:event-bus/explicit-cross",
		CrossAccount: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.False(t, explicit.CrossAccount)
}

func TestPutEvents_SchemaDiscovery(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "disc-bus"})
	require.NoError(t, err)

	busDescribed, err := b.DescribeEventBus(context.Background(), "disc-bus")
	require.NoError(t, err)

	_, err = b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{
		SourceArn: busDescribed.Arn,
	})
	require.NoError(t, err)

	_, err = b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "my.app", DetailType: "Order Created", Detail: `{"id":1}`, EventBusName: "disc-bus"},
	})
	require.NoError(t, err)

	schemas, _, err := b.ListSchemas(context.Background(), "discovered-schemas", "", "", 0)
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	assert.Contains(t, schemas[0].SchemaName, "disc-bus")
	assert.Contains(t, schemas[0].SchemaName, "my.app")
}

func TestPutEvents_SchemaDiscovery_StoppedDiscovererIsNoop(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "disc-bus-stopped"})
	require.NoError(t, err)

	busDescribed, err := b.DescribeEventBus(context.Background(), "disc-bus-stopped")
	require.NoError(t, err)

	created, err := b.CreateDiscoverer(context.Background(), eventbridge.CreateDiscovererInput{
		SourceArn: busDescribed.Arn,
	})
	require.NoError(t, err)

	_, err = b.StopDiscoverer(context.Background(), created.DiscovererID)
	require.NoError(t, err)

	_, err = b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "my.app", DetailType: "Order Created", Detail: `{"id":1}`, EventBusName: "disc-bus-stopped"},
	})
	require.NoError(t, err)

	_, _, err = b.ListSchemas(context.Background(), "discovered-schemas", "", "", 0)
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}
