package cloudwatchlogs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformer_CRUD(t *testing.T) {
	t.Parallel()

	baseProcessors := []map[string]any{
		{"parseJSON": map[string]any{}},
	}

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_get_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutTransformer("/aws/lambda/fn", baseProcessors)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tr, err := b.GetTransformer("/aws/lambda/fn")
				require.NoError(t, err)
				assert.Equal(t, "/aws/lambda/fn", tr.LogGroupIdentifier)
				require.Len(t, tr.Processors, 1)

				err = b.DeleteTransformer("/aws/lambda/fn")
				require.NoError(t, err)

				_, err = b.GetTransformer("/aws/lambda/fn")
				require.ErrorIs(t, err, cloudwatchlogs.ErrTransformerNotFound)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutTransformer("/grp", baseProcessors)
				require.NoError(t, err)
				two := []map[string]any{{"parseJSON": map[string]any{}}, {"addField": map[string]any{"key": "v"}}}
				err = b.PutTransformer("/grp", two)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tr, err := b.GetTransformer("/grp")
				require.NoError(t, err)
				assert.Len(t, tr.Processors, 2)
			},
		},
		{
			name: "get_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.GetTransformer("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrTransformerNotFound)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteTransformer("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrTransformerNotFound)
			},
		},
		{
			name: "put_empty_identifier_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutTransformer("", baseProcessors)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}
