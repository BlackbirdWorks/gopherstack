package cloudwatchlogs_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
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
				_, err := b.CreateLogGroup(t.Context(), "/aws/lambda/fn", "", "")
				require.NoError(t, err)
				err = b.PutTransformer(t.Context(), "/aws/lambda/fn", baseProcessors)
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
				_, err := b.CreateLogGroup(t.Context(), "/grp", "", "")
				require.NoError(t, err)
				err = b.PutTransformer(t.Context(), "/grp", baseProcessors)
				require.NoError(t, err)
				two := []map[string]any{{"parseJSON": map[string]any{}}, {"addField": map[string]any{"key": "v"}}}
				err = b.PutTransformer(t.Context(), "/grp", two)
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
				err := b.PutTransformer(t.Context(), "", baseProcessors)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
		{
			name: "put_nonexistent_log_group_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutTransformer(t.Context(), "/no/such/group", baseProcessors)
				require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupNotFound)
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

func TestPutTransformer_Validation(t *testing.T) {
	t.Parallel()

	one := []map[string]any{{"parseJSON": map[string]any{}}}
	twentyOne := make([]map[string]any, 21)

	for i := range twentyOne {
		twentyOne[i] = map[string]any{"parseJSON": map[string]any{}}
	}

	tests := []struct {
		wantErr    error
		name       string
		class      string
		processors []map[string]any
	}{
		{name: "ok", class: "STANDARD", processors: one},
		{name: "empty", class: "STANDARD", processors: nil, wantErr: cloudwatchlogs.ErrValidation},
		{name: "too_many", class: "STANDARD", processors: twentyOne, wantErr: cloudwatchlogs.ErrValidation},
		{
			name: "infrequent_access", class: cloudwatchlogs.LogGroupClassInfrequentAccess,
			processors: one, wantErr: cloudwatchlogs.ErrInvalidOperation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(t.Context(), "/g", tt.class, "")
			require.NoError(t, err)

			err = b.PutTransformer(t.Context(), "/g", tt.processors)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestTransformer_CreationTimeStableAndArnLookup(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := cloudwatchlogs.NewInMemoryBackend()
		_, err := b.CreateLogGroup(t.Context(), "/g", "", "")
		require.NoError(t, err)

		procs := []map[string]any{{"parseJSON": map[string]any{}}}
		require.NoError(t, b.PutTransformer(t.Context(), "/g", procs))

		first, err := b.GetTransformer("/g")
		require.NoError(t, err)

		time.Sleep(time.Minute)

		require.NoError(t, b.PutTransformer(t.Context(), "/g", procs))

		second, err := b.GetTransformer("arn:aws:logs:us-east-1:000000000000:log-group:/g:*")
		require.NoError(t, err)

		assert.Equal(t, first.CreatedAt, second.CreatedAt)
		assert.True(t, second.LastModifiedAt.After(first.LastModifiedAt))
	})
}
