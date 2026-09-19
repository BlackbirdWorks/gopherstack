package eventbridge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestResourcePolicy_Lifecycle(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.GetResourcePolicy(context.Background(), "my-registry")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)

	put, err := b.PutResourcePolicy(context.Background(), eventbridge.PutResourcePolicyInput{
		RegistryName: "my-registry",
		Policy:       `{"Version":"2012-10-17"}`,
	})
	require.NoError(t, err)
	assert.Equal(t, "1", put.RevisionID)

	updated, err := b.PutResourcePolicy(context.Background(), eventbridge.PutResourcePolicyInput{
		RegistryName: "my-registry",
		Policy:       `{"Version":"2012-10-18"}`,
		RevisionID:   put.RevisionID,
	})
	require.NoError(t, err)
	assert.Equal(t, "2", updated.RevisionID)

	err = b.DeleteResourcePolicy(context.Background(), "my-registry")
	require.NoError(t, err)

	_, err = b.GetResourcePolicy(context.Background(), "my-registry")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestResourcePolicy_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *eventbridge.InMemoryBackend)
		name string
	}{
		{
			name: "put missing policy",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.PutResourcePolicy(context.Background(), eventbridge.PutResourcePolicyInput{
					RegistryName: "r",
				})
				require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
			},
		},
		{
			name: "put stale revision",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.PutResourcePolicy(context.Background(), eventbridge.PutResourcePolicyInput{
					RegistryName: "r",
					Policy:       "{}",
				})
				require.NoError(t, err)

				_, err = b.PutResourcePolicy(context.Background(), eventbridge.PutResourcePolicyInput{
					RegistryName: "r",
					Policy:       "{}",
					RevisionID:   "999",
				})
				require.ErrorIs(t, err, eventbridge.ErrPreconditionFailed)
			},
		},
		{
			name: "delete not found",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				err := b.DeleteResourcePolicy(context.Background(), "missing")
				require.ErrorIs(t, err, eventbridge.ErrNotFound)
			},
		},
		{
			name: "default registry name",
			run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
				t.Helper()

				_, err := b.PutResourcePolicy(context.Background(), eventbridge.PutResourcePolicyInput{
					Policy: "{}",
				})
				require.NoError(t, err)

				_, err = b.GetResourcePolicy(context.Background(), "")
				require.NoError(t, err)
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
