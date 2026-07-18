package cloudwatchlogs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend(t *testing.T) *cloudwatchlogs.InMemoryBackend {
	t.Helper()
	b := cloudwatchlogs.NewInMemoryBackend()
	t.Cleanup(func() { b.Close() })

	return b
}

func TestResourcePolicy_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_and_describe",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				p, err := b.PutResourcePolicy("my-policy", `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
				assert.Equal(t, "my-policy", p.PolicyName)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies()
				require.Len(t, policies, 1)
				assert.Equal(t, "my-policy", policies[0].PolicyName)
				assert.JSONEq(t, `{"Version":"2012-10-17"}`, policies[0].PolicyDocument)
			},
		},
		{
			name: "put_multiple_sorted",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("z-policy", `{}`)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy("a-policy", `{}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies()
				require.Len(t, policies, 2)
				assert.Equal(t, "a-policy", policies[0].PolicyName)
				assert.Equal(t, "z-policy", policies[1].PolicyName)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("my-policy", `{"old":"doc"}`)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy("my-policy", `{"new":"doc"}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies()
				require.Len(t, policies, 1)
				assert.JSONEq(t, `{"new":"doc"}`, policies[0].PolicyDocument)
			},
		},
		{
			name: "delete_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("del-policy", `{}`)
				require.NoError(t, err)
				err = b.DeleteResourcePolicy("del-policy")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.Empty(t, b.DescribeResourcePolicies())
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteResourcePolicy("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrResourcePolicyNotFound)
			},
		},
		{
			name: "empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("", `{}`)
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
