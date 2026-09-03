package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestIndexPolicy_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_describe_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				p, err := b.PutIndexPolicy("/aws/lambda/fn", `{"fields":["@message"]}`)
				require.NoError(t, err)
				assert.Equal(t, "/aws/lambda/fn", p.LogGroupIdentifier)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies, _ := b.DescribeIndexPolicies([]string{"/aws/lambda/fn"}, "", 0)
				require.Len(t, policies, 1)
				assert.Equal(t, "/aws/lambda/fn", policies[0].LogGroupIdentifier)

				err := b.DeleteIndexPolicy("/aws/lambda/fn")
				require.NoError(t, err)

				emptied, _ := b.DescribeIndexPolicies([]string{"/aws/lambda/fn"}, "", 0)
				assert.Empty(t, emptied)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIndexPolicy("/grp", `{"old":"policy"}`)
				require.NoError(t, err)
				_, err = b.PutIndexPolicy("/grp", `{"new":"policy"}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies, _ := b.DescribeIndexPolicies([]string{"/grp"}, "", 0)
				require.Len(t, policies, 1)
				assert.JSONEq(t, `{"new":"policy"}`, policies[0].PolicyDocument)
			},
		},
		{
			name: "describe_sorted_by_identifier",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIndexPolicy("/z-grp", `{}`)
				require.NoError(t, err)
				_, err = b.PutIndexPolicy("/a-grp", `{}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies, _ := b.DescribeIndexPolicies([]string{"/z-grp", "/a-grp"}, "", 0)
				require.Len(t, policies, 2)
				assert.Equal(t, "/a-grp", policies[0].LogGroupIdentifier)
				assert.Equal(t, "/z-grp", policies[1].LogGroupIdentifier)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteIndexPolicy("nonexistent")
				require.ErrorIs(t, err, cloudwatchlogs.ErrIndexPolicyNotFound)
			},
		},
		{
			name: "put_empty_identifier_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIndexPolicy("", `{}`)
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
