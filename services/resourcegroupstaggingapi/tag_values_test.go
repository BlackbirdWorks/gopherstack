package resourcegroupstaggingapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestGetTagValues(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.RegisterProvider(func(_ context.Context) []resourcegroupstaggingapi.TaggedResource {
		return []resourcegroupstaggingapi.TaggedResource{
			{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod"}},
			{ResourceARN: "arn:2", ResourceType: "sqs:queue", Tags: map[string]string{"env": "dev"}},
			{ResourceARN: "arn:3", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod"}},
		}
	})

	envKey := "env"
	out, err := b.GetTagValues(context.Background(), &resourcegroupstaggingapi.GetTagValuesInput{Key: &envKey})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, []string{"dev", "prod"}, out.TagValues)
}

// TestGetTagValues_Pagination covers cursor-based pagination semantics and nil/missing
// Key edge cases, table-driven over a shared seeded backend.
func TestGetTagValues_Pagination(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"env": "dev", "region": "us-east-1"}},
		{ResourceARN: "arn:2", Tags: map[string]string{"env": "staging"}},
		{ResourceARN: "arn:3", Tags: map[string]string{"env": "prod"}},
		{ResourceARN: "arn:4", Tags: map[string]string{"other": "value"}},
	})

	tests := []struct {
		name       string
		key        *string
		token      *string
		wantValues []string
	}{
		{
			name:       "all_env_values",
			key:        ptr("env"),
			wantValues: []string{"dev", "prod", "staging"},
		},
		{
			name:       "token_after_dev",
			key:        ptr("env"),
			token:      ptr("dev"),
			wantValues: []string{"prod", "staging"},
		},
		{
			name:       "nil_key_returns_empty",
			key:        nil,
			wantValues: nil,
		},
		{
			name:       "key_with_no_resources",
			key:        ptr("nonexistent"),
			wantValues: nil,
		},
		{
			name:       "region_key",
			key:        ptr("region"),
			wantValues: []string{"us-east-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetTagValues(context.Background(), &resourcegroupstaggingapi.GetTagValuesInput{
				Key:             tt.key,
				PaginationToken: tt.token,
			})

			require.NoError(t, err)
			require.NotNil(t, out)

			if len(tt.wantValues) == 0 {
				assert.Empty(t, out.TagValues)
			} else {
				assert.Equal(t, tt.wantValues, out.TagValues)
			}
		})
	}
}

// TestGetTagValues_UnmatchedTokenExpired verifies that a PaginationToken that does not
// correspond to any current value for the given key returns
// PaginationTokenExpiredException, matching real AWS's documented behavior for an
// unresolvable pagination token.
func TestGetTagValues_UnmatchedTokenExpired(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"env": "dev"}},
	})

	key := "env"
	out, err := b.GetTagValues(context.Background(), &resourcegroupstaggingapi.GetTagValuesInput{
		Key:             &key,
		PaginationToken: ptr("nonexistent"),
	})

	require.ErrorIs(t, err, resourcegroupstaggingapi.ErrPaginationTokenExpired)
	assert.Nil(t, out)
}

func TestGetTagValues_Sorted(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	envKey := "env"
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"env": "staging"}},
		{ResourceARN: "arn:2", Tags: map[string]string{"env": "prod"}},
		{ResourceARN: "arn:3", Tags: map[string]string{"env": "dev"}},
	})

	out, err := b.GetTagValues(context.Background(), &resourcegroupstaggingapi.GetTagValuesInput{Key: &envKey})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, []string{"dev", "prod", "staging"}, out.TagValues)
}

func TestGetTagValues_TokenResumption(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "dev"}},
		{ResourceARN: "arn:2", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod"}},
		{ResourceARN: "arn:3", ResourceType: "sqs:queue", Tags: map[string]string{"env": "staging"}},
	})

	tok := "prod"
	key := "env"
	out, err := b.GetTagValues(
		context.Background(),
		&resourcegroupstaggingapi.GetTagValuesInput{Key: &key, PaginationToken: &tok},
	)

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, []string{"staging"}, out.TagValues)
}

// TestGetTagValues_Empty verifies that GetTagValues against a backend with no resources
// at all returns an empty (not nil-panicking) result, distinct from a key that simply
// has no matches in a populated backend (see TestGetTagValues_Pagination's
// key_with_no_resources case).
func TestGetTagValues_Empty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	key := "env"

	out, err := b.GetTagValues(context.Background(), &resourcegroupstaggingapi.GetTagValuesInput{Key: &key})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.NotNil(t, out.TagValues)
	assert.Empty(t, out.TagValues)
}
