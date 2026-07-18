package resourcegroupstaggingapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestGetTagKeys covers cursor-based pagination semantics: no token returns every key,
// a token matching a key resumes after it, a token matching the last key returns
// nothing more, and a token that matches nothing behaves like no token at all.
func TestGetTagKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", Tags: map[string]string{"alpha": "1", "beta": "2"}},
		{ResourceARN: "arn:2", Tags: map[string]string{"beta": "3", "gamma": "4"}},
		{ResourceARN: "arn:3", Tags: map[string]string{}},
	})

	tests := []struct {
		name     string
		token    *string
		wantKeys []string
	}{
		{name: "no_token_all_keys", wantKeys: []string{"alpha", "beta", "gamma"}},
		{name: "token_after_alpha", token: ptr("alpha"), wantKeys: []string{"beta", "gamma"}},
		{name: "token_after_beta", token: ptr("beta"), wantKeys: []string{"gamma"}},
		{name: "token_after_last", token: ptr("gamma"), wantKeys: nil},
		{name: "unmatched_token_returns_all", token: ptr("nonexistent"), wantKeys: []string{"alpha", "beta", "gamma"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := b.GetTagKeys(
				context.Background(),
				&resourcegroupstaggingapi.GetTagKeysInput{PaginationToken: tt.token},
			)
			require.NotNil(t, out)

			if len(tt.wantKeys) == 0 {
				assert.Empty(t, out.TagKeys)
			} else {
				assert.Equal(t, tt.wantKeys, out.TagKeys)
			}

			assert.Nil(t, out.PaginationToken)
		})
	}
}

// TestGetTagKeys_SimpleTwoResources verifies the common (unpaginated) case against a
// small two-resource dataset: keys from all resources are merged, sorted, and returned
// with a nil pagination token.
func TestGetTagKeys_SimpleTwoResources(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: map[string]string{"env": "prod", "team": "ops"}},
		{ResourceARN: "arn:2", ResourceType: "sqs:queue", Tags: map[string]string{"env": "dev", "owner": "alice"}},
	})

	out := b.GetTagKeys(context.Background(), &resourcegroupstaggingapi.GetTagKeysInput{})

	require.NotNil(t, out)
	assert.Equal(t, []string{"env", "owner", "team"}, out.TagKeys)
	assert.Nil(t, out.PaginationToken)
}

// TestGetTagKeys_ManyKeysNoSplit verifies that a key count below the default page size
// is returned as a single, unpaginated page.
func TestGetTagKeys_ManyKeysNoSplit(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	tags := make(map[string]string, 10)

	for _, k := range []string{"alpha", "beta", "delta", "epsilon", "gamma", "kappa", "omega", "sigma", "theta", "zeta"} {
		tags[k] = "v"
	}

	seedResources(b, []resourcegroupstaggingapi.TaggedResource{
		{ResourceARN: "arn:1", ResourceType: "sqs:queue", Tags: tags},
	})

	out := b.GetTagKeys(context.Background(), &resourcegroupstaggingapi.GetTagKeysInput{})

	require.NotNil(t, out)
	assert.Len(t, out.TagKeys, 10)
	assert.Nil(t, out.PaginationToken)
}

func TestGetTagKeys_Empty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out := b.GetTagKeys(context.Background(), &resourcegroupstaggingapi.GetTagKeysInput{})

	require.NotNil(t, out)
	assert.NotNil(t, out.TagKeys, "TagKeys must be non-nil empty slice")
	assert.Empty(t, out.TagKeys)
	assert.Nil(t, out.PaginationToken)
}
