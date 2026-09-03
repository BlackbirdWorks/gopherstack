package dax_test

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- Tags ----

func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *dax.InMemoryBackend) string
		tags    map[string]string
		check   func(t *testing.T, tags map[string]string)
		name    string
		wantErr bool
	}{
		{
			name: "tag cluster ARN",
			setup: func(b *dax.InMemoryBackend) string {
				c, _ := b.CreateCluster(validCreateInput("tagged2"))

				return c.ClusterArn
			},
			tags: map[string]string{"k1": "v1", "k2": "v2"},
			check: func(t *testing.T, tags map[string]string) {
				t.Helper()
				assert.Equal(t, "v1", tags["k1"])
				assert.Equal(t, "v2", tags["k2"])
			},
		},
		{
			// Real DAX only assigns ARNs to clusters -- types.ParameterGroup has no
			// Arn field in the SDK, so a parameter-group "ARN" never resolves to a
			// taggable resource and TagResource must reject it as not found.
			name: "parameter group ARN is not taggable",
			setup: func(b *dax.InMemoryBackend) string {
				_, _ = b.CreateParameterGroup("my-pg", "")

				return "arn:aws:dax:us-east-1:123456789012:parametergroup/my-pg"
			},
			tags:    map[string]string{"k": "v"},
			wantErr: true,
		},
		{
			// Same rationale: types.SubnetGroup has no Arn field in the real SDK.
			name: "subnet group ARN is not taggable",
			setup: func(b *dax.InMemoryBackend) string {
				_, _ = b.CreateSubnetGroup("my-sg", "", []string{"subnet-11111111"})

				return "arn:aws:dax:us-east-1:123456789012:subnetgroup/my-sg"
			},
			tags:    map[string]string{"k": "v"},
			wantErr: true,
		},
		{
			name: "not found",
			setup: func(_ *dax.InMemoryBackend) string {
				return "arn:aws:dax:us-east-1:123456789012:cache/no-such"
			},
			tags:    map[string]string{"k": "v"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			arn := tt.setup(b)

			_, err := b.TagResource(arn, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			tags, _, err := b.ListTags(arn, "")
			require.NoError(t, err)
			tt.check(t, tags)
		})
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cluster, err := b.CreateCluster(validCreateInput("untag-test"))
	require.NoError(t, err)

	_, err = b.TagResource(cluster.ClusterArn, map[string]string{"k1": "v1", "k2": "v2"})
	require.NoError(t, err)

	_, err = b.UntagResource(cluster.ClusterArn, []string{"k1"})
	require.NoError(t, err)

	tags, _, err := b.ListTags(cluster.ClusterArn, "")
	require.NoError(t, err)
	_, hasK1 := tags["k1"]
	assert.False(t, hasK1)
	assert.Equal(t, "v2", tags["k2"])
}

// ---- ListTags pagination ----

func TestListTagsPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	// Create cluster with many tags.
	input := validCreateInput("tagged-cluster")
	input.Tags = make(map[string]string, 15)
	for i := range 15 {
		input.Tags[string([]byte{'a' + byte(i)})+"-key"] = "val"
	}

	_, err := b.CreateCluster(input)
	require.NoError(t, err)

	clusterARN := "arn:aws:dax:us-east-1:123456789012:cache/tagged-cluster"

	// First page (page size = 10).
	page1, tok1, err := b.ListTags(clusterARN, "")
	require.NoError(t, err)
	assert.Len(t, page1, 10)
	assert.NotEmpty(t, tok1)

	// Second page.
	page2, tok2, err := b.ListTags(clusterARN, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 5)
	assert.Empty(t, tok2)

	// All tags accounted for, no duplicates.
	all := make(map[string]string)
	maps.Copy(all, page1)

	for k, v := range page2 {
		assert.NotContains(t, all, k, "duplicate key %s", k)
		all[k] = v
	}

	assert.Len(t, all, 15)
}

// TestListTagsPagination_StaleCursorAfterUntag demonstrates that when the
// tag key named by nextToken has since been removed via UntagResource, the
// next page must resume at the first remaining key after the cursor's sort
// position -- not silently restart pagination from the beginning, which
// would hand the caller tag keys it already consumed on the prior page.
func TestListTagsPagination_StaleCursorAfterUntag(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	input := validCreateInput("tagged-cluster-2")
	input.Tags = make(map[string]string, 15)
	for i := range 15 {
		input.Tags[string([]byte{'a' + byte(i)})+"-key"] = "val"
	}

	_, err := b.CreateCluster(input)
	require.NoError(t, err)

	clusterARN := "arn:aws:dax:us-east-1:123456789012:cache/tagged-cluster-2"

	page1, tok1, err := b.ListTags(clusterARN, "")
	require.NoError(t, err)
	require.Len(t, page1, 10)
	require.NotEmpty(t, tok1)

	// The cursor names the first key of page2; remove it before fetching page2.
	_, err = b.UntagResource(clusterARN, []string{tok1})
	require.NoError(t, err)

	page2, _, err := b.ListTags(clusterARN, tok1)
	require.NoError(t, err)

	for k := range page1 {
		if k == tok1 {
			continue
		}

		_, dup := page2[k]
		assert.False(t, dup, "page2 must not repeat key %q already returned in page1", k)
	}
}
