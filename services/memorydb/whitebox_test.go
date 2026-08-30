package memorydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTaggableCluster(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	return b.AddClusterInternal("tag-cluster", "db.r6g.large").ARN
}

func newTaggableACL(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	a, err := b.CreateACL(context.Background(), &createACLRequest{ACLName: "tag-acl"})
	require.NoError(t, err)

	return a.ARN
}

func newTaggableSubnetGroup(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	sg, err := b.CreateSubnetGroup(context.Background(), &createSubnetGroupRequest{SubnetGroupName: "tag-sg"})
	require.NoError(t, err)

	return sg.ARN
}

func newTaggableUser(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	u, err := b.CreateUser(context.Background(), &createUserRequest{
		UserName:     "tag-user",
		AccessString: "on ~* &* +@all",
		AuthenticationMode: authenticationModeReq{
			Type:      "password",
			Passwords: []string{"mypassword"},
		},
	})
	require.NoError(t, err)

	return u.ARN
}

func newTaggableParameterGroup(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	pg, err := b.CreateParameterGroup(context.Background(), &createParameterGroupRequest{
		ParameterGroupName: "tag-pg",
		Family:             "memorydb_redis7",
	})
	require.NoError(t, err)

	return pg.ARN
}

func newTaggableSnapshot(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	b.AddClusterInternal("tag-snap-cluster", "db.r6g.large")

	s, err := b.CreateSnapshot(context.Background(), &createSnapshotRequest{
		SnapshotName: "tag-snap",
		ClusterName:  "tag-snap-cluster",
	})
	require.NoError(t, err)

	return s.ARN
}

func newTaggableMultiRegionCluster(t *testing.T, b *InMemoryBackend) string {
	t.Helper()

	mrc, err := b.CreateMultiRegionCluster(context.Background(), &createMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "tag-mrc",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	return mrc.ARN
}

// registeredResourceKinds returns the distinct resourceKind values actually
// present in b.arnToResource, across every region -- the real tag-routing
// registry, not a hand-typed copy of it.
func registeredResourceKinds(b *InMemoryBackend) map[string]bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	kinds := make(map[string]bool)

	for _, store := range b.arnToResource {
		for _, ref := range store {
			kinds[ref.Kind] = true
		}
	}

	return kinds
}

// TestTagResource_AllRegisteredResourceKinds tags a resource of every kind
// registered in arnToResource (store.go) and reads the tags back, so a kind
// missing from tags.go's tagsForRef/applyTags/tagsMapForRef switches fails
// here instead of silently discarding tags (gopherstack-hi5t). The expected
// set is read back from the live registry rather than hand-typed, so a kind
// that gets registered into arnToResource without a matching row below also
// fails this test.
func TestTagResource_AllRegisteredResourceKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *InMemoryBackend) string
		name  string
	}{
		{name: resourceKindCluster, setup: newTaggableCluster},
		{name: resourceKindACL, setup: newTaggableACL},
		{name: resourceKindSubnetGroup, setup: newTaggableSubnetGroup},
		{name: resourceKindUser, setup: newTaggableUser},
		{name: resourceKindParameterGroup, setup: newTaggableParameterGroup},
		{name: resourceKindSnapshot, setup: newTaggableSnapshot},
		{name: resourceKindMultiRegionCluster, setup: newTaggableMultiRegionCluster},
	}

	seed := NewInMemoryBackend("123456789012", "us-east-1")
	covered := make(map[string]bool, len(tests))

	for _, tt := range tests {
		resourceArn := tt.setup(t, seed)

		seed.mu.RLock()
		_, ref, ok := seed.findARN(resourceArn)
		seed.mu.RUnlock()

		require.True(t, ok, "setup for %q did not register an ARN", tt.name)
		covered[ref.Kind] = true
	}

	require.Equal(t, registeredResourceKinds(seed), covered,
		"arnToResource contains a resource kind with no matching row in this test's table")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1")
			resourceArn := tt.setup(t, b)

			err := b.TagResource(context.Background(), resourceArn, map[string]string{"Env": "test"})
			require.NoError(t, err)

			got, err := b.ListTags(context.Background(), resourceArn)
			require.NoError(t, err)
			assert.Equal(t, "test", got["Env"])
		})
	}
}

// TestPaginateItems_NoSkipAcrossPages proves paginateItems' cursor resumes
// AT the item findStartIndex names, not after it. nextToken is set to
// items[limit] -- the first item of the next page, inclusive -- so decoding
// it must land on that same item; landing one past it (as findStartIndex
// previously did, returning i+1) silently drops exactly one item at every
// page boundary.
func TestPaginateItems_NoSkipAcrossPages(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c", "d", "e"}
	getName := func(s string) string { return s }

	var seen []string

	token := ""
	one := int32(1)

	for {
		page, next := paginateItems(items, token, &one, getName)
		seen = append(seen, page...)

		if next == "" {
			break
		}

		token = next
	}

	assert.Equal(t, items, seen, "pagination must visit every item exactly once, in order")
}
