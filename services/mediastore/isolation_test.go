package mediastore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// TestMediaStoreRegionIsolation verifies that containers created in one region
// are completely invisible to every other region: creating, describing,
// listing, and deleting in a region only ever touch that region's own state,
// and container ARNs embed the owning region.
func TestMediaStoreRegionIsolation(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"

	ctxEast := context.WithValue(context.Background(), regionContextKey{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKey{}, "us-west-2")

	tests := []struct {
		run  func(t *testing.T, b *InMemoryBackend)
		name string
	}{
		{
			name: "same name in two regions coexists independently",
			run: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				_, err := b.CreateContainer(ctxEast, accountID, "shared", nil)
				require.NoError(t, err)

				_, err = b.CreateContainer(ctxWest, accountID, "shared", nil)
				require.NoError(t, err, "same name in a different region must not collide")
			},
		},
		{
			name: "describe only sees the owning region",
			run: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				_, err := b.CreateContainer(ctxEast, accountID, "east-box", nil)
				require.NoError(t, err)

				got, err := b.DescribeContainer(ctxEast, "east-box")
				require.NoError(t, err)
				assert.Contains(t, got.ARN, ":us-east-1:", "ARN must embed owning region")

				_, err = b.DescribeContainer(ctxWest, "east-box")
				require.ErrorIs(t, err, awserr.ErrNotFound, "foreign region must not find it")
			},
		},
		{
			name: "list is scoped to the requesting region",
			run: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				_, err := b.CreateContainer(ctxEast, accountID, "east-only", nil)
				require.NoError(t, err)

				_, err = b.CreateContainer(ctxWest, accountID, "west-only", nil)
				require.NoError(t, err)

				eastList, _, err := b.ListContainers(ctxEast, "", 0)
				require.NoError(t, err)
				assert.Equal(t, []string{"east-only"}, containerNames(eastList))

				for _, c := range eastList {
					assert.Contains(t, c.ARN, ":us-east-1:")
				}

				westList, _, err := b.ListContainers(ctxWest, "", 0)
				require.NoError(t, err)
				assert.Equal(t, []string{"west-only"}, containerNames(westList))
			},
		},
		{
			name: "delete in one region leaves the other intact",
			run: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				_, err := b.CreateContainer(ctxEast, accountID, "dup", nil)
				require.NoError(t, err)

				_, err = b.CreateContainer(ctxWest, accountID, "dup", nil)
				require.NoError(t, err)

				require.NoError(t, b.DeleteContainer(ctxEast, "dup"))

				_, err = b.DescribeContainer(ctxEast, "dup")
				require.ErrorIs(t, err, awserr.ErrNotFound)

				stillThere, err := b.DescribeContainer(ctxWest, "dup")
				require.NoError(t, err, "delete in east must not affect west")
				assert.Contains(t, stillThere.ARN, ":us-west-2:")
			},
		},
		{
			name: "foreign-region delete returns not found",
			run: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				_, err := b.CreateContainer(ctxEast, accountID, "east-resource", nil)
				require.NoError(t, err)

				err = b.DeleteContainer(ctxWest, "east-resource")
				require.ErrorIs(t, err, awserr.ErrNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}

// containerNames returns the names of the given containers in order.
func containerNames(cs []*Container) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.Name)
	}

	return out
}
