package mediastore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

func newBackend() *mediastore.InMemoryBackend {
	return mediastore.NewInMemoryBackend()
}

func TestInMemoryBackend_DescribeContainer_ReturnsCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating returned container does not affect backend state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			_, err := b.CreateContainer(context.Background(), testAccountID, "copy-test", map[string]string{"k": "v"})
			require.NoError(t, err)

			c, err := b.DescribeContainer(context.Background(), "copy-test")
			require.NoError(t, err)

			// Mutate the returned copy.
			c.Tags["injected"] = "evil"
			c.Status = "MUTATED"

			// Backend state must be unchanged.
			c2, err := b.DescribeContainer(context.Background(), "copy-test")
			require.NoError(t, err)
			assert.Equal(t, "ACTIVE", c2.Status)
			_, hasInjected := c2.Tags["injected"]
			assert.False(t, hasInjected, "mutating returned container must not affect backend state")
		})
	}
}

func TestInMemoryBackend_ListContainers_ReturnsCopies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating listed containers does not affect backend state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			_, err := b.CreateContainer(
				context.Background(),
				testAccountID,
				"list-copy-a",
				map[string]string{"key": "val"},
			)
			require.NoError(t, err)

			all, _, err := b.ListContainers(context.Background(), "", 0)
			require.NoError(t, err)
			require.Len(t, all, 1)

			// Mutate returned copy.
			all[0].Tags["injected"] = "evil"

			// Backend state must be unchanged.
			all2, _, err := b.ListContainers(context.Background(), "", 0)
			require.NoError(t, err)
			require.Len(t, all2, 1)
			_, hasInjected := all2[0].Tags["injected"]
			assert.False(t, hasInjected, "mutating listed container must not affect backend state")
		})
	}
}

func TestInMemoryBackend_CreateContainer_ReturnsCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating returned container does not affect backend state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			c, err := b.CreateContainer(context.Background(), testAccountID, "create-copy", map[string]string{"k": "v"})
			require.NoError(t, err)

			// Mutate the returned copy.
			c.Tags["injected"] = "evil"
			c.Status = "MUTATED"
			if c.CreationTime != nil {
				zeroTime := c.CreationTime.Add(-1e9)
				c.CreationTime = &zeroTime
			}

			// Backend state must be unchanged.
			c2, err := b.DescribeContainer(context.Background(), "create-copy")
			require.NoError(t, err)
			assert.Equal(t, "ACTIVE", c2.Status)
			_, hasInjected := c2.Tags["injected"]
			assert.False(t, hasInjected, "mutating returned Container from CreateContainer must not affect backend")
		})
	}
}
