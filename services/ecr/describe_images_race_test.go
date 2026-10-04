package ecr_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestDescribeImages_ConcurrentReadersDoNotRaceOnTagOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []string
		wantTags []string
		readers  int
	}{
		{
			name:     "unsorted tags",
			tags:     []string{"zeta", "beta", "alpha"},
			wantTags: []string{"alpha", "beta", "zeta"},
			readers:  8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			be := ecr.NewInMemoryBackend("000000000000", "us-east-1", "localhost:5000")

			_, err := be.CreateRepository(ctx, "repo", "MUTABLE", false, "", "")
			require.NoError(t, err)

			for _, tag := range tt.tags {
				_, err = be.PutImage(ctx, "repo", ecr.Image{
					ImageManifest: `{"schemaVersion":2}`,
					ImageID:       ecr.ImageIdentifier{ImageTag: tag},
				})
				require.NoError(t, err)
			}

			var wg sync.WaitGroup

			results := make([][]ecr.Image, tt.readers)
			errs := make([]error, tt.readers)

			for i := range tt.readers {
				wg.Go(func() {
					results[i], errs[i] = be.DescribeImages(ctx, "repo", nil)
				})
			}

			wg.Wait()

			for i := range tt.readers {
				require.NoError(t, errs[i])
				require.Len(t, results[i], 1)
				assert.Equal(t, tt.wantTags, results[i][0].Tags)
			}
		})
	}
}
