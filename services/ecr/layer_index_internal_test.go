package ecr

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLayer(n int) string { return fmt.Sprintf("sha256:%064x", n) }

func testManifest(layers ...string) string {
	parts := make([]string, 0, len(layers))
	for _, l := range layers {
		parts = append(parts, fmt.Sprintf(`{"digest":%q}`, l))
	}

	return `{"schemaVersion":2,"layers":[` + strings.Join(parts, ",") + `]}`
}

func seedLayerRepo(t *testing.T, b *InMemoryBackend) {
	t.Helper()

	ctx := context.Background()
	_, err := b.CreateRepository(ctx, "r", "MUTABLE", false, "", "")
	require.NoError(t, err)

	now := time.Now()
	manifests := []string{
		testManifest(testLayer(1), testLayer(2)),
		testManifest(testLayer(2)),
		testManifest(testLayer(3)),
	}

	for i, m := range manifests {
		_, err = b.PutImage(ctx, "r", Image{
			ImageManifest: m,
			ImageID:       ImageIdentifier{ImageTag: fmt.Sprintf("t%d", i)},
			ImagePushedAt: now.Add(time.Duration(i) * time.Hour),
		})
		require.NoError(t, err)
	}
}

func scanLayerRefs(b *InMemoryBackend, layer string) []string {
	var out []string

	for _, img := range b.imagesByRepo.Get("r") {
		if strings.Contains(img.ImageManifest, layer) {
			out = append(out, img.ImageDigest)
		}
	}

	return out
}

func indexedLayerRefs(b *InMemoryBackend, layer string) []string {
	out := make([]string, 0, len(b.layerRefs.refs["r"][layer]))
	for d := range b.layerRefs.refs["r"][layer] {
		out = append(out, d)
	}

	return out
}

func TestLayerRefIndexStaysConsistent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		mutate func(t *testing.T, b *InMemoryBackend) *InMemoryBackend
		name   string
	}{
		{name: "after put", mutate: func(_ *testing.T, b *InMemoryBackend) *InMemoryBackend { return b }},
		{
			name: "after replace same digest",
			mutate: func(t *testing.T, b *InMemoryBackend) *InMemoryBackend {
				t.Helper()

				img := b.imagesByRepo.Get("r")[0]
				_, err := b.PutImage(ctx, "r", Image{
					ImageDigest: img.ImageDigest, ImageManifest: testManifest(testLayer(3)),
					ImageID: ImageIdentifier{ImageTag: "new"},
				})
				require.NoError(t, err)

				return b
			},
		},
		{
			name: "after batch delete by digest",
			mutate: func(t *testing.T, b *InMemoryBackend) *InMemoryBackend {
				t.Helper()

				d := b.imagesByRepo.Get("r")[0].ImageDigest
				_, _, err := b.BatchDeleteImage(ctx, "r", []ImageIdentifier{{ImageDigest: d}})
				require.NoError(t, err)

				return b
			},
		},
		{
			name: "after batch delete by tag keeps image",
			mutate: func(t *testing.T, b *InMemoryBackend) *InMemoryBackend {
				t.Helper()

				_, _, err := b.BatchDeleteImage(ctx, "r", []ImageIdentifier{{ImageTag: "t0"}})
				require.NoError(t, err)

				return b
			},
		},
		{
			name: "after lifecycle expiry",
			mutate: func(t *testing.T, b *InMemoryBackend) *InMemoryBackend {
				t.Helper()

				_, err := b.PutLifecyclePolicy(ctx, "r", `{"rules":[{"rulePriority":1,"action":{"type":"expire"},`+
					`"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":1}}]}`)
				require.NoError(t, err)

				return b
			},
		},
		{
			name: "after delete repository",
			mutate: func(t *testing.T, b *InMemoryBackend) *InMemoryBackend {
				t.Helper()

				_, err := b.DeleteRepository(ctx, "r", true)
				require.NoError(t, err)

				return b
			},
		},
		{
			name: "after snapshot restore",
			mutate: func(t *testing.T, b *InMemoryBackend) *InMemoryBackend {
				t.Helper()

				fresh := NewInMemoryBackend("123456789012", "us-east-1", "ecr.local")
				require.NoError(t, fresh.Restore(ctx, b.Snapshot(ctx)))

				return fresh
			},
		},
		{
			name: "after reset",
			mutate: func(_ *testing.T, b *InMemoryBackend) *InMemoryBackend {
				b.Reset()

				return b
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", "ecr.local")
			seedLayerRepo(t, b)
			b = tc.mutate(t, b)

			for n := 1; n <= 3; n++ {
				layer := testLayer(n)
				assert.ElementsMatch(t, scanLayerRefs(b, layer), indexedLayerRefs(b, layer), layer)
			}
		})
	}
}

func TestRecordLayerPullMatchesManifestScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		layer     string
		wantPulls int
	}{
		{name: "shared layer", layer: testLayer(2), wantPulls: 2},
		{name: "single layer", layer: testLayer(1), wantPulls: 1},
		{name: "unreferenced layer", layer: testLayer(9), wantPulls: 0},
		{name: "short digest falls back to scan", layer: "sha256:", wantPulls: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", "ecr.local")
			seedLayerRepo(t, b)

			b.recordLayerPullLocked("r", tc.layer)

			pulled := 0

			for _, img := range b.imagesByRepo.Get("r") {
				if !img.LastRecordedPullTime.IsZero() {
					pulled++

					assert.Contains(t, img.ImageManifest, tc.layer)
				}
			}

			assert.Equal(t, tc.wantPulls, pulled)
		})
	}
}

func BenchmarkRecordLayerPull(b *testing.B) {
	ctx := context.Background()
	be := NewInMemoryBackend("123456789012", "us-east-1", "ecr.local")

	_, err := be.CreateRepository(ctx, "r", "MUTABLE", false, "", "")
	require.NoError(b, err)

	for i := range 500 {
		_, err = be.PutImage(ctx, "r", Image{
			ImageManifest: testManifest(testLayer(i), testLayer(i+1000), testLayer(i+2000)),
		})
		require.NoError(b, err)
	}

	layer := testLayer(250)

	b.ReportAllocs()

	for b.Loop() {
		be.mu.Lock("bench")
		be.recordLayerPullLocked("r", layer)
		be.mu.Unlock()
	}
}
