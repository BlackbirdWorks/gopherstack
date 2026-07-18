//go:build !integration

package mediastoredata_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

func newTestBackend() *mediastoredata.InMemoryBackend {
	return mediastoredata.NewInMemoryBackend("us-east-1")
}

func TestInMemoryBackend_Stats_RunningCounters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		ops       func(b *mediastoredata.InMemoryBackend)
		name      string
		wantBytes int64
		wantCount int
	}{
		{
			name:      "empty_store",
			ops:       func(_ *mediastoredata.InMemoryBackend) {},
			wantCount: 0,
			wantBytes: 0,
		},
		{
			name: "two_objects_summed",
			ops: func(b *mediastoredata.InMemoryBackend) {
				_, _ = b.PutObject(ctx, "/a.mp4", []byte("hello"), "video/mp4", "", "TEMPORAL", "")
				_, _ = b.PutObject(ctx, "/b.mp4", []byte("world!"), "video/mp4", "", "TEMPORAL", "")
			},
			wantCount: 2,
			wantBytes: 11,
		},
		{
			name: "delete_decrements",
			ops: func(b *mediastoredata.InMemoryBackend) {
				_, _ = b.PutObject(ctx, "/x.mp4", []byte("data"), "video/mp4", "", "TEMPORAL", "")
				_ = b.DeleteObject(ctx, "/x.mp4")
			},
			wantCount: 0,
			wantBytes: 0,
		},
		{
			name: "overwrite_replaces_bytes",
			ops: func(b *mediastoredata.InMemoryBackend) {
				_, _ = b.PutObject(ctx, "/ov.mp4", []byte("short"), "video/mp4", "", "TEMPORAL", "")
				_, _ = b.PutObject(ctx, "/ov.mp4", []byte("longer content"), "video/mp4", "", "TEMPORAL", "")
			},
			wantCount: 1,
			wantBytes: 14,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			tt.ops(b)

			stats := b.Stats(ctx)
			assert.Equal(t, tt.wantCount, stats.ObjectCount)
			assert.Equal(t, tt.wantBytes, stats.TotalBytes)
		})
	}
}
