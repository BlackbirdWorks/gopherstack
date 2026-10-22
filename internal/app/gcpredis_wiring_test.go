package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpredisbackend "github.com/blackbirdworks/gopherstack/services/gcpredis"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestGCSRedisStore(t *testing.T) {
	t.Parallel()

	gcs := gcpstoragebackend.NewInMemoryBackend()
	_, err := gcs.CreateBucket("p", gcpstoragebackend.Patch{"name": []byte(`"dumps"`)})
	require.NoError(t, err)

	store := gcsRedisStore{b: gcs}

	tests := []struct {
		name    string
		bucket  string
		wantErr bool
	}{
		{name: "existing bucket", bucket: "dumps"},
		{name: "missing bucket", bucket: "nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			writeErr := store.WriteObject(tt.bucket, "a/dump.rdb", []byte("data"))
			if tt.wantErr {
				require.Error(t, writeErr)
				_, readErr := store.ReadObject(tt.bucket, "a/dump.rdb")
				require.Error(t, readErr)

				return
			}

			require.NoError(t, writeErr)

			got, err := store.ReadObject(tt.bucket, "a/dump.rdb")
			require.NoError(t, err)
			assert.Equal(t, []byte("data"), got)
		})
	}
}

func TestWireGCPRedis(t *testing.T) {
	t.Parallel()

	redisH := gcpredisbackend.NewHandler(gcpredisbackend.NewInMemoryBackend())
	t.Cleanup(redisH.Backend.Close)

	assert.NotPanics(t, func() { wireGCPRedis(map[string]service.Registerable{}) })
	assert.NotPanics(t, func() { wireGCPRedis(map[string]service.Registerable{"GCPRedis": redisH}) })
	assert.NotPanics(t, func() {
		wireGCPRedis(map[string]service.Registerable{
			"GCPRedis": redisH, "GCPStorage": gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend()),
		})
	})
}

func TestReserveFixedServicePorts_GCPRedis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"port inside pool range", 8100, 8200, true},
		{"port outside pool range", 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{GCPRedis: gcpredisbackend.Settings{Port: gcpredisbackend.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(gcpredisbackend.DefaultPort))
		})
	}
}
