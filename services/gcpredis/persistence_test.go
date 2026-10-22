package gcpredis_test

import (
	"context"
	"testing"

	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpredis"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   *redispb.Instance
		name string
	}{
		{name: "plain", in: basic()},
		{name: "auth", in: basic(func(i *redispb.Instance) { i.AuthEnabled = true })},
		{name: "tls", in: basic(func(i *redispb.Instance) {
			i.TransitEncryptionMode = redispb.Instance_SERVER_AUTHENTICATION
			i.Tier = redispb.Instance_STANDARD_HA
		})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := newEnv(t)
			inst := src.create(t, "snap", tt.in)

			auth := ""
			if inst.GetAuthEnabled() {
				as, err := src.client.GetInstanceAuthString(
					t.Context(),
					&redispb.GetInstanceAuthStringRequest{Name: inst.GetName()},
				)
				require.NoError(t, err)

				auth = as.GetAuthString()
			}

			require.NoError(t, src.dial(t, inst, auth).Set(t.Context(), "k", "v", 0).Err())

			blob := src.handler.Snapshot(t.Context())
			require.NotEmpty(t, blob)

			dst := newEnv(t)
			require.NoError(t, dst.handler.Restore(t.Context(), blob))

			got, err := dst.client.GetInstance(t.Context(), &redispb.GetInstanceRequest{Name: inst.GetName()})
			require.NoError(t, err)
			assert.Equal(t, redispb.Instance_READY, got.GetState())
			assert.Equal(t, inst.GetTier(), got.GetTier())
			assert.Len(t, got.GetServerCaCerts(), len(inst.GetServerCaCerts()))

			if auth != "" {
				as, authErr := dst.client.GetInstanceAuthString(
					t.Context(),
					&redispb.GetInstanceAuthStringRequest{Name: inst.GetName()},
				)
				require.NoError(t, authErr)
				assert.Equal(t, auth, as.GetAuthString())
			}

			v, err := dst.dial(t, got, auth).Get(t.Context(), "k").Result()
			require.NoError(t, err)
			assert.Equal(t, "v", v)

			ops := dst.client.LROClient.ListOperations(t.Context(), opsRequest())
			_, err = ops.Next()
			require.NoError(t, err, "operations survive a restore")
		})
	}
}

func TestRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()

	b := gcpredis.NewInMemoryBackend()
	t.Cleanup(b.Close)

	require.Error(t, b.Restore(context.Background(), []byte("{not json")))
	assert.NoError(t, b.Restore(context.Background(), b.Snapshot(context.Background())))
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	inst := e.create(t, "reset", basic())
	c := e.dial(t, inst, "")
	require.NoError(t, c.Ping(t.Context()).Err())

	e.handler.Reset()

	_, err := e.client.GetInstance(t.Context(), &redispb.GetInstanceRequest{Name: inst.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	require.Error(t, c.Ping(t.Context()).Err(), "the instance's Redis server is stopped")
}
