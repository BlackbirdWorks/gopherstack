package gcpkms_test

import (
	"context"
	"hash/crc32"
	"sync/atomic"
	"testing"
	"time"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/blackbirdworks/gopherstack/services/gcpkms"
)

const location = "projects/test-project/locations/global"

type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	client  *kms.KeyManagementClient
	handler *gcpkms.Handler
	clk     *clock
	opts    []option.ClientOption
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpkms.NewHandler(gcpkms.NewInMemoryBackend())
	h.Port = 0

	clk := &clock{}
	clk.ns.Store(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	h.Backend.SetClock(clk.now)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	opts := []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	client, err := kms.NewKeyManagementClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{client: client, handler: h, clk: clk, opts: opts}
}

func (e *env) ring(t *testing.T, id string) string {
	t.Helper()

	r, err := e.client.CreateKeyRing(t.Context(), &kmspb.CreateKeyRingRequest{
		Parent: location, KeyRingId: id, KeyRing: &kmspb.KeyRing{},
	})
	require.NoError(t, err)

	return r.GetName()
}

func (e *env) key(
	t *testing.T,
	ring, id string,
	p kmspb.CryptoKey_CryptoKeyPurpose,
	a kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm,
	mut ...func(*kmspb.CryptoKey),
) *kmspb.CryptoKey {
	t.Helper()

	k := &kmspb.CryptoKey{Purpose: p, VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: a}}
	for _, m := range mut {
		m(k)
	}

	out, err := e.client.CreateCryptoKey(t.Context(), &kmspb.CreateCryptoKeyRequest{
		Parent: ring, CryptoKeyId: id, CryptoKey: k,
	})
	require.NoError(t, err)

	return out
}

func crc(b []byte) *wrapperspb.Int64Value {
	return wrapperspb.Int64(int64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))))
}
