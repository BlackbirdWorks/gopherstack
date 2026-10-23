package gcpredis_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	redis "cloud.google.com/go/redis/apiv1"
	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/googleapis/gax-go/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/services/gcpredis"
)

const (
	loc  = "projects/test-project/locations/us-central1"
	zone = "us-central1-a"
)

type memStore struct {
	objs map[string][]byte
	mu   sync.Mutex
}

func (m *memStore) ReadObject(bucket, object string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, ok := m.objs[bucket+"/"+object]
	if !ok {
		return nil, context.DeadlineExceeded
	}

	return data, nil
}

func (m *memStore) WriteObject(bucket, object string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objs[bucket+"/"+object] = data

	return nil
}

type env struct {
	client  *redis.CloudRedisClient
	handler *gcpredis.Handler
	store   *memStore
	opts    []option.ClientOption
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpredis.NewHandler(gcpredis.NewInMemoryBackend())
	h.Port = 0

	store := &memStore{objs: map[string][]byte{}}
	h.Backend.SetObjectStore(store)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	opts := []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	client, err := redis.NewCloudRedisClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{client: client, handler: h, store: store, opts: opts}
}

type poller[T any] interface {
	Poll(ctx context.Context, opts ...gax.CallOption) (T, error)
	Done() bool
}

// await polls an operation until it is done and returns its result.
func await[T any](t *testing.T, op poller[T]) (T, error) {
	t.Helper()

	var (
		out T
		err error
	)

	require.Eventually(t, func() bool {
		out, err = op.Poll(t.Context())

		return op.Done()
	}, 10*time.Second, 2*time.Millisecond)

	return out, err
}

func basic(mut ...func(*redispb.Instance)) *redispb.Instance {
	in := &redispb.Instance{Tier: redispb.Instance_BASIC, MemorySizeGb: 1}
	for _, m := range mut {
		m(in)
	}

	return in
}

func (e *env) create(t *testing.T, id string, in *redispb.Instance) *redispb.Instance {
	t.Helper()

	op, err := e.client.CreateInstance(t.Context(), &redispb.CreateInstanceRequest{
		Parent: loc, InstanceId: id, Instance: in,
	})
	require.NoError(t, err)

	inst, err := await(t, op)
	require.NoError(t, err)

	return inst
}

func (e *env) dial(t *testing.T, inst *redispb.Instance, auth string) *goredis.Client {
	t.Helper()

	opts := &goredis.Options{Addr: net.JoinHostPort(inst.GetHost(), strconv.Itoa(int(inst.GetPort()))), Password: auth}

	if certs := inst.GetServerCaCerts(); len(certs) > 0 {
		pool := x509.NewCertPool()
		require.True(t, pool.AppendCertsFromPEM([]byte(certs[0].GetCert())))
		opts.TLSConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12, ServerName: "localhost"}
	}

	c := goredis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func goZ(score float64, member string) goredis.Z { return goredis.Z{Score: score, Member: member} }

func opsRequest() *longrunningpb.ListOperationsRequest {
	return &longrunningpb.ListOperationsRequest{Name: loc}
}
