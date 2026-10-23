package azureredis_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureredis"
)

var errNoBlob = errors.New("no such blob")

type memBlobs struct {
	objs map[string][]byte
	mu   sync.Mutex
}

func newBlobs() *memBlobs { return &memBlobs{objs: map[string][]byte{}} }

func (m *memBlobs) ReadBlob(container, blob string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, ok := m.objs[container+"/"+blob]
	if !ok {
		return nil, errNoBlob
	}

	return data, nil
}

func (m *memBlobs) WriteBlob(container, blob string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objs[container+"/"+blob] = data

	return nil
}

func newBackend(t *testing.T) (*azureredis.InMemoryBackend, *memBlobs) {
	t.Helper()

	b := azureredis.NewInMemoryBackend(azureredis.DefaultSettings())
	blobs := newBlobs()
	b.SetBlobStore(blobs)
	t.Cleanup(b.Close)

	return b, blobs
}

func ensure(t *testing.T, b *azureredis.InMemoryBackend, key string, spec azureredis.Spec) azureredis.Endpoint {
	t.Helper()

	ep, err := b.Ensure(key, spec)
	require.NoError(t, err)

	return ep
}

func basicSpec() azureredis.Spec {
	return azureredis.Spec{SKU: azureredis.SKUBasic, MinTLS: "1.2"}
}

func dialTLS(t *testing.T, ep azureredis.Endpoint, password string) *goredis.Client {
	t.Helper()

	c := goredis.NewClient(&goredis.Options{
		Addr:     net.JoinHostPort(ep.Host, strconv.Itoa(ep.SSLPort)),
		Password: password,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		},
	})
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func dialPlain(t *testing.T, ep azureredis.Endpoint, password string) *goredis.Client {
	t.Helper()

	c := goredis.NewClient(&goredis.Options{
		Addr: net.JoinHostPort(ep.Host, strconv.Itoa(ep.Port)), Password: password,
	})
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func setGet(ctx context.Context, c *goredis.Client, k, v string) error {
	if err := c.Set(ctx, k, v, 0).Err(); err != nil {
		return err //nolint:wrapcheck // test helper
	}

	return c.Get(ctx, k).Err() //nolint:wrapcheck // test helper
}
