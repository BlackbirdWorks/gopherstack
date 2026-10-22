package azureredis

import (
	"crypto/tls"
	"fmt"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

const (
	maxCaches = 128
	redisHost = "127.0.0.1"
	portLabel = "azureredis"
	tlsLabel  = "azureredis-tls"
)

// BlobStore reads and writes blobs in the Blob Storage emulator, for ImportData and ExportData.
type BlobStore interface {
	ReadBlob(container, blob string) ([]byte, error)
	WriteBlob(container, blob string, data []byte) error
}

type cache struct {
	engine *redisengine.Engine
	tls    *front
	plain  *front
	keys   *keyring
	minTLS *minTLS
	spec   Spec
}

// InMemoryBackend runs one embedded Redis server per cache, with a TLS port always on and a plaintext port only
// while the cache enables it.
type InMemoryBackend struct {
	mu     *lockmetrics.RWMutex
	alloc  *portalloc.Allocator
	blobs  BlobStore
	caches map[string]*cache
	cert   *tls.Certificate
	wg     *sync.WaitGroup
	cfg    Settings
}

// NewInMemoryBackend creates an empty backend on ephemeral loopback ports.
func NewInMemoryBackend(cfg Settings) *InMemoryBackend {
	return &InMemoryBackend{
		mu: lockmetrics.New("azureredis"), caches: map[string]*cache{}, cfg: cfg, wg: &sync.WaitGroup{},
	}
}

// SetPortPool makes caches take their Redis ports from alloc.
func (b *InMemoryBackend) SetPortPool(alloc *portalloc.Allocator) {
	b.mu.Lock("SetPortPool")
	defer b.mu.Unlock()

	b.alloc = alloc
}

// SetBlobStore connects ImportData and ExportData to Blob Storage.
func (b *InMemoryBackend) SetBlobStore(s BlobStore) {
	b.mu.Lock("SetBlobStore")
	defer b.mu.Unlock()

	b.blobs = s
}

func (b *InMemoryBackend) certificate() (tls.Certificate, error) {
	if b.cert != nil {
		return *b.cert, nil
	}

	cert, err := b.loadCertificate()
	if err != nil {
		return tls.Certificate{}, err
	}

	b.cert = &cert

	return cert, nil
}

func (b *InMemoryBackend) loadCertificate() (tls.Certificate, error) {
	certFile, keyFile := b.cfg.TLSCertFile, b.cfg.TLSKeyFile

	switch {
	case certFile != "" && keyFile == "":
		return tls.Certificate{}, errTLSKeyMissing
	case certFile == "" && keyFile != "":
		return tls.Certificate{}, errTLSCertMissing
	case certFile != "":
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("azureredis: load TLS certificate: %w", err)
		}

		return cert, nil
	}

	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("azureredis: generate self-signed certificate: %w", err)
	}

	return cert, nil
}

func cacheKey(key string) string { return strings.ToLower(key) }

// startCache launches the loopback server and its fronts. Caller holds b.mu.
func (b *InMemoryBackend) startCache(spec Spec, keys Keys) (*cache, error) {
	cert, err := b.certificate()
	if err != nil {
		return nil, err
	}

	eng, err := redisengine.Start(nil, portLabel)
	if err != nil {
		return nil, fmt.Errorf("start redis engine: %w", err)
	}

	c := &cache{engine: eng, keys: &keyring{}, minTLS: &minTLS{}, spec: spec}
	c.minTLS.set(spec.MinTLS)
	c.keys.set(keys)
	c.keys.install(eng.Mini.Server())
	requireKey(c, spec, keys)

	if c.tls, err = openFront(b.alloc, tlsLabel, eng.Addr(), c.minTLS.config(cert)); err != nil {
		c.close()

		return nil, err
	}

	if err = b.syncPlain(c, spec); err != nil {
		c.close()

		return nil, err
	}

	return c, nil
}

// syncPlain opens or closes the plaintext front to match spec.EnableNonSSLPort.
func (b *InMemoryBackend) syncPlain(c *cache, spec Spec) error {
	switch {
	case spec.EnableNonSSLPort && c.plain == nil:
		f, err := openFront(b.alloc, portLabel, c.engine.Addr(), nil)
		if err != nil {
			return err
		}

		c.plain = f
	case !spec.EnableNonSSLPort && c.plain != nil:
		c.plain.close()
		c.plain = nil
	}

	return nil
}

// requireKey makes the server demand the primary key, or nothing when the cache does not require authentication.
func requireKey(c *cache, spec Spec, keys Keys) {
	pw := keys.Primary
	if spec.AuthNotRequired {
		pw = ""
	}

	c.engine.Mini.RequireAuth(pw)
}

func (c *cache) close() {
	c.plain.close()
	c.tls.close()
	c.engine.Close()
}

func (c *cache) endpoint() Endpoint {
	return Endpoint{Host: redisHost, Port: c.plain.portOr0(), SSLPort: c.tls.portOr0()}
}

func (b *InMemoryBackend) lookup(key string) (*cache, error) {
	c, ok := b.caches[cacheKey(key)]
	if !ok {
		return nil, ErrNotFound
	}

	return c, nil
}

// Ensure creates the cache, or applies spec to an existing one, and returns where it listens.
func (b *InMemoryBackend) Ensure(key string, spec Spec) (Endpoint, error) {
	b.mu.Lock("Ensure")
	defer b.mu.Unlock()

	if c, ok := b.caches[cacheKey(key)]; ok {
		return b.update(c, spec)
	}

	if len(b.caches) >= maxCaches {
		return Endpoint{}, fmt.Errorf("%w: at most %d caches", ErrInvalid, maxCaches)
	}

	keys, err := newKeys()
	if err != nil {
		return Endpoint{}, fmt.Errorf("generate access keys: %w", err)
	}

	c, err := b.startCache(spec, keys)
	if err != nil {
		return Endpoint{}, err
	}

	b.caches[cacheKey(key)] = c

	return c.endpoint(), nil
}

func (b *InMemoryBackend) update(c *cache, spec Spec) (Endpoint, error) {
	if err := b.syncPlain(c, spec); err != nil {
		return Endpoint{}, err
	}

	c.spec = spec
	c.minTLS.set(spec.MinTLS)
	requireKey(c, spec, c.keys.get())

	return c.endpoint(), nil
}

// Endpoint reports where the cache listens.
func (b *InMemoryBackend) Endpoint(key string) (Endpoint, bool) {
	b.mu.RLock("Endpoint")
	defer b.mu.RUnlock()

	c, err := b.lookup(key)
	if err != nil {
		return Endpoint{}, false
	}

	return c.endpoint(), true
}

// Keys returns the cache's access keys.
func (b *InMemoryBackend) Keys(key string) (Keys, error) {
	b.mu.RLock("Keys")
	defer b.mu.RUnlock()

	c, err := b.lookup(key)
	if err != nil {
		return Keys{}, err
	}

	return c.keys.get(), nil
}

// RegenerateKey replaces one access key; the old value stops authenticating.
func (b *InMemoryBackend) RegenerateKey(key, keyType string) (Keys, error) {
	b.mu.Lock("RegenerateKey")
	defer b.mu.Unlock()

	c, err := b.lookup(key)
	if err != nil {
		return Keys{}, err
	}

	fresh, err := newKey()
	if err != nil {
		return Keys{}, fmt.Errorf("generate access key: %w", err)
	}

	keys := c.keys.get()

	switch {
	case strings.EqualFold(keyType, KeyPrimary):
		keys.Primary = fresh
	case strings.EqualFold(keyType, KeySecondary):
		keys.Secondary = fresh
	default:
		return Keys{}, fmt.Errorf("%w: keyType must be Primary or Secondary", ErrInvalid)
	}

	c.keys.set(keys)
	requireKey(c, c.spec, keys)

	return keys, nil
}

// Delete stops the cache's servers and discards its data.
func (b *InMemoryBackend) Delete(key string) error {
	b.mu.Lock("Delete")
	defer b.mu.Unlock()

	c, err := b.lookup(key)
	if err != nil {
		return err
	}

	delete(b.caches, cacheKey(key))
	c.close()

	return nil
}

// Reset stops every cache.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	old := b.caches
	b.caches = map[string]*cache{}
	b.mu.Unlock()

	for _, c := range old {
		c.close()
	}
}

// Close stops every cache.
func (b *InMemoryBackend) Close() { b.Reset() }
