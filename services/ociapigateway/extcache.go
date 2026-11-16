package ociapigateway

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultCacheTimeoutMs = 1000
	cachePoolSize         = 8
	cacheKeyPrefix        = "apigw:resp:"
)

var (
	errNoSecretResolver = errors.New("ociapigateway: no Vault available for the response cache secret")
	errCacheSecret      = errors.New("ociapigateway: response cache secret is not readable")
)

// cacheCredentials is the Vault secret format of docs apigatewayresponsecaching.htm; the username is optional.
type cacheCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type extClient struct {
	rdb  *redis.Client
	raw  string
	read time.Duration
	send time.Duration
}

// extCache holds one connection pool per gateway with an external response cache.
type extCache struct {
	mu      *lockmetrics.RWMutex
	clients map[string]*extClient
}

func newExtCache() *extCache {
	return &extCache{mu: lockmetrics.New("ociapigateway.extcache"), clients: map[string]*extClient{}}
}

func (c *extCache) reset() {
	c.mu.Lock("reset")
	defer c.mu.Unlock()

	for id, cl := range c.clients {
		_ = cl.rdb.Close()

		delete(c.clients, id)
	}
}

func (c *extCache) forgetGateway(gwID string) {
	c.mu.Lock("forgetGateway")
	defer c.mu.Unlock()

	if cl, ok := c.clients[gwID]; ok {
		_ = cl.rdb.Close()

		delete(c.clients, gwID)
	}
}

func (c *extCache) existing(gwID, raw string) *extClient {
	c.mu.RLock("existing")
	defer c.mu.RUnlock()

	if cl := c.clients[gwID]; cl != nil && cl.raw == raw {
		return cl
	}

	return nil
}

func (c *extCache) install(gwID string, cl *extClient) *extClient {
	c.mu.Lock("install")
	defer c.mu.Unlock()

	if cur := c.clients[gwID]; cur != nil {
		if cur.raw == cl.raw {
			_ = cl.rdb.Close()

			return cur
		}

		_ = cur.rdb.Close()
	}

	c.clients[gwID] = cl

	return cl
}

func ms(v *int) time.Duration {
	if v == nil {
		return defaultCacheTimeoutMs * time.Millisecond
	}

	return time.Duration(*v) * time.Millisecond
}

func (b *InMemoryBackend) cacheCredentials(d *respCacheDetails) (cacheCredentials, error) {
	b.mu.RLock("cacheCredentials")
	resolve := b.secrets
	b.mu.RUnlock()

	if resolve == nil {
		return cacheCredentials{}, errNoSecretResolver
	}

	raw, ok := resolve(d.AuthSecretID, *d.AuthSecretVersion)
	if !ok {
		return cacheCredentials{}, errCacheSecret
	}

	var cred cacheCredentials
	if err := json.Unmarshal([]byte(raw), &cred, jsonx.V1()); err != nil {
		return cacheCredentials{}, errCacheSecret
	}

	return cred, nil
}

// cacheClient returns the gateway's connection pool, building it from the Vault credentials on first use.
func (b *InMemoryBackend) cacheClient(gwID string, d *respCacheDetails) (*extClient, error) {
	if cl := b.extcache.existing(gwID, d.raw); cl != nil {
		return cl, nil
	}

	cred, err := b.cacheCredentials(d)
	if err != nil {
		return nil, err
	}

	srv := d.Servers[0]
	connect := ms(d.ConnectMs)
	dialer := b.guardedDialer()
	dialer.Timeout = connect

	var tlsCfg *tls.Config

	if isTrue(d.SSLEnabled) {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: *srv.Host}
		// Skipping verification is honoured only while the dialer pins the peer to loopback.
		tlsCfg.InsecureSkipVerify = isTrue(d.SSLVerifyDisabled) && !b.externalAllowed()
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:            net.JoinHostPort(*srv.Host, strconv.Itoa(*srv.Port)),
		Username:        cred.Username,
		Password:        cred.Password,
		Protocol:        2, //nolint:mnd // RESP2 is what every RESP server speaks
		DisableIdentity: true,
		MaxRetries:      -1,
		DialTimeout:     connect,
		ReadTimeout:     ms(d.ReadMs),
		WriteTimeout:    ms(d.SendMs),
		PoolSize:        cachePoolSize,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, derr := dialer.DialContext(ctx, network, addr)
			if derr != nil || tlsCfg == nil {
				return conn, derr
			}

			tc := tls.Client(conn, tlsCfg)
			if herr := tc.HandshakeContext(ctx); herr != nil {
				_ = conn.Close()

				return nil, herr
			}

			return tc, nil
		},
	})

	return b.extcache.install(gwID, &extClient{rdb: rdb, raw: d.raw, read: ms(d.ReadMs), send: ms(d.SendMs)}), nil
}

type storedResponse struct {
	Header  http.Header `json:"header"`
	Body    []byte      `json:"body"`
	Expires int64       `json:"expires"`
	Status  int         `json:"status"`
}

func cacheRedisKey(depID, key string) string {
	sum := sha256.Sum256([]byte(key))

	return cacheKeyPrefix + depID + ":" + hex.EncodeToString(sum[:])
}

// cacheLookup returns the stored response; an error means the cache server could not be used (BYPASS).
func (b *InMemoryBackend) cacheLookup(ctx context.Context, tgt target, key string) (*storedResponse, error) {
	cl, err := b.cacheClient(tgt.gateway, tgt.cacheCfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, cl.read)
	defer cancel()

	raw, err := cl.rdb.Get(ctx, cacheRedisKey(tgt.depID, key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil //nolint:nilnil // a miss is not an error
	}

	if err != nil {
		return nil, err
	}

	var e storedResponse
	if json.Unmarshal(raw, &e, jsonx.V1()) != nil || e.Expires <= b.clock().UnixNano() {
		return nil, nil //nolint:nilnil // an undecodable or expired entry is a miss
	}

	return &e, nil
}

// cacheStore writes the response; failures are ignored, the response is simply not reusable.
func (b *InMemoryBackend) cacheStore(
	ctx context.Context,
	tgt target,
	key string,
	e *storedResponse,
	ttl time.Duration,
) {
	cl, err := b.cacheClient(tgt.gateway, tgt.cacheCfg)
	if err != nil {
		return
	}

	val, err := json.Marshal(e, jsonx.V1())
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, cl.send)
	defer cancel()

	lg := logFrom(ctx)

	if err = cl.rdb.Set(ctx, cacheRedisKey(tgt.depID, key), val, ttl).Err(); err != nil {
		lg.exec(
			levelWarn,
			"responseCache.backendResponseStorageAborted",
			"Backend response was not stored in the cache.",
		)

		return
	}

	lg.exec(levelInfo, "responseCache.backendResponseStoredInCache", "Backend response was stored in the cache.")
}

func (b *InMemoryBackend) writeCached(w http.ResponseWriter, r *http.Request, e *storedResponse, cors *corsDoc) {
	h := w.Header()
	for k, vs := range e.Header {
		h[k] = append([]string(nil), vs...)
	}

	corsHeaders(h, cors, r.Header.Get("Origin"))
	h.Set(cacheStatusHeader, cacheHit)
	w.WriteHeader(e.Status)

	if r.Method != http.MethodHead {
		_, _ = w.Write(e.Body)
	}
}
