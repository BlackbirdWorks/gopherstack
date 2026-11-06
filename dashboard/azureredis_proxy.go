package dashboard

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	redisbackend "github.com/blackbirdworks/gopherstack/services/azureredis"
)

const (
	azureRedisProxyPrefix = "/dashboard/api/v1/azure/redis"
	redisMaxPattern       = 256
	redisMaxKeyName       = 1024
	redisMaxCacheID       = 512
	redisMask             = "••••••••"
	redisKeysField        = "keys"
	redisKeyField         = "key"
	redisTypeField        = "type"
	redisTruncatedField   = "truncated"
)

type redisCacheJSON struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	SKU              string `json:"sku"`
	MinTLS           string `json:"minTlsVersion"`
	Host             string `json:"host"`
	AccessKeysMasked string `json:"accessKeys"`
	Port             int    `json:"port"`
	SSLPort          int    `json:"sslPort"`
	Keys             int    `json:"keyCount"`
	NonSSLEnabled    bool   `json:"enableNonSslPort"`
	Persistence      bool   `json:"persistence"`
	AuthNotRequired  bool   `json:"authNotRequired"`
}

type redisKeyJSON struct {
	Key        string `json:"key"`
	Type       string `json:"type"`
	TTLSeconds int64  `json:"ttlSeconds"`
}

type redisRevealJSON struct {
	Cache string `json:"cache"`
}

// registerAzureRedisRoutes serves the Cache for Redis page from the backend in-process. The keys browser is
// read-only; access keys leave the server only through the reveal POST.
func (h *DashboardHandler) registerAzureRedisRoutes() {
	api := &azureRedisAPI{}
	if h.config.AzureRedisOps != nil {
		api.b = h.config.AzureRedisOps.Backend
	}

	r := h.SubRouter
	p := azureRedisProxyPrefix
	r.Add(http.MethodGet, p+"/caches", api.guard(api.caches))
	r.Add(http.MethodGet, p+"/keys", api.guard(api.scan))
	r.Add(http.MethodGet, p+"/value", api.guard(api.value))
	r.Add(http.MethodPost, p+"/access-keys/reveal", api.guard(api.reveal))
}

type azureRedisAPI struct {
	b *redisbackend.InMemoryBackend
}

func (a *azureRedisAPI) guard(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if a.b == nil {
			return cloudUnavailable(c, "Azure Cache for Redis")
		}

		return next(c)
	}
}

func (a *azureRedisAPI) fail(c *echo.Context, err error) error {
	if errors.Is(err, redisbackend.ErrNotFound) {
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
	}

	return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, err.Error())
}

func (a *azureRedisAPI) caches(c *echo.Context) error {
	infos := a.b.Caches()
	out := make([]redisCacheJSON, 0, len(infos))

	for _, i := range infos {
		name := i.ID
		if idx := strings.LastIndex(name, "/"); idx >= 0 {
			name = name[idx+1:]
		}

		out = append(out, redisCacheJSON{
			ID: i.ID, Name: name, SKU: i.Spec.SKU, MinTLS: i.Spec.MinTLS, Host: i.Endpoint.Host,
			Port: i.Endpoint.Port, SSLPort: i.Endpoint.SSLPort, Keys: i.Keys, NonSSLEnabled: i.Spec.EnableNonSSLPort,
			Persistence: i.Spec.Persistence, AuthNotRequired: i.Spec.AuthNotRequired, AccessKeysMasked: redisMask,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"caches": out})
}

func redisCacheParam(c *echo.Context) (string, bool) {
	id := c.QueryParam("cache")
	if id == "" || len(id) > redisMaxCacheID {
		_ = cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "cache is required")

		return "", false
	}

	return id, true
}

func redisDBParam(c *echo.Context) int {
	db, err := strconv.Atoi(c.QueryParam("db"))
	if err != nil {
		return 0
	}

	return db
}

func (a *azureRedisAPI) scan(c *echo.Context) error {
	id, ok := redisCacheParam(c)
	if !ok {
		return nil
	}

	match, cursor := c.QueryParam("match"), c.QueryParam("cursor")
	if len(match) > redisMaxPattern || len(cursor) > redisMaxKeyName {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "pattern or cursor is too long")
	}

	count, _ := strconv.Atoi(c.QueryParam("count"))

	entries, next, err := a.b.Scan(id, redisDBParam(c), match, cursor, count)
	if err != nil {
		return a.fail(c, err)
	}

	keys := make([]redisKeyJSON, 0, len(entries))
	for _, e := range entries {
		keys = append(keys, redisKeyJSON{Key: e.Key, Type: e.Type, TTLSeconds: redisTTL(e.TTL)})
	}

	return c.JSON(http.StatusOK, map[string]any{redisKeysField: keys, "next": next})
}

// redisTTL reports -1 for a key without expiry, as Redis does.
func redisTTL(d time.Duration) int64 {
	if d <= 0 {
		return -1
	}

	return int64((d + time.Second - 1) / time.Second)
}

func (a *azureRedisAPI) value(c *echo.Context) error {
	id, ok := redisCacheParam(c)
	if !ok {
		return nil
	}

	key := c.QueryParam("key")
	if key == "" || len(key) > redisMaxKeyName {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "key is required")
	}

	v, err := a.b.Value(id, redisDBParam(c), key)
	if err != nil {
		return a.fail(c, err)
	}

	elems := v.Elements
	if elems == nil {
		elems = []string{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		redisKeyField: v.Key, redisTypeField: v.Type, "text": v.Text, "elements": elems, "total": v.Total,
		redisTruncatedField: v.Truncated, "ttlSeconds": redisTTL(v.TTL),
	})
}

// reveal is the only route that sends the access keys out.
func (a *azureRedisAPI) reveal(c *echo.Context) error {
	var in redisRevealJSON
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	if in.Cache == "" || len(in.Cache) > redisMaxCacheID {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "cache is required")
	}

	keys, err := a.b.Keys(in.Cache)
	if err != nil {
		return a.fail(c, err)
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(http.StatusOK, map[string]string{"primary": keys.Primary, "secondary": keys.Secondary})
}
