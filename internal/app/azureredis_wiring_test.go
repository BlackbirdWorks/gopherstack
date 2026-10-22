package app

import (
	"crypto/tls"
	"net"
	"strconv"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azureredis"
)

func TestWireAzureRedis(t *testing.T) {
	t.Parallel()

	redisH := azureredis.NewHandler(azureredis.NewInMemoryBackend(azureredis.DefaultSettings()))
	t.Cleanup(redisH.Backend.Close)

	assert.NotPanics(t, func() { wireAzureRedis(map[string]service.Registerable{}) })
	assert.NotPanics(t, func() { wireAzureRedis(map[string]service.Registerable{"AzureRedis": redisH}) })
}

func TestAzureRedisAdapterBlobRoundTrip(t *testing.T) {
	t.Parallel()

	blobs := azureblob.NewInMemoryBackend()
	require.NoError(t, blobs.CreateContainer("exports"))

	b := azureredis.NewInMemoryBackend(azureredis.DefaultSettings())
	b.SetBlobStore(azureRedisBlobs{b: blobs})
	t.Cleanup(b.Close)

	a := &azureRedisAdapter{b: b}
	spec := azurearm.RedisSpec{SKU: "Basic", MinTLS: "1.2"}

	src, err := a.Ensure("src", spec)
	require.NoError(t, err)

	keys, err := a.Keys("src")
	require.NoError(t, err)
	require.NoError(t, redisClient(src, keys.Secondary).Set(t.Context(), "k", "v", 0).Err())

	require.NoError(t, a.Export("src", "RDB", "dump", "https://acct.blob.core.windows.net/exports?sig=x"))
	missing := a.Export("src", "RDB", "dump", "https://acct.blob.core.windows.net/nope?sig=x")
	require.ErrorIs(t, missing, azureredis.ErrInvalid)

	dst, err := a.Ensure("dst", spec)
	require.NoError(t, err)
	require.NoError(t, a.Import("dst", "RDB", []string{"https://acct.blob.core.windows.net/exports/dump.rdb?sig=x"}))

	dstKeys, err := a.Keys("dst")
	require.NoError(t, err)

	got, err := redisClient(dst, dstKeys.Primary).Get(t.Context(), "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", got)

	_, ok := a.Endpoint("dst")
	assert.True(t, ok)
	require.NoError(t, a.Delete("dst"))
	require.NoError(t, a.Delete("dst"), "deleting a missing cache is not an error")
}

func redisClient(ep azurearm.RedisEndpoint, password string) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:     net.JoinHostPort(ep.Host, strconv.Itoa(ep.SSLPort)),
		Password: password,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		},
	})
}
