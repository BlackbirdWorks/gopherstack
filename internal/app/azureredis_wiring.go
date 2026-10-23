package app

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azureredis"
)

// wireAzureRedis gives ARM's Microsoft.Cache provider the Redis servers and lets caches import and export RDB
// files through the Blob Storage emulator.
func wireAzureRedis(byName map[string]service.Registerable) {
	redisH, ok := byName["AzureRedis"].(*azureredis.Handler)
	if !ok {
		return
	}

	if armH, armOk := byName["AzureARM"].(*azurearm.Handler); armOk {
		armH.Registry.SetRedis(&azureRedisAdapter{b: redisH.Backend})
	}

	if blob, blobOk := byName["AzureBlob"].(*azureblob.Handler); blobOk {
		redisH.Backend.SetBlobStore(azureRedisBlobs{b: blob.Backend})
	}
}

// azureRedisBlobs lets Azure Cache for Redis read and write RDB files in the Blob Storage emulator.
type azureRedisBlobs struct {
	b azureblob.StorageBackend
}

func (s azureRedisBlobs) ReadBlob(container, blob string) ([]byte, error) {
	_, data, err := s.b.GetBlobAt(container, blob, "")
	if err != nil {
		return nil, fmt.Errorf("read blob %s/%s: %w", container, blob, err)
	}

	return data, nil
}

func (s azureRedisBlobs) WriteBlob(container, blob string, data []byte) error {
	if _, err := s.b.PutBlobWith(container, blob, data, azureblob.BlobAttrs{}); err != nil {
		return fmt.Errorf("write blob %s/%s: %w", container, blob, err)
	}

	return nil
}

// azureRedisAdapter adapts services/azureredis's backend to services/azurearm.Redis.
type azureRedisAdapter struct {
	b *azureredis.InMemoryBackend
}

func redisEndpointOf(ep azureredis.Endpoint) azurearm.RedisEndpoint {
	return azurearm.RedisEndpoint{Host: ep.Host, Port: ep.Port, SSLPort: ep.SSLPort}
}

func redisKeysOf(k azureredis.Keys, err error) (azurearm.RedisKeys, error) {
	return azurearm.RedisKeys{Primary: k.Primary, Secondary: k.Secondary}, err
}

func (a *azureRedisAdapter) Ensure(id string, spec azurearm.RedisSpec) (azurearm.RedisEndpoint, error) {
	ep, err := a.b.Ensure(id, azureredis.Spec{
		SKU: spec.SKU, MinTLS: spec.MinTLS, EnableNonSSLPort: spec.EnableNonSSLPort,
		Persistence: spec.Persistence, AuthNotRequired: spec.AuthNotRequired,
	})

	return redisEndpointOf(ep), err
}

func (a *azureRedisAdapter) Endpoint(id string) (azurearm.RedisEndpoint, bool) {
	ep, ok := a.b.Endpoint(id)

	return redisEndpointOf(ep), ok
}

func (a *azureRedisAdapter) Delete(id string) error {
	return ignoreMissing(a.b.Delete(id), azureredis.ErrNotFound)
}

func (a *azureRedisAdapter) Keys(id string) (azurearm.RedisKeys, error) {
	return redisKeysOf(a.b.Keys(id))
}

func (a *azureRedisAdapter) RegenerateKey(id, keyType string) (azurearm.RedisKeys, error) {
	return redisKeysOf(a.b.RegenerateKey(id, keyType))
}

func (a *azureRedisAdapter) Reboot(id, rebootType string) error { return a.b.Reboot(id, rebootType) }

func (a *azureRedisAdapter) Import(id, format string, files []string) error {
	return a.b.Import(id, format, files)
}

func (a *azureRedisAdapter) Export(id, format, prefix, container string) error {
	return a.b.Export(id, format, prefix, container)
}
