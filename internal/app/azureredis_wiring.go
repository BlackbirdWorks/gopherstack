package app

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
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

		if armH.Issuer != nil {
			redisH.Backend.SetTokenValidator(redisEntraValidator(armH.Issuer))
		}
	}

	if blob, blobOk := byName["AzureBlob"].(*azureblob.Handler); blobOk {
		redisH.Backend.SetBlobStore(azureRedisBlobs{b: blob.Backend})
	}
}

const redisEntraAppID = "acca5fbb-b7e4-4009-81f1-37e38fd66d78"

// redisEntraValidator accepts tokens the emulator's own token service issued for the Redis resource and returns the
// principal's object ID.
func redisEntraValidator(iss *aadauth.Issuer) azureredis.TokenValidator {
	return func(token string) (string, bool) {
		claims, err := iss.Parse(token)
		if err != nil {
			return "", false
		}

		aud, _ := claims["aud"].(string)
		if a := strings.TrimSuffix(aud, "/"); a != "https://redis.azure.com" && a != redisEntraAppID {
			return "", false
		}

		oid, _ := claims["oid"].(string)

		return oid, oid != ""
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
	fw := make([]azureredis.IPRange, 0, len(spec.Firewall))
	for _, r := range spec.Firewall {
		fw = append(fw, azureredis.IPRange{Start: r.Start, End: r.End})
	}

	assignments := make([]azureredis.AccessAssignment, 0, len(spec.Assignments))
	for _, g := range spec.Assignments {
		assignments = append(assignments, azureredis.AccessAssignment{
			ObjectID: g.ObjectID, Alias: g.Alias, Permissions: g.Permissions,
		})
	}

	ep, err := a.b.Ensure(id, azureredis.Spec{
		SKU: spec.SKU, MinTLS: spec.MinTLS, EnableNonSSLPort: spec.EnableNonSSLPort,
		Persistence: spec.Persistence, AuthNotRequired: spec.AuthNotRequired, Firewall: fw,
		DisableAccessKeys: spec.DisableAccessKeys, EntraEnabled: spec.EntraEnabled, Assignments: assignments,
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
