package azurearm

import (
	"maps"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// redisSizeMB is a cache's memory size by family and capacity.
func redisSizeMB(family string, capacity int) int {
	c := []int{250, 1000, 2500, 6000, 13000, 26000, 53000}
	pr := []int{0, 6000, 13000, 26000, 53000, 120000}

	if family == "P" && capacity >= 1 && capacity < len(pr) {
		return pr[capacity]
	}

	if family == "C" && capacity >= 0 && capacity < len(c) {
		return c[capacity]
	}

	return c[0]
}

func redisMaxClients(family string, capacity int) int {
	c := []int{256, 1000, 2000, 5000, 10000, 15000, 20000}
	pr := []int{0, 7500, 15000, 30000, 40000, 40000}

	if family == "P" && capacity >= 1 && capacity < len(pr) {
		return pr[capacity]
	}

	if family == "C" && capacity >= 0 && capacity < len(c) {
		return c[capacity]
	}

	return c[0]
}

// redisDefaultConfig is what Azure reports for settings the caller did not choose; the memory reservations are
// a tenth of the cache size.
func redisDefaultConfig(rec *redisRecord) map[string]string {
	reserve := strconv.Itoa(max(2, redisSizeMB(rec.Family, rec.Capacity)/10)) //nolint:mnd // 10% reservation

	return map[string]string{
		"maxmemory-policy":                "volatile-lru",
		"maxclients":                      strconv.Itoa(redisMaxClients(rec.Family, rec.Capacity)),
		"maxmemory-reserved":              reserve,
		"maxfragmentationmemory-reserved": reserve,
		"maxmemory-delta":                 reserve,
	}
}

func redisEffectiveConfig(rec *redisRecord) map[string]string {
	out := redisDefaultConfig(rec)
	maps.Copy(out, rec.Config)

	return out
}

func redisSpecOf(rec *redisRecord) RedisSpec {
	cfg := redisEffectiveConfig(rec)
	persist := strings.EqualFold(cfg["rdb-backup-enabled"], "true") ||
		strings.EqualFold(cfg["aof-backup-enabled"], "true")

	return RedisSpec{
		SKU: rec.SKU, MinTLS: rec.MinTLS, EnableNonSSLPort: rec.NonSSL,
		Persistence:     rec.SKU == redisSKUPremium && persist,
		AuthNotRequired: strings.EqualFold(cfg["authnotrequired"], "true"),
	}
}

func (p *RedisProvider) provisioningState(rec *redisRecord) string {
	if p.now().Before(rec.ReadyAt) {
		return redisStateCreating
	}

	return provisioningStateSucceeded
}

func (p *RedisProvider) cacheBody(id ResourceID, rec *redisRecord) map[string]any {
	typed := map[string]any{
		fieldProvisioningState: p.provisioningState(rec),
		ehFieldSKU: map[string]any{
			fieldName:         rec.SKU,
			"family":          rec.Family,
			redisPropCapacity: rec.Capacity,
		},
		"enableNonSslPort":   rec.NonSSL,
		redisPropMinTLS:      rec.MinTLS,
		redisPropPublic:      rec.PublicAccess,
		"redisVersion":       rec.Version,
		"redisConfiguration": redisEffectiveConfig(rec),
		"hostName":           redisHostName(rec.Name),
		"linkedServers":      linkedRefs(rec),
	}

	p.addEndpoint(typed, id)

	out := map[string]any{
		"id": id.ARMID(), fieldName: rec.Name, fieldType: namespaceMicrosoftCache + "/Redis",
		fieldLocation: rec.Location, fieldTags: tagsOrEmpty(rec.Tags),
		fieldProperties: ehMerge(map[string]any{}, rec.Extra, typed),
	}

	if len(rec.Zones) > 0 {
		out["zones"] = rec.Zones
	}

	if len(rec.Identity) > 0 {
		out[acFieldIdentity] = redisIdentity(id, rec.Identity)
	}

	return out
}

// addEndpoint reports the data plane's real ports: sslPort is the TLS endpoint, port the plaintext one, present
// only while enableNonSslPort is true.
func (p *RedisProvider) addEndpoint(props map[string]any, id ResourceID) {
	ep, ok := p.dataPlane.Endpoint(redisKey(id))
	if !ok {
		return
	}

	inst := map[string]any{"sslPort": ep.SSLPort, "isMaster": true, "isPrimary": true}
	props["sslPort"] = ep.SSLPort

	if ep.Port != 0 {
		props["port"] = ep.Port
		inst["nonSslPort"] = ep.Port
	}

	props["instances"] = []any{inst}
}

func redisIdentity(id ResourceID, ident map[string]any) map[string]any {
	out := cloneAnyMap(ident)

	if t, _ := out[fieldType].(string); strings.Contains(strings.ToLower(t), "systemassigned") {
		out["principalId"] = principalIDFor(id)
		out["tenantId"] = DefaultTenantID
	}

	return out
}

func linkedRefs(rec *redisRecord) []any {
	out := make([]any, 0, len(rec.Linked))

	for _, ls := range sortedLinked(rec) {
		out = append(out, map[string]any{"id": ls.LinkedID})
	}

	return out
}

func principalIDFor(id ResourceID) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.ToLower(id.ARMID()))).String()
}
