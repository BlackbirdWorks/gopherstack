package azurearm

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type redisSKUSpec struct {
	name     string
	family   string
	capacity int
}

func redisSKUOf(props map[string]any) (redisSKUSpec, error) {
	raw, _ := props[ehFieldSKU].(map[string]any)
	name, _ := raw[fieldName].(string)
	family, _ := raw["family"].(string)
	capacity, hasCap := raw[redisPropCapacity].(float64)

	if name == "" || family == "" || !hasCap {
		return redisSKUSpec{}, fmt.Errorf("%w: properties.sku requires name, family and capacity", ErrRedisInvalid)
	}

	s := redisSKUSpec{name: canonicalSKU(name), family: strings.ToUpper(family), capacity: int(capacity)}

	return s, s.validate()
}

func canonicalSKU(name string) string {
	for _, n := range []string{redisSKUBasic, redisSKUStandard, redisSKUPremium} {
		if strings.EqualFold(name, n) {
			return n
		}
	}

	return name
}

func (s redisSKUSpec) validate() error {
	premium := s.name == redisSKUPremium

	switch {
	case s.name != redisSKUBasic && s.name != redisSKUStandard && !premium:
		return fmt.Errorf("%w: sku name must be Basic, Standard or Premium", ErrRedisInvalid)
	case premium && (s.family != "P" || s.capacity < 1 || s.capacity > redisMaxPremium):
		return fmt.Errorf("%w: a Premium cache needs family P and capacity 1 to %d", ErrRedisInvalid, redisMaxPremium)
	case !premium && (s.family != "C" || s.capacity < 0 || s.capacity > redisMaxStandard):
		return fmt.Errorf("%w: a Basic or Standard cache needs family C and capacity 0 to %d",
			ErrRedisInvalid, redisMaxStandard)
	}

	return nil
}

const (
	redisMaxPremium  = 5
	redisMaxStandard = 6
)

func validRedisName(name string) error {
	if len(name) < 1 || len(name) > 63 || !redisNameRE.MatchString(name) {
		return fmt.Errorf("%w: the cache name must be 1-63 letters, digits and single hyphens", ErrRedisInvalid)
	}

	return nil
}

func validTLSVersion(v string) error {
	switch v {
	case "", "1.0", "1.1", redisDefaultTLS:
		return nil
	}

	return fmt.Errorf("%w: minimumTlsVersion must be 1.0, 1.1 or 1.2", ErrRedisInvalid)
}

func validPublicAccess(v string) error {
	switch v {
	case "", redisDefaultPublic, redisPublicDisabled:
		return nil
	}

	return fmt.Errorf("%w: publicNetworkAccess must be Enabled or Disabled", ErrRedisInvalid)
}

// redisConfigOf reads redisConfiguration, stringifying scalars (Azure carries every value as a string).
func redisConfigOf(v any) (map[string]string, error) {
	m, _ := v.(map[string]any)
	if len(m) > redisMaxConfigEntries {
		return nil, fmt.Errorf("%w: at most %d redisConfiguration entries", ErrRedisInvalid, redisMaxConfigEntries)
	}

	out := make(map[string]string, len(m))

	for k, val := range m {
		switch x := val.(type) {
		case string:
			out[k] = x
		case bool:
			out[k] = strconv.FormatBool(x)
		case float64:
			out[k] = strconv.FormatFloat(x, 'f', -1, 64)
		}
	}

	return out, nil
}

var redisOwnedProps = []string{ //nolint:gochecknoglobals // read-only list
	ehFieldSKU, "enableNonSslPort", redisPropMinTLS, redisPropPublic, "redisConfiguration", "redisVersion",
	"hostName", "port", "sslPort", "accessKeys", "instances", "linkedServers", "privateEndpointConnections",
}

// applyProps copies the request's modelled properties onto rec; fields the request omits keep their value.
func (p *RedisProvider) applyProps(rec *redisRecord, props map[string]any) error {
	if _, ok := props[ehFieldSKU]; ok {
		sku, err := redisSKUOf(props)
		if err != nil {
			return err
		}

		rec.SKU, rec.Family, rec.Capacity = sku.name, sku.family, sku.capacity
	}

	if v, ok := props["enableNonSslPort"].(bool); ok {
		rec.NonSSL = v
	}

	if err := applyStringProps(rec, props); err != nil {
		return err
	}

	if cfg, ok := props["redisConfiguration"]; ok {
		merged, err := redisConfigOf(cfg)
		if err != nil {
			return err
		}

		if rec.Config == nil {
			rec.Config = map[string]string{}
		}

		maps.Copy(rec.Config, merged)
	}

	maps.Copy(rec.Extra, ehExtra(map[string]any{fieldProperties: props}, redisOwnedProps...))

	return nil
}

func applyStringProps(rec *redisRecord, props map[string]any) error {
	if v, ok := props[redisPropMinTLS].(string); ok {
		if err := validTLSVersion(v); err != nil {
			return err
		}

		rec.MinTLS = v
	}

	if v, ok := props[redisPropPublic].(string); ok {
		if err := validPublicAccess(v); err != nil {
			return err
		}

		rec.PublicAccess = v
	}

	if v, ok := props["redisVersion"].(string); ok && v != "" {
		rec.Version = v
	}

	return nil
}

// newRecord returns a working copy of the cache's record (deep for its maps), or a fresh one.
func (p *RedisProvider) newRecord(id ResourceID) *redisRecord {
	now := p.now()
	rec := &redisRecord{
		Name: id.LeafName(), ResourceGrp: id.ResourceGroup, Location: DefaultLocation, MinTLS: redisDefaultTLS,
		PublicAccess: redisDefaultPublic, Version: redisDefaultVersion, CreatedAt: now.UTC(),
		ReadyAt: now.Add(redisCreateDuration), Extra: map[string]any{}, Config: map[string]string{},
	}

	if existing, ok := p.res[redisKey(id)]; ok {
		*rec = *existing
		rec.Extra, rec.Config, rec.Tags = cloneAnyMap(
			existing.Extra,
		), cloneStrMap(
			existing.Config,
		), cloneStrMap(
			existing.Tags,
		)
		rec.Firewall, rec.Linked = maps.Clone(existing.Firewall), maps.Clone(existing.Linked)
	}

	return rec
}

func cloneAnyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	maps.Copy(out, m)

	return out
}

func cloneStrMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	return maps.Clone(m)
}

func newOperationID() string { return uuid.NewString() }

func (p *RedisProvider) applyEnvelope(rec *redisRecord, body map[string]any) {
	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.Location = loc
	}

	if _, ok := body[fieldTags]; ok {
		rec.Tags = stringTags(body[fieldTags])
	}

	if z, ok := body["zones"].([]any); ok {
		rec.Zones = z
	}

	if ident, ok := body[acFieldIdentity].(map[string]any); ok && len(ident) > 0 {
		rec.Identity = ident
	}
}

func (p *RedisProvider) putCache(id ResourceID, body map[string]any) (map[string]any, error) {
	if err := validRedisName(id.LeafName()); err != nil {
		return nil, badRedisRequest(err)
	}

	if p.nameTakenElsewhere(id) {
		return nil, ErrRedisNameTaken
	}

	props, _ := body[fieldProperties].(map[string]any)
	if _, ok := props[ehFieldSKU]; !ok {
		if _, exists := p.res[redisKey(id)]; !exists {
			return nil, badRedisRequest(fmt.Errorf("%w: properties.sku is required", ErrRedisInvalid))
		}
	}

	rec := p.newRecord(id)
	rec.Extra, rec.Config = map[string]any{}, map[string]string{}

	return p.commit(id, rec, body, props)
}

// commit applies the request to rec, ensures the data plane matches and stores the record.
func (p *RedisProvider) commit(id ResourceID, rec *redisRecord, body, props map[string]any) (map[string]any, error) {
	p.applyEnvelope(rec, body)

	if err := p.applyProps(rec, props); err != nil {
		return nil, badRedisRequest(err)
	}

	if _, err := p.dataPlane.Ensure(redisKey(id), redisSpecOf(rec)); err != nil {
		return nil, badRedisRequest(err)
	}

	p.res[redisKey(id)] = rec

	return p.cacheBody(id, rec), nil
}

// Patch merges an Update request into an existing cache.
func (p *RedisProvider) Patch(id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("Patch")
	defer p.mu.Unlock()

	existing, err := p.cache(id)
	if err != nil {
		return nil, err
	}

	rec := p.newRecord(id)
	rec.ReadyAt = existing.ReadyAt
	props, _ := body[fieldProperties].(map[string]any)

	return p.commit(id, rec, body, props)
}

// CreateOperation registers an async operation that completes at readyAt and returns its ID.
func (p *RedisProvider) CreateOperation(readyAt time.Time) string {
	p.opMu.Lock()
	defer p.opMu.Unlock()

	id := newOperationID()

	p.ops[id] = readyAt
	if p.opOrder = append(p.opOrder, id); len(p.opOrder) > redisMaxOperations {
		delete(p.ops, p.opOrder[0])
		p.opOrder = p.opOrder[1:]
	}

	return id
}

// OperationDone reports whether an operation has completed; unknown (aged-out) operations are complete.
func (p *RedisProvider) OperationDone(id string) bool {
	p.opMu.Lock()
	ready, ok := p.ops[id]
	p.opMu.Unlock()

	return !ok || !p.now().Before(ready)
}

// ReadyAt is when the cache's current provisioning completes.
func (p *RedisProvider) ReadyAt(id ResourceID) time.Time {
	p.mu.RLock("ReadyAt")
	defer p.mu.RUnlock()

	if rec, ok := p.res[redisKey(id)]; ok {
		return rec.ReadyAt
	}

	return p.now()
}
