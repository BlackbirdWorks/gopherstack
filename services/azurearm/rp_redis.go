package azurearm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftCache = "Microsoft.Cache"
	redisType               = "redis"
	redisFirewallType       = "firewallRules"
	redisPatchType          = "patchSchedules"
	redisLinkedType         = "linkedServers"
	redisHostSuffix         = ".redis.cache.windows.net"
	redisStateCreating      = "Creating"
	redisCreateDuration     = 500 * time.Millisecond
	redisMaxOperations      = 1024
	redisMaxChildren        = 1000
	redisMaxConfigEntries   = 64
	redisDefaultTLS         = "1.2"
	redisDefaultVersion     = "6.0"
	redisDefaultPublic      = "Enabled"
	redisSKUBasic           = "Basic"
	redisSKUStandard        = "Standard"
	redisSKUPremium         = "Premium"
	redisPatchDefault       = "default"
	redisPublicDisabled     = "Disabled"
	redisPropMinTLS         = "minimumTlsVersion"
	redisPropPublic         = "publicNetworkAccess"
	redisPropCapacity       = "capacity"
	redisPropStatus         = "status"
	redisPropPrimaryKey     = "primaryKey"
	redisPropSecondaryKey   = "secondaryKey"
	redisCodeConflict       = "Conflict"
	redisCodeUnavailable    = "ServiceUnavailable"
)

func redisAPIVersions() []string {
	return []string{"2024-11-01", "2024-03-01", "2023-08-01", "2023-04-01", "2020-12-01"}
}

var redisNameRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9]|-[A-Za-z0-9])*$`)

type redisKind int

const (
	redisKindUnsupported redisKind = iota
	redisKindCache
	redisKindFirewall
	redisKindPatch
	redisKindLinked
)

type redisFirewall struct {
	Start string `json:"start"`
	End   string `json:"end"`
	Name  string `json:"name"`
}

type redisLinked struct {
	LinkedID string `json:"linkedId"`
	Location string `json:"location"`
	Role     string `json:"role"`
	Name     string `json:"name"`
}

// redisRecord is the ARM-side state of one cache. Keys, ports and data live in the Redis data plane.
type redisRecord struct {
	CreatedAt    time.Time                `json:"createdAt"`
	ReadyAt      time.Time                `json:"readyAt"`
	Tags         map[string]string        `json:"tags,omitempty"`
	Extra        map[string]any           `json:"extra,omitempty"`
	Config       map[string]string        `json:"config,omitempty"`
	Identity     map[string]any           `json:"identity,omitempty"`
	Firewall     map[string]redisFirewall `json:"firewall,omitempty"`
	Linked       map[string]redisLinked   `json:"linked,omitempty"`
	Name         string                   `json:"name"`
	ResourceGrp  string                   `json:"resourceGroup"`
	Location     string                   `json:"location"`
	SKU          string                   `json:"sku"`
	Family       string                   `json:"family"`
	MinTLS       string                   `json:"minTls"`
	PublicAccess string                   `json:"publicAccess"`
	Version      string                   `json:"version"`
	Patch        []any                    `json:"patch,omitempty"`
	Zones        []any                    `json:"zones,omitempty"`
	Capacity     int                      `json:"capacity"`
	NonSSL       bool                     `json:"nonSsl,omitempty"`
}

// RedisProvider implements ResourceProvider for Microsoft.Cache/redis. ARM keeps the resource metadata,
// firewall rules, patch schedule and linked servers; the Redis servers, access keys and data live behind the
// Redis seam.
type RedisProvider struct {
	now       func() time.Time
	dataPlane Redis
	mu        *lockmetrics.RWMutex
	res       map[string]*redisRecord
	ops       map[string]time.Time
	opOrder   []string
	opMu      sync.Mutex
}

// NewRedisProvider creates a RedisProvider; dataPlane may be nil.
func NewRedisProvider(dataPlane Redis) *RedisProvider {
	if dataPlane == nil {
		dataPlane = noopRedis{}
	}

	return &RedisProvider{
		now: time.Now, dataPlane: dataPlane, mu: lockmetrics.New("azurearm.redisprovider"),
		res: map[string]*redisRecord{}, ops: map[string]time.Time{},
	}
}

var _ ResourceProvider = (*RedisProvider)(nil)

// Namespace implements ResourceProvider.
func (p *RedisProvider) Namespace() string { return namespaceMicrosoftCache }

// ResourceTypes implements ResourceProvider.
func (p *RedisProvider) ResourceTypes() []ResourceTypeDef {
	v := redisAPIVersions()

	return []ResourceTypeDef{
		{Type: redisType, APIVersions: v, HasChildren: true},
		{Type: redisType + "/" + redisFirewallType, APIVersions: v},
		{Type: redisType + "/" + redisPatchType, APIVersions: v},
		{Type: redisType + "/" + redisLinkedType, APIVersions: v},
	}
}

func classifyRedisPath(id ResourceID) redisKind {
	switch strings.ToLower(strings.Join(id.Types, "/")) {
	case redisType:
		return redisKindCache
	case strings.ToLower(redisType + "/" + redisFirewallType):
		return redisKindFirewall
	case strings.ToLower(redisType + "/" + redisPatchType):
		return redisKindPatch
	case strings.ToLower(redisType + "/" + redisLinkedType):
		return redisKindLinked
	}

	return redisKindUnsupported
}

func errUnsupportedRedisType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.Cache resource type %q", ErrResourceNotFound, id.ResourceType())
}

func redisKey(id ResourceID) string {
	return strings.ToLower(ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: namespaceMicrosoftCache,
		Types: []string{redisType}, Names: id.Names[:1],
	}.ARMID())
}

func badRedisRequest(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidRequestBody, err)
}

func (p *RedisProvider) cache(id ResourceID) (*redisRecord, error) {
	rec, ok := p.res[redisKey(id)]
	if !ok {
		return nil, ErrRedisNotFound
	}

	return rec, nil
}

// Put implements ResourceProvider.
func (p *RedisProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	kind := classifyRedisPath(id)

	p.mu.Lock("Put")
	defer p.mu.Unlock()

	switch kind {
	case redisKindCache:
		return p.putCache(id, body)
	case redisKindFirewall:
		return p.putFirewall(id, body)
	case redisKindPatch:
		return p.putPatch(id, body)
	case redisKindLinked:
		return p.putLinked(id, body)
	case redisKindUnsupported:
	}

	return nil, errUnsupportedRedisType(id)
}

// Get implements ResourceProvider.
func (p *RedisProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	kind := classifyRedisPath(id)

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	if kind == redisKindUnsupported {
		return nil, errUnsupportedRedisType(id)
	}

	rec, err := p.cache(id)
	if err != nil {
		return nil, err
	}

	return p.childBody(id, kind, rec)
}

func (p *RedisProvider) childBody(id ResourceID, kind redisKind, rec *redisRecord) (map[string]any, error) {
	switch kind {
	case redisKindFirewall:
		fw, ok := rec.Firewall[strings.ToLower(id.LeafName())]
		if !ok {
			return nil, ErrRedisFirewallNotFound
		}

		return firewallBody(id, rec, fw), nil
	case redisKindPatch:
		return p.patchBody(id, rec)
	case redisKindLinked:
		ls, ok := rec.Linked[strings.ToLower(id.LeafName())]
		if !ok {
			return nil, ErrRedisLinkedNotFound
		}

		return linkedBody(id, rec, ls), nil
	case redisKindCache, redisKindUnsupported:
	}

	return p.cacheBody(id, rec), nil
}

// Delete implements ResourceProvider.
func (p *RedisProvider) Delete(_ context.Context, id ResourceID) error {
	kind := classifyRedisPath(id)

	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	switch kind {
	case redisKindCache:
		return p.deleteCache(id)
	case redisKindFirewall, redisKindPatch, redisKindLinked:
		return p.deleteChild(id, kind)
	case redisKindUnsupported:
	}

	return errUnsupportedRedisType(id)
}

func (p *RedisProvider) deleteCache(id ResourceID) error {
	key := redisKey(id)
	if _, ok := p.res[key]; !ok {
		return ErrRedisNotFound
	}

	delete(p.res, key)

	return p.dataPlane.Delete(key)
}

// List implements ResourceProvider; only caches are listable here.
func (p *RedisProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	var out []map[string]any

	if classifyRedisPath(id) == redisKindCache {
		for _, rec := range p.res {
			if id.ResourceGroup != "" && !resourceGroupsEqual(rec.ResourceGrp, id.ResourceGroup) {
				continue
			}

			rid := ResourceID{
				SubscriptionID: id.SubscriptionID, ResourceGroup: rec.ResourceGrp,
				Namespace: namespaceMicrosoftCache, Types: []string{redisType}, Names: []string{rec.Name},
			}
			out = append(out, p.cacheBody(rid, rec))
		}
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

// Reset implements ResourceProvider.
func (p *RedisProvider) Reset() {
	p.mu.Lock("Reset")
	p.res = map[string]*redisRecord{}
	p.mu.Unlock()

	p.opMu.Lock()
	p.ops, p.opOrder = map[string]time.Time{}, nil
	p.opMu.Unlock()
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *RedisProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for key, rec := range p.res {
		if resourceGroupsEqual(rec.ResourceGrp, resourceGroup) {
			delete(p.res, key)
			_ = p.dataPlane.Delete(key)
		}
	}
}

func redisHostName(name string) string { return name + redisHostSuffix }

func (p *RedisProvider) nameTakenElsewhere(id ResourceID) bool {
	for _, r := range p.res {
		if strings.EqualFold(r.Name, id.LeafName()) && !resourceGroupsEqual(r.ResourceGrp, id.ResourceGroup) {
			return true
		}
	}

	return false
}

func redisErrorDetails(err error) (errorEntry, bool) {
	table := []struct {
		err   error
		entry errorEntry
	}{
		{ErrRedisNotFound, notFoundEntry("The Redis cache was not found.")},
		{ErrRedisFirewallNotFound, notFoundEntry("The firewall rule was not found.")},
		{ErrRedisPatchNotFound, notFoundEntry("There are no patch schedules found for the Redis cache.")},
		{ErrRedisLinkedNotFound, notFoundEntry("The linked server was not found.")},
		{
			ErrRedisNameTaken,
			errorEntry{redisCodeConflict, "The Redis cache name is already in use.", http.StatusConflict},
		},
		{ErrRedisUnavailable, errorEntry{
			redisCodeUnavailable, "The Redis data plane is not available.", http.StatusServiceUnavailable,
		}},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	return errorEntry{}, false
}
