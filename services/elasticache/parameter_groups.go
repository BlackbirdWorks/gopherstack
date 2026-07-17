package elasticache

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// builtinParameterGroupFamilies returns the well-known default parameter group families.
func builtinParameterGroupFamilies() []struct{ family, name string } {
	return []struct{ family, name string }{
		{familyRedis7, "default.redis7"},
		{"redis6.x", "default.redis6.x"},
		{"redis5.0", "default.redis5.0"},
		{"redis4.0", "default.redis4.0"},
		{"redis3.2", "default.redis3.2"},
		{"redis2.8", "default.redis2.8"},
		{"memcached1.6", "default.memcached1.6"},
		{"memcached1.5", "default.memcached1.5"},
		{familyValkey8, "default.valkey8"},
		{familyValkey7, "default.valkey7"},
	}
}

// initDefaultParameterGroups seeds the well-known default parameter groups for the default region.
func (b *InMemoryBackend) initDefaultParameterGroups() {
	b.initDefaultParameterGroupsForRegion(b.region)
}

// initDefaultParameterGroupsForRegion seeds default parameter groups for the given region.
// Callers must NOT hold b.mu (it allocates directly into the map).
func (b *InMemoryBackend) initDefaultParameterGroupsForRegion(region string) {
	t := b.parameterGroups[region]
	if t == nil {
		t = store.New(cacheParameterGroupKeyFn)
		b.parameterGroups[region] = t
	}

	for _, dpg := range builtinParameterGroupFamilies() {
		pg := &CacheParameterGroup{
			Name:        dpg.name,
			Family:      dpg.family,
			Description: "Default parameter group for " + dpg.family,
			ARN:         buildARN("parametergroup:"+dpg.name, region, b.accountID),
			IsGlobal:    true,
			Parameters:  make(map[string]string),
			Tags:        tags.New("elasticache.pg." + dpg.name + ".tags"),
		}
		t.Put(pg)
	}
}

// The following lazy per-region store helpers return the resource map for the
// given region, creating it on first use. Callers must hold b.mu.

func (b *InMemoryBackend) parameterGroupARN(name string) string {
	return arn.Build("elasticache", b.region, b.accountID, "parametergroup:"+name)
}

func validateParamGroupFamily(engine, family string) error {
	switch engine {
	case engineMemcached:
		if !strings.HasPrefix(family, engineMemcached) {
			return fmt.Errorf(
				"parameter group family %q does not match engine memcached: %w",
				family,
				ErrInvalidParameterGroupFamily,
			)
		}
	case engineValkey:
		if !strings.HasPrefix(family, engineValkey) {
			return fmt.Errorf(
				"parameter group family %q does not match engine valkey: %w",
				family,
				ErrInvalidParameterGroupFamily,
			)
		}
	default:
		if !strings.HasPrefix(family, "redis") {
			return fmt.Errorf(
				"parameter group family %q does not match engine redis: %w",
				family,
				ErrInvalidParameterGroupFamily,
			)
		}
	}

	return nil
}

// CreateParameterGroup creates a new cache parameter group.
func (b *InMemoryBackend) CreateParameterGroup(
	ctx context.Context,
	name, family, description string,
) (*CacheParameterGroup, error) {
	b.mu.Lock("CreateParameterGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.parameterGroupsStore(region)
	if _, exists := tbl.Get(name); exists {
		return nil, ErrParameterGroupAlreadyExists
	}

	pg := &CacheParameterGroup{
		Name:        name,
		Family:      family,
		Description: description,
		ARN:         buildARN("parametergroup:"+name, region, b.accountID),
		IsGlobal:    false,
		Parameters:  make(map[string]string),
		Tags:        tags.New("elasticache.pg." + name + ".tags"),
	}
	tbl.Put(pg)

	return pg, nil
}

// DeleteParameterGroup removes a cache parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteParameterGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.parameterGroupsStore(region)
	pg, exists := tbl.Get(name)
	if !exists {
		return ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return ErrParameterGroupDefaultNotModifiable
	}

	pg.Tags.Close()
	tbl.Delete(name)

	return nil
}

// DescribeParameterGroups returns one parameter group by name, or a paginated list of all.
func (b *InMemoryBackend) DescribeParameterGroups(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[CacheParameterGroup], error) {
	b.mu.RLock("DescribeParameterGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.parameterGroupsStore(region), name, ErrParameterGroupNotFound, nil,
		func(pg CacheParameterGroup) string { return pg.Name }, marker, maxRecords)
}

// ModifyParameterGroup updates parameters in a cache parameter group.
func (b *InMemoryBackend) ModifyParameterGroup(
	ctx context.Context,
	name string,
	params map[string]string,
) (*CacheParameterGroup, error) {
	b.mu.Lock("ModifyParameterGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	pg, exists := b.parameterGroupsStore(region).Get(name)
	if !exists {
		return nil, ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return nil, ErrParameterGroupDefaultNotModifiable
	}

	maps.Copy(pg.Parameters, params)

	cp := *pg

	return &cp, nil
}

// ResetParameterGroup resets parameters in a cache parameter group to defaults.
func (b *InMemoryBackend) ResetParameterGroup(
	ctx context.Context,
	name string,
	paramNames []string,
	resetAll bool,
) (*CacheParameterGroup, error) {
	b.mu.Lock("ResetParameterGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	pg, exists := b.parameterGroupsStore(region).Get(name)
	if !exists {
		return nil, ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return nil, ErrParameterGroupDefaultNotModifiable
	}

	if resetAll {
		pg.Parameters = make(map[string]string)
	} else {
		for _, pname := range paramNames {
			delete(pg.Parameters, pname)
		}
	}

	cp := *pg

	return &cp, nil
}

// DescribeParameters lists parameters in a cache parameter group.
func (b *InMemoryBackend) DescribeParameters(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[CacheParameter], error) {
	b.mu.RLock("DescribeParameters")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	pg, exists := b.parameterGroupsStore(region).Get(name)
	if !exists {
		return page.Page[CacheParameter]{}, ErrParameterGroupNotFound
	}

	out := make([]CacheParameter, 0, len(pg.Parameters))
	for k, v := range pg.Parameters {
		out = append(out, CacheParameter{
			Name:         k,
			Value:        v,
			DataType:     dataTypeString,
			IsModifiable: true,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// DescribeEngineDefaultParameters returns the default parameters for a parameter group family.
func (b *InMemoryBackend) DescribeEngineDefaultParameters(
	_ context.Context,
	cacheParameterGroupFamily string,
	marker string,
	maxRecords int,
) (page.Page[CacheParameter], error) {
	b.mu.RLock("DescribeEngineDefaultParameters")
	defer b.mu.RUnlock()

	return page.New(
		builtinEngineDefaultParameters(cacheParameterGroupFamily),
		marker,
		maxRecords,
		elasticacheDefaultMaxRecords,
	), nil
}

// builtinEngineDefaultParameters returns well-known default parameters for a given family.
func builtinEngineDefaultParameters(family string) []CacheParameter {
	switch {
	case strings.HasPrefix(family, "memcached"):
		return builtinMemcachedDefaultParameters()
	case strings.HasPrefix(family, "valkey"):
		return builtinValkeyDefaultParameters()
	default:
		return builtinRedisDefaultParameters()
	}
}

func builtinMemcachedDefaultParameters() []CacheParameter {
	return []CacheParameter{
		{
			Name:          "max_item_size",
			Value:         "1048576",
			Description:   "Maximum size of a single item",
			DataType:      dataTypeInteger,
			AllowedValues: "1-1073741824",
			IsModifiable:  true,
		},
		{
			Name:          "chunk_size",
			Value:         "48",
			Description:   "Minimum space allocated for key+value+flags",
			DataType:      dataTypeInteger,
			AllowedValues: "1-4096",
			IsModifiable:  true,
		},
		{
			Name:          "max_simultaneous_connections",
			Value:         "65000",
			Description:   "Maximum number of simultaneous connections",
			DataType:      dataTypeInteger,
			AllowedValues: "1-65000",
			IsModifiable:  true,
		},
		{
			Name:          "backlog_queue_limit",
			Value:         "1024",
			Description:   "TCP backlog queue limit",
			DataType:      dataTypeInteger,
			AllowedValues: "1-10000",
			IsModifiable:  true,
		},
		{
			Name:          "cas_disabled",
			Value:         "0",
			Description:   "Disable CAS command",
			DataType:      dataTypeInteger,
			AllowedValues: "0,1",
			IsModifiable:  true,
		},
		{
			Name:          "loglevel",
			Value:         "notice",
			Description:   "Log verbosity level",
			DataType:      dataTypeString,
			AllowedValues: "notice,verbose,debug,trace",
			IsModifiable:  true,
		},
	}
}

func builtinValkeyDefaultParameters() []CacheParameter {
	return []CacheParameter{
		{
			Name:          "maxmemory-policy",
			Value:         "noeviction",
			Description:   "Eviction policy when memory is full",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesEvictionPolicy,
			IsModifiable:  true,
		},
		{
			Name:          "hz",
			Value:         "10",
			Description:   "Background task frequency in Hz",
			DataType:      dataTypeInteger,
			AllowedValues: "1-500",
			IsModifiable:  true,
		},
		{
			Name:          "timeout",
			Value:         "0",
			Description:   "Client idle timeout in seconds (0=disabled)",
			DataType:      dataTypeInteger,
			AllowedValues: allowedValuesMaxInt32,
			IsModifiable:  true,
		},
		{
			Name:          "tcp-keepalive",
			Value:         "300",
			Description:   "TCP keepalive interval in seconds",
			DataType:      dataTypeInteger,
			AllowedValues: allowedValuesMaxInt32,
			IsModifiable:  true,
		},
		{
			Name:          "maxmemory-samples",
			Value:         "5",
			Description:   "Samples for LRU/LFU approximation",
			DataType:      dataTypeInteger,
			AllowedValues: "1-64",
			IsModifiable:  true,
		},
		{
			Name:          "activerehashing",
			Value:         "yes",
			Description:   "Active rehashing",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
		{
			Name:          "lazyfree-lazy-eviction",
			Value:         "no",
			Description:   "Lazy free on eviction",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
		{
			Name:          "lazyfree-lazy-expire",
			Value:         "no",
			Description:   "Lazy free on expiry",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
	}
}

func builtinRedisDefaultParameters() []CacheParameter {
	return append(builtinValkeyDefaultParameters(), builtinRedisPersistenceParameters()...)
}

func builtinRedisPersistenceParameters() []CacheParameter {
	return []CacheParameter{
		{
			Name:          "appendonly",
			Value:         "no",
			Description:   "Enable AOF persistence",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
		{
			Name:          "appendfsync",
			Value:         "everysec",
			Description:   "AOF fsync policy",
			DataType:      dataTypeString,
			AllowedValues: "always,everysec,no",
			IsModifiable:  true,
		},
		{
			Name:          "no-appendfsync-on-rewrite",
			Value:         "no",
			Description:   "Disable fsync during BGSAVE/BGREWRITEAOF",
			DataType:      dataTypeString,
			AllowedValues: allowedValuesYesNo,
			IsModifiable:  true,
		},
		{
			Name:          "auto-aof-rewrite-percentage",
			Value:         "100",
			Description:   "Min AOF growth percent before rewrite",
			DataType:      dataTypeInteger,
			AllowedValues: "0-",
			IsModifiable:  true,
		},
		{
			Name:          "auto-aof-rewrite-min-size",
			Value:         "67108864",
			Description:   "Min AOF size before rewrite (bytes)",
			DataType:      dataTypeInteger,
			AllowedValues: "0-",
			IsModifiable:  true,
		},
		{
			Name:          "slowlog-log-slower-than",
			Value:         "10000",
			Description:   "Slow log threshold in microseconds",
			DataType:      dataTypeInteger,
			AllowedValues: "-1-",
			IsModifiable:  true,
		},
		{
			Name:          "slowlog-max-len",
			Value:         "128",
			Description:   "Maximum slow log entries",
			DataType:      dataTypeInteger,
			AllowedValues: "0-",
			IsModifiable:  true,
		},
		{
			Name:          "latency-monitor-threshold",
			Value:         "0",
			Description:   "Latency monitor threshold in milliseconds (0=disabled)",
			DataType:      dataTypeInteger,
			AllowedValues: "0-",
			IsModifiable:  true,
		},
	}
}
