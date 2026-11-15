package gcpredis

import (
	"math"
	"slices"
	"strconv"
	"strings"
)

type configKind int

const (
	cfgEnum configKind = iota
	cfgBool
	cfgInt
	cfgMaxMemoryGB
	cfgFlags
	cfgPercent
	cfgTimeout
	cfgDatabases
)

type configSpec struct {
	enum     []string
	since    int
	kind     configKind
	minValue int
}

const (
	maxConfigs      = 32
	v3              = 3
	v4              = 4
	v5              = 5
	v7              = 7
	minTimeout      = 120
	maxDatabases    = 100
	minMaxMemory    = 0.2
	maxPercent      = 100
	keyspaceFlagSet = "KEg$lshzxeA"
	cfgPolicy       = "maxmemory-policy"
)

// supportedConfigs are the redis_configs Memorystore accepts, with the first Redis major version supporting each
// (https://cloud.google.com/memorystore/docs/redis/supported-redis-configurations).
var supportedConfigs = map[string]configSpec{ //nolint:gochecknoglobals // immutable catalog
	"maxmemory-gb": {kind: cfgMaxMemoryGB, since: v3},
	cfgPolicy: {kind: cfgEnum, since: v3, enum: []string{
		"volatile-lru", "allkeys-lru", "volatile-lfu", "allkeys-lfu", "volatile-random", "allkeys-random",
		"volatile-ttl", "noeviction",
	}},
	"notify-keyspace-events":   {kind: cfgFlags, since: v3},
	"activedefrag":             {kind: cfgBool, since: v4},
	"databases":                {kind: cfgDatabases, since: v3},
	"lazyfree-lazy-eviction":   {kind: cfgBool, since: v7},
	"lazyfree-lazy-expire":     {kind: cfgBool, since: v7},
	"lazyfree-lazy-user-del":   {kind: cfgBool, since: v7},
	"lazyfree-lazy-user-flush": {kind: cfgBool, since: v7},
	"lfu-decay-time":           {kind: cfgInt, since: v4, minValue: 1},
	"lfu-log-factor":           {kind: cfgInt, since: v4, minValue: 1},
	"maxmemory-clients":        {kind: cfgPercent, since: v7},
	"stream-node-max-bytes":    {kind: cfgInt, since: v5},
	"stream-node-max-entries":  {kind: cfgInt, since: v5},
	"timeout":                  {kind: cfgTimeout, since: v3},
}

// lfuPolicies need Redis 4.0.
var lfuPolicies = []string{"volatile-lfu", "allkeys-lfu"} //nolint:gochecknoglobals // immutable catalog

func validateConfigs(configs map[string]string, version string, memoryGB int32) error {
	if len(configs) > maxConfigs {
		return errInvalid("redis_configs may hold at most %d entries.", maxConfigs)
	}

	major := majorVersion(version)

	for k, v := range configs {
		spec, ok := supportedConfigs[k]
		if !ok {
			return errInvalid("Redis config %q is not supported.", k)
		}

		if major < spec.since {
			return errInvalid("Redis config %q requires Redis %d.0 or newer.", k, spec.since)
		}

		if k == cfgPolicy && slices.Contains(lfuPolicies, v) && major < v4 {
			return errInvalid("Redis config %q value %q requires Redis 4.0 or newer.", k, v)
		}

		if err := spec.check(k, v, memoryGB); err != nil {
			return err
		}
	}

	return nil
}

func (s configSpec) check(key, val string, memoryGB int32) error {
	var ok bool

	switch s.kind {
	case cfgEnum:
		ok = slices.Contains(s.enum, val)
	case cfgBool:
		ok = val == "yes" || val == "no"
	case cfgInt:
		ok = intWithin(val, int64(s.minValue), math.MaxInt32)
	case cfgMaxMemoryGB:
		return checkMaxMemoryGB(key, val, memoryGB)
	case cfgFlags:
		ok = strings.Trim(val, keyspaceFlagSet) == ""
	case cfgPercent:
		_, ok = parsePercent(val)
	case cfgTimeout:
		ok = intWithin(val, 0, 0) || intWithin(val, minTimeout, math.MaxInt32)
	case cfgDatabases:
		ok = intWithin(val, 1, maxDatabases)
	}

	if !ok {
		return errInvalid("Invalid value %q for Redis config %q.", val, key)
	}

	return nil
}

func checkMaxMemoryGB(key, val string, memoryGB int32) error {
	f, err := strconv.ParseFloat(val, 64)
	low := minMaxMemory * float64(memoryGB)

	if err != nil || math.IsNaN(f) || f < low || f > float64(memoryGB) {
		return errInvalid(
			"Redis config %q must be between 20%% of the instance capacity (%g GiB) and the instance memory size (%d GiB).",
			key,
			low,
			memoryGB,
		)
	}

	return nil
}

func parsePercent(v string) (int, bool) {
	num, ok := strings.CutSuffix(v, "%")
	if !ok {
		return 0, false
	}

	n, err := strconv.Atoi(num)

	return n, err == nil && n >= 0 && n <= maxPercent
}

func intWithin(val string, lo, hi int64) bool {
	n, err := strconv.ParseInt(val, 10, 32)

	return err == nil && n >= lo && n <= hi
}
