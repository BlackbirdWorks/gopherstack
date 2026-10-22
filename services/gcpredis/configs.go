package gcpredis

import (
	"slices"
	"strconv"
	"strings"
)

type configKind int

const (
	cfgEnum configKind = iota
	cfgBool
	cfgInt
	cfgFloat
	cfgFlags
)

type configSpec struct {
	enum     []string
	since    int
	kind     configKind
	minValue int
}

const (
	maxConfigs = 32
	v3         = 3
	v4         = 4
	v5         = 5
	v6         = 6
)

// supportedConfigs are the redis_configs Memorystore accepts, with the first Redis major version supporting each.
var supportedConfigs = map[string]configSpec{ //nolint:gochecknoglobals // immutable catalog
	"maxmemory-policy": {kind: cfgEnum, since: v3, enum: []string{
		"volatile-lru", "allkeys-lru", "volatile-lfu", "allkeys-lfu", "volatile-random", "allkeys-random",
		"volatile-ttl", "noeviction",
	}},
	"notify-keyspace-events":   {kind: cfgFlags, since: v3},
	"activedefrag":             {kind: cfgBool, since: v4},
	"lfu-decay-time":           {kind: cfgInt, since: v4},
	"lfu-log-factor":           {kind: cfgInt, since: v4},
	"maxmemory-gb":             {kind: cfgFloat, since: v4},
	"lazyfree-lazy-eviction":   {kind: cfgBool, since: v4},
	"lazyfree-lazy-expire":     {kind: cfgBool, since: v4},
	"lazyfree-lazy-user-del":   {kind: cfgBool, since: v6},
	"stream-node-max-bytes":    {kind: cfgInt, since: v5},
	"stream-node-max-entries":  {kind: cfgInt, since: v5},
	"replica-ignore-maxmemory": {kind: cfgBool, since: v5},
	"slowlog-log-slower-than":  {kind: cfgInt, since: v3, minValue: -1},
	"slowlog-max-len":          {kind: cfgInt, since: v3},
}

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

		if err := spec.check(k, v, memoryGB); err != nil {
			return err
		}
	}

	return nil
}

func (s configSpec) check(key, val string, memoryGB int32) error {
	switch s.kind {
	case cfgEnum:
		if !slices.Contains(s.enum, val) {
			return errInvalid("Invalid value %q for Redis config %q.", val, key)
		}
	case cfgBool:
		if val != "yes" && val != "no" {
			return errInvalid("Redis config %q must be yes or no.", key)
		}
	case cfgInt:
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil || n < int64(s.minValue) {
			return errInvalid("Invalid value %q for Redis config %q.", val, key)
		}
	case cfgFloat:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil || f < 1 || f > float64(memoryGB) {
			return errInvalid("Redis config %q must be between 1 and the instance memory size (%d GiB).", key, memoryGB)
		}
	case cfgFlags:
		if strings.Trim(val, "KEg$lshzxetdmA") != "" {
			return errInvalid("Invalid value %q for Redis config %q.", val, key)
		}
	}

	return nil
}
