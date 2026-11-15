package gcpredis

import (
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/redis/apiv1/redispb"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

const (
	bytesPerGB       = int64(1) << 30
	defaultPolicy    = "volatile-lru"
	minutesPerHour   = 60
	secondsPerMinute = 60
)

// redisVersionString maps the API enum to the version the instance reports in INFO server.
func redisVersionString(v string) string {
	first, rest, _ := strings.Cut(strings.TrimPrefix(v, "REDIS_"), "_")
	if rest == "X" {
		rest = "2"
	}

	return first + "." + rest + ".0"
}

// engineSettings derives the engine parameters from an instance: maxmemory is maxmemory-gb, which defaults to the
// instance capacity, and the eviction policy defaults to volatile-lru as on Memorystore.
func engineSettings(pb *redispb.Instance, unit int64) redisengine.Settings {
	s := redisengine.DefaultSettings()
	s.Policy = defaultPolicy
	s.RedisVersion = redisVersionString(pb.GetRedisVersion())
	s.MaxMemory = int64(pb.GetMemorySizeGb()) * unit

	for k, v := range pb.GetRedisConfigs() {
		applyConfig(&s, k, v, unit)
	}

	return s
}

func applyConfig(s *redisengine.Settings, key, val string, unit int64) {
	if applyBoolConfig(s, key, val == "yes") {
		return
	}

	n, _ := strconv.Atoi(val)

	switch key {
	case "maxmemory-gb":
		f, _ := strconv.ParseFloat(val, 64)
		s.MaxMemory = int64(f * float64(unit))
	case cfgPolicy:
		s.Policy = val
	case "notify-keyspace-events":
		s.NotifyKeyspaceEvents = val
	case "lfu-decay-time":
		s.LFUDecayTime = n
	case "lfu-log-factor":
		s.LFULogFactor = n
	case "stream-node-max-bytes":
		s.StreamNodeMaxBytes = n
	case "stream-node-max-entries":
		s.StreamNodeMaxEntries = n
	case "databases":
		s.Databases = n
	case "timeout":
		s.Timeout = time.Duration(n) * time.Second
	case "maxmemory-clients":
		s.MaxMemoryClients, _ = parsePercent(val)
	}
}

func applyBoolConfig(s *redisengine.Settings, key string, on bool) bool {
	switch key {
	case "activedefrag":
		s.ActiveDefrag = on
	case "lazyfree-lazy-eviction":
		s.LazyfreeLazyEviction = on
	case "lazyfree-lazy-expire":
		s.LazyfreeLazyExpire = on
	case "lazyfree-lazy-user-del":
		s.LazyfreeLazyUserDel = on
	case "lazyfree-lazy-user-flush":
		s.LazyfreeLazyUserFlsh = on
	default:
		return false
	}

	return true
}

// tuneLocked applies rec's configuration to its running server. Must hold b.mu.
func (b *InMemoryBackend) tuneLocked(rec *instRec) error {
	if rec.engine == nil {
		return nil
	}

	return rec.engine.Tune(engineSettings(rec.pb, b.memoryUnit()))
}

func (b *InMemoryBackend) memoryUnit() int64 {
	if b.unit > 0 {
		return b.unit
	}

	return bytesPerGB
}
