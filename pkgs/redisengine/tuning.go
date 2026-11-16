package redisengine

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Eviction policies accepted by maxmemory-policy.
const (
	PolicyNoEviction     = "noeviction"
	PolicyAllKeysLRU     = "allkeys-lru"
	PolicyVolatileLRU    = "volatile-lru"
	PolicyAllKeysLFU     = "allkeys-lfu"
	PolicyVolatileLFU    = "volatile-lfu"
	PolicyAllKeysRandom  = "allkeys-random"
	PolicyVolatileRandom = "volatile-random"
	PolicyVolatileTTL    = "volatile-ttl"
)

const (
	defaultDatabases     = 16
	defaultSlowlogMicros = 10000
	defaultSlowlogMaxLen = 128
	defaultLFULogFactor  = 10
	defaultLFUDecayTime  = 1
	maxDatabases         = keyspaceDBs
)

// ErrInvalidSettings is returned by Tune for values Redis would reject.
var ErrInvalidSettings = errors.New("invalid engine settings")

// Settings are the runtime parameters Tune applies to an engine; DefaultSettings holds the Redis defaults.
type Settings struct {
	RedisVersion         string
	Policy               string
	NotifyKeyspaceEvents string
	MaxMemory            int64
	Timeout              time.Duration
	SlowlogLogSlowerThan time.Duration
	LFULogFactor         int
	LFUDecayTime         int
	StreamNodeMaxBytes   int
	StreamNodeMaxEntries int
	Databases            int
	SlowlogMaxLen        int
	MaxMemoryClients     int
	ActiveDefrag         bool
	LazyfreeLazyEviction bool
	LazyfreeLazyExpire   bool
	LazyfreeLazyUserDel  bool
	LazyfreeLazyUserFlsh bool
}

// DefaultSettings returns Redis's stock configuration: no memory limit, noeviction, no keyspace events.
func DefaultSettings() Settings {
	return Settings{
		Policy:               PolicyNoEviction,
		LFULogFactor:         defaultLFULogFactor,
		LFUDecayTime:         defaultLFUDecayTime,
		StreamNodeMaxBytes:   defaultNodeBytes,
		StreamNodeMaxEntries: defaultNodeEntries,
		Databases:            defaultDatabases,
		SlowlogLogSlowerThan: defaultSlowlogMicros * time.Microsecond,
		SlowlogMaxLen:        defaultSlowlogMaxLen,
	}
}

func (s Settings) validate() error {
	policies := []string{
		PolicyNoEviction, PolicyAllKeysLRU, PolicyVolatileLRU, PolicyAllKeysLFU, PolicyVolatileLFU,
		PolicyAllKeysRandom, PolicyVolatileRandom, PolicyVolatileTTL,
	}

	switch {
	case !slices.Contains(policies, s.Policy):
		return fmt.Errorf("%w: maxmemory-policy %q", ErrInvalidSettings, s.Policy)
	case strings.Trim(s.NotifyKeyspaceEvents, eventFlagChars) != "":
		return fmt.Errorf("%w: notify-keyspace-events %q", ErrInvalidSettings, s.NotifyKeyspaceEvents)
	case s.Databases < 1 || s.Databases > maxDatabases:
		return fmt.Errorf("%w: databases %d", ErrInvalidSettings, s.Databases)
	case s.MaxMemory < 0 || s.StreamNodeMaxBytes < 0 || s.StreamNodeMaxEntries < 0 || s.LFULogFactor < 0:
		return fmt.Errorf("%w: negative limit", ErrInvalidSettings)
	}

	return nil
}

// Tune applies s to the running server and replaces any earlier settings. It installs the server's pre-command
// hook, so engines whose owner installs another hook must not use it. Without a call the server behaves like a
// bare miniredis.
func (e *Engine) Tune(s Settings) error {
	if e == nil || e.Mini == nil {
		return fmt.Errorf("%w: engine not running", ErrInvalidSettings)
	}

	if err := s.validate(); err != nil {
		return err
	}

	if e.guard != nil {
		return fmt.Errorf("%w: tuning cannot be combined with user access", ErrInvalidSettings)
	}

	if e.tuner == nil {
		t, err := newTuner(e.Mini, e.auth)
		if err != nil {
			return err
		}

		e.tuner = t
	}

	e.tuner.apply(s)

	return nil
}

// Settings reports what Tune last applied, or DefaultSettings for an untuned engine.
func (e *Engine) Settings() Settings {
	if e == nil || e.tuner == nil {
		return DefaultSettings()
	}

	return *e.tuner.cfg.Load()
}

func (e *Engine) streamLimits() streamLimits {
	if e.tuner == nil {
		return defaultStreamLimits()
	}

	s := e.tuner.cfg.Load()

	return streamLimits{nodeBytes: s.StreamNodeMaxBytes, nodeEntries: s.StreamNodeMaxEntries}
}
