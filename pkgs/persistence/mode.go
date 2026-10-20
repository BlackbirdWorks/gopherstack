package persistence

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Mode selects how a service's state reaches disk.
type Mode string

const (
	// ModeSnapshot debounces a full snapshot after each mutating request (default).
	ModeSnapshot Mode = "snapshot"
	// ModeHybrid flushes only services changed since the last flush, on an interval and at shutdown.
	ModeHybrid Mode = "hybrid"

	systemPrefix = "system-"
)

// ErrInvalidMode flags a bad mode or service-mode entry.
var ErrInvalidMode = errors.New("persistence: invalid setting")

// ParseMode validates a mode name case-insensitively; empty means ModeSnapshot.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(strs.Fold(strings.TrimSpace(s))); m {
	case "", ModeSnapshot:
		return ModeSnapshot, nil
	case ModeHybrid:
		return m, nil
	default:
		return "", fmt.Errorf("%w: mode %q (want snapshot or hybrid)", ErrInvalidMode, s)
	}
}

// Policy decides, per service, whether it is persisted and in which mode.
// The zero value persists every service in ModeSnapshot.
type Policy struct {
	include   map[string]bool
	exclude   map[string]bool
	overrides map[string]Mode
	def       Mode
}

// NewPolicy builds a Policy. include (when non-empty) is an allow-list,
// exclude is applied after it, and overrides are "service=mode" entries.
// Names match case-insensitively.
func NewPolicy(def Mode, include, exclude, overrides []string) (Policy, error) {
	if def == "" {
		def = ModeSnapshot
	}

	p := Policy{def: def, include: foldSet(include), exclude: foldSet(exclude), overrides: map[string]Mode{}}

	for _, kv := range overrides {
		name, modeStr, ok := strings.Cut(kv, "=")
		if !ok || strings.TrimSpace(name) == "" {
			return Policy{}, fmt.Errorf("%w: service mode %q (want service=mode)", ErrInvalidMode, kv)
		}

		m, err := ParseMode(modeStr)
		if err != nil {
			return Policy{}, err
		}

		p.overrides[strs.Fold(strings.TrimSpace(name))] = m
	}

	return p, nil
}

func foldSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))

	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			set[strs.Fold(n)] = true
		}
	}

	return set
}

// ModeFor reports the mode for a service and whether it is persisted at all.
// Internal "system-*" entries are always persisted in ModeSnapshot (saved at shutdown).
func (p Policy) ModeFor(name string) (Mode, bool) {
	def := p.def
	if def == "" {
		def = ModeSnapshot
	}

	key := strs.Fold(name)
	if strings.HasPrefix(key, systemPrefix) {
		return ModeSnapshot, true
	}

	if len(p.include) > 0 && !p.include[key] {
		return def, false
	}

	if p.exclude[key] {
		return def, false
	}

	if m, ok := p.overrides[key]; ok {
		return m, true
	}

	return def, true
}

// Uses reports whether any service can resolve to mode under this policy.
func (p Policy) Uses(mode Mode) bool {
	def := p.def
	if def == "" {
		def = ModeSnapshot
	}

	if def == mode {
		return true
	}

	for _, m := range p.overrides {
		if m == mode {
			return true
		}
	}

	return false
}
