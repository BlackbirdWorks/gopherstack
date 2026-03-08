package chaos

import (
	"math/rand/v2"
	"sync"
	"time"
)

// activityLogMaxSize is the maximum number of activity events retained.
const activityLogMaxSize = 100

// ActivityEvent records a single fault injection event emitted by the middleware.
type ActivityEvent struct {
	Timestamp    time.Time `json:"timestamp"`
	Service      string    `json:"service"`
	Operation    string    `json:"operation"`
	Region       string    `json:"region"`
	FaultApplied string    `json:"faultApplied"`
	Probability  float64   `json:"probability"`
	Triggered    bool      `json:"triggered"`
}

// FaultError defines a custom HTTP error response returned when a fault is triggered.
type FaultError struct {
	Code       string `json:"code"`
	StatusCode int    `json:"statusCode"`
}

// defaultServiceUnavailableStatus is the HTTP status code for the default fault error.
const defaultServiceUnavailableStatus = 503

// defaultFaultError is the default error returned when a fault rule has no custom error.
//
//nolint:gochecknoglobals // read-only package-level sentinel value, analogous to an exported error
var defaultFaultError = FaultError{
	StatusCode: defaultServiceUnavailableStatus,
	Code:       "ServiceUnavailable",
}

// FaultRule defines a single fault injection rule. All fields are optional;
// omitting a field means "match any". Rules are evaluated sequentially;
// the first match wins.
type FaultRule struct {
	Error       *FaultError `json:"error,omitempty"`
	Service     string      `json:"service,omitempty"`
	Region      string      `json:"region,omitempty"`
	Operation   string      `json:"operation,omitempty"`
	Probability float64     `json:"probability,omitempty"`
}

// ShouldTrigger reports whether this fault should fire based on its probability.
// A probability of 0 is treated as 1.0 (always fire).
func (r FaultRule) ShouldTrigger() bool {
	p := r.Probability
	if p <= 0 {
		p = 1.0
	}

	if p >= 1.0 {
		return true
	}

	//nolint:gosec // weak random is intentional — fault injection is not security-sensitive
	return rand.Float64() < p
}

// EffectiveError returns the FaultError to use, falling back to the default.
func (r FaultRule) EffectiveError() FaultError {
	if r.Error != nil {
		return *r.Error
	}

	return defaultFaultError
}

// LatencyRange defines a min/max range for randomized latency.
type LatencyRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// NetworkEffects defines dynamic network simulation parameters. This supersedes
// the static LATENCY_MS configuration at runtime when any field is non-zero.
type NetworkEffects struct {
	LatencyRange *LatencyRange `json:"latencyRange,omitempty"`
	Latency      int           `json:"latency,omitempty"`
	Jitter       int           `json:"jitter,omitempty"`
}

// TotalDelayMs computes the total simulated delay in milliseconds from the
// network effects configuration. Returns 0 when no delay is configured.
func (n NetworkEffects) TotalDelayMs() int {
	total := n.Latency

	if n.LatencyRange != nil && n.LatencyRange.Max > n.LatencyRange.Min {
		span := n.LatencyRange.Max - n.LatencyRange.Min
		//nolint:gosec // weak random is intentional
		total += n.LatencyRange.Min + rand.IntN(span)
	}

	if n.Jitter > 0 {
		//nolint:gosec // weak random is intentional
		total += rand.IntN(n.Jitter)
	}

	return total
}

// ruleMatches reports whether the rule matches the given service, operation, and region.
// An empty string in the rule matches any value.
func ruleMatches(r FaultRule, svc, op, region string) bool {
	if r.Service != "" && r.Service != svc {
		return false
	}

	if r.Region != "" && r.Region != region {
		return false
	}

	if r.Operation != "" && r.Operation != op {
		return false
	}

	return true
}

// FaultStore is a thread-safe store for fault rules and network effects.
type FaultStore struct {
	activity []ActivityEvent
	effects  NetworkEffects
	rules    []FaultRule
	mu       sync.RWMutex
}

// NewFaultStore creates a new empty FaultStore.
func NewFaultStore() *FaultStore {
	return &FaultStore{
		rules: []FaultRule{},
	}
}

// GetRules returns a copy of the current fault rules.
func (s *FaultStore) GetRules() []FaultRule {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]FaultRule, len(s.rules))
	copy(result, s.rules)

	return result
}

// SetRules replaces the entire fault rule list.
func (s *FaultStore) SetRules(rules []FaultRule) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rules = make([]FaultRule, len(rules))
	copy(s.rules, rules)
}

// AppendRules appends rules to the existing rule list.
func (s *FaultStore) AppendRules(rules []FaultRule) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rules = append(s.rules, rules...)
}

// DeleteRules removes rules that match any rule in the provided list.
// Two rules match when all non-empty fields are equal.
func (s *FaultStore) DeleteRules(rules []FaultRule) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kept := s.rules[:0:len(s.rules)]

	for _, existing := range s.rules {
		if !rulesContainMatch(rules, existing) {
			kept = append(kept, existing)
		}
	}

	s.rules = kept
}

// rulesContainMatch reports whether any rule in candidates matches target.
func rulesContainMatch(candidates []FaultRule, target FaultRule) bool {
	for _, c := range candidates {
		if c.Service == target.Service &&
			c.Region == target.Region &&
			c.Operation == target.Operation {
			return true
		}
	}

	return false
}

// Match finds the first fault rule that matches the given service, operation,
// and region. Returns the matched rule and true, or a zero FaultRule and false
// if no rule matches.
func (s *FaultStore) Match(svc, op, region string) (FaultRule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, r := range s.rules {
		if ruleMatches(r, svc, op, region) {
			return r, true
		}
	}

	return FaultRule{}, false
}

// GetEffects returns a copy of the current network effects.
func (s *FaultStore) GetEffects() NetworkEffects {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.effects
}

// SetEffects replaces the current network effects.
func (s *FaultStore) SetEffects(effects NetworkEffects) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.effects = effects
}

// RecordActivity appends an activity event to the ring buffer.
// When the buffer exceeds activityLogMaxSize, the oldest entry is dropped.
func (s *FaultStore) RecordActivity(event ActivityEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.activity = append(s.activity, event)
	if len(s.activity) > activityLogMaxSize {
		s.activity = s.activity[len(s.activity)-activityLogMaxSize:]
	}
}

// GetActivity returns a copy of the activity log in reverse-chronological order
// (newest first).
func (s *FaultStore) GetActivity() []ActivityEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]ActivityEvent, len(s.activity))
	for i, e := range s.activity {
		result[len(s.activity)-1-i] = e
	}

	return result
}
