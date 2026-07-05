package sts

import "time"

// Test-only exports for the unexported trust-policy and token-validation helpers,
// so the corresponding table tests can live in the external sts_test package.

// Trust-policy action strings exposed for tests.
const (
	ActionAssumeRole          = actionAssumeRole
	ActionAssumeRoleWithSAML  = actionAssumeRoleWithSAML
	ActionAssumeRoleWithWebID = actionAssumeRoleWithWebID
)

// TrustEvalForTest mirrors the unexported trustEval evaluation context.
type TrustEvalForTest struct {
	ConditionCtx map[string]string
	Action       string
	CallerArn    string
	FederatedArn string
	ExternalID   string
}

// EvaluateAssumeRoleTrust exposes evaluateAssumeRoleTrust for tests.
func EvaluateAssumeRoleTrust(policyJSON string, ev TrustEvalForTest) error {
	return evaluateAssumeRoleTrust(policyJSON, trustEval{
		action:       ev.Action,
		callerArn:    ev.CallerArn,
		federatedArn: ev.FederatedArn,
		externalID:   ev.ExternalID,
		conditionCtx: ev.ConditionCtx,
	})
}

// WildcardMatch exposes wildcardMatch for tests.
func WildcardMatch(pattern, s string) bool { return wildcardMatch(pattern, s) }

// ValidateWebIdentityToken exposes validateWebIdentityToken for tests.
func ValidateWebIdentityToken(token string) error { return validateWebIdentityToken(token) }

// ValidateSAMLTemporalConditions exposes validateSAMLTemporalConditions for tests.
func ValidateSAMLTemporalConditions(assertion string) error {
	return validateSAMLTemporalConditions(assertion)
}

// ParseSAMLTime exposes parseSAMLTime for tests.
func ParseSAMLTime(value string) (time.Time, error) { return parseSAMLTime(value) }

// JWTTimeClaim exposes jwtTimeClaim for tests.
func JWTTimeClaim(claims map[string]any, key string) (time.Time, bool, error) {
	return jwtTimeClaim(claims, key)
}

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultSTSJanitorInterval

// SessionEvictThreshold exposes the opportunistic-eviction threshold for tests.
const SessionEvictThreshold = sessionEvictThreshold

// SessionCount returns the number of sessions currently stored in the backend.
// Used in tests to verify janitor eviction.
func (b *InMemoryBackend) SessionCount() int {
	b.mu.RLock("SessionCount")
	defer b.mu.RUnlock()

	return len(b.sessions)
}

// SetSessionExpiration overrides the expiration of the session identified by
// accessKeyID. Used in tests to fast-forward session expiry without waiting.
func (b *InMemoryBackend) SetSessionExpiration(accessKeyID string, exp time.Time) {
	b.mu.Lock("SetSessionExpiration")
	defer b.mu.Unlock()

	if s, ok := b.sessions[accessKeyID]; ok {
		s.Expiration = exp
	}
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	return h.janitor.TaskTimeout
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	return h.janitor.Interval
}

// GetJanitor returns the configured janitor instance for tests.
func (h *Handler) GetJanitor() *Janitor {
	return h.janitor
}

// HandlerOpsLen returns the number of supported operations.
func (h *Handler) HandlerOpsLen() int {
	return len(h.GetSupportedOperations())
}

// AddSessionInternal inserts a session directly into the backend for test seeding.
func (b *InMemoryBackend) AddSessionInternal(session *SessionInfo) {
	b.mu.Lock("AddSessionInternal")
	defer b.mu.Unlock()

	b.sessions[session.AccessKeyID] = session
}
