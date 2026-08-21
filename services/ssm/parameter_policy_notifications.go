package ssm

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ParameterPolicyNotifier receives Parameter Store policy-action
// notifications (ExpirationNotification / NoChangeNotification) so they can
// be delivered as real EventBridge events. Real AWS SSM emits an "aws.ssm" /
// "Parameter Store Policy Action" event with detail
// {"parameter-name": <name>, "policy-type": <Expiration|ExpirationNotification|
// NoChangeNotification>} -- confirmed via
// https://docs.aws.amazon.com/systems-manager/latest/userguide/sysman-paramstore-cwe.html.
//
// Implemented by an adapter wrapping the EventBridge backend and injected via
// SetParameterPolicyNotifier, so this package has no direct dependency on
// services/eventbridge -- the same injectable cross-service-hook pattern
// services/stepfunctions/asl uses for its EventBridgeIntegration (see
// services/eventbridge/sfn_integration.go for the analogous adapter).
type ParameterPolicyNotifier interface {
	// NotifyParameterPolicyAction is called once per (parameter, policy
	// instance) the first time that policy becomes due. parameterName is the
	// parameter's Name; policyType is "Expiration", "ExpirationNotification",
	// or "NoChangeNotification".
	NotifyParameterPolicyAction(ctx context.Context, parameterName, policyType string) error
}

// SetParameterPolicyNotifier configures the notifier used to publish
// Parameter Store policy-action events (see ParameterPolicyNotifier). Safe to
// call at any time, including after the janitor is already running. A nil
// notifier (the default) makes the policy-notification sweep a no-op --
// nothing is evaluated or marked as notified while unconfigured, so once a
// real notifier is injected, any policy that became due in the meantime is
// still reported on the next sweep rather than lost.
func (b *InMemoryBackend) SetParameterPolicyNotifier(n ParameterPolicyNotifier) {
	b.mu.Lock("SetParameterPolicyNotifier")
	defer b.mu.Unlock()
	b.parameterPolicyNotifier = n
}

// parameterStorePolicy is the generic JSON shape of a single entry in a
// PutParameter Policies array:
//
//	[{"Type":"NoChangeNotification","Version":"1.0","Attributes":{"After":"20","Unit":"Days"}}]
type parameterStorePolicy struct {
	Attributes map[string]string `json:"Attributes"`
	Type       string            `json:"Type"`
	Version    string            `json:"Version"`
}

const (
	policyTypeExpirationNotification = "ExpirationNotification"
	policyTypeNoChangeNotification   = "NoChangeNotification"
)

// parseParameterPolicies parses a Policies JSON string into its policy
// entries. Returns nil for an empty or malformed string (same permissive
// behavior as the pre-existing parameterExpiresAt in janitor.go).
func parseParameterPolicies(policiesJSON string) []parameterStorePolicy {
	if policiesJSON == "" {
		return nil
	}

	var policies []parameterStorePolicy
	if err := json.Unmarshal([]byte(policiesJSON), &policies); err != nil {
		return nil
	}

	return policies
}

// parameterPoliciesToWire converts a PutParameterInput-style Policies JSON
// string into the real ParameterMetadata.Policies wire shape
// ([]types.ParameterInlinePolicy, types/types.go:4840-4857) --
// DescribeParameters previously echoed the raw request string verbatim,
// which is not the real response shape at all: a real aws-sdk-go-v2 client
// would fail to unmarshal a JSON string where it expects an array of
// {PolicyText, PolicyType, PolicyStatus} objects. PolicyStatus is always
// "Finished": this backend applies every policy synchronously and in full
// on PutParameter, with no Pending/InProgress/Failed phase to observe.
func parameterPoliciesToWire(policiesJSON string) []ParameterInlinePolicy {
	parsed := parseParameterPolicies(policiesJSON)
	if len(parsed) == 0 {
		return nil
	}

	wire := make([]ParameterInlinePolicy, 0, len(parsed))
	for _, p := range parsed {
		text, err := json.Marshal(p)
		if err != nil {
			continue
		}

		wire = append(wire, ParameterInlinePolicy{
			PolicyText:   string(text),
			PolicyType:   p.Type,
			PolicyStatus: "Finished",
		})
	}

	return wire
}

// policyNotificationDedupKey builds a stable per-policy-instance dedupe key
// so a policy only ever notifies once per distinct (Type, Attributes)
// combination. This lets a parameter carry more than one ExpirationNotification
// (e.g. "Before 30 Days" and "Before 15 Days", both fired independently, per
// the AWS docs' own worked PutParameter example) without either being
// starved by the other's dedupe state.
func policyNotificationDedupKey(p parameterStorePolicy) string {
	keys := make([]string, 0, len(p.Attributes))
	for k := range p.Attributes {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var sb strings.Builder

	sb.WriteString(p.Type)

	for _, k := range keys {
		sb.WriteByte('|')
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(p.Attributes[k])
	}

	return sb.String()
}

// notificationUnitDuration converts a policy's "Unit" attribute (AWS
// supports "Days" and "Hours" -- confirmed via the parameter-store-policies
// user guide's ExpirationNotification/NoChangeNotification examples) to a
// time.Duration multiplier. Returns ok=false for an unrecognized unit so the
// caller can skip evaluation rather than guess at unspecified behavior.
func notificationUnitDuration(unit string, amount int) (time.Duration, bool) {
	switch unit {
	case "Days":
		return time.Duration(amount) * 24 * time.Hour, true
	case "Hours":
		return time.Duration(amount) * time.Hour, true
	default:
		return 0, false
	}
}

// expirationNotificationDueAt returns when an ExpirationNotification policy
// becomes due: "Before" "Unit" ahead of the parameter's Expiration policy
// timestamp. ok is false when the parameter has no Expiration policy (an
// ExpirationNotification with no expiration to count down to never fires) or
// the policy's Attributes are malformed/missing.
func expirationNotificationDueAt(expiresAt time.Time, expiresAtOK bool, p parameterStorePolicy) (time.Time, bool) {
	if !expiresAtOK {
		return time.Time{}, false
	}

	amount, err := strconv.Atoi(p.Attributes["Before"])
	if err != nil {
		return time.Time{}, false
	}

	dur, ok := notificationUnitDuration(p.Attributes["Unit"], amount)
	if !ok {
		return time.Time{}, false
	}

	return expiresAt.Add(-dur), true
}

// noChangeNotificationDueAt returns when a NoChangeNotification policy
// becomes due: "After" "Unit" after the parameter's LastModifiedDate. Per the
// parameter-store-policies user guide: "This policy determines when to send
// a notification by reading the LastModifiedTime attribute of the parameter.
// If you change or edit a parameter, the system resets the notification time
// period" -- callers must reset dedupe state on every PutParameter
// (see clearParameterPolicyNotificationStateLocked) so this recomputation is
// meaningful.
func noChangeNotificationDueAt(lastModified time.Time, p parameterStorePolicy) (time.Time, bool) {
	amount, err := strconv.Atoi(p.Attributes["After"])
	if err != nil {
		return time.Time{}, false
	}

	dur, ok := notificationUnitDuration(p.Attributes["Unit"], amount)
	if !ok {
		return time.Time{}, false
	}

	return lastModified.Add(dur), true
}

// duePolicyNotification identifies one policy-action notification that has
// become due for a specific parameter.
type duePolicyNotification struct {
	region        string
	parameterName string
	policyType    string
}

// collectDueParameterPolicyNotificationsLocked scans every parameter's
// Policies for ExpirationNotification/NoChangeNotification entries that have
// become due and have not yet been notified (per
// b.notifiedParameterPolicies), returning them and marking each as notified
// before returning. Must be called with b.mu held for writing.
//
// Marking-before-reporting (rather than after a successful publish) matches
// AWS's own documented "events are emitted on a best-effort basis" (i.e.
// at-most-once, not at-least-once) semantics: a failed publish is not
// retried indefinitely by real AWS either.
func (b *InMemoryBackend) collectDueParameterPolicyNotificationsLocked(now time.Time) []duePolicyNotification {
	var due []duePolicyNotification

	for region, params := range b.parameters {
		for _, p := range params.All() {
			policies := parseParameterPolicies(p.Policies)
			if len(policies) == 0 {
				continue
			}

			expiresAt, expiresAtOK := parameterExpiresAt(p.Policies)
			lastModified := time.Unix(int64(p.LastModifiedDate), 0).UTC()

			for _, policy := range policies {
				dueAt, ok := policyDueAt(policy, expiresAt, expiresAtOK, lastModified)
				if !ok || now.Before(dueAt) {
					continue
				}

				if b.markParameterPolicyNotifiedLocked(region, p.Name, policy) {
					due = append(due, duePolicyNotification{
						region:        region,
						parameterName: p.Name,
						policyType:    policy.Type,
					})
				}
			}
		}
	}

	return due
}

// policyDueAt dispatches a single policy entry to its type-specific due-time
// calculation. Only ExpirationNotification and NoChangeNotification are
// evaluated here -- Expiration itself is enforced separately by the
// pre-existing janitor sweep (sweepExpiredParameters), which deletes the
// parameter outright rather than emitting a notification.
func policyDueAt(
	policy parameterStorePolicy,
	expiresAt time.Time,
	expiresAtOK bool,
	lastModified time.Time,
) (time.Time, bool) {
	switch policy.Type {
	case policyTypeExpirationNotification:
		return expirationNotificationDueAt(expiresAt, expiresAtOK, policy)
	case policyTypeNoChangeNotification:
		return noChangeNotificationDueAt(lastModified, policy)
	default:
		return time.Time{}, false
	}
}

// markParameterPolicyNotifiedLocked records that the given policy instance
// has been notified for the named parameter, returning true if this is the
// first time (i.e. the caller should actually report it) or false if it was
// already marked. Must be called with b.mu held for writing.
func (b *InMemoryBackend) markParameterPolicyNotifiedLocked(
	region, name string,
	policy parameterStorePolicy,
) bool {
	if b.notifiedParameterPolicies[region] == nil {
		b.notifiedParameterPolicies[region] = make(map[string]map[string]struct{})
	}

	if b.notifiedParameterPolicies[region][name] == nil {
		b.notifiedParameterPolicies[region][name] = make(map[string]struct{})
	}

	notified := b.notifiedParameterPolicies[region][name]

	dedupKey := policyNotificationDedupKey(policy)
	if _, alreadyNotified := notified[dedupKey]; alreadyNotified {
		return false
	}

	notified[dedupKey] = struct{}{}

	return true
}

// clearParameterPolicyNotificationStateLocked forgets any previously-recorded
// notification dedupe state for a parameter. Called whenever a parameter is
// written (PutParameter always resets LastModifiedDate and wholesale-replaces
// Policies, both of which invalidate any prior notification eligibility --
// matches NoChangeNotification's documented "the system resets the
// notification time period" behavior) or deleted (cascade cleanup so no ghost
// rows survive the parameter itself). Must be called with b.mu held for
// writing.
func (b *InMemoryBackend) clearParameterPolicyNotificationStateLocked(region, name string) {
	delete(b.notifiedParameterPolicies[region], name)
	cleanupEmptyInnerMap(b.notifiedParameterPolicies, region)
}
