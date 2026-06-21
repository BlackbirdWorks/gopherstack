package iot

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Things                 map[string]*Thing        `json:"things"`
	Policies               map[string]*Policy       `json:"policies"`
	Rules                  map[string]*TopicRule    `json:"rules"`
	CertificateTransfers   map[string]string        `json:"certificateTransfers"`
	ThingBillingGroups     map[string]string        `json:"thingBillingGroups"`
	ThingThingGroups       map[string][]string      `json:"thingThingGroups"`
	PackageVersionSboms    map[string]*SbomDocument `json:"packageVersionSboms"`
	JobTargets             map[string][]string      `json:"jobTargets"`
	PolicyTargets          map[string][]string      `json:"policyTargets"`
	SecurityProfileTargets map[string][]string      `json:"securityProfileTargets"`
	ThingPrincipals        map[string][]string      `json:"thingPrincipals"`
	AuditMitigationTasks   map[string]string        `json:"auditMitigationTasks"`
	AuditTasks             map[string]string        `json:"auditTasks"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	things := make(map[string]*Thing, len(b.things))
	for k, v := range b.things {
		cp := cloneThing(v)
		things[k] = cp
	}

	policies := make(map[string]*Policy, len(b.policies))
	for k, v := range b.policies {
		policies[k] = clonePolicy(v)
	}

	rules := make(map[string]*TopicRule, len(b.rules))
	for k, v := range b.rules {
		rules[k] = cloneTopicRule(v)
	}

	certTransfers := make(map[string]string, len(b.certificateTransfers))
	maps.Copy(certTransfers, b.certificateTransfers)

	billingGroups := make(map[string]string, len(b.thingBillingGroups))
	maps.Copy(billingGroups, b.thingBillingGroups)

	thingGroups := copyStringSliceMap(b.thingThingGroups)

	sboms := make(map[string]*SbomDocument, len(b.packageVersionSboms))
	for k, v := range b.packageVersionSboms {
		sboms[k] = cloneSbomDocument(v)
	}

	snap := backendSnapshot{
		Things:                 things,
		Policies:               policies,
		Rules:                  rules,
		CertificateTransfers:   certTransfers,
		ThingBillingGroups:     billingGroups,
		ThingThingGroups:       thingGroups,
		PackageVersionSboms:    sboms,
		JobTargets:             copyStringSliceMap(b.jobTargets),
		PolicyTargets:          copyStringSliceMap(b.policyTargets),
		SecurityProfileTargets: copyStringSliceMap(b.securityProfileTargets),
		ThingPrincipals:        copyStringSliceMap(b.thingPrincipals),
		AuditMitigationTasks:   copyStringMap(b.auditMitigationTasks),
		AuditTasks:             copyStringMap(b.auditTasks),
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iot: failed to snapshot backend state", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "iot", data, &snap); err != nil {
		return err
	}

	ensureNonNilSnap(&snap)

	b.mu.Lock()
	defer b.mu.Unlock()

	things := make(map[string]*Thing, len(snap.Things))
	for k, v := range snap.Things {
		cp := cloneThing(v)
		things[k] = cp
	}

	policies := make(map[string]*Policy, len(snap.Policies))
	for k, v := range snap.Policies {
		policies[k] = clonePolicy(v)
	}

	rules := make(map[string]*TopicRule, len(snap.Rules))
	for k, v := range snap.Rules {
		rules[k] = cloneTopicRule(v)
	}

	b.things = things
	b.policies = policies
	b.rules = rules
	b.certificateTransfers = copyStringMap(snap.CertificateTransfers)
	b.thingBillingGroups = copyStringMap(snap.ThingBillingGroups)
	b.thingThingGroups = copyStringSliceMap(snap.ThingThingGroups)
	b.packageVersionSboms = make(map[string]*SbomDocument, len(snap.PackageVersionSboms))

	for k, v := range snap.PackageVersionSboms {
		b.packageVersionSboms[k] = cloneSbomDocument(v)
	}

	b.jobTargets = copyStringSliceMap(snap.JobTargets)
	b.policyTargets = copyStringSliceMap(snap.PolicyTargets)
	b.securityProfileTargets = copyStringSliceMap(snap.SecurityProfileTargets)
	b.thingPrincipals = copyStringSliceMap(snap.ThingPrincipals)
	b.auditMitigationTasks = copyStringMap(snap.AuditMitigationTasks)
	b.auditTasks = copyStringMap(snap.AuditTasks)

	return nil
}

func ensureNonNilSnap(snap *backendSnapshot) {
	if snap.Things == nil {
		snap.Things = make(map[string]*Thing)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]*Policy)
	}

	if snap.Rules == nil {
		snap.Rules = make(map[string]*TopicRule)
	}

	if snap.CertificateTransfers == nil {
		snap.CertificateTransfers = make(map[string]string)
	}

	if snap.ThingBillingGroups == nil {
		snap.ThingBillingGroups = make(map[string]string)
	}

	if snap.ThingThingGroups == nil {
		snap.ThingThingGroups = make(map[string][]string)
	}

	if snap.PackageVersionSboms == nil {
		snap.PackageVersionSboms = make(map[string]*SbomDocument)
	}

	if snap.JobTargets == nil {
		snap.JobTargets = make(map[string][]string)
	}

	if snap.PolicyTargets == nil {
		snap.PolicyTargets = make(map[string][]string)
	}

	if snap.SecurityProfileTargets == nil {
		snap.SecurityProfileTargets = make(map[string][]string)
	}

	if snap.ThingPrincipals == nil {
		snap.ThingPrincipals = make(map[string][]string)
	}

	if snap.AuditMitigationTasks == nil {
		snap.AuditMitigationTasks = make(map[string]string)
	}

	if snap.AuditTasks == nil {
		snap.AuditTasks = make(map[string]string)
	}
}

func copyStringMap(m map[string]string) map[string]string {
	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return cp
}

func copyStringSliceMap(m map[string][]string) map[string][]string {
	cp := make(map[string][]string, len(m))

	for k, v := range m {
		s := make([]string, len(v))
		copy(s, v)
		cp[k] = s
	}

	return cp
}

// Snapshot implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}
