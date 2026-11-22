package iot

import (
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every authorizer, billing group, job, policy, rule, thing group and other taggable
// IoT resource plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))
	seen := make(map[string]struct{}, len(b.resourceTags))

	for key, t := range b.resourceTags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: maps.Clone(t)})
	}

	if withUntagged {
		for _, key := range b.resourceARNsLocked() {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, TaggedEntry{ARN: key})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	return slices.Concat(
		arnsOf(b.authorizers, func(v *Authorizer) string { return v.AuthorizerARN }),
		arnsOf(b.billingGroups, func(v *BillingGroup) string { return v.BillingGroupARN }),
		arnsOf(b.scheduledAudits, func(v *ScheduledAudit) string { return v.ScheduledAuditARN }),
		arnsOf(b.mitigationActions, func(v *MitigationAction) string { return v.ActionARN }),
		arnsOf(b.commands, func(v *IoTCommand) string { return v.CommandARN }),
		arnsOf(b.certificateProviders, func(v *CertificateProvider) string { return v.ARN }),
		arnsOf(b.caCertificates, func(v *CACertificate) string { return v.CertificateARN }),
		arnsOf(b.iotPackages, func(v *IoTPackage) string { return v.PackageARN }),
		arnsOf(b.fleetMetrics, func(v *FleetMetric) string { return v.MetricARN }),
		arnsOf(b.customMetrics, func(v *CustomMetric) string { return v.MetricARN }),
		arnsOf(b.dimensions, func(v *Dimension) string { return v.ARN }),
		arnsOf(b.otaUpdates, func(v *OTAUpdate) string { return v.OTAUpdateARN }),
		arnsOf(b.jobs, func(v *Job) string { return v.JobARN }),
		arnsOf(b.jobTemplates, func(v *JobTemplate) string { return v.JobTemplateARN }),
		arnsOf(b.policies, func(v *Policy) string { return v.ARN }),
		arnsOf(b.streams, func(v *IoTStream) string { return v.StreamARN }),
		arnsOf(b.roleAliases, func(v *RoleAlias) string { return v.RoleAliasARN }),
		arnsOf(b.domainConfigs, func(v *DomainConfiguration) string { return v.DomainConfigurationARN }),
		arnsOf(b.provTemplates, func(v *ProvisioningTemplate) string { return v.TemplateARN }),
		arnsOf(b.securityProfiles, func(v *SecurityProfile) string { return v.SecurityProfileARN }),
		arnsOf(b.rules, func(v *TopicRule) string { return v.ARN }),
		arnsOf(b.thingGroups, func(v *ThingGroup) string { return v.ThingGroupARN }),
		arnsOf(b.thingTypes, func(v *ThingType) string { return v.ThingTypeARN }),
	)
}

func arnsOf[V any](t *store.Table[V], arnOf func(*V) string) []string {
	out := make([]string, 0, t.Len())
	for _, v := range t.All() {
		out = append(out, arnOf(v))
	}

	return out
}

// TagResource adds tags to resourceARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	return b.TagResourceGeneric(resourceARN, tags)
}
