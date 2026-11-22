package connect

import (
	"context"
	"maps"
	"strings"
)

// TaggedEntry is one taggable resource, for the Resource Groups Tagging API bridge.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

func (f family[V]) collectTagged(owns func(string) bool, out *[]TaggedEntry, taggedOnly bool) {
	if f.arnOf == nil {
		return
	}

	f.table.Range(func(v *V) bool {
		if tags := *f.tagsOf(v); (!taggedOnly || len(tags) > 0) && owns(f.arnOf(v)) {
			*out = append(*out, TaggedEntry{ARN: f.arnOf(v), Tags: maps.Clone(tags)})
		}

		return true
	})
}

// TaggedResources lists the caller's tagged instances, phone numbers, instance resources and use cases.
func (b *Backend) TaggedResources(ctx context.Context) []TaggedEntry {
	return b.entries(ctx, true)
}

// InventoryResources lists the caller's instances, phone numbers, instance resources and use cases, tagged or not.
func (b *Backend) InventoryResources(ctx context.Context) []TaggedEntry {
	return b.entries(ctx, false)
}

func (b *Backend) entries(ctx context.Context, taggedOnly bool) []TaggedEntry {
	account, region := b.scope(ctx)
	marker := ":" + region + ":" + account + ":"
	owns := func(arn string) bool { return strings.Contains(arn, marker) }

	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, r := range b.instances.All() {
		if (!taggedOnly || len(r.Instance.Tags) > 0) && owns(r.Instance.Arn) {
			out = append(out, TaggedEntry{ARN: r.Instance.Arn, Tags: maps.Clone(r.Instance.Tags)})
		}
	}

	for _, p := range b.phoneNumbers.All() {
		if (!taggedOnly || len(p.Phone.Tags) > 0) && owns(p.Phone.PhoneNumberArn) {
			out = append(out, TaggedEntry{ARN: p.Phone.PhoneNumberArn, Tags: maps.Clone(p.Phone.Tags)})
		}
	}

	b.users.collectTagged(owns, &out, taggedOnly)
	b.routingProfiles.collectTagged(owns, &out, taggedOnly)
	b.securityProfiles.collectTagged(owns, &out, taggedOnly)
	b.hierarchyGroups.collectTagged(owns, &out, taggedOnly)
	b.queues.collectTagged(owns, &out, taggedOnly)
	b.hours.collectTagged(owns, &out, taggedOnly)
	b.quickConnects.collectTagged(owns, &out, taggedOnly)
	b.flows.collectTagged(owns, &out, taggedOnly)
	b.modules.collectTagged(owns, &out, taggedOnly)
	b.agentStatuses.collectTagged(owns, &out, taggedOnly)
	b.prompts.collectTagged(owns, &out, taggedOnly)
	b.contacts.collectTagged(owns, &out, taggedOnly)
	b.integrations.collectTagged(owns, &out, taggedOnly)
	b.rules.collectTagged(owns, &out, taggedOnly)
	b.taskTemplates.collectTagged(owns, &out, taggedOnly)
	b.vocabularies.collectTagged(owns, &out, taggedOnly)
	b.views.collectTagged(owns, &out, taggedOnly)
	b.evalForms.collectTagged(owns, &out, taggedOnly)
	b.dataTables.collectTagged(owns, &out, taggedOnly)
	b.emails.collectTagged(owns, &out, taggedOnly)
	b.workspaces.collectTagged(owns, &out, taggedOnly)
	b.metricDefs.collectTagged(owns, &out, taggedOnly)
	b.attachedFiles.collectTagged(owns, &out, taggedOnly)
	b.notifications.collectTagged(owns, &out, taggedOnly)
	b.testCases.collectTagged(owns, &out, taggedOnly)
	b.evaluations.collectTagged(owns, &out, taggedOnly)
	b.trafficGroups.collectTagged(owns, &out, taggedOnly)
	b.extractions.collectTagged(owns, &out, taggedOnly)

	b.collectUseCaseTags(owns, &out, taggedOnly)

	return out
}

func (b *Backend) collectUseCaseTags(owns func(string) bool, out *[]TaggedEntry, taggedOnly bool) {
	for _, a := range b.integrations.table.All() {
		for _, u := range a.UseCases {
			arn := a.Association.IntegrationAssociationArn + "/use-case/" + u.UseCaseID
			if tags := a.UseCaseTags[u.UseCaseID]; (!taggedOnly || len(tags) > 0) && owns(arn) {
				*out = append(*out, TaggedEntry{ARN: arn, Tags: maps.Clone(tags)})
			}
		}
	}
}
