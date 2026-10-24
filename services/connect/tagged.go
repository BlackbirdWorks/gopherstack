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

func (f family[V]) collectTagged(owns func(string) bool, out *[]TaggedEntry) {
	if f.arnOf == nil {
		return
	}

	f.table.Range(func(v *V) bool {
		if tags := *f.tagsOf(v); len(tags) > 0 && owns(f.arnOf(v)) {
			*out = append(*out, TaggedEntry{ARN: f.arnOf(v), Tags: maps.Clone(tags)})
		}

		return true
	})
}

// TaggedResources lists the caller's tagged instances, phone numbers, instance resources and use cases.
func (b *Backend) TaggedResources(ctx context.Context) []TaggedEntry {
	account, region := b.scope(ctx)
	marker := ":" + region + ":" + account + ":"
	owns := func(arn string) bool { return strings.Contains(arn, marker) }

	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, r := range b.instances.All() {
		if len(r.Instance.Tags) > 0 && owns(r.Instance.Arn) {
			out = append(out, TaggedEntry{ARN: r.Instance.Arn, Tags: maps.Clone(r.Instance.Tags)})
		}
	}

	for _, p := range b.phoneNumbers.All() {
		if len(p.Phone.Tags) > 0 && owns(p.Phone.PhoneNumberArn) {
			out = append(out, TaggedEntry{ARN: p.Phone.PhoneNumberArn, Tags: maps.Clone(p.Phone.Tags)})
		}
	}

	b.users.collectTagged(owns, &out)
	b.routingProfiles.collectTagged(owns, &out)
	b.securityProfiles.collectTagged(owns, &out)
	b.hierarchyGroups.collectTagged(owns, &out)
	b.queues.collectTagged(owns, &out)
	b.hours.collectTagged(owns, &out)
	b.quickConnects.collectTagged(owns, &out)
	b.flows.collectTagged(owns, &out)
	b.modules.collectTagged(owns, &out)
	b.agentStatuses.collectTagged(owns, &out)
	b.prompts.collectTagged(owns, &out)
	b.contacts.collectTagged(owns, &out)
	b.integrations.collectTagged(owns, &out)
	b.rules.collectTagged(owns, &out)
	b.taskTemplates.collectTagged(owns, &out)
	b.vocabularies.collectTagged(owns, &out)
	b.views.collectTagged(owns, &out)
	b.evalForms.collectTagged(owns, &out)
	b.dataTables.collectTagged(owns, &out)
	b.emails.collectTagged(owns, &out)
	b.workspaces.collectTagged(owns, &out)
	b.trafficGroups.collectTagged(owns, &out)

	b.collectUseCaseTags(owns, &out)

	return out
}

func (b *Backend) collectUseCaseTags(owns func(string) bool, out *[]TaggedEntry) {
	for _, a := range b.integrations.table.All() {
		for _, u := range a.UseCases {
			arn := a.Association.IntegrationAssociationArn + "/use-case/" + u.UseCaseID
			if tags := a.UseCaseTags[u.UseCaseID]; len(tags) > 0 && owns(arn) {
				*out = append(*out, TaggedEntry{ARN: arn, Tags: maps.Clone(tags)})
			}
		}
	}
}
