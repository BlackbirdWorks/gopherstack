package connect

import (
	"maps"
	"strings"
)

func (b *Backend) tagsOfLocked(resourceARN string) (*map[string]string, error) {
	for _, r := range b.instances.All() {
		if r.Instance.Arn == resourceARN {
			return &r.Instance.Tags, nil
		}
	}

	for _, p := range b.phoneNumbers.All() {
		if p.Phone.PhoneNumberArn == resourceARN {
			return &p.Phone.Tags, nil
		}
	}

	for _, find := range append(b.extraFinders(), []func(string) *map[string]string{
		b.users.findARN, b.routingProfiles.findARN, b.securityProfiles.findARN, b.hierarchyGroups.findARN,
		b.queues.findARN, b.hours.findARN, b.quickConnects.findARN, b.flows.findARN, b.modules.findARN,
		b.agentStatuses.findARN, b.prompts.findARN, b.contacts.findARN, b.integrations.findARN,
	}...) {
		if t := find(resourceARN); t != nil {
			return t, nil
		}
	}

	if t := b.useCaseTagsLocked(resourceARN); t != nil {
		return t, nil
	}

	return nil, notFoundErr("Resource", resourceARN)
}

func (b *Backend) useCaseTagsLocked(resourceARN string) *map[string]string {
	_, after, ok := strings.CutLast(resourceARN, "/use-case/")
	if !ok {
		return nil
	}

	for _, a := range b.integrations.table.All() {
		if !strings.HasPrefix(resourceARN, a.Association.IntegrationAssociationArn+"/use-case/") {
			continue
		}

		id := after
		for _, u := range a.UseCases {
			if u.UseCaseID == id {
				t := a.UseCaseTags[id]

				return &t
			}
		}
	}

	return nil
}

// TagResource adds or replaces tags on any taggable resource.
func (b *Backend) TagResource(resourceARN string, add map[string]string) error {
	if len(add) == 0 {
		return invalidParamErr("tags is required")
	}

	if err := checkTags(add); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(resourceARN)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if err = checkTags(merged); err != nil {
		return err
	}

	*t = merged

	b.storeUseCaseTags(resourceARN, merged)

	return nil
}

func (b *Backend) storeUseCaseTags(resourceARN string, tags map[string]string) {
	_, after, ok := strings.CutLast(resourceARN, "/use-case/")
	if !ok {
		return
	}

	for _, a := range b.integrations.table.All() {
		if strings.HasPrefix(resourceARN, a.Association.IntegrationAssociationArn+"/use-case/") {
			if a.UseCaseTags == nil {
				a.UseCaseTags = map[string]map[string]string{}
			}

			a.UseCaseTags[after] = tags
		}
	}
}

// UntagResource removes tags by key.
func (b *Backend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(resourceARN)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	b.storeUseCaseTags(resourceARN, *t)

	return nil
}

// ListTagsForResource returns a copy of the resource's tags.
func (b *Backend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.tagsOfLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	out := maps.Clone(*t)
	if out == nil {
		out = map[string]string{}
	}

	return out, nil
}
