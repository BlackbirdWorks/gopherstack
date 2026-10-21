package azurearm

import (
	"context"
	"strings"
	"time"
)

const acPurgeScheduleDays = 7

func (p *AppConfigProvider) deletedBody(d *deletedAC) map[string]any {
	return map[string]any{
		"id": "/subscriptions/" + d.subscriptionID + "/providers/" + namespaceMicrosoftAppConfig + "/locations/" +
			d.location + "/" + acDeletedType + "/" + d.name,
		fieldName: d.name, fieldType: namespaceMicrosoftAppConfig + "/locations/" + acDeletedType,
		fieldProperties: map[string]any{
			"configurationStoreId": d.storeID, fieldLocation: d.location, fieldTags: tagsOrEmpty(d.tags),
			"deletionDate":           d.deletedAt.UTC().Format(time.RFC3339),
			"scheduledPurgeDate":     d.deletedAt.AddDate(0, 0, acPurgeScheduleDays).UTC().Format(time.RFC3339),
			"purgeProtectionEnabled": d.purgeProtected,
		},
	}
}

// DeletedStore returns a soft-deleted store.
func (p *AppConfigProvider) DeletedStore(_ context.Context, location, name string) (map[string]any, error) {
	p.mu.RLock("DeletedStore")
	defer p.mu.RUnlock()

	d, ok := p.deleted[deletedKey(location, name)]
	if !ok {
		return nil, ErrAppConfigDeletedNotFound
	}

	return p.deletedBody(d), nil
}

// ListDeletedStores lists the soft-deleted stores of a subscription.
func (p *AppConfigProvider) ListDeletedStores(_ context.Context, subscription string) []map[string]any {
	p.mu.RLock("ListDeletedStores")
	defer p.mu.RUnlock()

	out := []map[string]any{}

	for _, d := range p.deleted {
		if d.subscriptionID == subscription || d.subscriptionID == "" {
			out = append(out, p.deletedBody(d))
		}
	}

	sortByName(out)

	return out
}

// PurgeDeletedStore permanently removes a soft-deleted store.
func (p *AppConfigProvider) PurgeDeletedStore(_ context.Context, location, name string) error {
	p.mu.Lock("PurgeDeletedStore")
	defer p.mu.Unlock()

	key := deletedKey(location, name)

	d, ok := p.deleted[key]
	if !ok {
		return ErrAppConfigDeletedNotFound
	}

	if d.purgeProtected {
		return ErrAppConfigPurgeProtected
	}

	delete(p.deleted, key)

	return nil
}

// CheckName reports whether a store name is available.
func (p *AppConfigProvider) CheckName(_ context.Context, name string) map[string]any {
	p.mu.RLock("CheckName")
	defer p.mu.RUnlock()

	switch {
	case len(name) < acMinName || len(name) > acMaxName || !acNameChars(name):
		return map[string]any{
			acFieldAvailable: false, acFieldReason: "Invalid",
			"message": "The name must be 5 to 50 characters of letters, digits and hyphens.",
		}
	case p.nameInUse(name):
		return map[string]any{
			acFieldAvailable: false,
			acFieldReason:    "AlreadyExists",
			"message":        "The configuration store name is already in use.",
		}
	}

	return map[string]any{acFieldAvailable: true}
}

func (p *AppConfigProvider) nameInUse(name string) bool {
	for _, r := range p.res {
		if r.kind == acKindStore && strings.EqualFold(r.name, name) {
			return true
		}
	}

	for _, d := range p.deleted {
		if strings.EqualFold(d.name, name) {
			return true
		}
	}

	return false
}

const (
	acMinName = 5
	acMaxName = 50
)

func acNameChars(name string) bool {
	for _, r := range name {
		ok := r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		if !ok {
			return false
		}
	}

	return true
}
