package resourceexplorer2

import (
	"context"
	"maps"
	"strings"
)

func (b *InMemoryBackend) tagsOfLocked(ctx context.Context, resourceARN string) (*map[string]string, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	switch {
	case strings.Contains(resourceARN, ":index/"):
		i, ok := b.indexes.Get(resourceARN)
		if !ok || i.AccountID != account || i.Region != region || b.stateOf(i) == stateDeleted {
			return nil, notFound("index %s not found", resourceARN)
		}

		return &i.Tags, nil
	case strings.Contains(resourceARN, ":view/"):
		v, err := b.viewLocked(account, region, resourceARN)
		if err != nil {
			return nil, err
		}

		return &v.Tags, nil
	default:
		return nil, notFound("resource %s not found", resourceARN)
	}
}

// TagResource adds tags to an index or view.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, add map[string]string) error {
	if err := validateTags(add); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(ctx, resourceARN)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if len(merged) > maxTags {
		return invalid("a resource can have at most %d tags", maxTags)
	}

	*t = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(ctx, resourceARN)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	return nil
}

// ListTagsForResource returns the tags of an index or view.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.tagsOfLocked(ctx, resourceARN)
	if err != nil {
		return nil, err
	}

	out := maps.Clone(*t)
	if out == nil {
		out = map[string]string{}
	}

	return out, nil
}

// OrgConfiguration is the GetAccountLevelServiceConfiguration output.
type OrgConfiguration struct {
	AWSServiceAccessStatus string `json:"AWSServiceAccessStatus"`
	ServiceLinkedRole      string `json:"ServiceLinkedRole,omitempty"`
}

// GetAccountLevelServiceConfiguration reports organization-wide access for Resource Explorer.
func (b *InMemoryBackend) GetAccountLevelServiceConfiguration(ctx context.Context) OrgConfiguration {
	b.mu.RLock("GetAccountLevelServiceConfiguration")
	org := b.org
	b.mu.RUnlock()

	if org == nil || !org.ServiceAccessEnabled(servicePrincipal) {
		return OrgConfiguration{AWSServiceAccessStatus: "DISABLED"}
	}

	return OrgConfiguration{
		AWSServiceAccessStatus: "ENABLED",
		ServiceLinkedRole: "arn:aws:iam::" + b.callerAccount(ctx) + ":role/aws-service-role/" +
			servicePrincipal + "/AWSServiceRoleForResourceExplorer",
	}
}
