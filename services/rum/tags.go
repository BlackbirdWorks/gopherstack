package rum

import (
	"context"
	"maps"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const monitorResourcePrefix = "appmonitor/"

// tagTargetLocked resolves an app monitor ARN the caller owns.
func (b *InMemoryBackend) tagTargetLocked(sc scope, resourceARN string) (*AppMonitor, error) {
	parsed, err := awsarn.Parse(resourceARN)
	if err != nil || parsed.Service != serviceName || !strings.HasPrefix(parsed.Resource, monitorResourcePrefix) {
		return nil, invalid("%q is not a CloudWatch RUM app monitor ARN", resourceARN)
	}

	if parsed.Region != sc.region || parsed.AccountID != sc.account {
		return nil, notFound(resourceARN, "Resource %s does not exist", resourceARN)
	}

	return b.monitorByNameLocked(sc, strings.TrimPrefix(parsed.Resource, monitorResourcePrefix))
}

// TagResource adds or replaces tags on an app monitor.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, add map[string]string) error {
	if err := validateMonitorTags(add); err != nil {
		return err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	m, err := b.tagTargetLocked(sc, resourceARN)
	if err != nil {
		return err
	}

	merged := cloneMap(m.Tags)
	maps.Copy(merged, add)

	if len(merged) > maxTags {
		return invalid("%s", errTagsTooMany)
	}

	cp := *m
	cp.Tags = merged
	b.monitors.Put(&cp)

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, keys []string) error {
	sc := b.scopeOf(ctx)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	m, err := b.tagTargetLocked(sc, resourceARN)
	if err != nil {
		return err
	}

	cp := *m
	cp.Tags = cloneMap(m.Tags)

	for _, k := range keys {
		delete(cp.Tags, k)
	}

	b.monitors.Put(&cp)

	return nil
}

// ListTagsForResource returns the tags of an app monitor.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	m, err := b.tagTargetLocked(sc, resourceARN)
	if err != nil {
		return nil, err
	}

	return cloneMap(m.Tags), nil
}

// TaggedEntry is one taggable resource, for the Resource Groups Tagging API bridge.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists the caller's tagged app monitors.
func (b *InMemoryBackend) TaggedResources(ctx context.Context) []TaggedEntry {
	return b.entries(ctx, true)
}

// InventoryResources lists every app monitor of the caller.
func (b *InMemoryBackend) InventoryResources(ctx context.Context) []TaggedEntry {
	return b.entries(ctx, false)
}

func (b *InMemoryBackend) entries(ctx context.Context, taggedOnly bool) []TaggedEntry {
	sc := b.scopeOf(ctx)

	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	b.monitors.Range(func(m *AppMonitor) bool {
		if m.Account == sc.account && m.Region == sc.region && (!taggedOnly || len(m.Tags) > 0) {
			out = append(out, TaggedEntry{
				ARN:  arn.Build(serviceName, m.Region, m.Account, monitorResourcePrefix+m.Name),
				Tags: maps.Clone(m.Tags),
			})
		}

		return true
	})

	return out
}
