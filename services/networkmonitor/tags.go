package networkmonitor

import (
	"context"
	"fmt"
	"maps"
	"strings"
)

// ListTagsForResource returns tags for a monitor or probe by ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceARN string,
) (map[string]string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return b.lookupTagsByARN(region, resourceARN)
}

// TagResource adds or updates tags on a monitor or probe.
func (b *InMemoryBackend) TagResource(
	ctx context.Context,
	resourceARN string,
	tags map[string]string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	m, probe, err := b.findResourceByARN(region, resourceARN)
	if err != nil {
		return err
	}

	if probe != nil {
		if probe.Tags == nil {
			probe.Tags = make(map[string]string)
		}

		maps.Copy(probe.Tags, tags)
	} else if m != nil {
		if m.Tags == nil {
			m.Tags = make(map[string]string)
		}

		maps.Copy(m.Tags, tags)
	}

	return nil
}

// UntagResource removes tags from a monitor or probe.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context,
	resourceARN string,
	tagKeys []string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	m, probe, err := b.findResourceByARN(region, resourceARN)
	if err != nil {
		return err
	}

	if probe != nil {
		for _, k := range tagKeys {
			delete(probe.Tags, k)
		}
	} else if m != nil {
		for _, k := range tagKeys {
			delete(m.Tags, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) lookupTagsByARN(region, resourceARN string) (map[string]string, error) {
	m, probe, err := b.findResourceByARN(region, resourceARN)
	if err != nil {
		return nil, err
	}

	if probe != nil {
		return maps.Clone(probe.Tags), nil
	}

	return maps.Clone(m.Tags), nil
}

// findResourceByARN resolves an ARN to either a monitor or a probe (not both).
// Must be called with b.mu held (read or write).
func (b *InMemoryBackend) findResourceByARN(region, resourceARN string) (*Monitor, *Probe, error) {
	// ARN formats:
	//   monitor: arn:aws:networkmonitor:{region}:{acct}:monitor/{name}
	//   probe:   arn:aws:networkmonitor:{region}:{acct}:probe/{monitorName}/{probeId}
	parts := strings.SplitN(resourceARN, ":", arnColonParts)
	if len(parts) < arnColonParts {
		return nil, nil, fmt.Errorf("%w: invalid resource ARN", ErrNotFound)
	}

	resource := parts[arnColonParts-1]

	if monitorName, ok := strings.CutPrefix(resource, "monitor/"); ok {
		m, exists := b.monitors.Get(regionKey(region, monitorName))
		if !exists {
			return nil, nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
		}

		return m, nil, nil
	}

	if rest, ok := strings.CutPrefix(resource, "probe/"); ok {
		segments := strings.SplitN(rest, "/", probePathParts)
		if len(segments) != probePathParts {
			return nil, nil, fmt.Errorf("%w: invalid probe ARN", ErrNotFound)
		}

		monitorName, probeID := segments[0], segments[1]

		m, exists := b.monitors.Get(regionKey(region, monitorName))
		if !exists {
			return nil, nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
		}

		idx := findProbeIndex(m.Probes, probeID)
		if idx < 0 {
			return nil, nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
		}

		return nil, m.Probes[idx], nil
	}

	return nil, nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
}
