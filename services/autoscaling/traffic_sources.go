package autoscaling

import "fmt"

// AttachTrafficSources adds traffic sources to the specified Auto Scaling group.
func (b *InMemoryBackend) AttachTrafficSources(groupName string, trafficSources []TrafficSource) error {
	b.mu.Lock("AttachTrafficSources")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	type tsKey struct{ Identifier, Type string }

	existing := make(map[tsKey]bool, len(g.TrafficSources))
	for _, ts := range g.TrafficSources {
		existing[tsKey(ts)] = true
	}

	for _, ts := range trafficSources {
		k := tsKey(ts)
		if !existing[k] {
			g.TrafficSources = append(g.TrafficSources, ts)
		}
	}

	return nil
}

// DescribeTrafficSources returns the traffic sources attached to the group.
func (b *InMemoryBackend) DescribeTrafficSources(groupName string) ([]TrafficSourceState, error) {
	b.mu.RLock("DescribeTrafficSources")
	defer b.mu.RUnlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	result := make([]TrafficSourceState, 0, len(g.TrafficSources))
	for _, ts := range g.TrafficSources {
		result = append(result, TrafficSourceState{Identifier: ts.Identifier, Type: ts.Type, State: lbStateAdded})
	}

	return result, nil
}

// DetachTrafficSources removes traffic sources from the ASG.
func (b *InMemoryBackend) DetachTrafficSources(groupName string, trafficSources []TrafficSource) error {
	b.mu.Lock("DetachTrafficSources")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	type tsKey struct{ Identifier, Type string }
	removeSet := make(map[tsKey]bool, len(trafficSources))
	for _, ts := range trafficSources {
		removeSet[tsKey(ts)] = true
	}

	newTS := make([]TrafficSource, 0, len(g.TrafficSources))
	for _, ts := range g.TrafficSources {
		if !removeSet[tsKey(ts)] {
			newTS = append(newTS, ts)
		}
	}

	g.TrafficSources = newTS

	return nil
}
