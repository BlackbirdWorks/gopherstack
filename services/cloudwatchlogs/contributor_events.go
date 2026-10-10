package cloudwatchlogs

import "strings"

// ContributorEvent is a stored log event exposed read-only to Contributor Insights.
type ContributorEvent struct {
	LogGroup  string
	LogStream string
	Message   string
	Timestamp int64
}

// ContributorEvents returns events in [startMs, endMs) from groups in region matching
// patterns; a trailing '*' in a pattern is a prefix wildcard.
func (b *InMemoryBackend) ContributorEvents(region string, patterns []string, startMs, endMs int64) []ContributorEvent {
	b.mu.RLock("ContributorEvents")
	defer b.mu.RUnlock()

	var out []ContributorEvent

	for _, g := range b.groupsInRegion(region) {
		if !groupMatchesAny(g.LogGroupName, patterns) {
			continue
		}

		for _, s := range b.streamsInGroup(region, g.LogGroupName) {
			for _, ev := range s.events {
				if ev.Timestamp < startMs || ev.Timestamp >= endMs {
					continue
				}

				out = append(out, ContributorEvent{
					LogGroup:  g.LogGroupName,
					LogStream: s.LogStreamName,
					Message:   ev.Message,
					Timestamp: ev.Timestamp,
				})
			}
		}
	}

	return out
}

func groupMatchesAny(name string, patterns []string) bool {
	for _, p := range patterns {
		if prefix, ok := strings.CutSuffix(p, "*"); ok {
			if strings.HasPrefix(name, prefix) {
				return true
			}

			continue
		}

		if name == p {
			return true
		}
	}

	return false
}

// TransformerProcessors returns the transformer processors configured for a log group, if any.
func (b *InMemoryBackend) TransformerProcessors(groupName string) ([]map[string]any, bool) {
	b.mu.RLock("TransformerProcessors")
	defer b.mu.RUnlock()

	for _, t := range b.transformers.All() {
		if normalizeLogGroupIdentifier(t.LogGroupIdentifier) == groupName {
			return t.Processors, true
		}
	}

	return nil, false
}
