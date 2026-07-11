package cloudwatchlogs

// Code in this file replaces the pre-Phase-3.3 groupsStore(region)/streamsStore
// (region)/eventsStore(region)/subscriptionFiltersStore(region)/
// metricFiltersStore(region) helpers, which each lazily returned a raw
// map[string]... scoped to one region. store.Table has a single flat string
// key (see store_setup.go's groupTableKey et al.), so callers that used to
// index/range/delete on the returned map now call the small typed helpers
// below instead. All of them assume the caller already holds b.mu (read or
// write, as appropriate), exactly like the map-returning helpers they replace.

// --- log groups ---

func (b *InMemoryBackend) groupGet(region, name string) (*LogGroup, bool) {
	return b.groups.Get(groupTableKey(region, name))
}

func (b *InMemoryBackend) groupHas(region, name string) bool {
	return b.groups.Has(groupTableKey(region, name))
}

// groupPut inserts or replaces g, which must already have region and
// LogGroupName set.
func (b *InMemoryBackend) groupPut(g *LogGroup) {
	b.groups.Put(g)
}

func (b *InMemoryBackend) groupDelete(region, name string) bool {
	return b.groups.Delete(groupTableKey(region, name))
}

// groupsInRegion returns every log group registered under region, in
// unspecified order (mirrors the old groups[region] map's iteration).
func (b *InMemoryBackend) groupsInRegion(region string) []*LogGroup {
	return b.groupsByRegion.Get(region)
}

// --- log streams (events live inline on LogStream.events) ---

func (b *InMemoryBackend) streamGet(region, groupName, streamName string) (*LogStream, bool) {
	return b.streams.Get(streamTableKey(region, groupName, streamName))
}

func (b *InMemoryBackend) streamHas(region, groupName, streamName string) bool {
	return b.streams.Has(streamTableKey(region, groupName, streamName))
}

// streamPut inserts or replaces s, which must already have region,
// logGroupName, and LogStreamName set.
func (b *InMemoryBackend) streamPut(s *LogStream) {
	b.streams.Put(s)
}

func (b *InMemoryBackend) streamDelete(region, groupName, streamName string) bool {
	return b.streams.Delete(streamTableKey(region, groupName, streamName))
}

// streamsInGroup returns every stream in region+groupName, in unspecified
// order (mirrors the old streams[region][groupName] map's iteration). The
// returned slice is owned by the underlying index -- see [store.Index.Get] --
// so callers that need to delete while iterating must copy it first (see
// deleteStreamsInGroup).
func (b *InMemoryBackend) streamsInGroup(region, groupName string) []*LogStream {
	return b.streamsByGroup.Get(streamGroupKey(region, groupName))
}

// deleteStreamsInGroup removes every stream (and, since events are stored
// inline, every event) belonging to region+groupName. Used by DeleteLogGroup.
func (b *InMemoryBackend) deleteStreamsInGroup(region, groupName string) {
	for _, s := range append([]*LogStream(nil), b.streamsInGroup(region, groupName)...) {
		b.streams.Delete(logStreamKeyFn(s))
	}
}

// --- subscription filters ---

// subscriptionFiltersInGroup returns every subscription filter in
// region+groupName, in unspecified order (mirrors the old
// subscriptionFilters[region][groupName] slice). The returned slice is owned
// by the underlying index -- copy before deleting while iterating.
func (b *InMemoryBackend) subscriptionFiltersInGroup(region, groupName string) []*SubscriptionFilter {
	return b.subscriptionFiltersByGroup.Get(streamGroupKey(region, groupName))
}

func (b *InMemoryBackend) deleteSubscriptionFiltersInGroup(region, groupName string) {
	for _, f := range append([]*SubscriptionFilter(nil), b.subscriptionFiltersInGroup(region, groupName)...) {
		b.subscriptionFilters.Delete(subscriptionFilterKeyFn(f))
	}
}

// --- metric filters ---

func (b *InMemoryBackend) metricFilterGet(region, groupName, filterName string) (*MetricFilter, bool) {
	return b.metricFilters.Get(metricFilterTableKey(region, groupName, filterName))
}

// metricFiltersInGroup returns every metric filter in region+groupName, in
// unspecified order (mirrors the old metricFilters[region][groupName] map).
// The returned slice is owned by the underlying index -- copy before deleting
// while iterating.
func (b *InMemoryBackend) metricFiltersInGroup(region, groupName string) []*MetricFilter {
	return b.metricFiltersByGroup.Get(streamGroupKey(region, groupName))
}

func (b *InMemoryBackend) deleteMetricFiltersInGroup(region, groupName string) {
	for _, f := range append([]*MetricFilter(nil), b.metricFiltersInGroup(region, groupName)...) {
		b.metricFilters.Delete(metricFilterKeyFn(f))
	}
}
