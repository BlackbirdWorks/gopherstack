package kinesis

import "time"

// onDemandShardCapacityBytesPerSec is the per-shard write-throughput
// threshold that triggers Kinesis Data Streams' documented on-demand
// auto-scaling: "Kinesis Data Streams monitors traffic for each shard. When
// the incoming traffic exceeds 500 KB/s per shard, it splits the shard
// within 15 minutes." (docs.aws.amazon.com/streams/latest/dev/
// how-do-i-size-a-stream.html#hotshards, "Handle read and write throughput
// exceptions"). "500 KB" is read as 500 KiB (bytesPerKiB), matching this
// file's existing KiB-as-binary convention (UpdateMaxRecordSize) --
// AWS itself is inconsistent about decimal vs. binary KB/MB across its own
// docs, so this is a disclosed assumption, not a verified exact byte count.
const onDemandShardCapacityBytesPerSec = 500 * bytesPerKiB

// onDemandThroughputWindow is the sliding window this emulator uses to
// estimate a stream's "current write throughput" for the ON_DEMAND
// auto-scale trigger above. Real AWS sizes on-demand capacity off a 30-day
// peak-throughput history ("up to double the peak write throughput observed
// in the previous 30 days," how-do-i-size-a-stream.html#ondemandmode) that
// this emulator has no model for (see PARITY.md gaps); a short sliding
// window is a disclosed, deliberately simpler approximation that still
// reacts to sustained load within a single process's lifetime instead of
// carrying 30 days of per-record history.
const onDemandThroughputWindow = 60 * time.Second

// onDemandAutoScaleDoublingFactor is how much maybeAutoScaleOnDemand grows
// the open shard count when the threshold above is crossed, matching "up to
// double the peak write throughput observed" (how-do-i-size-a-stream.html)
// and UpdateShardCount's own existing doubling cap (resharding.go).
const onDemandAutoScaleDoublingFactor = 2

// throughputSample is one write's contribution to a writeThroughputTracker's
// sliding window.
type throughputSample struct {
	at    time.Time
	bytes int
}

// writeThroughputTracker estimates a single stream's recent write
// throughput. It is transient, in-memory-only state (InMemoryBackend.
// throughputTrackers, never wired into backendSnapshot): losing it across a
// restart only resets the estimate to zero, the same as a real AWS stream's
// scaling history would not survive this emulator's process boundary
// either. Not safe for concurrent use by itself -- callers hold
// InMemoryBackend.throughputMu.
type writeThroughputTracker struct {
	samples []throughputSample
}

// recordAndRate appends a write of n bytes at now, prunes samples older than
// onDemandThroughputWindow, and returns the resulting bytes/sec rate over
// that window.
func (t *writeThroughputTracker) recordAndRate(now time.Time, n int) float64 {
	t.samples = append(t.samples, throughputSample{at: now, bytes: n})

	cutoff := now.Add(-onDemandThroughputWindow)

	i := 0
	for i < len(t.samples) && t.samples[i].at.Before(cutoff) {
		i++
	}
	t.samples = t.samples[i:]

	total := 0
	for _, s := range t.samples {
		total += s.bytes
	}

	return float64(total) / onDemandThroughputWindow.Seconds()
}

// maybeAutoScaleOnDemand records a PutRecord write of n bytes against
// stream's rolling throughput window and, for an ON_DEMAND stream whose
// resulting rate exceeds the documented per-shard 500 KiB/s threshold
// (onDemandShardCapacityBytesPerSec) aggregated across its currently open
// shards, doubles the open shard count via reshardTo -- approximating real
// AWS's per-shard hot-shard split ("it splits the shard within 15 minutes")
// with a whole-stream doubling instead of splitting only the specific
// overloaded shard, since this emulator has no per-shard write-rate
// attribution (only a per-stream aggregate). Capped at maxShardsPerStream,
// matching every other resharding path in this file. Must be called with
// stream.mu held (same as reshardTo, which it calls).
func (b *InMemoryBackend) maybeAutoScaleOnDemand(key string, stream *Stream, now time.Time, n int) {
	if stream.StreamMode != streamModeOnDemand {
		return
	}

	openShards := countOpenShards(stream.Shards)
	if openShards <= 0 || openShards >= maxShardsPerStream {
		return
	}

	b.throughputMu.Lock("maybeAutoScaleOnDemand")
	tr, ok := b.throughputTrackers[key]
	if !ok {
		tr = &writeThroughputTracker{}
		b.throughputTrackers[key] = tr
	}
	rate := tr.recordAndRate(now, n)
	b.throughputMu.Unlock()

	capacity := float64(openShards) * onDemandShardCapacityBytesPerSec
	if rate <= capacity {
		return
	}

	target := min(openShards*onDemandAutoScaleDoublingFactor, maxShardsPerStream)
	reshardTo(stream, target)
}
