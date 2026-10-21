package gcplogging

import (
	"context"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
)

const (
	destLogging  = "logging.googleapis.com/"
	destPubSub   = "pubsub.googleapis.com/"
	destStorage  = "storage.googleapis.com/"
	destBigQuery = "bigquery.googleapis.com/"
)

type delivery struct {
	sink  *loggingpb.LogSink
	entry *loggingpb.LogEntry
}

func entryKey(e *loggingpb.LogEntry) string {
	return e.GetLogName() + "\x00" + e.GetInsertId() + "\x00" + strconv.FormatInt(
		e.GetTimestamp().AsTime().UnixNano(),
		10,
	)
}

// ingest routes entries to buckets and external destinations, then notifies metrics and tails.
func (b *InMemoryBackend) ingest(ctx context.Context, entries []*loggingpb.LogEntry) {
	deliveries := make([]delivery, 0, len(entries))

	b.mu.Lock("WriteLogEntries")

	for _, e := range entries {
		parent := parentOf(e.GetLogName())
		b.seedLocked(parent)

		doc := newDoc(e)
		deliveries = append(deliveries, b.routeLocked(parent, e, doc)...)
		b.updateMetricsLocked(parent, doc)
	}

	b.mu.Unlock()

	b.deliver(ctx, deliveries)
	b.tails.publish(entries)
}

func (b *InMemoryBackend) routeLocked(parent string, e *loggingpb.LogEntry, doc *entryDoc) []delivery {
	var out []delivery

	prefix := parent + "/sinks/"

	for name, sink := range b.sinks {
		if !strings.HasPrefix(name, prefix) || sink.GetDisabled() {
			continue
		}

		if !b.compiled(sink.GetFilter()).matches(doc) || b.excluded(parent, sink, doc) {
			continue
		}

		dest := sink.GetDestination()
		if rest, ok := strings.CutPrefix(dest, destLogging); ok {
			b.storeLocked(rest, e)

			continue
		}

		out = append(out, delivery{sink: sink, entry: e})
	}

	return out
}

// excluded applies sink exclusions and, for the _Default sink, the parent's log exclusions.
func (b *InMemoryBackend) excluded(parent string, sink *loggingpb.LogSink, doc *entryDoc) bool {
	for _, ex := range sink.GetExclusions() {
		if !ex.GetDisabled() && b.compiled(ex.GetFilter()).matches(doc) && ex.GetFilter() != "" {
			return true
		}
	}

	if sink.GetName() != defaultBucketID {
		return false
	}

	prefix := parent + "/exclusions/"

	for name, ex := range b.exclusions {
		if strings.HasPrefix(name, prefix) && !ex.GetDisabled() && b.compiled(ex.GetFilter()).matches(doc) {
			return true
		}
	}

	return false
}

// storeLocked appends e to the bucket (or project) a logging destination names.
func (b *InMemoryBackend) storeLocked(dest string, e *loggingpb.LogEntry) {
	name := dest

	if parentRe.MatchString(dest) {
		b.seedLocked(dest)

		name = bucketName(dest, globalLocation, defaultBucketID)
	}

	bd, ok := b.buckets[name]
	if !ok || !bd.deleteRequested.IsZero() {
		return
	}

	key := entryKey(e)
	if _, dup := bd.keys[key]; dup {
		return
	}

	bd.keys[key] = struct{}{}
	bd.entries = append(bd.entries, e)

	b.expireLocked(bd)

	for len(bd.entries) > maxEntriesPerBucket {
		b.dropFirstLocked(bd)
	}
}

func (b *InMemoryBackend) dropFirstLocked(bd *bucketData) {
	delete(bd.keys, entryKey(bd.entries[0]))
	bd.entries[0] = nil
	bd.entries = bd.entries[1:]
}

// expireLocked drops entries older than the bucket retention; entries are ordered by receive time.
func (b *InMemoryBackend) expireLocked(bd *bucketData) {
	days := bd.pb.GetRetentionDays()
	if days <= 0 {
		days = defaultRetentionDays
	}

	cutoff := b.now().Add(-time.Duration(days) * 24 * time.Hour)

	for len(bd.entries) > 0 && bd.entries[0].GetReceiveTimestamp().AsTime().Before(cutoff) {
		b.dropFirstLocked(bd)
	}
}
