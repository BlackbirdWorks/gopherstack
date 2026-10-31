package gcpfirestore

import (
	"sort"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/proto"
)

// DocumentEventKind names a document change, matching the suffix of the google.cloud.firestore.document.v1 types.
type DocumentEventKind string

const (
	// DocumentCreated is a document appearing.
	DocumentCreated DocumentEventKind = "created"
	// DocumentUpdated is an existing document changing.
	DocumentUpdated DocumentEventKind = "updated"
	// DocumentDeleted is a document being removed.
	DocumentDeleted DocumentEventKind = "deleted"
)

// DocumentEvent describes one committed document change. Old is nil for a create, New is nil for a delete.
// UpdateMask lists the top-level fields that changed on an update.
type DocumentEvent struct {
	Time       time.Time
	Old        *firestorepb.Document
	New        *firestorepb.Document
	Kind       DocumentEventKind
	UpdateMask []string
}

// DocumentObserver is called after a commit, outside the backend lock, once per changed document.
type DocumentObserver func(DocumentEvent)

// ObserveDocuments registers fn to see every document change from now on.
func (b *InMemoryBackend) ObserveDocuments(fn DocumentObserver) {
	b.mu.Lock("ObserveDocuments")
	defer b.mu.Unlock()

	b.observers = append(b.observers, fn)
}

func (b *InMemoryBackend) queueDocLocked(old, cur *firestorepb.Document, at time.Time) {
	if len(b.observers) == 0 || (old == nil && cur == nil) {
		return
	}

	ev := DocumentEvent{Time: at, Old: cloneNil(old), New: cloneNil(cur)}

	switch {
	case old == nil:
		ev.Kind = DocumentCreated
	case cur == nil:
		ev.Kind = DocumentDeleted
	default:
		ev.Kind, ev.UpdateMask = DocumentUpdated, changedFields(old, cur)
	}

	b.pending = append(b.pending, ev)
}

func cloneNil(d *firestorepb.Document) *firestorepb.Document {
	if d == nil {
		return nil
	}

	return cloneDoc(d)
}

func changedFields(old, cur *firestorepb.Document) []string {
	seen := map[string]struct{}{}

	for k, v := range cur.GetFields() {
		if ov, ok := old.GetFields()[k]; !ok || !proto.Equal(ov, v) {
			seen[k] = struct{}{}
		}
	}

	for k := range old.GetFields() {
		if _, ok := cur.GetFields()[k]; !ok {
			seen[k] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

// flushDocEvents delivers queued events. Callers defer it before taking b.mu so it runs after the unlock.
func (b *InMemoryBackend) flushDocEvents() {
	b.mu.Lock("flushDocEvents")
	events, observers := b.pending, b.observers
	b.pending = nil
	b.mu.Unlock()

	for _, ev := range events {
		for _, fn := range observers {
			fn(ev)
		}
	}
}
