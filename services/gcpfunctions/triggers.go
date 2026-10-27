package gcpfunctions

import (
	"slices"
	"strings"

	"cloud.google.com/go/functions/apiv2/functionspb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
)

const (
	attrBucket    = "bucket"
	attrDatabase  = "database"
	attrNamespace = "namespace"
	attrDocument  = "document"
	defaultNS     = "(default)"
)

// BucketLookup tells event triggers whether a Cloud Storage bucket exists.
type BucketLookup interface {
	BucketExists(bucket string) bool
}

// SetBucketLookup wires the GCS emulator so storage triggers can be checked against real buckets.
func (b *InMemoryBackend) SetBucketLookup(l BucketLookup) {
	b.mu.Lock("SetBucketLookup")
	defer b.mu.Unlock()

	b.buckets = l
}

func storageKinds() []string {
	return []string{"finalized", "deleted", "archived", "metadataUpdated"}
}

func firestoreKinds() []string {
	return []string{"created", "updated", "deleted", gcpevents.FirestoreWritten}
}

func hasKind(prefix, typ string, kinds []string) bool {
	suffix, ok := strings.CutPrefix(typ, prefix)

	return ok && slices.Contains(kinds, suffix)
}

func isStorageType(typ string) bool {
	return hasKind(gcpevents.StorageTypePrefix, typ, storageKinds())
}

func isFirestoreType(typ string) bool {
	return hasKind(gcpevents.FirestoreTypePrefix, typ, firestoreKinds())
}

func supportedEventTypes() string {
	out := make([]string, 1, 1+len(storageKinds())+len(firestoreKinds()))
	out[0] = EventTypePubSub

	for _, k := range storageKinds() {
		out = append(out, gcpevents.StorageTypePrefix+k)
	}

	for _, k := range firestoreKinds() {
		out = append(out, gcpevents.FirestoreTypePrefix+k)
	}

	return strings.Join(out, ", ")
}

// checkFilters rejects filters on attributes outside allowed, empty values, duplicates, bad operators and bad
// path patterns, and returns the filters by attribute.
func checkFilters(
	et *functionspb.EventTrigger,
	allowed []string,
	patterned ...string,
) (map[string]*functionspb.EventFilter, error) {
	out := map[string]*functionspb.EventFilter{}

	for _, f := range et.GetEventFilters() {
		a := f.GetAttribute()

		switch {
		case !slices.Contains(allowed, a):
			return nil, errInvalid("Attribute %q cannot be used to filter events of type %q; use %s.",
				a, et.GetEventType(), strings.Join(allowed, ", "))
		case f.GetValue() == "":
			return nil, errInvalid("Event filter %q needs a value.", a)
		case out[a] != nil:
			return nil, errInvalid("Attribute %q is filtered more than once.", a)
		}

		switch f.GetOperator() {
		case "":
		case gcpevents.OperatorPathPattern:
			if !slices.Contains(patterned, a) {
				return nil, errInvalid("Attribute %q does not support path patterns.", a)
			}

			if _, err := gcpevents.CompilePathPattern(f.GetValue()); err != nil {
				return nil, errInvalid("Invalid path pattern %q for attribute %q.", f.GetValue(), a)
			}
		default:
			return nil, errInvalid("Unsupported operator %q for attribute %q: want %q or none.",
				f.GetOperator(), a, gcpevents.OperatorPathPattern)
		}

		out[a] = f
	}

	return out, nil
}

func (b *InMemoryBackend) validateStorageTrigger(et *functionspb.EventTrigger) error {
	if et.GetPubsubTopic() != "" {
		return errInvalid("pubsub_topic is only valid for %s events.", EventTypePubSub)
	}

	fs, err := checkFilters(et, []string{attrBucket}, attrBucket)
	if err != nil {
		return err
	}

	f := fs[attrBucket]
	if f == nil {
		return errInvalid("Events of type %q require a filter for the %q attribute.", et.GetEventType(), attrBucket)
	}

	b.mu.RLock("validateStorageTrigger")
	lookup := b.buckets
	b.mu.RUnlock()

	if lookup != nil && f.GetOperator() == "" && !lookup.BucketExists(f.GetValue()) {
		return errInvalid("Bucket %q does not exist.", f.GetValue())
	}

	return nil
}

func validateFirestoreTrigger(et *functionspb.EventTrigger) error {
	if et.GetPubsubTopic() != "" {
		return errInvalid("pubsub_topic is only valid for %s events.", EventTypePubSub)
	}

	fs, err := checkFilters(et, []string{attrDatabase, attrNamespace, attrDocument}, attrDocument)
	if err != nil {
		return err
	}

	if fs[attrDatabase] == nil {
		return errInvalid("Events of type %q require a filter for the %q attribute.", et.GetEventType(), attrDatabase)
	}

	return nil
}

// matchFilters reports whether attrs satisfy every event filter; a missing attribute never matches.
func matchFilters(filters []*functionspb.EventFilter, attrs map[string]string) bool {
	for _, f := range filters {
		got, ok := attrs[f.GetAttribute()]
		if !ok || !gcpevents.MatchFilter(f.GetOperator(), f.GetValue(), got) {
			return false
		}
	}

	return true
}
