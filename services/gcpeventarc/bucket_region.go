package gcpeventarc

import (
	"slices"
	"strings"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
)

// multiRegionPrefix maps a Cloud Storage multi-region to the Eventarc location prefix it contains.
func multiRegionPrefix(loc string) (string, bool) {
	switch strings.ToUpper(loc) {
	case "US":
		return "us-", true
	case "EU":
		return "europe-", true
	case "ASIA":
		return "asia-", true
	}

	return "", false
}

// bucketMatchesLocation applies the documented rule that a bucket must be in the same region or
// multi-region as the trigger (docs.cloud.google.com/eventarc/docs/run/create-trigger-storage-gcloud).
// Dual-regions and the global location are not constrained.
func bucketMatchesLocation(bucketLoc, location string) bool {
	if location == globalLocation {
		return true
	}

	if prefix, ok := multiRegionPrefix(bucketLoc); ok {
		return strings.HasPrefix(location, prefix)
	}

	if strings.Contains(bucketLoc, "-") {
		return strings.EqualFold(bucketLoc, location)
	}

	return true
}

// checkBucketRegion rejects a Cloud Storage trigger whose bucket lives in another region. It must run
// without the backend lock held.
func (b *InMemoryBackend) checkBucketRegion(location string, filters []*eventarcpb.EventFilter) error {
	b.mu.RLock("checkBucketRegion")
	buckets := b.svc.Buckets
	b.mu.RUnlock()

	if buckets == nil || !isStorageType(eventType(filters)) {
		return nil
	}

	for _, f := range filters {
		if f.GetAttribute() != attrBucket || f.GetOperator() != "" {
			continue
		}

		if loc, ok := buckets.BucketLocation(f.GetValue()); ok && !bucketMatchesLocation(loc, location) {
			return errInvalid("Bucket %q is in %s; it must be in the same region or multi-region as the trigger (%s).",
				f.GetValue(), loc, location)
		}
	}

	return nil
}

// withStoredType adds the stored trigger's type filter to filters, which an update need not repeat.
func (b *InMemoryBackend) withStoredType(name string, filters []*eventarcpb.EventFilter) []*eventarcpb.EventFilter {
	if eventType(filters) != "" {
		return filters
	}

	b.mu.RLock("withStoredType")
	defer b.mu.RUnlock()

	rec, ok := b.triggers[name]
	if !ok {
		return filters
	}

	stored := &eventarcpb.EventFilter{Attribute: attrType, Value: eventType(rec.pb.GetEventFilters())}

	return append(slices.Clone(filters), stored)
}
