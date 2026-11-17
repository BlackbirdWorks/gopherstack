package gcpeventarc

import (
	"slices"
	"strings"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
)

// multiRegion maps a Cloud Storage multi-region to its Eventarc location name and region prefix.
func multiRegion(loc string) (string, string, bool) {
	switch strings.ToUpper(loc) {
	case "US":
		return "us", "us-", true
	case "EU":
		return "eu", "europe-", true
	case "ASIA":
		return "asia", "asia-", true
	}

	return "", "", false
}

// bucketMatchesLocation applies the documented rule that a bucket must be in the same region or
// multi-region as the trigger (docs.cloud.google.com/eventarc/docs/understand-locations). A multi-region
// bucket accepts the multi-region trigger location and, leniently, its regions; dual-regions
// (NAM4, EUR4, ASIA1 and configurable "a+b" pairs) and the global location are not constrained.
func bucketMatchesLocation(bucketLoc, location string) bool {
	location = strings.ToLower(location)
	if location == globalLocation {
		return true
	}

	if name, prefix, ok := multiRegion(bucketLoc); ok {
		return location == name || strings.HasPrefix(location, prefix)
	}

	if strings.Contains(bucketLoc, "+") || !strings.Contains(bucketLoc, "-") {
		return true
	}

	return strings.EqualFold(bucketLoc, location)
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
