package gcplogging

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Limits from docs.cloud.google.com/logging/quotas; the page names no error code for them.
const (
	maxSinksPerParent       = 200
	maxBucketsPerParent     = 100
	maxViewsPerBucket       = 30
	maxMetricsPerParent     = 500
	maxRestrictedFields     = 20
	maxRestrictedFieldBytes = 800
	maxIndexConfigs         = 20
	listRequestsPerMinute   = 60
	tailEntriesPerMinute    = 60000
	maxWriteLogNames        = 1000
	maxWriteRequestBytes    = 10 << 20
	quotaWindow             = time.Minute
	recvSlack               = 1 << 20
	maxTrackedParents       = 1024
)

func restrictableRoot(root string) bool {
	switch root {
	case "textPayload", "jsonPayload", "protoPayload", "httpRequest", "labels", "sourceLocation":
		return true
	default:
		return false
	}
}

func countKeys[V any](m map[string]V, prefix string, skip func(string) bool) int {
	n := 0

	for name := range m {
		if strings.HasPrefix(name, prefix) && !skip(name) {
			n++
		}
	}

	return n
}

func builtinSink(name string) bool {
	return strings.HasSuffix(name, "/sinks/"+defaultBucketID) || strings.HasSuffix(name, "/sinks/"+requiredBucketID)
}

func builtinBucket(name string) bool {
	return strings.HasSuffix(name, "/buckets/"+defaultBucketID) || strings.HasSuffix(name, "/buckets/"+requiredBucketID)
}

func (b *InMemoryBackend) checkSinkCountLocked(parent string) error {
	if countKeys(b.sinks, parent+"/sinks/", builtinSink) >= maxSinksPerParent {
		return errExhausted("%s already has the maximum of %d sinks.", parent, maxSinksPerParent)
	}

	return nil
}

func (b *InMemoryBackend) checkBucketCountLocked(parent string) error {
	if countKeys(b.buckets, parent+"/locations/", builtinBucket) >= maxBucketsPerParent {
		return errExhausted("%s already has the maximum of %d log buckets.", parent, maxBucketsPerParent)
	}

	return nil
}

func (b *InMemoryBackend) checkViewCountLocked(bucket string) error {
	notAll := func(n string) bool { return strings.HasSuffix(n, "/views/"+allLogsViewID) }

	if countKeys(b.views, bucket+"/views/", notAll) >= maxViewsPerBucket {
		return errExhausted("Bucket %s already has the maximum of %d log views.", bucket, maxViewsPerBucket)
	}

	return nil
}

func (b *InMemoryBackend) checkMetricCountLocked(parent string) error {
	if countKeys(b.metrics, parent+"/metrics/", func(string) bool { return false }) >= maxMetricsPerParent {
		return errExhausted("%s already has the maximum of %d log-based metrics.", parent, maxMetricsPerParent)
	}

	return nil
}

// validateBucketIndexing checks restricted_fields and index_configs against the documented limits.
func validateBucketIndexing(bk *loggingpb.LogBucket) error {
	if len(bk.GetRestrictedFields()) > maxRestrictedFields {
		return errInvalid("A bucket can restrict at most %d fields.", maxRestrictedFields)
	}

	for _, f := range bk.GetRestrictedFields() {
		root, _, _ := strings.Cut(f, ".")
		if len(f) > maxRestrictedFieldBytes || !restrictableRoot(root) {
			return errInvalid("Field [%s] cannot be restricted: it must start with textPayload, jsonPayload, "+
				"protoPayload, httpRequest, labels or sourceLocation and be at most %d bytes.", f, maxRestrictedFieldBytes)
		}
	}

	if len(bk.GetIndexConfigs()) > maxIndexConfigs {
		return errExhausted("A bucket can index at most %d custom fields.", maxIndexConfigs)
	}

	seen := map[string]struct{}{}

	for _, ic := range bk.GetIndexConfigs() {
		if ic.GetFieldPath() == "" || ic.GetType() == loggingpb.IndexType_INDEX_TYPE_UNSPECIFIED {
			return errInvalid("index_configs entries need a field_path and a type.")
		}

		if _, dup := seen[ic.GetFieldPath()]; dup {
			return errInvalid("Field [%s] is indexed more than once.", ic.GetFieldPath())
		}

		seen[ic.GetFieldPath()] = struct{}{}
	}

	return nil
}

// stampIndexConfigs sets the output-only create_time of index configs that are new or changed.
func stampIndexConfigs(prev, next *loggingpb.LogBucket, now time.Time) {
	old := map[string]*loggingpb.IndexConfig{}
	for _, ic := range prev.GetIndexConfigs() {
		old[ic.GetFieldPath()] = ic
	}

	for _, ic := range next.GetIndexConfigs() {
		if o, ok := old[ic.GetFieldPath()]; ok && o.GetType() == ic.GetType() {
			ic.CreateTime = o.GetCreateTime()

			continue
		}

		ic.CreateTime = timestamppb.New(now)
	}
}

func parentsOf(names []string) []string {
	var out []string

	for _, n := range names {
		if p := parentOf(n); p != "" && !slices.Contains(out, p) {
			out = append(out, p)
		}
	}

	return out
}

// admitList charges one ListLogEntries call to each parent it reads and refuses past the per-minute limit.
func (b *InMemoryBackend) admitList(names []string) error {
	parents := parentsOf(names)

	b.mu.Lock("admitList")
	defer b.mu.Unlock()

	now := b.now()

	if len(b.listCalls) > maxTrackedParents {
		for p, calls := range b.listCalls {
			if len(calls) == 0 || now.Sub(calls[len(calls)-1]) >= quotaWindow {
				delete(b.listCalls, p)
			}
		}
	}

	for _, p := range parents {
		calls := slices.DeleteFunc(b.listCalls[p], func(t time.Time) bool { return now.Sub(t) >= quotaWindow })
		if len(calls) >= listRequestsPerMinute {
			b.listCalls[p] = calls

			return errExhausted(
				"Quota exceeded: %s allows %d ListLogEntries requests per minute.",
				p,
				listRequestsPerMinute,
			)
		}

		b.listCalls[p] = calls
	}

	for _, p := range parents {
		b.listCalls[p] = append(b.listCalls[p], now)
	}

	return nil
}

func checkWriteRequest(req *loggingpb.WriteLogEntriesRequest, size int) error {
	if size > maxWriteRequestBytes {
		return errInvalid("Request size %d exceeds the maximum of %d bytes.", size, maxWriteRequestBytes)
	}

	names := map[string]struct{}{}

	for _, e := range req.GetEntries() {
		names[cmp.Or(e.GetLogName(), req.GetLogName())] = struct{}{}
	}

	if len(names) > maxWriteLogNames {
		return errInvalid("A write can name at most %d distinct logs, got %d.", maxWriteLogNames, len(names))
	}

	return nil
}
