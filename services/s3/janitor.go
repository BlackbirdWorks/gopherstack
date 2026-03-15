package s3

import (
	"context"
	"encoding/xml"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// lifecycleConfiguration mirrors the AWS S3 XML lifecycle configuration schema
// used to persist and evaluate lifecycle rules stored in StoredBucket.LifecycleConfig.
type lifecycleConfiguration struct {
	Rules []lifecycleRule `xml:"Rule"`
}

type lifecycleRule struct {
	NoncurrentVersionExpiration    lifecycleNoncurrentVersionExpiration `xml:"NoncurrentVersionExpiration"`
	AbortIncompleteMultipartUpload lifecycleAbortIncomplete             `xml:"AbortIncompleteMultipartUpload"`
	Filter                         lifecycleFilter                      `xml:"Filter"`
	Prefix                         string                               `xml:"Prefix"`
	ID                             string                               `xml:"ID"`
	Status                         string                               `xml:"Status"`
	Expiration                     lifecycleExpiration                  `xml:"Expiration"`
	Transitions                    []lifecycleTransition                `xml:"Transition"`
	NoncurrentVersionTransitions   []lifecycleNoncurrentTransition      `xml:"NoncurrentVersionTransition"`
}

// prefix returns the effective filter prefix for the rule. The nested
// Filter.Prefix and Filter.And.Prefix take precedence over the legacy
// top-level Prefix field.
func (r *lifecycleRule) prefix() string {
	if r.Filter.Prefix != "" {
		return r.Filter.Prefix
	}

	if ap := r.Filter.andPrefix(); ap != "" {
		return ap
	}

	return r.Prefix
}

type lifecycleFilter struct {
	And    *lifecycleFilterAnd `xml:"And"`
	Tag    *lifecycleTag       `xml:"Tag"`
	Prefix string              `xml:"Prefix"`
}

// lifecycleFilterAnd combines multiple filter conditions (Prefix + Tags).
type lifecycleFilterAnd struct {
	Prefix string         `xml:"Prefix"`
	Tags   []lifecycleTag `xml:"Tag"`
}

// lifecycleTag is a key/value pair used in lifecycle rule tag filters.
type lifecycleTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// tags returns the effective tag filters for the lifecycle rule, combining the
// top-level Tag and the nested And/Tag elements.
func (f *lifecycleFilter) tags() []lifecycleTag {
	if f.And != nil {
		return f.And.Tags
	}

	if f.Tag != nil {
		return []lifecycleTag{*f.Tag}
	}

	return nil
}

// andPrefix returns the prefix from an And filter, if present.
func (f *lifecycleFilter) andPrefix() string {
	if f.And != nil {
		return f.And.Prefix
	}

	return ""
}

type lifecycleExpiration struct {
	Date string `xml:"Date"`
	Days int    `xml:"Days"`
}

// lifecycleNoncurrentVersionExpiration specifies when noncurrent object versions expire.
type lifecycleNoncurrentVersionExpiration struct {
	NoncurrentDays *int `xml:"NoncurrentDays"`
}

// lifecycleAbortIncomplete specifies when to abort incomplete multipart uploads.
type lifecycleAbortIncomplete struct {
	DaysAfterInitiation *int `xml:"DaysAfterInitiation"`
}

// lifecycleTransition specifies when objects transition to a different storage class.
// In a mock, storage class transitions are recorded but not enforced.
type lifecycleTransition struct {
	StorageClass string `xml:"StorageClass"`
	Date         string `xml:"Date"`
	Days         int    `xml:"Days"`
}

// lifecycleNoncurrentTransition specifies when noncurrent versions transition storage class.
type lifecycleNoncurrentTransition struct {
	StorageClass   string `xml:"StorageClass"`
	NoncurrentDays int    `xml:"NoncurrentDays"`
}

const (
	defaultJanitorInterval = 500 * time.Millisecond

	// janitorBatchSize is the maximum number of objects deleted from a pending
	// bucket per janitor tick. This keeps each tick short while the queue is
	// visibly draining on the live metrics dashboard.
	janitorBatchSize = 100
)

// Janitor is the S3 background worker that drains buckets queued for async
// deletion and records queue-depth metrics for the live dashboard.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new S3 Janitor for the given backend.
// The janitor interval is taken from the provided settings;
// if zero, it falls back to defaultJanitorInterval.
func NewJanitor(backend *InMemoryBackend, settings Settings) *Janitor {
	interval := settings.JanitorInterval
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.runOnce(ctx)
			j.sweepLifecycle(ctx)
		}
	}
}

// runOnce performs one pass: records queue depth, then processes pending buckets.
func (j *Janitor) runOnce(ctx context.Context) {
	b := j.Backend

	// Snapshot pending bucket names under a short read-lock across all regions.
	b.mu.RLock("S3Janitor")
	pending := make([]string, 0)

	for _, regionBuckets := range b.buckets {
		for name, bucket := range regionBuckets {
			if bucket.DeletePending {
				pending = append(pending, name)
			}
		}
	}
	b.mu.RUnlock()

	telemetry.RecordWorkerQueueDepth("s3", "BucketCleaner", len(pending))
	telemetry.RecordWorkerTask("s3", "BucketCleaner", "success")

	for _, name := range pending {
		j.processBucket(ctx, name)
	}
}

// processBucket deletes up to janitorBatchSize objects from a pending bucket, then
// removes the bucket itself once it is empty.
func (j *Janitor) processBucket(ctx context.Context, name string) {
	b := j.Backend

	// Search for the bucket across all regions
	b.mu.RLock("S3Janitor.processBucket")
	bucket, foundRegion := findBucketAcrossRegions(b.buckets, name)
	b.mu.RUnlock()

	if bucket == nil {
		return
	}

	// Delete a batch of objects under the bucket lock.
	bucket.mu.Lock("S3Janitor.processBucket")
	count := deleteBatch(bucket.Objects, janitorBatchSize)

	telemetry.RecordWorkerItems("s3", "BucketCleaner", count)

	remaining := len(bucket.Objects)
	bucket.mu.Unlock()

	if remaining > 0 {
		// More objects remain; they will be picked up on the next tick.
		return
	}

	// Bucket is empty — remove it from the region map and clean up the region
	// entry if it has become empty to prevent unbounded map accumulation.
	// Guard the index removal: only delete the entry if it still points at
	// foundRegion, so a future replacement of the bucket name does not
	// accidentally lose its index entry.
	// Also purge any orphaned uploads and tags that reference this bucket to
	// prevent unbounded memory growth (resource leaks).
	b.mu.Lock("S3Janitor.removeBucket")
	if regionBuckets, exists := b.buckets[foundRegion]; exists {
		delete(regionBuckets, name)

		if len(regionBuckets) == 0 {
			delete(b.buckets, foundRegion)
		}
	}

	if b.bucketIndex[name] == foundRegion {
		delete(b.bucketIndex, name)
	}

	// Purge in-progress multipart uploads that belong to this bucket.
	delete(b.uploads, name)

	// Purge per-object tags whose key is prefixed with "<bucketName>/".
	prefix := name + "/"
	for tagKey := range b.tags {
		if strings.HasPrefix(tagKey, prefix) {
			delete(b.tags, tagKey)
		}
	}

	b.mu.Unlock()
	bucket.mu.Close()

	logger.Load(ctx).InfoContext(ctx, "S3 janitor: bucket deleted", "bucket", name)
}

// sweepLifecycle iterates over all active buckets, evaluates lifecycle rules,
// and deletes objects that have exceeded their expiration age.
func (j *Janitor) sweepLifecycle(ctx context.Context) {
	b := j.Backend
	now := time.Now().UTC()
	totalEvicted := 0

	// Snapshot bucket names, lifecycle configs, and per-object tags under a
	// read-lock so we can evaluate tag-based lifecycle filters without holding
	// b.mu during the (potentially slow) object scan.
	b.mu.RLock("S3Janitor.sweepLifecycle")
	type bucketSnapshot struct {
		bucket    *StoredBucket
		tagsByKey map[string][]types.Tag
		name      string
		lcXML     string
	}
	var snapshots []bucketSnapshot

	for _, regionBuckets := range b.buckets {
		for name, bucket := range regionBuckets {
			if bucket.DeletePending || bucket.LifecycleConfig == "" {
				continue
			}
			bucket.mu.RLock("S3Janitor.sweepLifecycleLCRead")
			lcXML := bucket.LifecycleConfig
			bucket.mu.RUnlock()
			if lcXML == "" {
				continue
			}

			// Snapshot tags for this bucket's objects.
			tagsByKey := make(map[string][]types.Tag)
			pfx := name + "/"
			for k, v := range b.tags {
				if strings.HasPrefix(k, pfx) {
					tagsByKey[k] = v
				}
			}

			snapshots = append(snapshots, bucketSnapshot{
				name:      name,
				bucket:    bucket,
				lcXML:     lcXML,
				tagsByKey: tagsByKey,
			})
		}
	}
	b.mu.RUnlock()

	for _, snap := range snapshots {
		evicted := j.applyLifecycleRules(ctx, snap.bucket, snap.name, snap.lcXML, snap.tagsByKey, now)
		totalEvicted += evicted
	}

	if totalEvicted > 0 {
		telemetry.RecordWorkerItems("s3", "LifecycleSweeper", totalEvicted)
	}

	telemetry.RecordWorkerTask("s3", "LifecycleSweeper", "success")
}

// applyLifecycleRules parses lifecycle rules and deletes expired objects from a bucket.
// tagsByKey is a snapshot of the object-tags map for this bucket (key format: "bucket/key/versionID").
// Returns the number of objects evicted.
func (j *Janitor) applyLifecycleRules(
	ctx context.Context,
	bucket *StoredBucket,
	bucketName, lcXML string,
	tagsByKey map[string][]types.Tag,
	now time.Time,
) int {
	var cfg lifecycleConfiguration
	if err := xml.Unmarshal([]byte(lcXML), &cfg); err != nil {
		logger.Load(ctx).WarnContext(ctx, "S3 janitor: failed to parse lifecycle config",
			"bucket", bucketName, "error", err)

		return 0
	}

	evicted := 0

	for i := range cfg.Rules {
		rule := &cfg.Rules[i]
		if !strings.EqualFold(rule.Status, "Enabled") {
			continue
		}

		prefix := rule.prefix()
		tagFilters := rule.Filter.tags()

		// Days-based expiration of current versions.
		// Days >= 0 triggers expiration (Days=0 means expire immediately).
		// Days < 0 is invalid and skipped.
		if rule.Expiration.Days >= 0 && rule.Expiration.Date == "" {
			expireBefore := now.Add(-time.Duration(rule.Expiration.Days) * 24 * time.Hour)
			evicted += j.evictExpiredObjects(bucket, bucketName, prefix, tagFilters, tagsByKey, expireBefore)
		}

		// Date-based expiration of current versions.
		// Use the parsed expireDate as the cutoff so only objects older than that
		// date are removed (not all objects indiscriminately).
		if rule.Expiration.Date != "" {
			expireDate, parseErr := parseLifecycleDate(rule.Expiration.Date)
			if parseErr == nil && now.After(expireDate) {
				evicted += j.evictExpiredObjects(bucket, bucketName, prefix, tagFilters, tagsByKey, expireDate)
			}
		}

		// Noncurrent version expiration: delete versions that are not the latest
		// and are older than NoncurrentDays. A nil pointer means the rule is absent.
		if rule.NoncurrentVersionExpiration.NoncurrentDays != nil {
			noncurrentBefore := now.Add(
				-time.Duration(*rule.NoncurrentVersionExpiration.NoncurrentDays) * 24 * time.Hour,
			)
			evicted += j.evictNoncurrentVersions(bucket, prefix, noncurrentBefore)
		}

		// Abort incomplete multipart uploads older than DaysAfterInitiation.
		// A nil pointer means the rule is absent; 0 means abort immediately.
		if rule.AbortIncompleteMultipartUpload.DaysAfterInitiation != nil {
			abortBefore := now.Add(
				-time.Duration(*rule.AbortIncompleteMultipartUpload.DaysAfterInitiation) * 24 * time.Hour,
			)
			j.abortStaleMultipartUploads(bucketName, abortBefore)
		}

		// Transitions: in a mock, storage class transitions are not enforced but
		// the rule is parsed and accepted without error.
	}

	if evicted > 0 {
		logger.Load(ctx).InfoContext(ctx, "S3 janitor: lifecycle objects evicted",
			"bucket", bucketName, "count", evicted)
	}

	return evicted
}

// evictExpiredObjects deletes objects from the bucket that match the prefix (and
// optional tag filters), whose latest version was last modified before expireBefore.
// tagFilters is a list of key/value pairs that must all be present on an object's
// latest version for the rule to apply (empty slice means "match all objects").
// tagsByKey is the pre-snapshotted object-tag map keyed by "bucket/key/versionID".
func (j *Janitor) evictExpiredObjects(
	bucket *StoredBucket,
	bucketName, prefix string,
	tagFilters []lifecycleTag,
	tagsByKey map[string][]types.Tag,
	expireBefore time.Time,
) int {
	evicted := j.collectExpiredKeys(bucket, bucketName, prefix, tagFilters, tagsByKey, expireBefore)
	if len(evicted) == 0 {
		return 0
	}

	j.cleanupEvictedTags(bucket, evicted)

	return len(evicted)
}

// collectExpiredKeys scans the bucket under its lock, deletes objects that have
// expired and match the filters, and returns the evicted keys.
func (j *Janitor) collectExpiredKeys(
	bucket *StoredBucket,
	bucketName, prefix string,
	tagFilters []lifecycleTag,
	tagsByKey map[string][]types.Tag,
	expireBefore time.Time,
) []string {
	var evicted []string

	bucket.mu.Lock("S3Janitor.evictExpiredObjects")
	for key, obj := range bucket.Objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		if !isExpiredAndMatches(obj, key, bucketName, tagFilters, tagsByKey, expireBefore) {
			continue
		}

		evicted = append(evicted, key)
		delete(bucket.Objects, key)
		obj.mu.Close()
	}
	bucket.mu.Unlock()

	return evicted
}

// isExpiredAndMatches returns true when the object's latest version has expired
// before expireBefore and satisfies the tag filters (if any).
// obj.mu is acquired internally so callers need not hold it.
func isExpiredAndMatches(
	obj *StoredObject,
	key, bucketName string,
	tagFilters []lifecycleTag,
	tagsByKey map[string][]types.Tag,
	expireBefore time.Time,
) bool {
	// Acquire obj.mu once to read both the modification time and the version ID
	// atomically; reading LatestVersionID without obj.mu would race with writers
	// (PutObject/DeleteObject) that update it under obj.mu.Lock.
	obj.mu.RLock("isExpiredAndMatches")
	latestMod, latestVID := latestVersionModAndID(obj)
	obj.mu.RUnlock()

	if latestMod.IsZero() || latestMod.After(expireBefore) {
		return false
	}

	if len(tagFilters) == 0 {
		return true
	}

	if latestVID == "" {
		return false
	}

	objTags := tagsByKey[bucketName+"/"+key+"/"+latestVID]

	return objectMatchesTags(objTags, tagFilters)
}

// latestVersionModAndID returns the LastModified time and VersionID of the latest
// non-deleted version. Must be called with obj.mu held.
func latestVersionModAndID(obj *StoredObject) (time.Time, string) {
	for _, ver := range obj.Versions {
		if ver.IsLatest && !ver.Deleted {
			return ver.LastModified, ver.VersionID
		}
	}

	return time.Time{}, ""
}

// cleanupEvictedTags removes tag entries for evicted keys from b.tags.
// Must be called after the bucket lock has been released to preserve lock ordering.
func (j *Janitor) cleanupEvictedTags(bucket *StoredBucket, evictedKeys []string) {
	b := j.Backend
	bucketPfx := bucket.Name + "/"
	evictedPrefixes := make(map[string]struct{}, len(evictedKeys))

	for _, key := range evictedKeys {
		evictedPrefixes[bucketPfx+key+"/"] = struct{}{}
	}

	b.mu.Lock("S3Janitor.evictExpiredObjects.tags")
	for k := range b.tags {
		// Tag keys have the format "bucket/key/versionID".
		// Derive the object prefix ("bucket/key/") by trimming after the last '/'.
		if lastSlash := strings.LastIndex(k, "/"); lastSlash >= 0 {
			if _, isEvicted := evictedPrefixes[k[:lastSlash+1]]; isEvicted {
				delete(b.tags, k)
			}
		}
	}
	b.mu.Unlock()
}

// objectMatchesTags returns true when all tag filters are satisfied by the given tag list.
func objectMatchesTags(objTags []types.Tag, filters []lifecycleTag) bool {
	for _, f := range filters {
		found := false

		for _, t := range objTags {
			if tagMatchesFilter(t, f) {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

// tagMatchesFilter returns true when the S3 Tag matches the lifecycle filter key/value.
func tagMatchesFilter(t types.Tag, f lifecycleTag) bool {
	return t.Key != nil && *t.Key == f.Key &&
		t.Value != nil && *t.Value == f.Value
}

// findBucketAcrossRegions returns the bucket and its region for the given bucket name,
// or nil and an empty string if not found. Must be called with b.mu held.
func findBucketAcrossRegions(buckets map[string]map[string]*StoredBucket, name string) (*StoredBucket, string) {
	for region, regionBuckets := range buckets {
		if bkt, exists := regionBuckets[name]; exists {
			return bkt, region
		}
	}

	return nil, ""
}

// deleteBatch deletes up to maxCount objects from the map, returning the number deleted.
func deleteBatch(objects map[string]*StoredObject, maxCount int) int {
	count := 0
	for key, obj := range objects {
		delete(objects, key)
		obj.mu.Close()
		count++

		if count >= maxCount {
			return count
		}
	}

	return count
}

// evictNoncurrentVersions deletes non-latest object versions (noncurrent versions)
// from the bucket that match the prefix and were superseded before noncurrentBefore.
// Returns the number of noncurrent versions deleted.
func (j *Janitor) evictNoncurrentVersions(bucket *StoredBucket, prefix string, noncurrentBefore time.Time) int {
	bucket.mu.Lock("S3Janitor.evictNoncurrentVersions")
	defer bucket.mu.Unlock()

	evicted := 0

	for key, obj := range bucket.Objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		obj.mu.Lock("S3Janitor.evictNoncurrentVersions.obj")

		for vid, ver := range obj.Versions {
			if ver.IsLatest {
				continue
			}

			if ver.LastModified.Before(noncurrentBefore) {
				delete(obj.Versions, vid)
				evicted++
			}
		}

		// Remove the object entry entirely if it has no versions left.
		if len(obj.Versions) == 0 {
			obj.mu.Unlock()
			delete(bucket.Objects, key)
			obj.mu.Close()
		} else {
			obj.mu.Unlock()
		}
	}

	return evicted
}

// abortStaleMultipartUploads removes multipart uploads for the given bucket that
// were initiated before abortBefore.
func (j *Janitor) abortStaleMultipartUploads(bucketName string, abortBefore time.Time) {
	b := j.Backend

	b.mu.Lock("S3Janitor.abortStaleMultipartUploads")
	defer b.mu.Unlock()

	bucketUploads, ok := b.uploads[bucketName]
	if !ok {
		return
	}

	for uploadID, upload := range bucketUploads {
		if upload.Initiated.Before(abortBefore) {
			delete(bucketUploads, uploadID)
		}
	}
}

// parseLifecycleDate parses a lifecycle date string. It tries RFC3339Nano
// (which also matches RFC3339 and timestamps with fractional seconds), then
// a bare date-time format, then a date-only format.
func parseLifecycleDate(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	if t, err := time.Parse("2006-01-02T15:04:05Z", s); err == nil {
		return t, nil
	}

	return time.Parse("2006-01-02", s)
}
