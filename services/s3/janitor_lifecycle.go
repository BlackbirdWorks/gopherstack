package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// lifecycleConfiguration mirrors the AWS S3 XML lifecycle configuration schema
// used to persist and evaluate lifecycle rules stored in StoredBucket.LifecycleConfig.
type lifecycleConfiguration struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rules   []lifecycleRule `xml:"Rule"`
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
	Days *int   `xml:"Days"`
	Date string `xml:"Date"`
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

// sweepLifecycle iterates over all active buckets, evaluates lifecycle rules,
// and deletes objects that have exceeded their expiration age.
func (j *Janitor) sweepLifecycle(ctx context.Context) {
	b := j.Backend
	now := time.Now().UTC()
	totalEvicted := 0

	// Snapshot bucket names, lifecycle configs, and per-object tags under a
	// read-lock so we can evaluate tag-based lifecycle filters without holding
	// b.mu during the (potentially slow) object scan.
	type bucketSnapshot struct {
		bucket    *StoredBucket
		tagsByKey map[string][]types.Tag
		name      string
		lcXML     string
	}
	var snapshots []bucketSnapshot

	func() {
		b.mu.RLock("S3Janitor.sweepLifecycle")
		defer b.mu.RUnlock()

		for _, bucket := range b.buckets.All() {
			name := bucket.Name
			if bucket.DeletePending || bucket.LifecycleConfig == "" {
				continue
			}

			var lcXML string
			func() {
				bucket.mu.RLock("S3Janitor.sweepLifecycleLCRead")
				defer bucket.mu.RUnlock()

				lcXML = bucket.LifecycleConfig
			}()
			if lcXML == "" {
				continue
			}

			// Snapshot tags for this bucket's objects.
			tagsByKey := make(map[string][]types.Tag)
			pfx := name + "/"
			for k, v := range b.tags {
				if strings.HasPrefix(k, pfx) {
					tagsByKey[k] = slices.Clone(v)
				}
			}

			snapshots = append(snapshots, bucketSnapshot{
				name:      name,
				bucket:    bucket,
				lcXML:     lcXML,
				tagsByKey: tagsByKey,
			})
		}
	}()

	for _, snap := range snapshots {
		evicted := j.applyLifecycleRules(
			ctx,
			snap.bucket,
			snap.name,
			snap.lcXML,
			snap.tagsByKey,
			now,
		)
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
		if !strings.EqualFold(rule.Status, statusEnabled) {
			continue
		}

		evicted += j.applyLifecycleRule(bucket, bucketName, rule, tagsByKey, now)
	}

	if evicted > 0 {
		logger.Load(ctx).InfoContext(ctx, "S3 janitor: lifecycle objects evicted",
			"bucket", bucketName, "count", evicted)
	}

	return evicted
}

// applyLifecycleRule applies all expiration, abort, and transition actions for a single
// enabled lifecycle rule. Returns the number of objects evicted.
func (j *Janitor) applyLifecycleRule(
	bucket *StoredBucket,
	bucketName string,
	rule *lifecycleRule,
	tagsByKey map[string][]types.Tag,
	now time.Time,
) int {
	prefix := rule.prefix()
	tagFilters := rule.Filter.tags()
	evicted := 0

	if rule.Expiration.Days != nil && rule.Expiration.Date == "" {
		expireBefore := now.Add(-time.Duration(*rule.Expiration.Days) * 24 * time.Hour)
		evicted += j.evictExpiredObjects(
			bucket,
			bucketName,
			prefix,
			tagFilters,
			tagsByKey,
			expireBefore,
		)
	}

	if rule.Expiration.Date != "" {
		expireDate, parseErr := parseLifecycleDate(rule.Expiration.Date)
		if parseErr == nil && now.After(expireDate) {
			evicted += j.evictExpiredObjects(
				bucket,
				bucketName,
				prefix,
				tagFilters,
				tagsByKey,
				expireDate,
			)
		}
	}

	if rule.NoncurrentVersionExpiration.NoncurrentDays != nil {
		noncurrentBefore := now.Add(
			-time.Duration(*rule.NoncurrentVersionExpiration.NoncurrentDays) * 24 * time.Hour,
		)
		evicted += j.evictNoncurrentVersions(bucket, prefix, noncurrentBefore)
	}

	if rule.AbortIncompleteMultipartUpload.DaysAfterInitiation != nil {
		abortBefore := now.Add(
			-time.Duration(
				*rule.AbortIncompleteMultipartUpload.DaysAfterInitiation,
			) * 24 * time.Hour,
		)
		j.abortStaleMultipartUploads(bucketName, abortBefore)
	}

	j.applyTransitions(bucket, prefix, tagFilters, tagsByKey, rule.ID, rule.Transitions, now)
	j.applyNoncurrentTransitions(bucket, prefix, rule.ID, rule.NoncurrentVersionTransitions, now)

	return evicted
}

// applyTransitions processes all Transition entries for a lifecycle rule, applying
// days-based and date-based storage class transitions.
func (j *Janitor) applyTransitions(
	bucket *StoredBucket,
	prefix string,
	tagFilters []lifecycleTag,
	tagsByKey map[string][]types.Tag,
	ruleID string,
	transitions []lifecycleTransition,
	now time.Time,
) {
	for _, tr := range transitions {
		if tr.Days > 0 && tr.StorageClass != "" {
			j.applyStorageClassTransitions(bucket, prefix, tagFilters, tagsByKey, ruleID,
				tr.StorageClass, now, time.Duration(tr.Days)*24*time.Hour, "")
		}
	}

	for _, tr := range transitions {
		if tr.Date == "" || tr.StorageClass == "" {
			continue
		}
		transitionDate, parseErr := parseLifecycleDate(tr.Date)
		if parseErr == nil && now.After(transitionDate) {
			j.applyStorageClassTransitions(bucket, prefix, tagFilters, tagsByKey, ruleID,
				tr.StorageClass, now, 0, tr.Date)
		}
	}
}

// applyNoncurrentTransitions processes NoncurrentVersionTransition entries for a lifecycle rule.
func (j *Janitor) applyNoncurrentTransitions(
	bucket *StoredBucket,
	prefix string,
	ruleID string,
	transitions []lifecycleNoncurrentTransition,
	now time.Time,
) {
	for _, tr := range transitions {
		if tr.NoncurrentDays <= 0 || tr.StorageClass == "" {
			continue
		}
		j.applyNoncurrentStorageClassTransitions(bucket, prefix, ruleID, tr.StorageClass, now,
			time.Duration(tr.NoncurrentDays)*24*time.Hour)
	}
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
	defer bucket.mu.Unlock()

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

	return evicted
}

// isExpiredAndMatches returns true when the object's latest version has expired
// before expireBefore and satisfies the tag filters (if any).
// Returns false if the latest version is protected by object lock (legal hold or
// active retention period) — lifecycle rules must not override WORM protection.
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
	var latestMod time.Time
	var latestVID string
	var locked bool
	func() {
		obj.mu.RLock("isExpiredAndMatches")
		defer obj.mu.RUnlock()

		latestMod, latestVID = latestVersionModAndID(obj)
		locked = isLatestVersionLocked(obj)
	}()

	if latestMod.IsZero() || latestMod.After(expireBefore) {
		return false
	}

	// Object lock (WORM) takes precedence over lifecycle rules.
	if locked {
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

// isLatestVersionLocked reports whether the latest non-deleted version of obj has
// an active legal hold or an unexpired retention period.
// Must be called with obj.mu held (at least RLock).
func isLatestVersionLocked(obj *StoredObject) bool {
	for _, ver := range obj.Versions {
		if !ver.IsLatest || ver.Deleted {
			continue
		}
		if ver.LegalHold {
			return true
		}
		if ver.RetentionMode != "" && !ver.RetainUntil.IsZero() &&
			time.Now().Before(ver.RetainUntil) {
			return true
		}
	}

	return false
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
	defer b.mu.Unlock()

	for k := range b.tags {
		// Tag keys have the format "bucket/key/versionID".
		// Derive the object prefix ("bucket/key/") by trimming after the last '/'.
		if lastSlash := strings.LastIndex(k, "/"); lastSlash >= 0 {
			if _, isEvicted := evictedPrefixes[k[:lastSlash+1]]; isEvicted {
				delete(b.tags, k)
			}
		}
	}
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

// GetExpirationHeader calculates the x-amz-expiration header for an object
// based on the bucket's lifecycle configuration.
func (j *Janitor) GetExpirationHeader(
	lcXML string,
	key string,
	tags []types.Tag,
	lastModified time.Time,
) string {
	if lcXML == "" {
		return ""
	}

	var cfg lifecycleConfiguration
	if err := xml.Unmarshal([]byte(lcXML), &cfg); err != nil {
		return ""
	}

	var earliestExpiry time.Time
	var matchingRuleID string

	for i := range cfg.Rules {
		rule := &cfg.Rules[i]
		if !strings.EqualFold(rule.Status, statusEnabled) {
			continue
		}

		prefix := rule.prefix()
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		tagFilters := rule.Filter.tags()
		if len(tagFilters) > 0 && !objectMatchesTags(tags, tagFilters) {
			continue
		}

		var expiry time.Time
		if rule.Expiration.Days != nil && rule.Expiration.Date == "" {
			// S3 rounds up to the next midnight UTC
			const hoursInDay = 24
			expiry = lastModified.Add(time.Duration(*rule.Expiration.Days) * hoursInDay * time.Hour)
			expiry = time.Date(expiry.Year(), expiry.Month(), expiry.Day(), 0, 0, 0, 0, time.UTC).
				Add(hoursInDay * time.Hour)
		} else if rule.Expiration.Date != "" {
			expiry, _ = parseLifecycleDate(rule.Expiration.Date)
		}

		if !expiry.IsZero() && (earliestExpiry.IsZero() || expiry.Before(earliestExpiry)) {
			earliestExpiry = expiry
			matchingRuleID = rule.ID
		}
	}

	if earliestExpiry.IsZero() {
		return ""
	}

	return fmt.Sprintf(`expiry-date="%s", rule-id="%s"`,
		earliestExpiry.Format(time.RFC1123), matchingRuleID)
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

// isNoncurrentVersionLocked reports whether a noncurrent object version is
// protected by an active object-lock (legal hold or retention mode).
func isNoncurrentVersionLocked(ver *StoredObjectVersion) bool {
	if ver.LegalHold {
		return true
	}

	return ver.RetentionMode != "" && !ver.RetainUntil.IsZero() &&
		time.Now().Before(ver.RetainUntil)
}

// evictNoncurrentVersions deletes non-latest object versions (noncurrent versions)
// from the bucket that match the prefix and were superseded before noncurrentBefore.
// Returns the number of noncurrent versions deleted.
//
// Performance: object keys are collected under a fast RLock pass. Each object is
// then processed under a brief per-iteration write lock rather than holding the bucket
// write lock for the entire sweep. This lets concurrent readers and writers proceed
// between objects instead of being blocked for the full duration.
func (j *Janitor) evictNoncurrentVersions(
	bucket *StoredBucket,
	prefix string,
	noncurrentBefore time.Time,
) int {
	// Phase 1: collect candidate keys under read lock.
	var keys []string
	func() {
		bucket.mu.RLock("S3Janitor.evictNoncurrentVersions.scan")
		defer bucket.mu.RUnlock()

		keys = make([]string, 0, len(bucket.Objects))

		for key := range bucket.Objects {
			if strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
	}()

	evicted := 0

	// Phase 2: process one object at a time, holding bucket write lock only briefly
	// per object so concurrent operations are not blocked for the entire sweep.
	for _, key := range keys {
		func() {
			bucket.mu.Lock("S3Janitor.evictNoncurrentVersions.obj")
			defer bucket.mu.Unlock()

			obj, ok := bucket.Objects[key]
			if !ok {
				// Object was deleted since the scan phase.
				return
			}

			var isEmpty bool
			func() {
				obj.mu.Lock("S3Janitor.evictNoncurrentVersions.versions")
				defer obj.mu.Unlock()

				for vid, ver := range obj.Versions {
					if ver.IsLatest || isNoncurrentVersionLocked(ver) {
						continue
					}

					if ver.LastModified.Before(noncurrentBefore) {
						delete(obj.Versions, vid)
						evicted++
					}
				}

				isEmpty = len(obj.Versions) == 0
			}()

			if isEmpty {
				delete(bucket.Objects, key)
				obj.mu.Close()
			}
		}()
	}

	return evicted
}

// applyStorageClassTransitions updates the StorageClass of current (latest) object versions
// that match prefix+tagFilters and are older than transitionAfter duration (or past transitionDate
// when date != ""). Only transitions that change the storage class are recorded.
//
// Performance: the bucket map is read under RLock (not Lock) because we only modify per-object
// fields protected by obj.mu. This allows concurrent readers of the bucket while the sweep runs.
func (j *Janitor) applyStorageClassTransitions(
	bucket *StoredBucket,
	prefix string,
	tagFilters []lifecycleTag,
	tagsByKey map[string][]types.Tag,
	ruleID, targetClass string,
	now time.Time,
	transitionAfter time.Duration,
	transitionDate string,
) {
	bucket.mu.RLock("applyStorageClassTransitions")
	defer bucket.mu.RUnlock()

	for _, obj := range bucket.Objects {
		obj.mu.Lock("applyStorageClassTransitions-obj")

		ver, ok := obj.Versions[obj.LatestVersionID]
		if !ok || ver.Deleted {
			obj.mu.Unlock()

			continue
		}

		if !strings.HasPrefix(ver.Key, prefix) {
			obj.mu.Unlock()

			continue
		}

		tagKey := bucket.Name + "/" + ver.Key + "/" + ver.VersionID
		if !objectMatchesTags(tagsByKey[tagKey], tagFilters) {
			obj.mu.Unlock()

			continue
		}

		var shouldTransition bool
		if transitionDate != "" {
			// Date-based: object was last modified before the transition date.
			shouldTransition = ver.LastModified.Before(now)
		} else {
			// Days-based: object age exceeds transitionAfter.
			shouldTransition = now.Sub(ver.LastModified) >= transitionAfter
		}

		if !shouldTransition {
			obj.mu.Unlock()

			continue
		}

		fromClass := ver.StorageClass
		if fromClass == "" {
			fromClass = storageStandard
		}

		if fromClass != targetClass {
			ver.StorageClass = targetClass
			ver.StorageClassTransitions = append(
				ver.StorageClassTransitions,
				StorageClassTransition{
					TransitionedAt: now,
					FromClass:      fromClass,
					ToClass:        targetClass,
					RuleID:         ruleID,
				},
			)
		}

		obj.mu.Unlock()
	}
}

// applyNoncurrentStorageClassTransitions updates the StorageClass of noncurrent (non-latest)
// object versions that are older than noncurrentAfter.
//
// Performance: uses RLock on the bucket because we only modify per-object version fields,
// which are protected by obj.mu. This allows concurrent reads of the bucket during the sweep.
func (j *Janitor) applyNoncurrentStorageClassTransitions(
	bucket *StoredBucket,
	prefix, ruleID, targetClass string,
	now time.Time,
	noncurrentAfter time.Duration,
) {
	bucket.mu.RLock("applyNoncurrentStorageClassTransitions")
	defer bucket.mu.RUnlock()

	for _, obj := range bucket.Objects {
		obj.mu.Lock("applyNoncurrentSCT-obj")

		for vid, ver := range obj.Versions {
			if vid == obj.LatestVersionID || ver.Deleted {
				continue
			}

			if !strings.HasPrefix(ver.Key, prefix) {
				continue
			}

			if now.Sub(ver.LastModified) < noncurrentAfter {
				continue
			}

			fromClass := ver.StorageClass
			if fromClass == "" {
				fromClass = storageStandard
			}

			if fromClass != targetClass {
				ver.StorageClass = targetClass
				ver.StorageClassTransitions = append(
					ver.StorageClassTransitions,
					StorageClassTransition{
						TransitionedAt: now,
						FromClass:      fromClass,
						ToClass:        targetClass,
						RuleID:         ruleID,
					},
				)
			}
		}

		obj.mu.Unlock()
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
