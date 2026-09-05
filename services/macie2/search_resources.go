package macie2

import (
	"slices"
	"sort"
)

// SearchResourcesSimpleCriterion mirrors types.SearchResourcesSimpleCriterion
// (aws-sdk-go-v2/service/macie2@v1.54.4 types/types.go:2777): Values are
// OR'd together, then Comparator EQ/NE applies across that OR set.
type SearchResourcesSimpleCriterion struct {
	Comparator string
	Key        string
	Values     []string
}

// SearchResourcesTagCriterionPair mirrors types.SearchResourcesTagCriterionPair.
type SearchResourcesTagCriterionPair struct {
	Key   string
	Value string
}

// SearchResourcesTagCriterion mirrors types.SearchResourcesTagCriterion.
type SearchResourcesTagCriterion struct {
	Comparator string
	TagValues  []SearchResourcesTagCriterionPair
}

// SearchResourcesCriterion mirrors types.SearchResourcesCriteria: a single
// condition, either property-based or tag-based.
type SearchResourcesCriterion struct {
	SimpleCriterion *SearchResourcesSimpleCriterion
	TagCriterion    *SearchResourcesTagCriterion
}

// SearchResourcesCriteriaBlock mirrors types.SearchResourcesCriteriaBlock:
// its And list is AND-joined per the SDK's own doc comment ("If you specify
// more than one condition, Amazon Macie uses AND logic to join the
// conditions").
type SearchResourcesCriteriaBlock struct {
	And []SearchResourcesCriterion
}

// SearchResourcesBucketCriteria mirrors types.SearchResourcesBucketCriteria.
type SearchResourcesBucketCriteria struct {
	Includes *SearchResourcesCriteriaBlock
	Excludes *SearchResourcesCriteriaBlock
}

// SearchResourcesSortCriteria mirrors types.SearchResourcesSortCriteria.
type SearchResourcesSortCriteria struct {
	AttributeName string
	OrderBy       string
}

const (
	searchResourcesKeyAccountID                   = "ACCOUNT_ID"
	searchResourcesKeyS3BucketName                = "S3_BUCKET_NAME"
	searchResourcesKeyS3BucketEffectivePermission = "S3_BUCKET_EFFECTIVE_PERMISSION"
	searchResourcesKeyS3BucketSharedAccess        = "S3_BUCKET_SHARED_ACCESS"
	searchResourcesComparatorNE                   = "NE"

	sortAttrAccountID               = "ACCOUNT_ID"
	sortAttrResourceName            = "RESOURCE_NAME"
	sortAttrS3ClassifiableObjectCnt = "S3_CLASSIFIABLE_OBJECT_COUNT"
	sortAttrS3ClassifiableSizeBytes = "S3_CLASSIFIABLE_SIZE_IN_BYTES"
)

// searchResourcesSimpleFieldValue resolves a SimpleCriterionKey to this
// backend's S3BucketMetadata. AUTOMATED_DISCOVERY_MONITORING_STATUS is a
// real key (types/enums.go) but has no backing field on S3BucketMetadata --
// left unfiltered (ok=false) rather than invented, same convention as
// bucketStringField's documented gap for unmodeled properties.
func searchResourcesSimpleFieldValue(bkt *S3BucketMetadata, key string) (string, bool) {
	switch key {
	case searchResourcesKeyAccountID:
		return bkt.AccountID, true
	case searchResourcesKeyS3BucketName:
		return bkt.BucketName, true
	case searchResourcesKeyS3BucketEffectivePermission:
		return bkt.PublicAccess, true
	case searchResourcesKeyS3BucketSharedAccess:
		return bkt.SharedAccess, true
	}

	return "", false
}

func matchesSearchResourcesSimpleCriterion(bkt *S3BucketMetadata, c *SearchResourcesSimpleCriterion) bool {
	v, ok := searchResourcesSimpleFieldValue(bkt, c.Key)
	if !ok {
		return true
	}

	matched := slices.Contains(c.Values, v)
	if c.Comparator == searchResourcesComparatorNE {
		return !matched
	}

	return matched
}

// searchResourcesTagPairMatches matches a stored tag entry (bkt.Tags[i], a
// map[string]any with "key"/"value" string entries -- the same casing the
// real SDK's KeyValuePair wire shape uses, types/types.go:1764) against one
// TagCriterionPair. An empty Key or Value on the pair means "don't filter on
// this half", matching TagCriterionPair's doc: "tag keys, tag values, or tag
// key and value pairs".
func searchResourcesTagPairMatches(tag map[string]any, p SearchResourcesTagCriterionPair) bool {
	key, _ := tag["key"].(string)
	value, _ := tag["value"].(string)

	if p.Key != "" && key != p.Key {
		return false
	}

	return p.Value == "" || value == p.Value
}

func matchesSearchResourcesTagCriterion(bkt *S3BucketMetadata, c *SearchResourcesTagCriterion) bool {
	matched := false

	for _, tag := range bkt.Tags {
		for _, p := range c.TagValues {
			if searchResourcesTagPairMatches(tag, p) {
				matched = true

				break
			}
		}
	}

	if c.Comparator == searchResourcesComparatorNE {
		return !matched
	}

	return matched
}

func matchesSearchResourcesCriterion(bkt *S3BucketMetadata, c SearchResourcesCriterion) bool {
	if c.SimpleCriterion != nil && !matchesSearchResourcesSimpleCriterion(bkt, c.SimpleCriterion) {
		return false
	}

	if c.TagCriterion != nil && !matchesSearchResourcesTagCriterion(bkt, c.TagCriterion) {
		return false
	}

	return true
}

func matchesSearchResourcesBlock(bkt *S3BucketMetadata, block *SearchResourcesCriteriaBlock) bool {
	if block == nil {
		return true
	}

	for _, c := range block.And {
		if !matchesSearchResourcesCriterion(bkt, c) {
			return false
		}
	}

	return true
}

// matchesSearchResourcesBucketCriteria applies BucketCriteria.Includes (a
// bucket must match every And condition to be kept) and BucketCriteria.Excludes
// (a bucket matching every And condition there is dropped), per each field's
// own doc comment on SearchResourcesBucketCriteria (types/types.go:2735).
func matchesSearchResourcesBucketCriteria(bkt *S3BucketMetadata, criteria *SearchResourcesBucketCriteria) bool {
	if criteria == nil {
		return true
	}

	if !matchesSearchResourcesBlock(bkt, criteria.Includes) {
		return false
	}

	return criteria.Excludes == nil || !matchesSearchResourcesBlock(bkt, criteria.Excludes)
}

// sortSearchResourcesBuckets mirrors sortBuckets' shape but over
// SearchResourcesSortAttributeName's own enum (ACCOUNT_ID/RESOURCE_NAME/
// S3_CLASSIFIABLE_OBJECT_COUNT/S3_CLASSIFIABLE_SIZE_IN_BYTES, types/enums.go),
// a distinct set from BucketSortCriteria's DescribeBuckets attributes.
func sortSearchResourcesBuckets(buckets []*S3BucketMetadata, sortBy *SearchResourcesSortCriteria) {
	if sortBy == nil {
		sort.Slice(buckets, func(i, k int) bool {
			if buckets[i].BucketName != buckets[k].BucketName {
				return buckets[i].BucketName < buckets[k].BucketName
			}

			return buckets[i].BucketArn < buckets[k].BucketArn
		})

		return
	}

	desc := sortBy.OrderBy == sortOrderDesc

	sort.Slice(buckets, func(i, k int) bool {
		less, tied, ok := searchResourcesSortLess(buckets[i], buckets[k], sortBy.AttributeName)
		if !ok {
			return false
		}

		if tied {
			return buckets[i].BucketArn < buckets[k].BucketArn
		}

		if desc {
			return !less
		}

		return less
	})
}

// searchResourcesSortLess reports (less, tied, ok) for one attribute
// comparison; ok is false only for an unrecognised/unbacked attribute (e.g.
// sensitivityScore has no scan data here, same structural gap as sortBuckets'
// default case), leaving relative order unchanged for that pair. less/tied
// must not be conflated with "unrecognised" -- a known attribute where a > b
// legitimately reports (false, false, true), and desc sort depends on that
// case still reaching the `!less` branch below.
func searchResourcesSortLess(a, b *S3BucketMetadata, attr string) (bool, bool, bool) {
	switch attr {
	case sortAttrAccountID:
		return a.AccountID < b.AccountID, a.AccountID == b.AccountID, true
	case sortAttrResourceName:
		return a.BucketName < b.BucketName, a.BucketName == b.BucketName, true
	case sortAttrS3ClassifiableObjectCnt:
		less := a.ClassifiableObjectCount < b.ClassifiableObjectCount
		tied := a.ClassifiableObjectCount == b.ClassifiableObjectCount

		return less, tied, true
	case sortAttrS3ClassifiableSizeBytes:
		less := a.ClassifiableSizeInBytes < b.ClassifiableSizeInBytes
		tied := a.ClassifiableSizeInBytes == b.ClassifiableSizeInBytes

		return less, tied, true
	}

	return false, false, false
}

// matchingBucketToMap builds a MatchingBucket wire object (types/types.go:1880)
// from fields this backend genuinely tracks: accountId/bucketName/
// classifiableObjectCount/classifiableSizeInBytes/objectCount/sizeInBytes.
// automatedDiscoveryMonitoringStatus, errorCode/errorMessage, jobDetails,
// lastAutomatedDiscoveryTime, objectCountByEncryptionType, sensitivityScore,
// sizeInBytesCompressed, and the unclassifiable* fields have no backing data
// on S3BucketMetadata (no error simulation, no per-bucket encryption-type
// breakdown, no sensitivity scan) and are omitted rather than fabricated.
func matchingBucketToMap(bkt *S3BucketMetadata) map[string]any {
	return map[string]any{
		bucketFieldAccountID:               bkt.AccountID,
		bucketFieldName:                    bkt.BucketName,
		bucketFieldClassifiableObjectCount: bkt.ClassifiableObjectCount,
		bucketFieldClassifiableSizeInBytes: bkt.ClassifiableSizeInBytes,
		bucketFieldObjectCount:             bkt.ObjectCount,
		bucketFieldSizeInBytes:             bkt.SizeInBytes,
	}
}

// SearchResources returns a page of S3 resources matching bucketCriteria,
// sorted by sortBy.
func (b *InMemoryBackend) SearchResources(
	bucketCriteria *SearchResourcesBucketCriteria,
	sortBy *SearchResourcesSortCriteria,
	maxResults int,
	nextToken string,
) ([]map[string]any, string, error) {
	b.mu.RLock("SearchResources")
	defer b.mu.RUnlock()

	buckets := b.s3Buckets.All()
	all := make([]*S3BucketMetadata, 0, len(buckets))

	for _, bkt := range buckets {
		if !matchesSearchResourcesBucketCriteria(bkt, bucketCriteria) {
			continue
		}

		cp := *bkt
		all = append(all, &cp)
	}

	sortSearchResourcesBuckets(all, sortBy)

	pageItems, next := paginate(all, nextToken, b.paginationSecret, maxResults)

	out := make([]map[string]any, 0, len(pageItems))
	for _, bkt := range pageItems {
		out = append(out, map[string]any{"matchingBucket": matchingBucketToMap(bkt)})
	}

	return out, next, nil
}
